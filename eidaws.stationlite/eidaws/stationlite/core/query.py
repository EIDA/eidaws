# -*- coding: utf-8 -*-

import collections
import logging

from sqlalchemy import or_
from sqlalchemy.sql.expression import null

from eidaws.stationlite.core import orm
from eidaws.stationlite.core.utils import Epoch, RestrictedStatus, ChannelEpoch
from eidaws.utils.misc import Route
from eidaws.utils.settings import (
    FDSNWS_QUERY_WILDCARD_MULT_CHAR,
    FDSNWS_QUERY_WILDCARD_SINGLE_CHAR,
)
from eidaws.utils.sncl import (
    StreamEpoch,
    StreamEpochs,
    StreamEpochsHandler,
    none_as_max,
)


logger = logging.getLogger(__name__)


# ----------------------------------------------------------------------------
def resolve_vnetwork(session, stream_epoch, like_escape="/"):
    """
    Resolve a stream epoch regarding virtual networks.

    :returns: List of :py:class:`~eidaws.utils.sncl.StreamEpochs`
        object instances.
    :rtype: list
    """
    if stream_epoch.network == FDSNWS_QUERY_WILDCARD_MULT_CHAR or (
        len(stream_epoch.network) <= 2
        and set(stream_epoch.network)
        == set([FDSNWS_QUERY_WILDCARD_SINGLE_CHAR])
    ):
        logger.debug(
            f"Not resolving VNETs (stream_epoch.network == {stream_epoch.network})"
        )
        return []

    sql_stream_epoch = stream_epoch.fdsnws_to_sql_wildcards()
    logger.debug(f"(VNET) Processing request for (SQL) {stream_epoch!r}")

    query = (
        session.query(orm.VirtualChannelEpoch)
        .join(orm.VirtualChannelEpochGroup)
        .join(orm.Station)
        .filter(
            orm.VirtualChannelEpochGroup.code.like(
                sql_stream_epoch.network, escape=like_escape
            )
        )
        .filter(
            orm.Station.code.like(sql_stream_epoch.station, escape=like_escape)
        )
        .filter(
            orm.VirtualChannelEpoch.channel.like(
                sql_stream_epoch.channel, escape=like_escape
            )
        )
        .filter(
            orm.VirtualChannelEpoch.location.like(
                sql_stream_epoch.location, escape=like_escape
            )
        )
    )

    if sql_stream_epoch.starttime:
        # NOTE(damb): compare to None for undefined endtime (i.e. instrument
        # currently operating)
        query = query.filter(
            (orm.VirtualChannelEpoch.endtime > sql_stream_epoch.starttime)
            | (orm.VirtualChannelEpoch.endtime == None)
        )  # noqa
    if sql_stream_epoch.endtime:
        query = query.filter(
            orm.VirtualChannelEpoch.starttime < sql_stream_epoch.endtime
        )

    # slice stream epochs
    sliced_ses = []
    for s in query.all():
        # print('Query response: {0!r}'.format(StreamEpoch.from_orm(s)))
        with none_as_max(s.endtime) as end:
            se = StreamEpochs(
                network=s.network.code,
                station=s.station.code,
                location=s.location,
                channel=s.channel,
                epochs=[(s.starttime, end)],
            )
            se.modify_with_temporal_constraints(
                start=sql_stream_epoch.starttime, end=sql_stream_epoch.endtime
            )
            sliced_ses.append(se)

    logger.debug(f"(VNET) Found {sliced_ses!r} matching {stream_epoch!r}")

    return [se for ses in sliced_ses for se in ses]


def query_routes(
    session,
    stream_epoch,
    service,
    level="channel",
    access="any",
    method=None,
    minlat=-90.0,
    maxlat=90.0,
    minlon=-180.0,
    maxlon=180.0,
    like_escape="/",
    trim_to_stream_epoch=True,
):
    """
    Return routes for a given stream epoch.

    :param session: SQLAlchemy session
    :type session: :py:class:`sqlalchemy.orm.session.Session`
    :param stream_epoch: StreamEpoch the database query is performed with
    :type stream_epoch: :py:class:`~eidaws.utils.sncl.StreamEpoch`
    :param str service: String specifying the webservice
    :param str level: Optional `fdsnws-station` *level* parameter
    :param str access: Optional access parameter
    :param method: Optional list of FDSNWS method tokens to be filter for
    :type method: List of str or None
    :param float minlat: Latitude larger than or equal to the specified minimum
    :param float maxlat: Latitude smaller than or equal to the specified
        maximum
    :param float minlon: Longitude larger than or equal to the specified
        minimum
    :param float maxlon: Longitude smaller than or equal to the specified
        maximum
    :param str like_escape: Character used for the `SQL ESCAPE` statement
    :param bool trim_to_stream_epoch: Indicates if resulting stream epochs
        should be trimmed to the `stream_epoch`'s epoch (if possible)
    :return: List of :py:class:`~eidaws.utils.misc.Route` objects
    :rtype: list
    """
    sql_stream_epoch = stream_epoch.fdsnws_to_sql_wildcards()
    logger.debug(f"Processing request for (SQL) {sql_stream_epoch!r}")

    sta = sql_stream_epoch.station
    loc = sql_stream_epoch.location
    cha = sql_stream_epoch.channel

    query = _create_route_query(
        session,
        service,
        **sql_stream_epoch._asdict(short_keys=True),
        level=level,
        access=access,
        method=method,
        minlat=minlat,
        maxlat=maxlat,
        minlon=minlon,
        maxlon=maxlon,
        like_escape=like_escape,
    )
    routes = collections.defaultdict(StreamEpochsHandler)
    for row in query.all():
        # print(f"Query response: {row!r}")
        # NOTE(damb): Adjust epoch in case the orm.Epoch is smaller/larger
        # than the RoutingEpoch (regarding time constraints); at least one
        # starttime is mandatory to be configured
        starttimes = [row[4], row[6]]
        endtimes = [row[5], row[7]]

        if trim_to_stream_epoch:
            starttimes.append(sql_stream_epoch.starttime)
            endtimes.append(sql_stream_epoch.endtime)

        starttime = max(t for t in starttimes if t is not None)
        try:
            endtime = min(t for t in endtimes if t is not None)
        except ValueError:
            endtime = None

        if endtime is not None and endtime <= starttime:
            continue

        sta = row[1]
        loc = row[2]
        cha = row[3]
        if level == "network":
            sta = loc = cha = "*"
        elif level == "station":
            loc = cha = "*"

        # NOTE(damb): Set endtime to 'max' if undefined (i.e. device currently
        # acquiring data).
        with none_as_max(endtime) as end:
            stream_epoch = StreamEpoch.from_sncl(
                network=row[0],
                station=sta,
                location=loc,
                channel=cha,
                starttime=starttime,
                endtime=end,
            )

            routes[row[8]].add(stream_epoch)

    return [
        Route(url=url, stream_epochs=streams)
        for url, streams in routes.items()
    ]


def query_stationlite(
    session, stream_epoch, cha_epochs_handler, merge, like_escape="/"
):

    sql_stream_epoch = stream_epoch.fdsnws_to_sql_wildcards()
    logger.debug(f"Processing request for (SQL) {sql_stream_epoch!r}")

    query = (
        session.query(
            orm.Network.code,
            orm.Station.code,
            orm.ChannelEpoch.locationcode,
            orm.ChannelEpoch.code,
            orm.Epoch.starttime,
            orm.Epoch.endtime,
            orm.Epoch.restrictedstatus,
        )
        .join(orm.EpochType, orm.Epoch.epochtype_ref == orm.EpochType.id)
        .join(orm.Network, orm.ChannelEpoch.network_ref == orm.Network.id)
        .join(orm.Station, orm.ChannelEpoch.station_ref == orm.Station.id)
        .filter(orm.ChannelEpoch.epoch_ref == orm.Epoch.id)
        .filter(
            orm.Network.code.like(sql_stream_epoch.network, escape=like_escape)
        )
        .filter(
            orm.Station.code.like(sql_stream_epoch.station, escape=like_escape)
        )
        .filter(
            orm.ChannelEpoch.code.like(
                sql_stream_epoch.channel, escape=like_escape
            )
        )
        .filter(
            orm.ChannelEpoch.locationcode.like(
                sql_stream_epoch.location, escape=like_escape
            )
        )
        .filter(orm.EpochType.type == Epoch.CHANNEL)
        .distinct()
        .order_by(
            orm.Network.code,
            orm.Station.code,
            orm.ChannelEpoch.locationcode,
            orm.ChannelEpoch.code,
        )
    )

    start = sql_stream_epoch.starttime
    end = sql_stream_epoch.endtime
    if start:
        # NOTE(damb): compare to None for undefined endtime (i.e. device
        # currently operating)
        query = query.filter(
            (orm.Epoch.endtime > start) | (orm.Epoch.endtime == None)
        )  # noqa
    if end:
        query = query.filter(orm.Epoch.starttime < end)

    for row in query.all():
        # print(f"Query response: {row!r}")
        starttimes = [row[4], sql_stream_epoch.starttime]
        endtimes = [row[5], sql_stream_epoch.endtime]

        starttime = max(t for t in starttimes if t is not None)
        try:
            endtime = min(t for t in endtimes if t is not None)
        except ValueError:
            endtime = None

        if endtime is not None and endtime <= starttime:
            continue

        # NOTE(damb): Set endtime to 'max' if undefined (i.e. device currently
        # acquiring data).
        with none_as_max(endtime) as end:
            cha_epoch = ChannelEpoch(
                network=row[0],
                station=row[1],
                location=row[2],
                channel=row[3],
                starttime=starttime,
                endtime=end,
                restrictedStatus=row[6],
            )

            if merge:
                cha_epochs_handler.merge(cha_epoch, merge_epochs=True)
            else:
                cha_epochs_handler.add(cha_epoch)


def _create_route_query(
    session,
    service,
    net,
    sta,
    loc,
    cha,
    start,
    end,
    level,
    access,
    method,
    minlat,
    maxlat,
    minlon,
    maxlon,
    like_escape,
):
    if service == "station" and level == "network":
        query = _create_route_query_net_epochs(
            session,
            service,
            net,
            sta,
            loc,
            cha,
            like_escape=like_escape,
        )
    elif service == "station" and level == "station":
        query = _create_route_query_sta_epochs(
            session,
            service,
            net,
            sta,
            loc,
            cha,
            like_escape=like_escape,
        )
    else:
        query = _create_route_query_cha_epochs(
            session,
            service,
            net,
            sta,
            loc,
            cha,
            like_escape=like_escape,
        )

    query = query.filter(
        (orm.StationEpoch.latitude >= minlat)
        & (orm.StationEpoch.latitude <= maxlat)
    ).filter(
        (orm.StationEpoch.longitude >= minlon)
        & (orm.StationEpoch.longitude <= maxlon)
    )
    if start:
        # NOTE(damb): compare to None for undefined endtime (i.e. device
        # currently operating)
        query = query.filter(
            (orm.Epoch.endtime > start) | (orm.Epoch.endtime == None)
        )  # noqa
    if end:
        query = query.filter(orm.Epoch.starttime < end)

    if access != "any":
        query = query.filter(
            orm.Epoch.restrictedstatus == RestrictedStatus.from_str(access)
        )

    if method:
        query = query.filter(
            or_(orm.Endpoint.url.like(f"%{m}") for m in method)
        )

    return query


def _create_route_query_cha_epochs(
    session,
    service,
    net,
    sta,
    loc,
    cha,
    like_escape,
):
    return (
        session.query(
            orm.Network.code,
            orm.Station.code,
            orm.ChannelEpoch.locationcode,
            orm.ChannelEpoch.code,
            orm.Epoch.starttime,
            orm.Epoch.endtime,
            orm.Routing.starttime,
            orm.Routing.endtime,
            orm.Endpoint.url,
        )
        .join(orm.Routing, orm.Routing.epoch_ref == orm.Epoch.id)
        .join(orm.EpochType, orm.Epoch.epochtype_ref == orm.EpochType.id)
        .join(orm.Network, orm.ChannelEpoch.network_ref == orm.Network.id)
        .join(orm.Station, orm.ChannelEpoch.station_ref == orm.Station.id)
        .join(orm.StationEpoch, orm.StationEpoch.station_ref == orm.Station.id)
        .join(orm.Endpoint, orm.Routing.endpoint_ref == orm.Endpoint.id)
        .join(orm.Service, orm.Endpoint.service_ref == orm.Service.id)
        .filter(orm.ChannelEpoch.epoch_ref == orm.Epoch.id)
        .filter(orm.Network.code.like(net, escape=like_escape))
        .filter(orm.Station.code.like(sta, escape=like_escape))
        .filter(orm.ChannelEpoch.code.like(cha, escape=like_escape))
        .filter(orm.ChannelEpoch.locationcode.like(loc, escape=like_escape))
        .filter(orm.Service.name == service)
        .filter(orm.EpochType.type == Epoch.CHANNEL)
        .distinct()
    )


def _create_route_query_sta_epochs(
    session,
    service,
    net,
    sta,
    loc,
    cha,
    like_escape,
):
    return (
        session.query(
            orm.Network.code,
            orm.Station.code,
            null(),
            null(),
            orm.Epoch.starttime,
            orm.Epoch.endtime,
            orm.Routing.starttime,
            orm.Routing.endtime,
            orm.Endpoint.url,
        )
        .join(orm.Routing, orm.Routing.epoch_ref == orm.Epoch.id)
        .join(orm.EpochType, orm.Epoch.epochtype_ref == orm.EpochType.id)
        .join(orm.Network, orm.ChannelEpoch.network_ref == orm.Network.id)
        .join(orm.Station, orm.ChannelEpoch.station_ref == orm.Station.id)
        .join(orm.StationEpoch, orm.StationEpoch.station_ref == orm.Station.id)
        .join(orm.Endpoint, orm.Routing.endpoint_ref == orm.Endpoint.id)
        .join(orm.Service, orm.Endpoint.service_ref == orm.Service.id)
        .filter(orm.StationEpoch.epoch_ref == orm.Epoch.id)
        .filter(orm.Network.code.like(net, escape=like_escape))
        .filter(orm.Station.code.like(sta, escape=like_escape))
        .filter(orm.ChannelEpoch.code.like(cha, escape=like_escape))
        .filter(orm.ChannelEpoch.locationcode.like(loc, escape=like_escape))
        .filter(orm.Service.name == service)
        .filter(orm.EpochType.type == Epoch.STATION)
        .distinct()
    )


def _create_route_query_net_epochs(
    session,
    service,
    net,
    sta,
    loc,
    cha,
    like_escape,
):
    return (
        session.query(
            orm.Network.code,
            null(),
            null(),
            null(),
            orm.Epoch.starttime,
            orm.Epoch.endtime,
            orm.Routing.starttime,
            orm.Routing.endtime,
            orm.Endpoint.url,
        )
        .join(orm.Routing, orm.Routing.epoch_ref == orm.Epoch.id)
        .join(orm.EpochType, orm.Epoch.epochtype_ref == orm.EpochType.id)
        .join(orm.Network, orm.ChannelEpoch.network_ref == orm.Network.id)
        .join(orm.NetworkEpoch, orm.NetworkEpoch.network_ref == orm.Network.id)
        .join(orm.Station, orm.ChannelEpoch.station_ref == orm.Station.id)
        .join(orm.StationEpoch, orm.StationEpoch.station_ref == orm.Station.id)
        .join(orm.Endpoint, orm.Routing.endpoint_ref == orm.Endpoint.id)
        .join(orm.Service, orm.Endpoint.service_ref == orm.Service.id)
        .filter(orm.NetworkEpoch.epoch_ref == orm.Epoch.id)
        .filter(orm.Network.code.like(net, escape=like_escape))
        .filter(orm.Station.code.like(sta, escape=like_escape))
        .filter(orm.ChannelEpoch.code.like(cha, escape=like_escape))
        .filter(orm.ChannelEpoch.locationcode.like(loc, escape=like_escape))
        .filter(orm.Service.name == service)
        .filter(orm.EpochType.type == Epoch.NETWORK)
        .distinct()
    )
