# -*- coding: utf-8 -*-
"""
DB query facilities for eidaws-stationlite
"""

import collections
import logging

from sqlalchemy import or_

from eidaws.stationlite.engine import orm
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


logger = logging.getLogger("eidaws.stationlite.engine.db_query")


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
        session.query(orm.StreamEpoch)
        .join(orm.StreamEpochGroup)
        .join(orm.Station)
        .filter(
            orm.StreamEpochGroup.code.like(
                sql_stream_epoch.network, escape=like_escape
            )
        )
        .filter(
            orm.Station.code.like(sql_stream_epoch.station, escape=like_escape)
        )
        .filter(
            orm.StreamEpoch.channel.like(
                sql_stream_epoch.channel, escape=like_escape
            )
        )
        .filter(
            orm.StreamEpoch.location.like(
                sql_stream_epoch.location, escape=like_escape
            )
        )
    )

    if sql_stream_epoch.starttime:
        # NOTE(damb): compare to None for undefined endtime (i.e. instrument
        # currently operating)
        query = query.filter(
            (orm.StreamEpoch.endtime > sql_stream_epoch.starttime)
            | (orm.StreamEpoch.endtime == None)
        )  # noqa
    if sql_stream_epoch.endtime:
        query = query.filter(
            orm.StreamEpoch.starttime < sql_stream_epoch.endtime
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

    logger.debug("Found %r matching %r" % (sorted(sliced_ses), stream_epoch,))

    return [se for ses in sliced_ses for se in ses]


def find_streamepochs_and_routes(
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
    :return: List of :py:class:`~eidaws.utils.misc.Route` objects
    :rtype: list
    """
    logger.debug(f"Processing request for (SQL) {stream_epoch!r}")
    sql_stream_epoch = stream_epoch.fdsnws_to_sql_wildcards()

    sta = sql_stream_epoch.station
    loc = sql_stream_epoch.location
    cha = sql_stream_epoch.channel

    query = (
        session.query(
            orm.ChannelEpoch.code,
            orm.ChannelEpoch.locationcode,
            orm.ChannelEpoch.starttime,
            orm.ChannelEpoch.endtime,
            orm.Network.code,
            orm.Station.code,
            orm.Routing.starttime,
            orm.Routing.endtime,
            orm.Endpoint.url,
        )
        .join(
            orm.Routing, orm.Routing.channel_epoch_ref == orm.ChannelEpoch.id
        )
        .join(orm.Endpoint, orm.Routing.endpoint_ref == orm.Endpoint.id)
        .join(orm.Service)
        .join(orm.Network)
        .join(orm.Station)
        .join(orm.StationEpoch)
        .filter(
            orm.Network.code.like(sql_stream_epoch.network, escape=like_escape)
        )
        .filter(orm.Station.code.like(sta, escape=like_escape))
        .filter(
            (orm.StationEpoch.latitude >= minlat)
            & (orm.StationEpoch.latitude <= maxlat)
        )
        .filter(
            (orm.StationEpoch.longitude >= minlon)
            & (orm.StationEpoch.longitude <= maxlon)
        )
        .filter(orm.ChannelEpoch.code.like(cha, escape=like_escape))
        .filter(orm.ChannelEpoch.locationcode.like(loc, escape=like_escape))
        .filter(orm.Service.name == service)
    )

    if sql_stream_epoch.starttime:
        # NOTE(damb): compare to None for undefined endtime (i.e. device
        # currently operating)
        query = query.filter(
            (orm.ChannelEpoch.endtime > sql_stream_epoch.starttime)
            | (orm.ChannelEpoch.endtime == None)
        )  # noqa
    if sql_stream_epoch.endtime:
        query = query.filter(
            orm.ChannelEpoch.starttime < sql_stream_epoch.endtime
        )

    if access != "any":
        query = query.filter(orm.ChannelEpoch.restrictedstatus == access)

    if method:
        query = query.filter(
            or_(orm.Endpoint.url.like(f"%{m}") for m in method)
        )

    routes = collections.defaultdict(StreamEpochsHandler)

    for row in query.all():
        # print('Query response: {0!r}'.format(row))
        # NOTE(damb): Adjust epoch in case the ChannelEpoch is smaller/larger
        # than the RoutingEpoch (regarding time constraints); at least one
        # starttime is mandatory to be configured
        starttime = max(
            t
            for t in (row[2], row[6], sql_stream_epoch.starttime)
            if t is not None
        )

        try:
            endtime = min(
                t
                for t in (row[3], row[7], sql_stream_epoch.endtime)
                if t is not None
            )
        except ValueError:
            endtime = None

        if endtime is not None and endtime <= starttime:
            continue

        sta = row[5]
        loc = row[1]
        cha = row[0]

        # NOTE(damb): level reduction
        if level == "network":
            sta = loc = cha = "*"
        elif level == "station":
            loc = cha = "*"

        # NOTE(damb): Set endtime to 'max' if undefined (i.e. device currently
        # acquiring data).
        with none_as_max(endtime) as end:
            stream_epoch = StreamEpoch.from_sncl(
                network=row[4],
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
