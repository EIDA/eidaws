# -*- coding: utf-8 -*-

import collections
import datetime
import functools
import io
import logging
import warnings

import requests

from urllib.parse import urlparse, urlunparse, urljoin

from cached_property import cached_property
from lxml import etree
from obspy import read_inventory, UTCDateTime
from sqlalchemy.orm.exc import MultipleResultsFound
from sqlalchemy import inspect

from eidaws.stationlite.harvest.request import (
    binary_request,
    RequestsError,
    NoContent,
)
from eidaws.stationlite.engine import orm
from eidaws.stationlite.engine.utils import (
    Epoch as _Epoch,
    RestrictedStatus as _RestrictedStatus,
)
from eidaws.stationlite.settings import STL_HARVEST_DEFAULT_SERVICES
from eidaws.stationlite.harvest.validate import (
    _get_method_token,
    validate_method_token,
    validate_major_version,
    ValidationError,
)
from eidaws.utils.error import Error
from eidaws.utils.settings import (
    FDSNWS_QUERY_METHOD_TOKEN,
    FDSNWS_QUERYAUTH_METHOD_TOKEN,
    FDSNWS_EXTENT_METHOD_TOKEN,
    FDSNWS_EXTENTAUTH_METHOD_TOKEN,
    FDSNWS_STATION_PATH_QUERY,
    FDSNWS_DATASELECT_PATH_QUERY,
    FDSNWS_DATASELECT_PATH_QUERYAUTH,
    FDSNWS_AVAILABILITY_PATH_QUERY,
    FDSNWS_AVAILABILITY_PATH_QUERYAUTH,
    FDSNWS_AVAILABILITY_PATH_EXTENT,
    FDSNWS_AVAILABILITY_PATH_EXTENTAUTH,
    EIDAWS_WFCATALOG_PATH_QUERY,
)
from eidaws.utils.sncl import Stream, StreamEpoch


class Harvester:
    """
    Abstract base class for harvesters, harvesting EIDA nodes.

    :param str node_id: EIDA node identifier
    :param str url_routing_config: URL to routing configuration file.
    """

    LOGGER = "eidaws.stationlite.harvest.harvester.harvester"

    NS_ROUTINGXML = "{http://geofon.gfz-potsdam.de/ns/Routing/1.0/}"

    class HarvesterError(Error):
        """Base harvester error ({})."""

    class ValidationError(HarvesterError):
        """ValidationError ({})."""

    class RoutingConfigXMLParsingError(HarvesterError):
        """Error while parsing routing configuration ({})."""

    class IntegrityError(HarvesterError):
        """IntegrityError ({})."""

    def __init__(self, url):
        self._url = url

        self.logger = logging.getLogger(self.LOGGER)

    @property
    def url(self):
        return self._url

    @cached_property
    def config(self):
        # proxy for fetching the config
        if self.url.startswith("file"):
            try:
                with open(self.url[7:], "rb") as ifd:
                    return io.BytesIO(ifd.read())
            except OSError as err:
                raise self.HarvesterError(err)
        else:
            try:
                req = functools.partial(requests.get, self.url)
                with binary_request(req, timeout=60) as resp:
                    return resp
            except RequestsError as err:
                raise self.HarvesterError(err)

    def harvest(self, session):
        """Harvest the routing configuration."""

        try:
            self._harvest_localconfig(session)
        except (RequestsError, etree.XMLSyntaxError) as err:
            raise self.HarvesterError(err)

    def _harvest_localconfig(self, session):
        raise NotImplementedError

    @staticmethod
    def _update_lastseen(obj):
        obj.lastseen = datetime.datetime.utcnow()

    @staticmethod
    def parse_endtime(endtime):
        # reset endtime due to 'end=""'
        if endtime is None or not endtime.strip():
            return None

        return UTCDateTime(endtime, iso8601=True).datetime


# ----------------------------------------------------------------------------
class RoutingHarvester(Harvester):
    """
    Implementation of an harvester harvesting the routing information from an
    EIDA node's routing service local configuration. The routing configuration
    is stored within

    .. code::

        <ns0:routing>
            <ns0:route>
                ...
            <ns0:route>
        </ns0:routing>

    elements.

    This harvester relies on the ``eida-routing`` ``localconfig``
    configuration files.

    :param str node_id: EIDA node identifier
    :param str url_routing_config: URL to ``eida-routing``
        ``localconfig`` configuration files
    :param list services: List of EIDA services to be harvested
    :param bool force_restricted: Automatically correct the *dataselect* method
        token for restricted :py:class:`orm.ChannelEpoch` objects
        (default: ``True``)
    """

    STATION_TAG = "station"

    DEFAULT_RESTRICTED_STATUS = _RestrictedStatus.OPEN

    class StationXMLParsingError(Harvester.HarvesterError):
        """Error while parsing StationXML: ({})"""

    BaseNode = collections.namedtuple("BaseNode", ["restricted_status"])

    def __init__(self, url_routing_config, **kwargs):
        super().__init__(url_routing_config)

        self._services = kwargs.get("services", STL_HARVEST_DEFAULT_SERVICES)
        self._force_restricted = kwargs.get("force_restricted", True)

    def _harvest_localconfig(self, session):

        route_tag = f"{self.NS_ROUTINGXML}route"
        _services = [f"{self.NS_ROUTINGXML}{s}" for s in self._services]

        self.logger.debug(f"Harvesting routes for: {self.url!r}")
        # event driven parsing
        for event, route_element in etree.iterparse(
            self.config, events=("end",), tag=route_tag
        ):

            if event == "end" and len(route_element):

                routed_stream = Stream.from_route_attrs(
                    **dict(route_element.attrib)
                )
                query_params = routed_stream._as_query_string()

                url_fdsnws_station = self._extract_fdsnws_station_url(
                    route_element
                )
                if url_fdsnws_station is None:
                    continue

                url_fdsnws_station = (
                    f"{url_fdsnws_station}?{query_params}&level=channel"
                )

                # XXX(damb): For every single route resolve FDSN wildcards
                # using the route's station service.
                # XXX(damb): Use the station service's GET method since the
                # POST method requires temporal constraints (both starttime and
                # endtime).
                # ----
                self.logger.debug(
                    f"Resolving routing: (Request: {url_fdsnws_station!r})."
                )
                nets = []
                stas = []
                chas = []
                try:
                    req = functools.partial(requests.get, url_fdsnws_station)
                    with binary_request(req, timeout=60) as station_xml:
                        epochs = self._harvest_from_stationxml(
                            session, station_xml
                        )

                except NoContent as err:
                    self.logger.warning(str(err))
                    continue

                self._configure_routings(
                    session,
                    route_element,
                    epochs,
                    services=_services,
                    routed_stream=routed_stream,
                )

                # TODO(damb): Show stats for updated/inserted elements

    def _harvest_from_stationxml(self, session, station_xml):
        """
        Create/update Network, Station and ChannelEpoch objects from a
        STATIONXML file.

        :param session: SQLAlchemy session
        :type session: :py:class:`sqlalchemy.orm.session.Session`
        :param station_xml: Station XML file stream
        :type station_xml: :py:class:`io.BinaryIO`
        """

        try:
            inventory = read_inventory(station_xml, format="STATIONXML")
        except Exception as err:
            raise self.StationXMLParsingError(err)

        epochs = []
        for inv_network in inventory.networks:
            net_epoch, base_node = self._emerge_network_epoch(
                session, inv_network
            )
            epochs.append(net_epoch)

            for inv_station in inv_network.stations:
                sta_epoch, base_node = self._emerge_station_epoch(
                    session, inv_station, base_node
                )
                epochs.append(sta_epoch)

                for inv_channel in inv_station.channels:
                    cha_epoch = self._emerge_channel_epoch(
                        session,
                        inv_channel,
                        net_epoch.network,
                        sta_epoch.station,
                        base_node,
                    )
                    epochs.append(cha_epoch)

        return epochs

    def _configure_routings(
        self, session, route_element, epochs, services, routed_stream
    ):
        def validate_epoch(epoch, service_tag):
            if inspect(epoch).deleted:
                # In case a orm.Epoch object is marked as deleted but harvested
                # within the same harvesting run this is a strong hint for an
                # integrity issue within the FDSN station InventoryXML.
                raise self.IntegrityError(
                    f"Inventory integrity issue for {epoch!r}"
                )

            if service_tag in (
                "dataselect",
                "availability",
            ) and epoch.epoch.restrictedstatus not in (
                _RestrictedStatus.OPEN,
                _RestrictedStatus.CLOSED,
            ):
                raise self.IntegrityError(
                    "Unable to handle restricted status "
                    f"{epoch.epoch.restrictedstatus!r} for {epoch!r}."
                )

        def autocorrect_url(url, service_tag, restricted_status):
            if service_tag not in (
                "dataselect",
                "availability",
            ):
                return [url]

            # NOTE (damb): Always add .*/query / .*/queryauth path (w.r.t.
            # restricted_status)
            tokens = []
            if _RestrictedStatus.OPEN == restricted_status:
                tokens.append(FDSNWS_QUERY_METHOD_TOKEN)
                if service_tag == "availability":
                    t = _get_method_token(url)
                    if t is None:
                        tokens.append(FDSNWS_EXTENT_METHOD_TOKEN)
                    elif t == FDSNWS_EXTENT_METHOD_TOKEN:
                        tokens = [FDSNWS_EXTENT_METHOD_TOKEN]

            elif _RestrictedStatus.CLOSED == restricted_status:
                tokens.append(FDSNWS_QUERYAUTH_METHOD_TOKEN)
                if service_tag == "availability":
                    t = _get_method_token(url)
                    if t is None:
                        tokens.append(FDSNWS_EXTENTAUTH_METHOD_TOKEN)
                    elif t in (
                        FDSNWS_EXTENT_METHOD_TOKEN,
                        FDSNWS_EXTENTAUTH_METHOD_TOKEN,
                    ):
                        tokens = [FDSNWS_EXTENTAUTH_METHOD_TOKEN]

            else:
                ValueError(f"Invalid restricted status: {restricted_status!r}")

            return [urljoin(url, t) for t in tokens]

        for service_element in route_element.iter(*services):
            # only consider priority=1
            priority = service_element.get("priority")
            if not priority or int(priority) != 1:
                self.logger.debug(
                    f"Skipping {service_element} due to incompatible priority "
                    f"{priority!r}."
                )
                continue

            # remove xml namespace
            service_tag = service_element.tag[len(self.NS_ROUTINGXML) :]
            endpoint_url = service_element.get("address")
            if not endpoint_url:
                raise self.RoutingConfigXMLParsingError(
                    "Missing 'address' attrib."
                )

            service = self._emerge_service(session, service_tag)
            self.logger.debug(
                f"Processing routes for {routed_stream!r}"
                f"(service={service_element.tag!r}, "
                f"endpoint={endpoint_url!r})."
            )

            try:
                routing_starttime = UTCDateTime(
                    service_element.get("start"), iso8601=True
                ).datetime
                routing_endtime = self.parse_endtime(
                    service_element.get("end")
                )
            except Exception as err:
                raise self.RoutingConfigXMLParsingError(err)

            # configure routings
            for epoch in epochs:
                # XXX(damb): Store orm.NetworkEpoch and orm.StationEpoch for
                # service=station, only
                if service_tag != "station" and (
                    isinstance(epoch, orm.NetworkEpoch)
                    or isinstance(epoch, orm.StationEpoch)
                ):
                    continue

                try:
                    validate_epoch(epoch, service_tag)
                except self.IntegrityError as err:
                    warnings.warn(str(err))
                    if (
                        session.query(type(epoch))
                        .filter(type(epoch).id == epoch.id)
                        .delete()
                    ):
                        self.logger.warning(
                            f"Marked {epoch!r} due to integrity error as "
                            "deleted."
                        )
                    continue

                endpoint_urls = [endpoint_url]
                if self._force_restricted:
                    endpoint_urls = autocorrect_url(
                        endpoint_url,
                        service_tag,
                        epoch.epoch.restrictedstatus,
                    )

                endpoints = []
                for url in endpoint_urls:
                    try:
                        validate_method_token(
                            url,
                            service_tag,
                            restricted_status=epoch.epoch.restrictedstatus,
                        )
                    except ValidationError as err:
                        self.logger.warning(
                            f"Skipping {epoch!r} due to: {err}"
                        )
                        continue

                    endpoints.append(
                        self._emerge_endpoint(session, url, service)
                    )

                for endpoint in endpoints:
                    self.logger.debug(
                        "Processing Epoch<->Endpoint relation "
                        f"{epoch!r}<->{endpoint!r} "
                        f"(routing_starttime={routing_starttime!r}, "
                        f"routing_endtime={routing_endtime!r}) ..."
                    )

                    _ = self._emerge_routing(
                        session,
                        epoch,
                        endpoint,
                        routing_starttime,
                        routing_endtime,
                    )

    def _emerge_service(self, session, service_tag):
        """
        Factory method for a :py:class:`orm.Service` object.
        """
        try:
            service = (
                session.query(orm.Service)
                .filter(orm.Service.name == service_tag)
                .one_or_none()
            )
        except MultipleResultsFound as err:
            raise self.IntegrityError(err)

        if service is None:
            service = orm.Service(name=service_tag)
            session.add(service)
            self.logger.debug(
                f"Created new {type(service)} object {service!r}"
            )

        return service

    def _emerge_endpoint(self, session, url, service):
        """
        Factory method for a :py:class:`orm.Endpoint` object.
        """

        try:
            endpoint = (
                session.query(orm.Endpoint)
                .filter(orm.Endpoint.url == url)
                .one_or_none()
            )
        except MultipleResultsFound as err:
            raise self.IntegrityError(err)

        if endpoint is None:
            endpoint = orm.Endpoint(url=url, service=service)
            session.add(endpoint)
            self.logger.debug(
                f"Created new {type(endpoint)} object {endpoint!r}"
            )

        return endpoint

    def _emerge_network_epoch(self, session, network):
        """
        Factory method for a :py:class:`orm.NetworkEpoch` object.

        :param session: SQLAlchemy session object
        :type session: :py:class:`sqlalchemy.orm.session.Session`
        :param station: StationXML network object
        :type station: :py:class:`obspy.core.inventory.network.Network`

        :returns: Tuple of :py:class:`orm.NetworkEpoch``object and
            :py:class:`self.BaseNode`
        :rtype: tuple
        """
        start_date = network.start_date.datetime
        end_date_or_none = self.get_end_date(network)
        restricted_status = self.get_restricted_status(
            network, default=self.DEFAULT_RESTRICTED_STATUS
        )

        # check for available, overlapping orm.NetworkEpoch (not identical)
        # XXX(damb): Overlapping orm.NetworkEpochs regarding time constraints
        # are updated (i.e. implemented as: delete - insert).
        query = (
            session.query(orm.NetworkEpoch)
            .join(orm.Network)
            .filter(orm.Network.code == network.code)
            .filter(orm.NetworkEpoch.description == network.description)
        )
        query = self._filter_overlapping(query, _Epoch.NETWORK, network)
        epochs_to_update = set(query.all())
        if epochs_to_update:
            query_str = "{}".format(str(query).replace("\n", " "))
            self.logger.warning(
                "Found overlapping orm.NetworkEpoch objects "
                f"{epochs_to_update!r} (matching SQL query {query_str!r})"
            )

        # check for orm.NetworkEpoch with modified restricted status property
        query = (
            session.query(orm.NetworkEpoch)
            .join(orm.Network)
            .join(orm.Epoch)
            .join(orm.EpochType)
            .filter(orm.Network.code == network.code)
            .filter(orm.NetworkEpoch.description == network.description)
            .filter(orm.Epoch.starttime == start_date)
            .filter(orm.Epoch.endtime == end_date_or_none)
            .filter(orm.Epoch.restrictedstatus != restricted_status)
            .filter(orm.EpochType.type == _Epoch.NETWORK)
        )
        epochs_to_update |= set(query.all())
        self._mark_as_deleted(session, epochs_to_update, orm.NetworkEpoch)

        try:
            net = (
                session.query(orm.Network)
                .filter(orm.Network.code == network.code)
                .one_or_none()
            )
        except MultipleResultsFound as err:
            raise self.IntegrityError(err)

        # check if network already available - else create a new one
        if net is None:
            net = orm.Network(code=network.code)
            epoch = self.create_epoch(
                session,
                starttime=start_date,
                endtime=end_date_or_none,
                restricted_status=restricted_status,
                epoch_type=_Epoch.NETWORK,
            )
            net_epoch = orm.NetworkEpoch(
                epoch=epoch, description=network.description
            )
            net.network_epochs.append(net_epoch)
            self.logger.debug(f"Created new {type(net)} object {net!r}")

            session.add(net)

        else:
            self.logger.debug(f"Updating {net!r} ...")
            # check for available orm.NetworkEpoch - else create a new one
            try:
                net_epoch = (
                    session.query(orm.NetworkEpoch)
                    .join(orm.Epoch)
                    .join(orm.EpochType)
                    .filter(orm.NetworkEpoch.network == net)
                    .filter(
                        orm.NetworkEpoch.description == network.description
                    )
                    .filter(orm.Epoch.starttime == start_date)
                    .filter(orm.Epoch.endtime == end_date_or_none)
                    .filter(orm.Epoch.restrictedstatus == restricted_status)
                    .filter(orm.EpochType.type == _Epoch.NETWORK)
                    .one_or_none()
                )
            except MultipleResultsFound as err:
                raise self.IntegrityError(err)

            if net_epoch is None:
                epoch = self.create_epoch(
                    session,
                    starttime=start_date,
                    endtime=end_date_or_none,
                    restricted_status=restricted_status,
                    epoch_type=_Epoch.NETWORK,
                )
                net_epoch = orm.NetworkEpoch(
                    epoch=epoch,
                    network=net,
                    description=network.description,
                )
                net.network_epochs.append(net_epoch)
                self.logger.debug(
                    f"Created new {type(net_epoch)} object {net_epoch!r}"
                )
            else:
                self._update_lastseen(net_epoch)

        return net_epoch, self.BaseNode(restricted_status=restricted_status)

    def _emerge_station_epoch(self, session, station, base_node):
        """
        Factory method for a :py:class:`orm.StationEpoch` object.

        :param session: SQLAlchemy session object
        :type session: :py:class:`sqlalchemy.orm.session.Session`
        :param station: StationXML station object
        :type station: :py:class:`obspy.core.inventory.station.Station`
        :param base_node: Parent base node element shipping properties to be
            inherited
        :type base_node: :py:class:`self.BaseNode`

        :returns: Tuple of :py:class:`orm.StationEpoch`` object and
            :py:class:`self.BaseNode`
        :rtype: tuple
        """
        start_date = station.start_date.datetime
        end_date_or_none = self.get_end_date(station)
        restricted_status = self.get_restricted_status(
            station, base_node, default=self.DEFAULT_RESTRICTED_STATUS
        )

        # check for available, overlapping orm.StationEpoch (not identical)
        # XXX(damb): Overlapping orm.StationEpochs regarding time constraints
        # are updated (i.e. implemented as: delete - insert).
        query = (
            session.query(orm.StationEpoch)
            .join(orm.Station)
            .filter(orm.Station.code == station.code)
            .filter(orm.StationEpoch.description == station.description)
            .filter(orm.StationEpoch.longitude == station.longitude)
            .filter(orm.StationEpoch.latitude == station.latitude)
        )
        query = self._filter_overlapping(query, _Epoch.STATION, station)
        epochs_to_update = set(query.all())
        if epochs_to_update:
            query_str = "{}".format(str(query).replace("\n", " "))
            self.logger.warning(
                "Found overlapping orm.StationEpoch objects "
                f"{epochs_to_update!r} (matching SQL query {query_str!r})"
            )

        # check for orm.StationEpoch with modified restricted status property
        query = (
            session.query(orm.StationEpoch)
            .join(orm.Station)
            .join(orm.Epoch)
            .join(orm.EpochType)
            .filter(orm.Station.code == station.code)
            .filter(orm.StationEpoch.description == station.description)
            .filter(orm.StationEpoch.longitude == station.longitude)
            .filter(orm.StationEpoch.latitude == station.latitude)
            .filter(orm.Epoch.starttime == start_date)
            .filter(orm.Epoch.endtime == end_date_or_none)
            .filter(orm.Epoch.restrictedstatus != restricted_status)
            .filter(orm.EpochType.type == _Epoch.STATION)
        )
        epochs_to_update |= set(query.all())
        self._mark_as_deleted(session, epochs_to_update, orm.StationEpoch)

        try:
            sta = (
                session.query(orm.Station)
                .filter(orm.Station.code == station.code)
                .one_or_none()
            )
        except MultipleResultsFound as err:
            raise self.IntegrityError(err)

        # check if station already available - else create a new one
        if sta is None:
            sta = orm.Station(code=station.code)
            epoch = self.create_epoch(
                session,
                starttime=start_date,
                endtime=end_date_or_none,
                restricted_status=restricted_status,
                epoch_type=_Epoch.STATION,
            )
            sta_epoch = orm.StationEpoch(
                epoch=epoch,
                description=station.description,
                latitude=station.latitude,
                longitude=station.longitude,
            )
            sta.station_epochs.append(sta_epoch)
            self.logger.debug(f"Created new {type(sta)} object {sta!r}")

            session.add(sta)

        else:
            self.logger.debug(f"Updating {sta!r} ...")
            # check for available orm.StationEpoch - else create a new one
            try:
                sta_epoch = (
                    session.query(orm.StationEpoch)
                    .join(orm.Epoch)
                    .join(orm.EpochType)
                    .filter(orm.StationEpoch.station == sta)
                    .filter(
                        orm.StationEpoch.description == station.description
                    )
                    .filter(orm.StationEpoch.longitude == station.longitude)
                    .filter(orm.StationEpoch.latitude == station.latitude)
                    .filter(orm.Epoch.starttime == start_date)
                    .filter(orm.Epoch.endtime == end_date_or_none)
                    .filter(orm.Epoch.restrictedstatus == restricted_status)
                    .filter(orm.EpochType.type == _Epoch.STATION)
                    .one_or_none()
                )
            except MultipleResultsFound as err:
                raise self.IntegrityError(err)

            if sta_epoch is None:
                epoch = self.create_epoch(
                    session,
                    starttime=start_date,
                    endtime=end_date_or_none,
                    restricted_status=restricted_status,
                    epoch_type=_Epoch.STATION,
                )
                sta_epoch = orm.StationEpoch(
                    epoch=epoch,
                    description=station.description,
                    latitude=station.latitude,
                    longitude=station.longitude,
                )
                sta.station_epochs.append(sta_epoch)
                self.logger.debug(
                    f"Created new {type(sta_epoch)} object {sta_epoch!r}"
                )
            else:
                self._update_lastseen(sta_epoch)

        return sta_epoch, self.BaseNode(restricted_status=restricted_status)

    def _emerge_channel_epoch(
        self, session, channel, network, station, base_node
    ):
        """
        Factory method for a :py:class:`orm.ChannelEpoch` object.

        :param session: SQLAlchemy session object
        :type session: :py:class:`sqlalchemy.orm.session.Session`
        :param channel: StationXML channel object
        :type channel: :py:class:`obspy.core.inventory.channel.Channel`
        :param network: Network referenced by the channel epoch
        :type network:
        :py:class:`eidaws.stationlite.engine.orm.Network`
        :param station: Station referenced by the channel epoch
        :type station:
        :py:class:`eidaws.stationlite.engine.orm.Station`
        :param base_node: Parent base node element shipping properties to be
            inherited
        :type base_node: :py:class:`self.BaseNode`

        :returns: :py:class:`orm.Channel` object
        :rtype: :py:class:`orm.Channel`
        """
        start_date = channel.start_date.datetime
        end_date_or_none = self.get_end_date(channel)
        restricted_status = self.get_restricted_status(
            channel, base_node, default=self.DEFAULT_RESTRICTED_STATUS
        )

        # check for available, overlapping orm.ChannelEpoch (not identical)
        # XXX(damb): Overlapping orm.ChannelEpochs regarding time constraints
        # are updated (i.e. implemented as: delete - insert).
        query = (
            session.query(orm.ChannelEpoch)
            .filter(orm.ChannelEpoch.network == network)
            .filter(orm.ChannelEpoch.station == station)
            .filter(orm.ChannelEpoch.code == channel.code)
            .filter(orm.ChannelEpoch.locationcode == channel.location_code)
        )
        query = self._filter_overlapping(query, _Epoch.CHANNEL, channel)
        epochs_to_update = set(query.all())
        if epochs_to_update:
            query_str = "{}".format(str(query).replace("\n", " "))
            self.logger.warning(
                "Found overlapping orm.ChannelEpoch objects "
                f"{epochs_to_update!r} (matching SQL query {query_str!r})"
            )

        # check for orm.ChannelEpoch with modified restrictedstatus property
        query = (
            session.query(orm.ChannelEpoch)
            .join(orm.Epoch)
            .join(orm.EpochType)
            .filter(orm.ChannelEpoch.network == network)
            .filter(orm.ChannelEpoch.station == station)
            .filter(orm.ChannelEpoch.locationcode == channel.location_code)
            .filter(orm.ChannelEpoch.code == channel.code)
            .filter(orm.Epoch.starttime == start_date)
            .filter(orm.Epoch.endtime == end_date_or_none)
            .filter(orm.Epoch.restrictedstatus != restricted_status)
            .filter(orm.EpochType.type == _Epoch.CHANNEL)
        )
        epochs_to_update |= set(query.all())
        self._mark_as_deleted(session, epochs_to_update, orm.ChannelEpoch)

        # check for an identical orm.ChannelEpoch
        try:
            cha_epoch = (
                session.query(orm.ChannelEpoch)
                .join(orm.Epoch)
                .join(orm.EpochType)
                .filter(orm.Epoch.starttime == channel.start_date.datetime)
                .filter(orm.Epoch.endtime == end_date_or_none)
                .filter(
                    orm.Epoch.restrictedstatus == channel.restricted_status
                )
                .filter(orm.EpochType.type == _Epoch.CHANNEL)
                .filter(orm.ChannelEpoch.code == channel.code)
                .filter(orm.ChannelEpoch.locationcode == channel.location_code)
                .filter(orm.ChannelEpoch.station == station)
                .filter(orm.ChannelEpoch.network == network)
                .one_or_none()
            )
        except MultipleResultsFound as err:
            raise self.IntegrityError(err)

        if cha_epoch is None:
            epoch = self.create_epoch(
                session,
                starttime=channel.start_date.datetime,
                endtime=end_date_or_none,
                restricted_status=restricted_status,
                epoch_type=_Epoch.CHANNEL,
            )
            cha_epoch = orm.ChannelEpoch(
                epoch=epoch,
                code=channel.code,
                locationcode=channel.location_code,
                station=station,
                network=network,
            )
            self.logger.debug(
                f"Created new {type(cha_epoch)} object {cha_epoch!r}"
            )
            session.add(cha_epoch)
        else:
            self._update_lastseen(cha_epoch)

        return cha_epoch

    def _emerge_routing(self, session, epoch, endpoint, start, end):
        """
        Factory method for a :py:class:`orm.Routing` object.
        """
        # XXX(damb): Check for overlapping orm.Routing regarding time
        # constraints are updated (i.e. implemented as: delete - insert).
        query = (
            session.query(orm.Routing)
            .filter(orm.Routing.endpoint == endpoint)
            .filter(orm.Routing.epoch == epoch.epoch)
        )

        if end is None:
            query = query.filter(
                (orm.Routing.starttime != start)
                & (
                    # open orm.Routing interval
                    (orm.Routing.endtime == None)
                    |
                    # start in orm.Routing interval
                    (start < orm.Routing.endtime)
                )
            )
        else:
            query = query.filter(
                # open orm.Routing interval
                (
                    (orm.Routing.starttime != start)
                    & (orm.Routing.endtime == None)
                    & (end > orm.Routing.starttime)
                )
                | (
                    (orm.Routing.endtime != None)
                    & (
                        (orm.Routing.starttime != start)
                        | (orm.Routing.endtime != end)
                    )
                    & (
                        # start in orm.Routing interval
                        (
                            (orm.Routing.starttime < start)
                            & (start < orm.Routing.endtime)
                        )
                        |
                        # end in orm.Routing interval
                        (
                            (orm.Routing.starttime < end)
                            & (end < orm.Routing.endtime)
                        )
                    )
                )
            )

        overlapping = query.all()
        if overlapping:
            query_str = "{}".format(str(query).replace("\n", " "))
            msg = (
                f"Found overlapping orm.Routing objects {overlapping!r} "
                f"(matching SQL query {query_str!r}) for {epoch!r}"
            )
            if isinstance(epoch, orm.ChannelEpoch):
                self.logger.warning(msg)
            else:
                # XXX(damb): For both orm.NetworkEpoch and orm.StationEpoch
                # objects the routing definition from eidaws-routing
                # localconfig configuration files causes conflicts. Therefore,
                # as a workaround, we use the union of the defined epochs.
                starttime = min([r.starttime for r in overlapping] + [start])
                endtimes = [r.endtime for r in overlapping] + [end]
                endtime = None
                if None not in endtimes:
                    endtime = max(endtimes)

                start = starttime
                end = endtime
                self.logger.debug(
                    f"{msg}, resetting routing epoch "
                    f"(routing_starttime={start!r}, routing_endtime={end!r})"
                )

        # delete overlapping orm.Routing entries
        for r in overlapping:
            if session.delete(r):
                self.logger.debug(f"Marked {r!r} as deleted")

        # check for an identical orm.Routing
        try:
            routing = (
                session.query(orm.Routing)
                .filter(orm.Routing.endpoint == endpoint)
                .filter(orm.Routing.epoch == epoch.epoch)
                .filter(orm.Routing.starttime == start)
                .filter(orm.Routing.endtime == end)
                .one_or_none()
            )
        except MultipleResultsFound as err:
            raise self.IntegrityError(err)

        if routing is None:
            routing = orm.Routing(
                endpoint=endpoint,
                epoch=epoch.epoch,
                starttime=start,
                endtime=end,
            )
            self.logger.debug(
                f"Created new {type(routing)} object {routing!r}"
            )
            session.add(routing)

        else:
            self._update_lastseen(routing)

        return routing

    def _extract_fdsnws_station_url(self, route_element):
        def extract_routes_by_priority(route_element, priority=1):
            return [
                e
                for e in route_element.iter()
                if int(e.get("priority", 0)) == priority
            ]

        def has_routes_with_valid_priority(route_element):
            return bool(extract_routes_by_priority(route_element, priority=1))

        station_tag = f"{self.NS_ROUTINGXML}{self.STATION_TAG}"
        # extract fdsn-station service url for each route
        urls = set(
            [
                e.get("address")
                for e in route_element.iter(station_tag)
                if int(e.get("priority", 0)) == 1
            ]
        )

        if not urls or not has_routes_with_valid_priority(route_element):
            return None

        if len(urls) > 1:
            # NOTE(damb): Currently we cannot handle multiple
            # fdsn-station urls i.e. for multiple routed epochs
            raise self.IntegrityError(
                (
                    "Multiple <station></station> elements for "
                    f"{route_element} ({urls})."
                )
            )

        url = urls.pop()
        validate_major_version(url, "station")
        validate_method_token(url, "station")

        return url

    def _mark_as_deleted(self, session, epochs, orm_type):
        for epoch in epochs:
            _ = (
                session.query(orm.Routing)
                .filter(orm.Routing.epoch == epoch)
                .delete()
            )

            if (
                session.query(orm_type)
                .filter(orm_type.id == epoch.id)
                .delete()
            ):
                self.logger.debug(f"Removed referenced {epoch!r}.")

    @staticmethod
    def create_epoch(
        session, starttime, endtime, restricted_status, epoch_type
    ):
        try:
            e_type = (
                session.query(orm.EpochType)
                .filter(orm.EpochType == epoch_type)
                .one_or_none()
            )
        except MultipleResultsFound as err:
            raise self.IntegrityError(err)

        if e_type is None:
            e_type = orm.EpochType(type=epoch_type)

        return orm.Epoch(
            starttime=starttime,
            endtime=endtime,
            restrictedstatus=restricted_status,
            type=e_type,
        )

    @staticmethod
    def get_restricted_status(inv_epoch_obj, base_node=None, default=None):
        retval = default
        if base_node is not None:
            retval = base_node.restricted_status

        if inv_epoch_obj.restricted_status is not None:
            retval = _RestrictedStatus.from_str(
                inv_epoch_obj.restricted_status
            )

        return retval

    @staticmethod
    def get_end_date(inv_epoch_obj):
        retval = inv_epoch_obj.end_date
        if retval is None:
            return None
        return retval.datetime

    @staticmethod
    def _update_epoch(epoch, **kwargs):
        """
        Update basenode epoch properties.

        :param epoch: Epoch to be updated.
        :param kwargs: Keyword value parameters to be updated.

        Allowed parameters are:
        * ``restricted_status``
        """
        restricted_status = kwargs.get("restricted_status")

        if (
            epoch.restrictedstatus != restricted_status
            and restricted_status is not None
        ):
            epoch.epoch.restrictedstatus = restricted_status

    @staticmethod
    def _filter_overlapping(query, epoch_type, inv_obj):
        """
        Apply a filter to ``query`` in order to detect overlapping epoch
        intervals which are not equal.
        """
        start_date = inv_obj.start_date.datetime
        end_date = RoutingHarvester.get_end_date(inv_obj)

        query = (
            query.join(orm.Epoch)
            .join(orm.EpochType)
            .filter(orm.EpochType.type == epoch_type)
        )
        if end_date is None:
            query = query.filter(
                (orm.Epoch.starttime != start_date)
                & (
                    # open orm.Epoch interval
                    (orm.Epoch.endtime == None)
                    |
                    # start_date in orm.Epoch interval
                    (start_date < orm.Epoch.endtime)
                )
            )
        else:
            query = query.filter(
                # open orm.Epoch interval
                (
                    (orm.Epoch.starttime != start_date)
                    & (orm.Epoch.endtime == None)
                    & (end_date > orm.Epoch.starttime)
                )
                | (
                    (orm.Epoch.endtime != None)
                    & (
                        (orm.Epoch.starttime != start_date)
                        | (orm.Epoch.endtime != end_date)
                    )
                    & (
                        # start_date in orm.Epoch interval
                        (
                            (orm.Epoch.starttime < start_date)
                            & (start_date < orm.Epoch.endtime)
                        )
                        |
                        # end_date in orm.Epoch interval
                        (
                            (orm.Epoch.starttime < end_date)
                            & (end_date < orm.Epoch.endtime)
                        )
                    )
                )
            )

        return query


class VNetHarvester(Harvester):
    """
    Implementation of an harvester harvesting the virtual network information
    from an EIDA node. Usually, the information is stored within the routing
    service's local configuration.

    This harvester does not rely on the EIDA routing service anymore.
    """

    class VNetHarvesterError(Harvester.HarvesterError):
        """Base error for virtual netowork harvesting ({})."""

    def _harvest_localconfig(self, session):

        vnet_tag = f"{self.NS_ROUTINGXML}vnetwork"
        stream_tag = f"{self.NS_ROUTINGXML}stream"

        self.logger.debug(f"Harvesting virtual networks for: {self.url!r}")

        # event driven parsing
        for event, vnet_element in etree.iterparse(
            self.config, events=("end",), tag=vnet_tag
        ):
            if event == "end" and len(vnet_element):

                vnet = self._emerge_virtual_channel_epoch_group(
                    session, vnet_element
                )

                for stream_element in vnet_element.iter(tag=stream_tag):
                    self.logger.debug(
                        f"Processing stream element: {stream_element}"
                    )
                    # convert attributes to dict
                    vstream = Stream.from_route_attrs(
                        **dict(stream_element.attrib)
                    )
                    try:
                        vstream_starttime = UTCDateTime(
                            stream_element.get("start"), iso8601=True
                        ).datetime
                        vstream_endtime = self.parse_endtime(
                            stream_element.get("end")
                        )
                    except Exception as err:
                        raise self.RoutingConfigXMLParsingError(err)

                    # deserialize to StreamEpoch object
                    vstream_epoch = StreamEpoch(
                        stream=vstream,
                        starttime=vstream_starttime,
                        endtime=vstream_endtime,
                    )

                    self.logger.debug(f"Processing {vstream_epoch!r} ...")

                    sql_vstream_epoch = vstream_epoch.fdsnws_to_sql_wildcards()

                    # check if the stream epoch definition is valid i.e. there
                    # must be at least one matching orm.ChannelEpoch
                    query = (
                        session.query(orm.ChannelEpoch)
                        .join(orm.Epoch)
                        .join(orm.EpochType)
                        .join(orm.Network)
                        .join(orm.Station)
                        .filter(orm.EpochType.type == _Epoch.CHANNEL)
                        .filter(
                            orm.Network.code.like(sql_vstream_epoch.network)
                        )
                        .filter(
                            orm.Station.code.like(sql_vstream_epoch.station)
                        )
                        .filter(
                            orm.ChannelEpoch.locationcode.like(
                                sql_vstream_epoch.location
                            )
                        )
                        .filter(
                            orm.ChannelEpoch.code.like(
                                sql_vstream_epoch.channel
                            )
                        )
                        .filter(
                            (orm.Epoch.endtime == None)  # noqa
                            | (orm.Epoch.endtime > sql_vstream_epoch.starttime)
                        )
                    )

                    if sql_vstream_epoch.endtime:
                        query = query.filter(
                            orm.Epoch.starttime < sql_vstream_epoch.endtime
                        )

                    cha_epochs = query.all()
                    if not cha_epochs:
                        self.logger.warn(
                            "No orm.ChannelEpoch matching virtual channel "
                            f"epoch definition for {vstream_epoch!r}"
                        )
                        continue

                    for cha_epoch in cha_epochs:
                        self.logger.debug(
                            "Processing virtual network configuration for "
                            f"{type(cha_epoch)} object {cha_epoch!r}."
                        )
                        self._emerge_virtual_channel_epoch(
                            session, cha_epoch, vstream_epoch, vnet
                        )

        # TODO(damb): Show stats for updated/inserted elements

    def _emerge_virtual_channel_epoch_group(self, session, element):
        """
        Factory method for a :py:class:`orm.VirtualChannelEpochGroup`
        """
        vnet_code = element.get("networkCode")
        if not vnet_code:
            raise self.VNetHarvesterError("Missing 'networkCode' attribute.")

        try:
            vnet = (
                session.query(orm.VirtualChannelEpochGroup)
                .filter(orm.VirtualChannelEpochGroup.code == vnet_code)
                .one_or_none()
            )
        except MultipleResultsFound as err:
            raise self.IntegrityError(err)

        # check if virtual network already available - else create a new one
        if vnet is None:
            vnet = orm.VirtualChannelEpochGroup(code=vnet_code)
            self.logger.debug(f"Created new {type(vnet)} object {vnet!r}")
            session.add(vnet)

        return vnet

    def _emerge_virtual_channel_epoch(
        self, session, channel_epoch, stream_epoch, vnet
    ):
        """
        Factory method for a :py:class:`orm.VirtualChannelEpoch` object.
        """
        # XXX(damb): Overlapping orm.VirtualChannelEpoch objects regarding time
        # constraints are updated (i.e. implemented as: delete - insert).
        query = (
            session.query(orm.VirtualChannelEpoch)
            .join(orm.Network)
            .join(orm.Station)
            .filter(orm.Network.code == channel_epoch.network.code)
            .filter(orm.Station.code == channel_epoch.station.code)
            .filter(
                orm.VirtualChannelEpoch.virtual_channel_epoch_group == vnet
            )
            .filter(orm.VirtualChannelEpoch.channel == channel_epoch.code)
            .filter(
                orm.VirtualChannelEpoch.location == channel_epoch.locationcode
            )
        )

        if stream_epoch.endtime is None:
            query = query.filter(
                (
                    (
                        orm.VirtualChannelEpoch.starttime
                        < stream_epoch.starttime
                    )
                    & (
                        (orm.VirtualChannelEpoch.endtime == None)  # noqa
                        | (
                            stream_epoch.starttime
                            < orm.VirtualChannelEpoch.endtime
                        )
                    )
                )
                | (orm.VirtualChannelEpoch.starttime > stream_epoch.starttime)
            )
        else:
            query = query.filter(
                (
                    (
                        orm.VirtualChannelEpoch.starttime
                        < stream_epoch.starttime
                    )
                    & (
                        (orm.VirtualChannelEpoch.endtime == None)  # noqa
                        | (
                            stream_epoch.starttime
                            < orm.VirtualChannelEpoch.endtime
                        )
                    )
                )
                | (
                    (
                        orm.VirtualChannelEpoch.starttime
                        > stream_epoch.starttime
                    )
                    & (
                        stream_epoch.endtime
                        > orm.VirtualChannelEpoch.starttime
                    )
                )
            )

        vcha_epochs = query.all()

        if vcha_epochs:
            self.logger.warning(
                "Found overlapping orm.VirtualChannelEpoch objects: "
                f"{vcha_epochs}"
            )

        for vcha_epoch in vcha_epochs:
            if session.delete(vcha_epoch):
                self.logger.info(
                    f"Removed orm.VirtualChannelEpoch {vcha_epoch!r}"
                    f"(matching query: {query})."
                )

        # check for an identical orm.VirtualChannelEpoch
        try:
            vcha_epoch = (
                session.query(orm.VirtualChannelEpoch)
                .join(orm.Network)
                .join(orm.Station)
                .filter(orm.Network.code == channel_epoch.network.code)
                .filter(orm.Station.code == channel_epoch.station.code)
                .filter(
                    orm.VirtualChannelEpoch.virtual_channel_epoch_group == vnet
                )
                .filter(orm.VirtualChannelEpoch.channel == channel_epoch.code)
                .filter(
                    orm.VirtualChannelEpoch.location
                    == channel_epoch.locationcode
                )
                .filter(
                    orm.VirtualChannelEpoch.starttime == stream_epoch.starttime
                )
                .filter(
                    orm.VirtualChannelEpoch.endtime == stream_epoch.endtime
                )
                .one_or_none()
            )
        except MultipleResultsFound as err:
            raise self.IntegrityError(err)

        if vcha_epoch is None:
            vcha_epoch = orm.VirtualChannelEpoch(
                channel=channel_epoch.code,
                location=channel_epoch.locationcode,
                starttime=stream_epoch.starttime,
                endtime=stream_epoch.endtime,
                station=channel_epoch.station,
                network=channel_epoch.network,
                virtual_channel_epoch_group=vnet,
            )
            self.logger.debug(
                f"Created new {type(vcha_epoch)} object {vcha_epoch!r}"
            )
            session.add(vcha_epoch)

        else:
            self._update_lastseen(vcha_epoch)

        return vcha_epoch
