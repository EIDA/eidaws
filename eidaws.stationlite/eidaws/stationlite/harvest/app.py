# -*- coding: utf-8 -*-
"""
eidaws-stationlite harvesting facilities.
"""

import argparse
import collections
import datetime
import functools
import logging
import logging.config
import logging.handlers  # needed for handlers defined in logging.conf
import os
import sys
import traceback
import warnings
import yaml

import requests

from urllib.parse import urlparse, urljoin

from cached_property import cached_property
from fasteners import InterProcessLock
from jsonschema import validate, ValidationError
from lxml import etree
from obspy import read_inventory, UTCDateTime
from sqlalchemy import create_engine, inspect
from sqlalchemy.exc import OperationalError
from sqlalchemy.orm.exc import MultipleResultsFound

from eidaws.stationlite.engine import db, orm
from eidaws.stationlite.harvest.request import (
    binary_request,
    RequestsError,
    NoContent,
)
from eidaws.stationlite.settings import (
    STL_HARVEST_BASE_ID,
    STL_HARVEST_DEFAULT_NO_ROUTES,
    STL_HARVEST_DEFAULT_NO_VNETWORKS,
    STL_HARVEST_DEFAULT_PATH_PIDFILE,
    STL_HARVEST_DEFAULT_PATH_CONFIG,
    STL_HARVEST_DEFAULT_PATH_LOGGING_CONF,
    STL_HARVEST_DEFAULT_SERVICES,
    STL_HARVEST_DEFAULT_STRICT_RESTRICTED,
    STL_HARVEST_DEFAULT_TRUNCATE_TIMESTAMP,
    STL_HARVEST_DEFAULT_URL_DB,
    STL_HARVEST_DEFAULT_URLS_ROUTING,
)
from eidaws.stationlite.version import __version__
from eidaws.utils.app import (
    prepare_cli_config,
    AppError,
    ConfigurationError,
    CustomParser,
)
from eidaws.utils.config import (
    to_boolean,
    re_path,
    interpolate_environment_variables,
    ConversionMap as _ConversionMap,
)
from eidaws.utils.error import Error, ExitCodes
from eidaws.utils.misc import realpath, real_file_path
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


def _get_method_token(url):
    """
    Utility function returning the method token from the URL's path.

    :param str url: URL
    :returns: Method token
    :retval: str
    """
    token = urlparse(url).path.split("/")[-1]

    try:
        float(token)
    except ValueError:
        return None

    return token


# ----------------------------------------------------------------------------
class NothingToDo(Error):
    """Nothing to do."""


class AlreadyHarvesting(Error):
    """There seems to be a harvesting process already in action ({})."""


# ----------------------------------------------------------------------------
class Harvester:
    """
    Abstract base class for harvesters, harvesting EIDA nodes.

    :param str node_id: EIDA node identifier
    :param str url_routing_config: URL to routing configuration file.
    """

    LOGGER = "eidaws.stationlite.harvest.harvester"

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
        self._config = None

        self.logger = logging.getLogger(self.LOGGER)

    @property
    def url(self):
        return self._url

    @property
    def config(self):
        # proxy for fetching the config from the EIDA node
        if self._config is None:
            req = functools.partial(requests.get, self.url)
            with binary_request(req, timeout=60) as resp:
                self._config = resp

        return self._config

    @staticmethod
    def _update_lastseen(obj):
        obj.lastseen = datetime.datetime.utcnow()

    def harvest(self, session):
        raise NotImplementedError


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

    DEFAULT_RESTRICTED_STATUS = "open"

    class StationXMLParsingError(Harvester.HarvesterError):
        """Error while parsing StationXML: ({})"""

    BaseNode = collections.namedtuple("BaseNode", ["restricted_status"])

    def __init__(self, url_routing_config, **kwargs):
        super().__init__(url_routing_config)

        self._services = kwargs.get("services", STL_HARVEST_DEFAULT_SERVICES)
        self._force_restricted = kwargs.get("force_restricted", True)

    # __init__ ()

    def harvest(self, session):
        """Harvest the routing configuration."""

        def validate_cha_epoch(cha_epoch, service_tag):
            if inspect(cha_epoch).deleted:
                # In case a orm.ChannelEpoch object is marked
                # as deleted but harvested within the same
                # harvesting run this is a strong hint for an
                # integrity issue within the FDSN station
                # InventoryXML.
                raise self.IntegrityError(
                    f"Inventory integrity issue for {cha_epoch!r}"
                )

            if (
                service_tag in ("dataselect", "availability")
                and "partial" == cha_epoch.restrictedstatus
            ):
                raise self.IntegrityError(
                    "Unable to handle 'partial' restrictedStatus for "
                    f"ChannelEpoch {cha_epoch!r}."
                )

        def autocorrect_url(url, service_tag, restricted_status):
            if service_tag not in ("dataselect", "availability",):
                return [url]

            tokens = []
            if "open" == restricted_status:
                tokens.append(FDSNWS_QUERY_METHOD_TOKEN)
                if service_tag == "availability":
                    t = _get_method_token(url)
                    if t is None:
                        tokens.append(FDSNWS_EXTENT_METHOD_TOKEN)
                    elif t == FDSNWS_EXTENT_METHOD_TOKEN:
                        tokens = [FDSNWS_EXTENT_METHOD_TOKEN]

            elif "closed" == restricted_status:
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

        route_tag = f"{self.NS_ROUTINGXML}route"
        _services = [f"{self.NS_ROUTINGXML}{s}" for s in self._services]

        self.logger.debug(f"Harvesting routes for: {self.url!r}")
        # event driven parsing
        for event, route_element in etree.iterparse(
            self.config, events=("end",), tag=route_tag
        ):

            if event == "end" and len(route_element):

                stream = Stream.from_route_attrs(**dict(route_element.attrib))
                attrs = dict(stream._asdict())
                # create query parameters from stream attrs
                query_params = "&".join(
                    [
                        f"{query_param}={query_val}"
                        for query_param, query_val in attrs.items()
                    ]
                )

                # extract fdsn-station service url for each route
                urls = set(
                    [
                        e.get("address")
                        for e in route_element.iter(
                            f"{self.NS_ROUTINGXML}{self.STATION_TAG}"
                        )
                        if int(e.get("priority", 0)) == 1
                    ]
                )

                if (
                    len(urls) == 0
                    and len(
                        [
                            e
                            for e in route_element.iter()
                            if int(e.get("priority", 0)) == 1
                        ]
                    )
                    == 0
                ):
                    # NOTE(damb): Skip routes which contain exclusively
                    # 'priority == 2' services
                    continue

                elif len(urls) > 1:
                    # NOTE(damb): Currently we cannot handle multiple
                    # fdsn-station urls i.e. for multiple routed epochs
                    raise self.IntegrityError(
                        (
                            "Missing <station></station> element for "
                            f"{route_element} ({urls})."
                        )
                    )

                _url_fdsn_station = (
                    f"{urls.pop()}?{query_params}&level=channel"
                )

                self._validate_url_path(_url_fdsn_station, "station")

                # XXX(damb): For every single route resolve FDSN wildcards
                # using the route's station service.
                # XXX(damb): Use the station service's GET method since the
                # POST method requires temporal constraints (both starttime and
                # endtime).
                # ----
                self.logger.debug(
                    f"Resolving routing: (Request: {_url_fdsn_station!r})."
                )
                nets = []
                stas = []
                chas = []
                try:
                    # TODO(damb): Request might be too large. Implement fix.
                    req = functools.partial(requests.get, _url_fdsn_station)
                    with binary_request(req, timeout=60) as station_xml:
                        nets, stas, chas = self._harvest_from_stationxml(
                            session, station_xml
                        )

                except NoContent as err:
                    self.logger.warning(str(err))
                    continue

                for service_element in route_element.iter(*_services):
                    # only consider priority=1
                    priority = service_element.get("priority")
                    if not priority or int(priority) != 1:
                        self.logger.info(
                            f"Skipping {service_element} due to priority "
                            f"{priority!r}."
                        )
                        continue

                    # remove xml namespace
                    service_tag = service_element.tag[
                        len(self.NS_ROUTINGXML) :
                    ]
                    endpoint_url = service_element.get("address")
                    if not endpoint_url:
                        raise self.RoutingConfigXMLParsingError(
                            "Missing 'address' attrib."
                        )

                    service = self._emerge_service(session, service_tag)
                    self.logger.debug(
                        f"Processing routes for {stream!r}"
                        f"(service={service_element.tag}, "
                        f"endpoint={endpoint_url})."
                    )

                    try:
                        routing_starttime = UTCDateTime(
                            service_element.get("start"), iso8601=True
                        ).datetime
                        routing_endtime = service_element.get("end")
                        # reset endtime due to 'end=""'
                        routing_endtime = (
                            UTCDateTime(routing_endtime, iso8601=True).datetime
                            if routing_endtime is not None
                            and routing_endtime.strip()
                            else None
                        )
                    except Exception as err:
                        raise self.RoutingConfigXMLParsingError(err)

                    # configure routings
                    for cha_epoch in chas:

                        try:
                            validate_cha_epoch(cha_epoch)
                        except self.IntegrityError as err:
                            warnings.warn(str(err))
                            self.logger.warning(err)
                            if (
                                session.query(orm.ChannelEpoch)
                                .filter(orm.ChannelEpoch.id == cha_epoch.id)
                                .delete()
                            ):
                                self.logger.warning(
                                    f"Removed {cha_epoch!r} due to integrity "
                                    "error."
                                )
                            continue

                        endpoint_urls = [endpoint_url]
                        if self._force_restricted:
                            endpoint_urls = autocorrect_url(
                                endpoint_url,
                                service_tag,
                                cha_epoch.restrictedstatus,
                            )

                        endpoints = []
                        for url in endpoint_urls:
                            try:
                                self._validate_url_path(url, service_tag)
                            except self.IntegrityError as err:
                                self.logger.warning(
                                    f"Skipping {cha_epoch} due to: {err}"
                                )
                                continue

                            endpoints.append(
                                self._emerge_endpoint(session, url, service)
                            )

                        for endpoint in endpoints:
                            self.logger.debug(
                                "Processing ChannelEpoch<->Endpoint relation "
                                f"{cha_epoch}<->{endpoint} ..."
                            )

                            _ = self._emerge_routing(
                                session,
                                cha_epoch,
                                endpoint,
                                routing_starttime,
                                routing_endtime,
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

        nets = []
        stas = []
        chas = []
        for inv_network in inventory.networks:
            self.logger.debug(f"Processing network: {inv_network!r}")
            net, base_node = self._emerge_network(session, inv_network)
            nets.append(net)

            for inv_station in inv_network.stations:
                self.logger.debug(f"Processing station: {inv_station!r}")
                sta, base_node = self._emerge_station(
                    session, inv_station, base_node
                )
                stas.append(sta)

                for inv_channel in inv_station.channels:
                    self.logger.debug(f"Processing channel: {inv_channel!r}")
                    cha_epoch = self._emerge_channelepoch(
                        session, inv_channel, net, sta, base_node
                    )
                    chas.append(cha_epoch)

        return nets, stas, chas

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
            self.logger.debug(f"Created new service object {service!r}")

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
            self.logger.debug(f"Created new endpoint object {endpoint!r}")

        return endpoint

    def _emerge_network(self, session, network):
        """
        Factory method for a :py:class:`orm.Network` object.

        :param session: SQLAlchemy session object
        :type session: :py:class:`sqlalchemy.orm.session.Session`
        :param station: StationXML network object
        :type station: :py:class:`obspy.core.inventory.network.Network`

        :returns: Tuple of :py:class:`orm.Network``object and
            :py:class:`self.BaseNode`
        :rtype: tuple

        .. note::

            Currently for network epochs there is no validation performed if an
            overlapping epoch exists.
        """
        try:
            net = (
                session.query(orm.Network)
                .filter(orm.Network.code == network.code)
                .one_or_none()
            )
        except MultipleResultsFound as err:
            raise self.IntegrityError(err)

        end_date = network.end_date
        if end_date is not None:
            end_date = end_date.datetime

        restricted_status = (
            self.DEFAULT_RESTRICTED_STATUS
            if network.restricted_status is None
            else network.restricted_status
        )

        # check if network already available - else create a new one
        if net is None:
            net = orm.Network(code=network.code)
            net_epoch = orm.NetworkEpoch(
                description=network.description,
                starttime=network.start_date.datetime,
                endtime=end_date,
                restrictedstatus=restricted_status,
            )
            net.network_epochs.append(net_epoch)
            self.logger.debug(f"Created new network object {net!r}")

            session.add(net)

        else:
            self.logger.debug(f"Updating {net!r} ...")
            # check for available network_epoch - else create a new one
            try:
                net_epoch = (
                    session.query(orm.NetworkEpoch)
                    .join(orm.Network)
                    .filter(orm.NetworkEpoch.network == net)
                    .filter(
                        orm.NetworkEpoch.description == network.description
                    )
                    .filter(
                        orm.NetworkEpoch.starttime
                        == network.start_date.datetime
                    )
                    .filter(orm.NetworkEpoch.endtime == end_date)
                    .filter(
                        orm.NetworkEpoch.restrictedstatus == restricted_status
                    )
                    .one_or_none()
                )
            except MultipleResultsFound as err:
                raise self.IntegrityError(err)

            if net_epoch is None:
                net_epoch = orm.NetworkEpoch(
                    description=network.description,
                    starttime=network.start_date.datetime,
                    endtime=end_date,
                    restrictedstatus=restricted_status,
                )
                net.network_epochs.append(net_epoch)
                self.logger.debug(
                    f"Created new network_epoch object {net_epoch!r}"
                )
            else:
                # XXX(damb): silently update epoch parameters
                self._update_epoch(
                    net_epoch, restricted_status=restricted_status
                )
                self._update_lastseen(net_epoch)

        return net, self.BaseNode(restricted_status=restricted_status)

    def _emerge_station(self, session, station, base_node):
        """
        Factory method for a :py:class:`orm.Station` object.

        :param session: SQLAlchemy session object
        :type session: :py:class:`sqlalchemy.orm.session.Session`
        :param station: StationXML station object
        :type station: :py:class:`obspy.core.inventory.station.Station`
        :param base_node: Parent base node element shipping properties to be
            inherited
        :type base_node: :py:class:`self.BaseNode`

        :returns: Tuple of :py:class:`orm.Station``object and
            :py:class:`self.BaseNode`
        :rtype: tuple

        .. note::

            Currently for station epochs there is no validation performed if an
            overlapping epoch exists.
        """
        try:
            sta = (
                session.query(orm.Station)
                .filter(orm.Station.code == station.code)
                .one_or_none()
            )
        except MultipleResultsFound as err:
            raise self.IntegrityError(err)

        end_date = station.end_date
        if end_date is not None:
            end_date = end_date.datetime

        restricted_status = (
            base_node.restricted_status
            if station.restricted_status is None
            else station.restricted_status
        )

        # check if station already available - else create a new one
        if sta is None:
            sta = orm.Station(code=station.code)
            station_epoch = orm.StationEpoch(
                description=station.description,
                starttime=station.start_date.datetime,
                endtime=end_date,
                latitude=station.latitude,
                longitude=station.longitude,
                restrictedstatus=station.restricted_status,
            )
            sta.station_epochs.append(station_epoch)
            self.logger.debug(f"Created new station object {sta!r}")

            session.add(sta)

        else:
            self.logger.debug(f"Updating {sta!r} ...")
            # check for available station_epoch - else create a new one
            try:
                sta_epoch = (
                    session.query(orm.StationEpoch)
                    .filter(orm.StationEpoch.station == sta)
                    .filter(
                        orm.StationEpoch.description == station.description
                    )
                    .filter(
                        orm.StationEpoch.starttime
                        == station.start_date.datetime
                    )
                    .filter(orm.StationEpoch.endtime == end_date)
                    .filter(orm.StationEpoch.latitude == station.latitude)
                    .filter(orm.StationEpoch.longitude == station.longitude)
                    .one_or_none()
                )
            except MultipleResultsFound as err:
                raise self.IntegrityError(err)

            if sta_epoch is None:
                station_epoch = orm.StationEpoch(
                    description=station.description,
                    starttime=station.start_date.datetime,
                    endtime=end_date,
                    latitude=station.latitude,
                    longitude=station.longitude,
                    restrictedstatus=restricted_status,
                )
                sta.station_epochs.append(station_epoch)
                self.logger.debug(
                    f"Created new station_epoch object {station_epoch!r}"
                )
            else:
                # XXX(damb): silently update inherited base node parameters
                self._update_epoch(
                    sta_epoch, restricted_status=restricted_status
                )
                self._update_lastseen(sta_epoch)

        return sta, self.BaseNode(restricted_status=restricted_status)

    def _emerge_channelepoch(
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
        :py:class:`eidangservices.stationlite.engine.orm.Network`
        :param station: Station referenced by the channel epoch
        :type station:
        :py:class:`eidangservices.stationlite.engine.orm.Station`
        :param base_node: Parent base node element shipping properties to be
            inherited
        :type base_node: :py:class:`self.BaseNode`

        :returns: :py:class:`orm.Channel` object
        :rtype: :py:class:`orm.Channel`
        """
        end_date = channel.end_date
        if end_date is not None:
            end_date = end_date.datetime

        restricted_status = (
            base_node.restricted_status
            if channel.restricted_status is None
            else channel.restricted_status
        )

        # check for available, overlapping channel_epoch (not identical)
        # XXX(damb): Overlapping orm.ChannelEpochs regarding time constraints
        # are updated (i.e. implemented as: delete - insert).
        query = (
            session.query(orm.ChannelEpoch)
            .filter(orm.ChannelEpoch.network == network)
            .filter(orm.ChannelEpoch.station == station)
            .filter(orm.ChannelEpoch.code == channel.code)
            .filter(orm.ChannelEpoch.locationcode == channel.location_code)
        )

        # check if overlapping with ChannelEpoch already existing
        if end_date is None:
            query = query.filter(
                (
                    (orm.ChannelEpoch.starttime < channel.start_date.datetime)
                    & (
                        (orm.ChannelEpoch.endtime == None)
                        | (  # noqa
                            channel.start_date.datetime
                            < orm.ChannelEpoch.endtime
                        )
                    )
                )
                | (orm.ChannelEpoch.starttime > channel.start_date.datetime)
            )
        else:
            query = query.filter(
                (
                    (orm.ChannelEpoch.starttime < channel.start_date.datetime)
                    & (
                        (orm.ChannelEpoch.endtime == None)
                        | (  # noqa
                            channel.start_date.datetime
                            < orm.ChannelEpoch.endtime
                        )
                    )
                )
                | (
                    (orm.ChannelEpoch.starttime > channel.start_date.datetime)
                    & (end_date > orm.ChannelEpoch.starttime)
                )
            )

        cha_epochs_to_update = query.all()

        if cha_epochs_to_update:
            self.logger.warning(
                "Found overlapping orm.ChannelEpoch objects "
                f"{cha_epochs_to_update!r}"
            )

        # check for ChannelEpochs with changed restricted status property
        query = (
            session.query(orm.ChannelEpoch)
            .filter(orm.ChannelEpoch.network == network)
            .filter(orm.ChannelEpoch.station == station)
            .filter(orm.ChannelEpoch.code == channel.code)
            .filter(orm.ChannelEpoch.locationcode == channel.location_code)
            .filter(
                orm.ChannelEpoch.restrictedstatus != channel.restricted_status
            )
        )

        cha_epochs_to_update.extend(query.all())

        # delete affected (overlapping/ changed restrictedstatus) epochs
        # including the corresponding orm.Routing entries
        for cha_epoch in cha_epochs_to_update:
            _ = (
                session.query(orm.Routing)
                .filter(orm.Routing.channel_epoch == cha_epoch)
                .delete()
            )

            if (
                session.query(orm.ChannelEpoch)
                .filter(orm.ChannelEpoch.id == cha_epoch.id)
                .delete()
            ):
                self.logger.info(f"Removed referenced {cha_epoch!r}.")

        # check for an identical orm.ChannelEpoch
        try:
            cha_epoch = (
                session.query(orm.ChannelEpoch)
                .filter(orm.ChannelEpoch.code == channel.code)
                .filter(orm.ChannelEpoch.locationcode == channel.location_code)
                .filter(
                    orm.ChannelEpoch.starttime == channel.start_date.datetime
                )
                .filter(orm.ChannelEpoch.endtime == end_date)
                .filter(orm.ChannelEpoch.station == station)
                .filter(orm.ChannelEpoch.network == network)
                .filter(
                    orm.ChannelEpoch.restrictedstatus
                    == channel.restricted_status
                )
                .one_or_none()
            )
        except MultipleResultsFound as err:
            raise self.IntegrityError(err)

        if cha_epoch is None:
            cha_epoch = orm.ChannelEpoch(
                code=channel.code,
                locationcode=channel.location_code,
                starttime=channel.start_date.datetime,
                endtime=end_date,
                station=station,
                network=network,
                restrictedstatus=restricted_status,
            )
            self.logger.debug(
                f"Created new channel_epoch object {cha_epoch!r}"
            )
            session.add(cha_epoch)
        else:
            self._update_lastseen(cha_epoch)

        return cha_epoch

    def _emerge_routing(self, session, cha_epoch, endpoint, start, end):
        """
        Factory method for a :py:class:`orm.Routing` object.
        """
        # check for available, overlapping routing(_epoch)(not identical)
        # XXX(damb): Overlapping orm.Routing regarding time constraints
        # are updated (i.e. implemented as: delete - insert).
        query = (
            session.query(orm.Routing)
            .filter(orm.Routing.endpoint == endpoint)
            .filter(orm.Routing.channel_epoch == cha_epoch)
        )

        # check if overlapping with ChannelEpoch already existing
        if end is None:
            query = query.filter(
                (
                    (orm.Routing.starttime < start)
                    & (
                        (orm.Routing.endtime == None)
                        | (start < orm.Routing.endtime)  # noqa
                    )
                )
                | (orm.Routing.starttime > start)
            )
        else:
            query = query.filter(
                (
                    (orm.Routing.starttime < start)
                    & (
                        (orm.Routing.endtime == None)
                        | (start < orm.Routing.endtime)  # noqa
                    )
                )
                | (
                    (orm.Routing.starttime > start)
                    & (end > orm.Routing.starttime)
                )
            )

        routings = query.all()

        if routings:
            self.logger.warning(
                f"Found overlapping orm.Routing objects {routings}"
            )

        # delete overlapping orm.Routing entries
        for routing in routings:
            if session.delete(routing):
                self.logger.info(
                    f"Removed {routing!r} (matching query: {query})."
                )

        # check for an identical orm.Routing
        try:
            routing = (
                session.query(orm.Routing)
                .filter(orm.Routing.endpoint == endpoint)
                .filter(orm.Routing.channel_epoch == cha_epoch)
                .filter(orm.Routing.starttime == start)
                .filter(orm.Routing.endtime == end)
                .one_or_none()
            )
        except MultipleResultsFound as err:
            raise self.IntegrityError(err)

        if routing is None:
            routing = orm.Routing(
                endpoint=endpoint,
                channel_epoch=cha_epoch,
                starttime=start,
                endtime=end,
            )
            self.logger.debug(f"Created routing object {routing!r}")
        else:
            self._update_lastseen(routing)

        return routing

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
            epoch.restrictedstatus = restricted_status

    def _validate_url_path(self, url, service, restricted_status="open"):
        """
        Validate FDSN/EIDA service URLs.

        :param str url: URL to validate
        :param str service: Service identifier.
        :param str restricted_status: Restricted status of the related channel
            epoch.
        :raises Harvester.ValidationError: If the URL path does not match the
            the service specifications.
        """
        p = urlparse(url).path

        if "station" == service and p == FDSNWS_STATION_PATH_QUERY:
            return
        elif "dataselect" == service:
            if (
                "open" == restricted_status
                and p == FDSNWS_DATASELECT_PATH_QUERY
            ) or (
                "closed" == restricted_status
                and p == FDSNWS_DATASELECT_PATH_QUERYAUTH
            ):
                return
        elif "availability" == service:
            if (
                "open" == restricted_status
                and p
                in (
                    FDSNWS_AVAILABILITY_PATH_QUERY,
                    FDSNWS_AVAILABILITY_PATH_EXTENT,
                )
            ) or (
                "closed" == restricted_status
                and p
                in (
                    FDSNWS_AVAILABILITY_PATH_QUERYAUTH,
                    FDSNWS_AVAILABILITY_PATH_EXTENTAUTH,
                )
            ):
                return

        elif "wfcatalog" == service and p == EIDAWS_WFCATALOG_PATH_QUERY:
            return

        raise Harvester.ValidationError(f"Invalid path {p!r} for URL {url!r}.")


class VNetHarvester(Harvester):
    """
    Implementation of an harvester harvesting the virtual network information
    from an EIDA node. Usually, the information is stored within the routing
    service's local configuration.

    This harvester does not rely on the EIDA routing service anymore.
    """

    class VNetHarvesterError(Harvester.HarvesterError):
        """Base error for virtual netowork harvesting ({})."""

    def harvest(self, session):

        vnet_tag = f"{self.NS_ROUTINGXML}vnetwork"
        stream_tag = f"{self.NS_ROUTINGXML}stream"

        self.logger.debug(f"Harvesting virtual networks for: {self.url!r}")

        # event driven parsing
        for event, vnet_element in etree.iterparse(
            self.config, events=("end",), tag=vnet_tag
        ):
            if event == "end" and len(vnet_element):

                vnet = self._emerge_streamepoch_group(session, vnet_element)

                for stream_element in vnet_element.iter(tag=stream_tag):
                    self.logger.debug(
                        f"Processing stream element: {stream_element}"
                    )
                    # convert attributes to dict
                    stream = Stream.from_route_attrs(
                        **dict(stream_element.attrib)
                    )
                    try:
                        stream_starttime = UTCDateTime(
                            stream_element.get("start"), iso8601=True
                        ).datetime
                        endtime = stream_element.get("end")
                        # reset endtime due to 'end=""'
                        stream_endtime = (
                            UTCDateTime(endtime, iso8601=True).datetime
                            if endtime is not None and endtime.strip()
                            else None
                        )
                    except Exception as err:
                        raise self.RoutingConfigXMLParsingError(err)

                    # deserialize to StreamEpoch object
                    stream_epoch = StreamEpoch(
                        stream=stream,
                        starttime=stream_starttime,
                        endtime=stream_endtime,
                    )

                    self.logger.debug(f"Processing {stream_epoch!r} ...")

                    sql_stream_epoch = stream_epoch.fdsnws_to_sql_wildcards()

                    # check if the stream epoch definition is valid i.e. there
                    # must be at least one matching ChannelEpoch
                    query = (
                        session.query(orm.ChannelEpoch)
                        .join(orm.Network)
                        .join(orm.Station)
                        .filter(
                            orm.Network.code.like(sql_stream_epoch.network)
                        )
                        .filter(
                            orm.Station.code.like(sql_stream_epoch.station)
                        )
                        .filter(
                            orm.ChannelEpoch.locationcode.like(
                                sql_stream_epoch.location
                            )
                        )
                        .filter(
                            orm.ChannelEpoch.code.like(
                                sql_stream_epoch.channel
                            )
                        )
                        .filter(
                            (orm.ChannelEpoch.endtime == None)
                            | (  # noqa
                                orm.ChannelEpoch.endtime
                                > sql_stream_epoch.starttime
                            )
                        )
                    )

                    if sql_stream_epoch.endtime:
                        query = query.filter(
                            orm.ChannelEpoch.starttime
                            < sql_stream_epoch.endtime
                        )

                    cha_epochs = query.all()
                    if not cha_epochs:
                        self.logger.warn(
                            "No ChannelEpoch matching stream epoch "
                            f"{stream_epoch!r}"
                        )
                        continue

                    for cha_epoch in cha_epochs:
                        self.logger.debug(
                            "Processing virtual network configuration for "
                            f"ChannelEpoch object {cha_epoch!r}."
                        )
                        self._emerge_streamepoch(
                            session, cha_epoch, stream_epoch, vnet
                        )

        # TODO(damb): Show stats for updated/inserted elements

    def _emerge_streamepoch_group(self, session, element):
        """
        Factory method for a :py:class:`orm.StreamEpochGroup`
        """
        net_code = element.get("networkCode")
        if not net_code:
            raise self.VNetHarvesterError("Missing 'networkCode' attribute.")

        try:
            vnet = (
                session.query(orm.StreamEpochGroup)
                .filter(orm.StreamEpochGroup.code == net_code)
                .one_or_none()
            )
        except MultipleResultsFound as err:
            raise self.IntegrityError(err)

        # check if network already available - else create a new one
        if vnet is None:
            vnet = orm.StreamEpochGroup(code=net_code)
            self.logger.debug(f"Created new StreamEpochGroup object {vnet!r}")
            session.add(vnet)

        else:
            self.logger.debug(f"Updating orm.StreamEpochGroup object {vnet!r}")

        return vnet

    def _emerge_streamepoch(self, session, channel_epoch, stream_epoch, vnet):
        """
        Factory method for a :py:class:`orm.StreamEpoch` object.
        """
        # check if overlapping with a StreamEpoch already existing
        # XXX(damb): Overlapping orm.StreamEpoch objects regarding time
        # constraints are updated (i.e. implemented as: delete - insert).
        query = (
            session.query(orm.StreamEpoch)
            .join(orm.Network)
            .join(orm.Station)
            .filter(orm.Network.code == channel_epoch.network.code)
            .filter(orm.Station.code == channel_epoch.station.code)
            .filter(orm.StreamEpoch.stream_epoch_group == vnet)
            .filter(orm.StreamEpoch.channel == channel_epoch.code)
            .filter(orm.StreamEpoch.location == channel_epoch.locationcode)
        )

        if stream_epoch.endtime is None:
            query = query.filter(
                (
                    (orm.StreamEpoch.starttime < stream_epoch.starttime)
                    & (
                        (orm.StreamEpoch.endtime == None)
                        | (  # noqa
                            stream_epoch.starttime < orm.StreamEpoch.endtime
                        )
                    )
                )
                | (orm.StreamEpoch.starttime > stream_epoch.starttime)
            )
        else:
            query = query.filter(
                (
                    (orm.StreamEpoch.starttime < stream_epoch.starttime)
                    & (
                        (orm.StreamEpoch.endtime == None)
                        | (  # noqa
                            stream_epoch.starttime < orm.StreamEpoch.endtime
                        )
                    )
                )
                | (
                    (orm.StreamEpoch.starttime > stream_epoch.starttime)
                    & (stream_epoch.endtime > orm.StreamEpoch.starttime)
                )
            )

        stream_epochs = query.all()

        if stream_epochs:
            self.logger.warning(
                "Found overlapping orm.StreamEpoch objects {stream_epochs}"
            )

        for se in stream_epochs:
            if session.delete(se):
                self.logger.info(
                    f"Removed orm.StreamEpoch {se!r}"
                    f"(matching query: {query})."
                )

        # check for an identical orm.StreamEpoch
        try:
            se = (
                session.query(orm.StreamEpoch)
                .join(orm.Network)
                .join(orm.Station)
                .filter(orm.Network.code == channel_epoch.network.code)
                .filter(orm.Station.code == channel_epoch.station.code)
                .filter(orm.StreamEpoch.stream_epoch_group == vnet)
                .filter(orm.StreamEpoch.channel == channel_epoch.code)
                .filter(orm.StreamEpoch.location == channel_epoch.locationcode)
                .filter(orm.StreamEpoch.starttime == stream_epoch.starttime)
                .filter(orm.StreamEpoch.endtime == stream_epoch.endtime)
                .one_or_none()
            )
        except MultipleResultsFound as err:
            raise self.IntegrityError(err)

        if se is None:
            se = orm.StreamEpoch(
                channel=channel_epoch.code,
                location=channel_epoch.locationcode,
                starttime=stream_epoch.starttime,
                endtime=stream_epoch.endtime,
                station=channel_epoch.station,
                network=channel_epoch.network,
                stream_epoch_group=vnet,
            )
            self.logger.debug(
                f"Created new StreamEpoch object instance {se!r}"
            )
            session.add(se)

        else:
            self._update_lastseen(se)
            self.logger.debug(
                f"Found existing StreamEpoch object instance {se!r}"
            )

        return se


class StationLiteHarvestApp:
    """
    Implementation of the harvesting application for EIDA StationLite.
    """

    PROG = "eida-stationlite-harvest"

    DB_PRAGMAS = ["PRAGMA journal_mode=WAL"]

    JSON_SCHEMA = {
        "type": "object",
        "properties": {
            "no_routes": {"type": "boolean"},
            "no_vnetworks": {"type": "boolean"},
            "path_pidfile": {
                "oneOf": [
                    {"type": "null"},
                    {"type": "string", "pattern": r"^/"},
                ]
            },
            "path_logging_conf": {
                "oneOf": [{"type": "null"}, {"type": "string"}]
            },
            "strict_restricted": {"type": "boolean"},
            "services": {
                "type": "array",
                "items": {
                    "type": "string",
                    "enum": ["dataselect", "station", "wfcatalog"],
                },
                "uniqueItems": True,
            },
            "truncate": {
                "oneOf": [
                    {"type": "null"},
                    {"type": "string", "format": "date"},
                    {"type": "string", "format": "date-time"},
                ],
            },
            "sqlalchemy_database_uri": {"type": "string", "format": "uri"},
            "urls_localconfig": {
                "type": "array",
                "items": {"type": "string", "format": "uri"},
                "uniqueItems": True,
            },
        },
        "additionalProperties": False,
    }

    @cached_property
    def config(self):

        default_config = {
            "no_routes": STL_HARVEST_DEFAULT_NO_ROUTES,
            "no_vnetworks": STL_HARVEST_DEFAULT_NO_VNETWORKS,
            "path_pidfile": STL_HARVEST_DEFAULT_PATH_PIDFILE,
            "path_logging_conf": STL_HARVEST_DEFAULT_PATH_LOGGING_CONF,
            "strict_restricted": STL_HARVEST_DEFAULT_STRICT_RESTRICTED,
            "services": STL_HARVEST_DEFAULT_SERVICES,
            "truncate": STL_HARVEST_DEFAULT_TRUNCATE_TIMESTAMP,
            "sqlalchemy_database_uri": STL_HARVEST_DEFAULT_URL_DB,
            "urls_localconfig": STL_HARVEST_DEFAULT_URLS_ROUTING,
        }

        # do all the dirty work of parsing and merging the configuration
        def _parse_urls_localconfig(ifd):
            return [url.strip() for url in ifd.read().split()]

        def _validate(config_dict):
            try:
                validate(
                    instance=dict(config_dict), schema=self.JSON_SCHEMA,
                )
            except ValidationError as err:
                raise ConfigurationError(str(err))

        def configure_logging(config_dict):
            try:
                path_logging_conf = realpath(config_dict["path_logging_conf"])
            except (KeyError, TypeError):
                path_logging_conf = None

            self.logger = self._setup_logger(
                path_logging_conf, capture_warnings=True
            )

        # CLI configuration
        args = self._build_cli_parser().parse_args()
        cli_config = {}
        if args.path_urls_localconfig:
            cli_config["urls_localconfig"] = _parse_urls_localconfig(
                args.path_urls_localconfig
            )
        cli_config.update(
            prepare_cli_config(
                args, attrs_to_remove=["path_config", "path_urls_localconfig"]
            )
        )

        if args.path_config is None:
            config_dict = collections.ChainMap(cli_config, default_config)
            _validate(config_dict)
            configure_logging(config_dict)
            return config_dict

        # configuration from yaml configuration file
        file_config = {}
        try:
            with open(args.path_config) as ifd:
                _file_config = yaml.safe_load(ifd)
        except yaml.YAMLError as err:
            warnings.warn(f"Exception while parsing configuration file: {err}")
        except FileNotFoundError as err:
            warnings.warn(
                f"Configuration file not found ({err}). Using defaults."
            )
        else:
            if _file_config is not None and isinstance(
                _file_config.get(STL_HARVEST_BASE_ID),
                (collections.abc.Mapping, collections.abc.MutableMapping),
            ):

                def stl_harvest_path(*args):
                    return re_path(STL_HARVEST_BASE_ID, *args)

                class ConversionMap(_ConversionMap):
                    MAP = {
                        stl_harvest_path("no_routes"): to_boolean,
                        stl_harvest_path("no_vnetworks"): to_boolean,
                        stl_harvest_path("strict_restricted"): to_boolean,
                    }

                # interpolate environment variables
                _file_config = interpolate_environment_variables(
                    _file_config,
                    STL_HARVEST_BASE_ID,
                    os.environ,
                    converter=ConversionMap(),
                )
                file_config = _file_config[STL_HARVEST_BASE_ID]

        config_dict = collections.ChainMap(
            cli_config, file_config, default_config
        )

        _validate(config_dict)
        configure_logging(config_dict)
        return config_dict

    def run(self):
        """
        Run application.
        """
        # configure SQLAlchemy logging
        # log_level = self.logger.getEffectiveLevel()
        # logging.getLogger('sqlalchemy.engine').setLevel(log_level)

        exit_code = ExitCodes.EXIT_SUCCESS

        self.logger.info(f"{self.PROG}: Version v{__version__}")
        self.logger.debug(f"Configuration: {dict(self.config)!r}")

        try:
            path_pidfile = self.config["path_pidfile"]
            pid_lock = InterProcessLock(path_pidfile)
            pid_lock_gotten = pid_lock.acquire(blocking=False)
            if not pid_lock_gotten:
                raise AlreadyHarvesting(self.path_pidfile)
            self.logger.debug(
                f"Aquired PID lock {self.config['path_pidfile']!r}"
            )

            if (
                self.config["no_routes"]
                and self.config["no_vnetworks"]
                and not self.config["truncate"]
            ):
                raise NothingToDo()

            harvesting = not (
                self.config["no_routes"] and self.config["no_vnetworks"]
            )

            Session = db.ScopedSession()
            engine = create_engine(self.config["sqlalchemy_database_uri"])
            Session.configure(bind=engine)

            if engine.name == "sqlite":
                db.configure_sqlite(self.DB_PRAGMAS)

            # TODO(damb): Implement multithreaded harvesting using a thread
            # pool.
            try:
                if harvesting:
                    self.logger.info("Start harvesting.")

                if not self.config["no_routes"]:
                    self._harvest_routes(Session)
                else:
                    self.logger.info(
                        "Disabled processing <route></route> information."
                    )

                if not self.config["no_vnetworks"]:
                    self._harvest_vnetworks(Session)
                else:
                    self.logger.info(
                        "Disabled processing <vnetwork></vnetwork> "
                        "information."
                    )

                if harvesting:
                    self.logger.info("Finished harvesting successfully.")

                if self.config["truncate"]:
                    self.logger.warning("Removing outdated data.")
                    session = Session()
                    with db.session_guard(session) as _session:
                        num_removed_rows = db.clean(
                            _session, UTCDateTime(self.config["truncate"]),
                        )
                        self.logger.info(
                            f"Number of rows removed: {num_removed_rows}"
                        )

            except OperationalError as err:
                raise db.StationLiteDBEngineError(err)

        # TODO(damb): signal handling
        except Error as err:
            self.logger.error(err)
            exit_code = ExitCodes.EXIT_ERROR
        except Exception as err:
            exc_type, exc_value, exc_traceback = sys.exc_info()
            self.logger.critical("Local Exception: %s" % err)
            self.logger.critical(
                "Traceback information: "
                + repr(
                    traceback.format_exception(
                        exc_type, exc_value, exc_traceback
                    )
                )
            )
            exit_code = ExitCodes.EXIT_ERROR
        finally:
            try:
                if pid_lock_gotten:
                    pid_lock.release()
            except NameError:
                pass

        sys.exit(exit_code)

    def _build_cli_parser(self, parents=[]):
        """
        Configure a parser.

        :param list parents: list of parent parsers
        :returns: parser
        :rtype: :py:class:`argparse.ArgumentParser`
        """
        parser = CustomParser(
            prog=self.PROG,
            description="Harvest routes for eidaws-stationlite.",
            parents=parents,
        )
        # optional arguments
        parser.add_argument(
            "--version",
            "-V",
            action="version",
            version="%(prog)s version " + __version__,
        )
        parser.add_argument(
            "--url-file",
            type=argparse.FileType("r"),
            dest="path_urls_localconfig",
            metavar="PATH",
            help=(
                "Path to a configuration file containing a list of URLs to "
                "eidaws-routing localconfig configurations. If a - is passed "
                "the input is read from stdin."
            ),
        )
        parser.add_argument(
            "-S",
            "--services",
            nargs="+",
            type=str,
            metavar="SERVICE",
            choices=sorted(STL_HARVEST_DEFAULT_SERVICES),
            help=(
                "Whitespace-separated list of services to "
                "be cached. (choices: {%(choices)s}) "
                "(default: {%(default)s})"
            ),
        )
        parser.add_argument(
            "--strict-restricted",
            action="store_true",
            dest="strict_restricted",
            help=(
                "Perform a strict validation of channel "
                "epochs to use the correct "
                "dataselect method token depending on "
                "their restricted status. By default method "
                "tokens are adjusted automatically. "
                "(default: %(default)s)"
            ),
        )
        parser.add_argument(
            "--no-routes",
            action="store_true",
            dest="no_routes",
            help="Do not harvest <route></route> information.",
        )
        parser.add_argument(
            "--no-vnetworks",
            action="store_true",
            dest="no_vnetworks",
            help="Do not harvest <vnetwork></vnetwork> information.",
        )
        parser.add_argument(
            "-t",
            "--truncate",
            type=str,
            metavar="TIMESTAMP",
            help="Truncate DB (delete outdated information).",
        )
        parser.add_argument(
            "--db",
            type=str,
            dest="sqlalchemy_database_uri",
            metavar="URL",
            help=(
                "DB URL indicating the database dialect and "
                "connection arguments."
            ),
        )
        parser.add_argument(
            "-P",
            "--pid-file",
            type=str,
            metavar="PATH",
            dest="path_pidfile",
            help="Path to PID file. (default: {%(default)s})",
        )
        parser.add_argument(
            "-c",
            "--config",
            type=real_file_path,
            metavar="PATH",
            dest="path_config",
            default=STL_HARVEST_DEFAULT_PATH_CONFIG,
            help="Path to configuration file.",
        )
        parser.add_argument(
            "--logging-conf",
            dest="path_logging_conf",
            metavar="PATH",
            help="Path to logging configuration file.",
        )
        return parser

    def _setup_logger(self, path_logging_conf=None, capture_warnings=False):
        """
        Initialize the logger of the application.
        """
        logging.basicConfig(level=logging.WARNING)

        LOGGER = STL_HARVEST_BASE_ID

        if path_logging_conf is not None:
            try:
                logging.config.fileConfig(path_logging_conf)
                logger = logging.getLogger(LOGGER)
                logger.info(
                    "Using logging configuration read from "
                    f"{path_logging_conf!r}."
                )
            except Exception as err:
                print(
                    f"WARNING: Setup logging failed for {path_logging_conf!r} "
                    f"with error: {err!r}."
                )
                logger = logging.getLogger(LOGGER)
        else:
            logger = logging.getLogger(LOGGER)
            logger.addHandler(logging.NullHandler())

        logging.captureWarnings(bool(capture_warnings))

        return logger

    def _harvest_routes(self, Session):
        """
        Harvest the EIDA node's ``<route></route>`` information.

        :param Session: A configured Session class reference
        :type Session: :py:class:`sqlalchemy.orm.session.Session`
        """
        for url in self.config["urls_localconfig"]:
            self.logger.info(f"Processing routes from URL: {url!r}")
            try:
                h = RoutingHarvester(
                    url,
                    services=self.config["services"],
                    force_restricted=not self.config["strict_restricted"],
                )

                session = Session()
                # XXX(damb): Maintain sessions within the scope of a
                # harvesting process.
                with db.session_guard(session) as _session:
                    h.harvest(_session)

            except RequestsError as err:
                self.logger.warning(str(err))
            except Harvester.HarvesterError as err:
                self.logger.error(str(err))

    def _harvest_vnetworks(self, Session):
        """
        Harvest the EIDA node's ``<vnetwork></vnetwork>`` information.

        :param Session: A configured Session class reference
        :type Session: :py:class:`sqlalchemy.orm.session.Session`
        """
        for url in self.config["urls_localconfig"]:

            self.logger.info(f"Processing virtual networks from URL: {url!r}")
            try:
                # harvest virtual network configuration
                h = VNetHarvester(url)
                session = Session()
                # XXX(damb): Maintain sessions within the scope of a
                # harvesting process.
                with db.session_guard(session) as _session:
                    h.harvest(_session)

            except RequestsError as err:
                self.logger.warning(str(err))
            except Harvester.HarvesterError as err:
                self.logger.error(str(err))


# ----------------------------------------------------------------------------
def main():
    """
    main function for EIDA stationlite harvesting
    """

    app = StationLiteHarvestApp()

    try:
        _ = app.config
    except AppError as err:
        # handle errors during the application configuration
        print(
            'ERROR: Application configuration failed "%s".' % err,
            file=sys.stderr,
        )
        sys.exit(ExitCodes.EXIT_ERROR)

    app.run()


# -----------------------------------------------------------------------------
if __name__ == "__main__":
    main()
