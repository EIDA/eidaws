# -*- coding: utf-8 -*-

import aiohttp
import asyncio
import datetime
import hashlib
import io

from lxml import etree

from eidaws.federator.fdsnws_station_xml.parser import StationXMLSchema
from eidaws.federator.settings import (
    FED_BASE_ID,
    FED_STATION_XML_FORMAT,
    FED_STATION_XML_SERVICE_ID,
)
from eidaws.federator.utils.misc import _serialize_query_params
from eidaws.federator.utils.process import (
    group_routes_by,
    BaseRequestProcessor,
)
from eidaws.federator.utils.worker import (
    with_exception_handling,
    BaseAsyncWorker,
)
from eidaws.federator.utils.request import FdsnRequestHandler
from eidaws.utils.settings import (
    FDSNWS_NO_CONTENT_CODES,
    STATIONXML_TAGS_NETWORK,
    STATIONXML_TAGS_STATION,
    STATIONXML_TAGS_CHANNEL,
)


class _StationXMLAsyncWorker(BaseAsyncWorker):
    """
    A worker task implementation operating on `StationXML
    <https://www.fdsn.org/xml/station/>`_ ``NetworkType`` ``BaseNodeType``
    element granularity.
    """

    QUERY_FORMAT = FED_STATION_XML_FORMAT

    def __init__(
        self,
        request,
        session,
        response,
        write_lock,
        prepare_callback=None,
        level="station",
        **kwargs,
    ):
        super().__init__(
            request,
            session,
            response,
            write_lock,
            prepare_callback=prepare_callback,
            **kwargs,
        )

        self._level = level

        self._network_elements = {}

    @with_exception_handling(ignore_runtime_exception=True)
    async def run(
        self, route, query_params, net, req_method="GET", **req_kwargs
    ):

        self.logger.debug(f"Fetching data for network: {net}")

        # granular request strategy
        tasks = [
            self._fetch(
                _route, query_params, req_method=req_method, **req_kwargs
            )
            for _route in route
        ]

        responses = await asyncio.gather(*tasks, return_exceptions=False)

        for resp in responses:

            station_xml = await self._parse_response(resp)

            if station_xml is None:
                continue

            for net_element in station_xml.iter(STATIONXML_TAGS_NETWORK):
                self._merge_net_element(net_element, level=self._level)

        if self._network_elements:
            async with self._lock:
                if not self._response.prepared:

                    if self._prepare_callback is not None:
                        await self._prepare_callback(self._response)
                    else:
                        await self._response.prepare(self.request)

                for (
                    net_element,
                    sta_elements,
                ) in self._network_elements.values():
                    data = self._serialize_net_element(
                        net_element, sta_elements
                    )
                    await self._response.write(data)

        await self.finalize()

    async def finalize(self):
        self._network_elements = {}

    async def _parse_response(self, resp):
        if resp is None:
            return None

        try:
            ifd = io.BytesIO(await resp.read())
        except asyncio.TimeoutError as err:
            self.logger.warning(f"Socket read timeout: {type(err)}")
            return None
        else:
            # TODO(damb): Check if there is a non-blocking alternative
            # implementation
            return etree.parse(ifd).getroot()

    def _merge_net_element(self, net_element, level):
        """
        Merge a `StationXML
        <https://www.fdsn.org/xml/station/fdsn-station-1.0.xsd>`_
        ``<Network></Network>`` element into the internal element tree.
        """
        if level in ("channel", "response"):
            # merge <Channel></Channel> elements into
            # <Station></Station> from the correct
            # <Network></Network> epoch element
            (
                loaded_net_element,
                loaded_sta_elements,
            ) = self._deserialize_net_element(net_element)

            loaded_net_element, sta_elements = self._emerge_net_element(
                loaded_net_element
            )

            # append / merge <Station></Station> elements
            for key, loaded_sta_element in loaded_sta_elements.items():
                try:
                    sta_element = sta_elements[key]
                except KeyError:
                    sta_elements[key] = loaded_sta_element
                else:
                    # XXX(damb): Channels are ALWAYS appended; no merging
                    # is performed
                    sta_element[1].extend(loaded_sta_element[1])

        elif level == "station":
            # append <Station></Station> elements to the
            # corresponding <Network></Network> epoch
            (
                loaded_net_element,
                loaded_sta_elements,
            ) = self._deserialize_net_element(net_element)

            loaded_net_element, sta_elements = self._emerge_net_element(
                loaded_net_element
            )

            # append <Station></Station> elements if
            # unknown
            for key, loaded_sta_element in loaded_sta_elements.items():
                sta_elements.setdefault(key, loaded_sta_element)

        elif level == "network":
            _ = self._emerge_net_element(net_element)
        else:
            raise ValueError(f"Unknown level: {level!r}")

    def _emerge_net_element(self, net_element):
        """
        Emerge a ``<Network></Network>`` epoch element. If the
        ``<Network></Network>`` element is unknown it is automatically
        appended to the list of already existing network elements.

        :param net_element: Network element to be emerged
        :type net_element: :py:class:`lxml.etree.Element`
        :returns: Emerged ``<Network></Network>`` element
        """
        return self._network_elements.setdefault(
            self._make_key(net_element), (net_element, {})
        )

    def _deserialize_net_element(self, net_element, hash_method=hashlib.md5):
        """
        Deserialize and demultiplex ``net_element``.
        """

        def emerge_sta_elements(net_element):
            for tag in STATIONXML_TAGS_STATION:
                for sta_element in net_element.findall(tag):
                    yield sta_element

        def emerge_cha_elements(sta_element):
            for tag in STATIONXML_TAGS_CHANNEL:
                for cha_element in sta_element.findall(tag):
                    yield cha_element

        sta_elements = {}
        for sta_element in emerge_sta_elements(net_element):

            cha_elements = []
            for cha_element in emerge_cha_elements(sta_element):

                cha_elements.append(cha_element)
                cha_element.getparent().remove(cha_element)

            sta_elements[self._make_key(sta_element)] = (
                sta_element,
                cha_elements,
            )
            sta_element.getparent().remove(sta_element)

        return net_element, sta_elements

    def _serialize_net_element(self, net_element, sta_elements={}):
        for sta_element, cha_elements in sta_elements.values():
            # XXX(damb): No deepcopy is performed since the processor is thrown
            # away anyway.
            sta_element.extend(cha_elements)
            net_element.append(sta_element)

        return etree.tostring(net_element)

    async def _fetch(self, route, query_params, req_method="GET", **kwargs):
        req_handler = FdsnRequestHandler(
            **route._asdict(), query_params=query_params
        )
        req_handler.format = self.QUERY_FORMAT

        req = getattr(req_handler, req_method.lower())(self._session)
        try:
            resp = await req(**kwargs)
        except (aiohttp.ClientError, asyncio.TimeoutError) as err:
            msg = (
                f"Error while executing request: error={type(err)}, "
                f"req_handler={req_handler!r}, method={req_method}"
            )
            await self._handle_error(msg=msg)
            await self.update_cretry_budget(req_handler.url, 503)

            return None

        msg = (
            f"Response: {resp.reason}: resp.status={resp.status}, "
            f"resp.request_info={resp.request_info}, "
            f"resp.url={resp.url}, resp.headers={resp.headers}"
        )

        try:
            resp.raise_for_status()
        except aiohttp.ClientResponseError:
            if resp.status == 413:
                await self._handle_413()
            else:
                await self._handle_error(msg=msg)

            return None
        else:
            if resp.status != 200:
                if resp.status in FDSNWS_NO_CONTENT_CODES:
                    self.logger.info(msg)
                else:
                    await self._handle_error(msg=msg)

                return None

        self.logger.debug(msg)

        await self.update_cretry_budget(req_handler.url, resp.status)
        return resp

    @staticmethod
    def _make_key(element, hash_method=hashlib.md5):
        """
        Compute hash for ``element`` based on the elements' attributes.
        """
        key_args = sorted(element.attrib.items())
        return hash_method(str(key_args).encode("utf-8")).digest()


class StationXMLRequestProcessor(BaseRequestProcessor):

    SERVICE_ID = FED_STATION_XML_SERVICE_ID

    LOGGER = ".".join([FED_BASE_ID, SERVICE_ID, "process"])
    QUERY_PARAM_SERIALIZER = StationXMLSchema

    STATIONXML_SOURCE = "EIDA-Federator"
    STATIONXML_SENDER = "EIDA"
    STATIONXML_HEADER = (
        '<?xml version="1.0" encoding="UTF-8"?>'
        '<FDSNStationXML xmlns="http://www.fdsn.org/xml/station/1" '
        'schemaVersion="1.1">'
        "<Source>{}</Source>"
        "<Sender>{}</Sender>"
        "<Created>{}</Created>"
    )
    STATIONXML_FOOTER = "</FDSNStationXML>"

    def __init__(self, request, **kwargs):
        super().__init__(
            request, **kwargs,
        )

        self._level = self.query_params.get("level", "station")

    @property
    def content_type(self):
        return "application/xml"

    async def _prepare_response(self, response):
        response.content_type = self.content_type
        response.charset = self.charset
        await response.prepare(self.request)

        header = self.STATIONXML_HEADER.format(
            self.STATIONXML_SOURCE,
            self.STATIONXML_SENDER,
            datetime.datetime.utcnow().isoformat(),
        )
        header = header.encode("utf-8")
        await response.write(header)

    def _emerge_worker(self, session, response, lock):
        return _StationXMLAsyncWorker(
            self.request,
            session,
            response,
            lock,
            prepare_callback=self._prepare_response,
            level=self._level,
        )

    async def _dispatch(self, pool, routes, req_method, **req_kwargs):
        """
        Dispatch jobs onto ``pool``.
        """
        qp = _serialize_query_params(
            self.query_params, self.QUERY_PARAM_SERIALIZER
        )

        grouped_routes = group_routes_by(routes, key="network")
        for net, _routes in grouped_routes.items():
            self.logger.debug(
                f"Creating job: Network={net}, route={_routes!r}"
            )
            await pool.submit(
                _routes, qp, net, req_method=req_method, **req_kwargs,
            )

    async def _write_response_footer(self, response):
        footer = self.STATIONXML_FOOTER.encode("utf-8")
        await response.write(footer)
