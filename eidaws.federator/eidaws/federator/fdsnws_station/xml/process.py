# -*- coding: utf-8 -*-

import asyncio
import datetime
import hashlib
import io

from lxml import etree

from eidaws.federator.fdsnws_station.xml.parser import StationXMLSchema
from eidaws.federator.settings import (
    FED_BASE_ID,
    FED_STATION_XML_SERVICE_ID,
)
from eidaws.federator.utils.process import (
    group_routes_by,
    UnsortedResponse,
)
from eidaws.federator.utils.worker import (
    with_exception_handling,
    BaseWorker,
    NetworkLevelMixin,
)
from eidaws.federator.utils.misc import create_job_context
from eidaws.utils.misc import make_context_logger
from eidaws.utils.settings import (
    STATIONXML_TAGS_NETWORK,
    STATIONXML_TAGS_STATION,
    STATIONXML_TAGS_CHANNEL,
)


class _StationXMLWorker(NetworkLevelMixin, BaseWorker):
    """
    A worker task implementation operating on `StationXML
    <https://www.fdsn.org/xml/station/>`_ ``NetworkType`` ``BaseNodeType``
    element granularity.
    """

    SERVICE_ID = FED_STATION_XML_SERVICE_ID
    QUERY_PARAM_SERIALIZER = StationXMLSchema

    LOGGER = ".".join([FED_BASE_ID, SERVICE_ID, "worker"])

    @property
    def level(self):
        return self.query_params["level"]

    @with_exception_handling(ignore_runtime_exception=True)
    async def run(
        self, route, net, req_method="GET", context=None, **req_kwargs
    ):
        context = context or {}
        context["buffer"] = {}

        # context logging
        try:
            logger = make_context_logger(self._logger, *context["logger_ctx"])
        except (TypeError, KeyError):
            logger = self.logger
        finally:
            context["logger"] = logger

        logger.debug(
            f"Fetching data for network: {net!r} (num_routes={len(route)})"
        )

        # TODO(damb): Currently, limiting the number of concurrent connection
        # is guaranteed by sharing an aiohttp.TCPConnector instance. Though,
        # instead of limiting the connection based on a HTTP connection pool it
        # would be better to limit the overall number of tasks in order to keep
        # the memory footprint low. -> Create bound number of tasks by means of
        # globally sharing an task pool. Similar to
        # https://docs.aiohttp.org/en/stable/client_reference.html#
        # aiohttp.TCPConnector,
        # to be able to limit task creation on a) overall and b) on a per host
        # basis.

        # granular request strategy
        tasks = [
            self._fetch(
                _route,
                parser_cb=self._parse_response,
                req_method=req_method,
                context={
                    "logger_ctx": create_job_context(
                        self.request, parent_ctx=context.get("logger_ctx")
                    )
                },
                **req_kwargs,
            )
            for _route in route
        ]
        results = await asyncio.gather(*tasks, return_exceptions=False)

        logger.debug(
            f"Merging StationXML network element (net={net!r}, "
            f"level={self.level!r}) ..."
        )
        for _, station_xml in results:
            if station_xml is None:
                continue

            for net_element in station_xml.iter(STATIONXML_TAGS_NETWORK):
                self._merge_net_element(
                    net_element, level=self.level, context=context
                )

        for (
            net_element,
            sta_elements,
        ) in context["buffer"].values():
            serialized = self._serialize_net_element(net_element, sta_elements)
            async with self._lock:
                await self._drain.drain(serialized)

        await self.finalize()

    async def _parse_response(self, resp):
        if resp is None:
            return None

        ifd = io.BytesIO(await resp.read())
        # TODO(damb): Check if there is a non-blocking alternative
        # implementation
        try:
            return etree.parse(ifd).getroot()
        except etree.XMLSyntaxError:
            return None

    def _merge_net_element(self, net_element, level, context):
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
                loaded_net_element, context
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
                loaded_net_element, context
            )

            # append <Station></Station> elements if
            # unknown
            for key, loaded_sta_element in loaded_sta_elements.items():
                sta_elements.setdefault(key, loaded_sta_element)

        elif level == "network":
            _ = self._emerge_net_element(net_element, context)
        else:
            raise ValueError(f"Unknown level: {level!r}")

    def _emerge_net_element(self, net_element, context):
        """
        Emerge a ``<Network></Network>`` epoch element. If the
        ``<Network></Network>`` element is unknown it is automatically
        appended to the list of already existing network elements.

        :param net_element: Network element to be emerged
        :type net_element: :py:class:`lxml.etree.Element`
        :returns: Emerged ``<Network></Network>`` element
        """
        return context["buffer"].setdefault(
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

    @staticmethod
    def _make_key(element, hash_method=hashlib.md5):
        """
        Compute hash for ``element`` based on the elements' attributes.
        """
        key_args = sorted(element.attrib.items())
        return hash_method(str(key_args).encode("utf-8")).digest()


class StationXMLRequestProcessor(UnsortedResponse):

    SERVICE_ID = FED_STATION_XML_SERVICE_ID

    LOGGER = ".".join([FED_BASE_ID, SERVICE_ID, "process"])

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

    @property
    def content_type(self):
        return "application/xml"

    async def _prepare_response(self, response):
        await super()._prepare_response(response)

        header = self.STATIONXML_HEADER.format(
            self.STATIONXML_SOURCE,
            self.STATIONXML_SENDER,
            datetime.datetime.utcnow().isoformat(),
        )
        header = header.encode("utf-8")
        await response.write(header)

    def _create_worker(self, request, session, drain, lock=None, **kwargs):
        return _StationXMLWorker(
            request,
            session,
            drain,
            lock=lock,
            *kwargs,
        )

    async def _dispatch(self, pool, routes, req_method, **req_kwargs):
        """
        Dispatch jobs onto ``pool``.
        """
        grouped_routes = group_routes_by(routes, key="network")
        for net, _routes in grouped_routes.items():
            ctx = {"logger_ctx": create_job_context(self.request)}
            self.logger.debug(
                f"Creating job: context={ctx!r}, network={net}, "
                f"route={_routes!r}"
            )
            await pool.submit(
                _routes,
                net,
                req_method=req_method,
                context=ctx,
                **req_kwargs,
            )

    async def _write_response_footer(self, response):
        footer = self.STATIONXML_FOOTER.encode("utf-8")
        await response.write(footer)
