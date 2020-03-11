# -*- coding: utf-8 -*-

import aiohttp
import logging

from aiohttp import web
from aiohttp_cors import CorsViewMixin

from webargs.aiohttpparser import parser

from eidaws.federator.settings import FED_BASE_ID, FED_STATION_XML_SERVICE_ID
from eidaws.federator.utils.strict import keyword_parser
from eidaws.federator.utils.misc import make_context_logger
from eidaws.federator.utils.parser import fdsnws_parser
from eidaws.federator.fdsnws_station_xml.parser import StationXMLSchema
from eidaws.federator.fdsnws_station_xml.process import (
    StationXMLRequestProcessor,
)
from eidaws.utils.schema import StreamEpochSchema, ManyStreamEpochSchema


class StationXMLView(web.View, CorsViewMixin):

    LOGGER = ".".join((FED_BASE_ID, FED_STATION_XML_SERVICE_ID, "view"))

    SERVICE_ID = FED_STATION_XML_SERVICE_ID

    def __init__(self, request):
        super().__init__(request)
        self._logger = logging.getLogger(self.LOGGER)
        self.logger = make_context_logger(self._logger, self.request)

        self._client_timeout = aiohttp.ClientTimeout(
            connect=120, sock_connect=2, sock_read=30
        )

    async def get(self):

        # strict parameter validation
        await keyword_parser.parse(
            (StationXMLSchema, StreamEpochSchema),
            self.request,
            locations=("query",),
        )

        # parse query parameters
        self.request[FED_BASE_ID + ".query_params"] = await parser.parse(
            StationXMLSchema(), self.request, locations=("query",)
        )

        stream_epochs_dict = await fdsnws_parser.parse(
            ManyStreamEpochSchema(context={"request": self.request}),
            self.request,
            locations=("query",),
        )
        self.request[FED_BASE_ID + ".stream_epochs"] = stream_epochs_dict[
            "stream_epochs"
        ]

        self.logger.debug(self.request[FED_BASE_ID + ".query_params"])
        self.logger.debug(self.request[FED_BASE_ID + ".stream_epochs"])

        config = self.request.config_dict["config"]

        # process request
        processor = StationXMLRequestProcessor(
            self.request,
            config[self.SERVICE_ID]["url_routing"],
            proxy_netloc=config[self.SERVICE_ID]["proxy_netloc"],
        )

        processor.post = False

        return await processor.federate(timeout=self._client_timeout)

    async def post(self):

        # strict parameter validation
        await keyword_parser.parse(
            StationXMLSchema, self.request, locations=("form",),
        )

        # parse query parameters
        self.request[
            FED_BASE_ID + ".query_params"
        ] = await fdsnws_parser.parse(
            StationXMLSchema(), self.request, locations=("form",)
        )

        stream_epochs_dict = await fdsnws_parser.parse(
            ManyStreamEpochSchema(context={"request": self.request}),
            self.request,
            locations=("form",),
        )
        self.request[FED_BASE_ID + ".stream_epochs"] = stream_epochs_dict[
            "stream_epochs"
        ]

        self.logger.debug(self.request[FED_BASE_ID + ".query_params"])
        self.logger.debug(self.request[FED_BASE_ID + ".stream_epochs"])

        config = self.request.config_dict["config"]

        # process request
        processor = StationXMLRequestProcessor(
            self.request,
            config[self.SERVICE_ID]["url_routing"],
            proxy_netloc=config[self.SERVICE_ID]["proxy_netloc"],
        )

        processor.post = True

        return await processor.federate(timeout=self._client_timeout)
