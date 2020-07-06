# -*- coding: utf-8 -*-

import aiohttp
import logging

from aiohttp import web
from aiohttp_cors import CorsViewMixin
from webargs.aiohttpparser import parser

from eidaws.federator.settings import FED_BASE_ID
from eidaws.federator.utils.strict import keyword_parser
from eidaws.federator.utils.mixin import ConfigMixin
from eidaws.federator.utils.parser import fdsnws_parser
from eidaws.utils.misc import get_req_config, make_context_logger
from eidaws.utils.schema import StreamEpochSchema, ManyStreamEpochSchema
from eidaws.utils.settings import (
    REQUEST_CONFIG_KEY,
    KEY_REQUEST_QUERY_PARAMS,
    KEY_REQUEST_STREAM_EPOCHS,
)


class BaseView(web.View, CorsViewMixin, ConfigMixin):

    LOGGER = FED_BASE_ID + ".view"

    def __init__(self, request, schema, processor_cls):
        super().__init__(request)
        self._logger = logging.getLogger(self.LOGGER)
        self.logger = make_context_logger(self._logger, self.request)

        self._schema = schema
        self._processor_cls = processor_cls

        assert self.SERVICE_ID, f"Invalid service_id: {self.SERVICE_ID}"

    @property
    def client_timeout(self):
        return aiohttp.ClientTimeout(
            connect=self.config["endpoint_timeout_connect"],
            sock_connect=self.config["endpoint_timeout_sock_connect"],
            sock_read=self.config["endpoint_timeout_sock_read"],
        )

    async def get(self):
        await self._parse_get()

        self.logger.debug(
            get_req_config(self.request, KEY_REQUEST_QUERY_PARAMS)
        )
        self.logger.debug(
            get_req_config(self.request, KEY_REQUEST_STREAM_EPOCHS)
        )

        processor = self._processor_cls(self.request)
        processor.post = False

        return await processor.federate(timeout=self.client_timeout)

    async def post(self):
        await self._parse_post()

        self.logger.debug(
            get_req_config(self.request, KEY_REQUEST_QUERY_PARAMS)
        )
        self.logger.debug(
            get_req_config(self.request, KEY_REQUEST_STREAM_EPOCHS)
        )

        processor = self._processor_cls(self.request)
        processor.post = True

        return await processor.federate(timeout=self.client_timeout)

    async def _parse_get(self):
        # strict parameter validation
        await keyword_parser.parse(
            (self._schema, StreamEpochSchema),
            self.request,
            locations=("query",),
        )

        # parse query parameters
        self.request[REQUEST_CONFIG_KEY][
            KEY_REQUEST_QUERY_PARAMS
        ] = await parser.parse(
            self._schema(), self.request, locations=("query",)
        )

        stream_epochs_dict = await fdsnws_parser.parse(
            ManyStreamEpochSchema(context={"request": self.request}),
            self.request,
            locations=("query",),
        )
        self.request[REQUEST_CONFIG_KEY][
            KEY_REQUEST_STREAM_EPOCHS
        ] = stream_epochs_dict["stream_epochs"]

    async def _parse_post(self):
        # strict parameter validation
        await keyword_parser.parse(
            self._schema, self.request, locations=("form",),
        )

        # parse query parameters
        self.request[REQUEST_CONFIG_KEY][
            KEY_REQUEST_QUERY_PARAMS
        ] = await fdsnws_parser.parse(
            self._schema(), self.request, locations=("form",)
        )

        stream_epochs_dict = await fdsnws_parser.parse(
            ManyStreamEpochSchema(context={"request": self.request}),
            self.request,
            locations=("form",),
        )
        self.request[REQUEST_CONFIG_KEY][
            KEY_REQUEST_STREAM_EPOCHS
        ] = stream_epochs_dict["stream_epochs"]
