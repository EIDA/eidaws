# -*- coding: utf-8 -*-

from webargs.aiohttpparser import parser

from eidaws.federator.settings import (
    FED_BASE_ID,
    FED_WFCATALOG_JSON_SERVICE_ID,
)
from eidaws.federator.eidaws_wfcatalog.json.parser import (
    StreamEpochSchema,
    ManyStreamEpochSchema,
    WFCatalogSchema,
)
from eidaws.federator.eidaws_wfcatalog.json.process import (
    WFCatalogRequestProcessor,
)
from eidaws.federator.utils.parser import fdsnws_parser
from eidaws.federator.utils.strict import keyword_parser
from eidaws.federator.utils.view import BaseView
from eidaws.utils.settings import (
    REQUEST_CONFIG_KEY,
    KEY_REQUEST_QUERY_PARAMS,
    KEY_REQUEST_STREAM_EPOCHS,
)


class WFCatalogView(BaseView):

    LOGGER = ".".join((FED_BASE_ID, FED_WFCATALOG_JSON_SERVICE_ID, "view"))

    SERVICE_ID = FED_WFCATALOG_JSON_SERVICE_ID

    def __init__(self, request):
        super().__init__(
            request,
            schema=WFCatalogSchema,
            processor_cls=WFCatalogRequestProcessor,
        )

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
            self._schema,
            self.request,
            locations=("form",),
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
