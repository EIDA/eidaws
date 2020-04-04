# -*- coding: utf-8 -*-

from eidaws.federator.settings import (
    FED_BASE_ID,
    FED_WFCATALOG_JSON_SERVICE_ID,
)
from eidaws.federator.eidaws_wfcatalog.parser import WFCatalogSchema
from eidaws.federator.eidaws_wfcatalog.process import WFCatalogRequestProcessor
from eidaws.federator.utils.view import BaseView


class WFCatalogView(BaseView):

    LOGGER = ".".join((FED_BASE_ID, FED_WFCATALOG_JSON_SERVICE_ID, "view"))

    SERVICE_ID = FED_WFCATALOG_JSON_SERVICE_ID

    def __init__(self, request):
        super().__init__(
            request,
            schema=WFCatalogSchema,
            processor_cls=WFCatalogRequestProcessor,
        )
