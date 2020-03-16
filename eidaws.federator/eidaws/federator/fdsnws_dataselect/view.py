# -*- coding: utf-8 -*-

from eidaws.federator.settings import FED_BASE_ID, FED_DATASELECT_MINISEED_SERVICE_ID
from eidaws.federator.fdsnws_dataselect.parser import DataselectSchema
from eidaws.federator.fdsnws_dataselect.process import (
    DataselectRequestProcessor,
)
from eidaws.federator.utils.view import BaseView


class DataselectView(BaseView):

    LOGGER = ".".join((FED_BASE_ID, FED_DATASELECT_MINISEED_SERVICE_ID, "view"))

    SERVICE_ID = FED_DATASELECT_MINISEED_SERVICE_ID

    def __init__(self, request):
        super().__init__(
            request,
            schema=DataselectSchema,
            processor_cls=DataselectRequestProcessor,
        )
