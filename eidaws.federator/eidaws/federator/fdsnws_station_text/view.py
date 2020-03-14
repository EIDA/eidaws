# -*- coding: utf-8 -*-

from eidaws.federator.settings import FED_BASE_ID, FED_STATION_TEXT_SERVICE_ID
from eidaws.federator.fdsnws_station_text.parser import StationTextSchema
from eidaws.federator.fdsnws_station_text.process import (
    StationTextRequestProcessor,
)
from eidaws.federator.utils.view import BaseView


class StationTextView(BaseView):

    LOGGER = ".".join((FED_BASE_ID, FED_STATION_TEXT_SERVICE_ID, "view"))

    SERVICE_ID = FED_STATION_TEXT_SERVICE_ID

    def __init__(self, request):
        super().__init__(
            request,
            StationTextSchema,
            processor_cls=StationTextRequestProcessor,
        )
