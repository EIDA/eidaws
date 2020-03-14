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
        super().__init__(request, StationTextSchema)

    async def get(self):

        await super().get()

        # process request
        processor = StationTextRequestProcessor(
            self.request,
            self.config["url_routing"],
        )

        processor.post = False

        return await processor.federate(timeout=self.client_timeout)

    async def post(self):

        await super().post()

        # process request
        processor = StationTextRequestProcessor(
            self.request,
            self.config["url_routing"],
        )

        processor.post = True

        return await processor.federate(timeout=self.client_timeout)


BaseView.register(StationTextView)
