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

        config = self.request.config_dict["config"]

        # process request
        processor = StationTextRequestProcessor(
            self.request,
            config[self.SERVICE_ID]["url_routing"],
            proxy_netloc=config[self.SERVICE_ID]["proxy_netloc"],
        )

        processor.post = False

        return await processor.federate()

    async def post(self):

        await super().post()

        config = self.request.config_dict["config"]

        # process request
        processor = StationTextRequestProcessor(
            self.request,
            config[self.SERVICE_ID]["url_routing"],
            proxy_netloc=config[self.SERVICE_ID]["proxy_netloc"],
        )

        processor.post = True

        return await processor.federate()


BaseView.register(StationTextView)
