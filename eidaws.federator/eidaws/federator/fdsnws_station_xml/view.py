# -*- coding: utf-8 -*-

import aiohttp

from eidaws.federator.settings import FED_BASE_ID, FED_STATION_XML_SERVICE_ID
from eidaws.federator.fdsnws_station_xml.parser import StationXMLSchema
from eidaws.federator.fdsnws_station_xml.process import (
    StationXMLRequestProcessor,
)
from eidaws.federator.utils.view import BaseView


class StationXMLView(BaseView):

    LOGGER = ".".join((FED_BASE_ID, FED_STATION_XML_SERVICE_ID, "view"))

    SERVICE_ID = FED_STATION_XML_SERVICE_ID

    def __init__(self, request):
        super().__init__(request, StationXMLSchema)

    async def get(self):

        await super().get()

        # process request
        processor = StationXMLRequestProcessor(
            self.request,
            self.config["url_routing"],
        )

        processor.post = False

        return await processor.federate(timeout=self.client_timeout)

    async def post(self):

        await super().post()

        # process request
        processor = StationXMLRequestProcessor(
            self.request,
            self.config["url_routing"],
        )

        processor.post = True

        return await processor.federate(timeout=self.client_timeout)


BaseView.register(StationXMLView)
