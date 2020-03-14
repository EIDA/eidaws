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

        self._client_timeout = aiohttp.ClientTimeout(
            connect=None, sock_connect=2, sock_read=5
        )

    async def get(self):

        await super().get()

        config = self.request.config_dict["config"]

        # process request
        processor = StationXMLRequestProcessor(
            self.request,
            config[self.SERVICE_ID]["url_routing"],
        )

        processor.post = False

        return await processor.federate(timeout=self._client_timeout)

    async def post(self):

        await super().post()

        config = self.request.config_dict["config"]

        # process request
        processor = StationXMLRequestProcessor(
            self.request,
            config[self.SERVICE_ID]["url_routing"],
        )

        processor.post = True

        return await processor.federate(timeout=self._client_timeout)


BaseView.register(StationXMLView)
