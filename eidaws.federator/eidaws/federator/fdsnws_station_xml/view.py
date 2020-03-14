# -*- coding: utf-8 -*-

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
        super().__init__(
            request, StationXMLSchema, processor_cls=StationXMLRequestProcessor
        )
