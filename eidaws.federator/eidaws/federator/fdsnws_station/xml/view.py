# -*- coding: utf-8 -*-

from eidaws.federator.settings import FED_STATION_XML_SERVICE_ID
from eidaws.federator.fdsnws_station.xml.parser import StationXMLSchema
from eidaws.federator.fdsnws_station.xml.process import (
    StationXMLRequestProcessor,
)
from eidaws.federator.fdsnws_station.view import StationView as _StationView


StationView = _StationView(
    FED_STATION_XML_SERVICE_ID, StationXMLSchema, StationXMLRequestProcessor
)
