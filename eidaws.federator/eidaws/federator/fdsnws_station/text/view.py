# -*- coding: utf-8 -*-

from eidaws.federator.settings import FED_STATION_TEXT_SERVICE_ID
from eidaws.federator.fdsnws_station.text.parser import StationTextSchema
from eidaws.federator.fdsnws_station.text.process import (
    StationTextRequestProcessor,
)
from eidaws.federator.fdsnws_station.view import StationView as _StationView


StationView = _StationView(
    FED_STATION_TEXT_SERVICE_ID, StationTextSchema, StationTextRequestProcessor
)
