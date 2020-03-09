# -*- coding: utf-8 -*-

from eidaws.federator.fdsnws_station_text.route import setup_routes
from eidaws.federator.utils.app import create_app as _create_app
from eidaws.federator.settings import FED_STATION_TEXT_SERVICE_ID


SERVICE_ID = FED_STATION_TEXT_SERVICE_ID


def create_app(config_dict, **kwargs):

    return _create_app(SERVICE_ID, config_dict, setup_routes, **kwargs)
