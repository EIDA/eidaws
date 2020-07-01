# -*- coding: utf-8 -*-

from eidaws.federator.fdsnws_dataselect.miniseed.route import setup_routes
from eidaws.federator.utils.app import create_app as _create_app
from eidaws.federator.settings import FED_DATASELECT_MINISEED_SERVICE_ID


SERVICE_ID = FED_DATASELECT_MINISEED_SERVICE_ID


def create_app(config_dict, **kwargs):

    return _create_app(SERVICE_ID, config_dict, setup_routes, **kwargs)
