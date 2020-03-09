# -*- coding: utf-8 -*-

from aiohttp import web

from eidaws.federator.fdsnws_station_text.view import StationTextView
from eidaws.federator.settings import FED_STATION_PATH_TEXT
from eidaws.utils.settings import FDSNWS_QUERY_METHOD_TOKEN


FED_STATION_TEXT_PATH_QUERY = (
    FED_STATION_PATH_TEXT + "/" + FDSNWS_QUERY_METHOD_TOKEN
)


def setup_routes(app):

    app.add_routes([web.view(FED_STATION_TEXT_PATH_QUERY, StationTextView)])
