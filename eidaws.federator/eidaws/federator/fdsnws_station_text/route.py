# -*- coding: utf-8 -*-
from aiohttp import web

from eidaws.federator.fdsnws_station_text.view import StationTextView
from eidaws.federator.settings import FED_STATION_PATH_TEXT
from eidaws.utils.settings import FDSNWS_QUERY_METHOD_TOKEN


def setup_routes(app):

    fed_station_text_path_query = (FED_STATION_PATH_TEXT + '/' +
                                   FDSNWS_QUERY_METHOD_TOKEN)

    app.add_routes([web.view(fed_station_text_path_query, StationTextView)])
