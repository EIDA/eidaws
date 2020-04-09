# -*- coding: utf-8 -*-

import pathlib

from aiohttp import web

from eidaws.federator.fdsnws_station_xml.view import StationXMLView
from eidaws.federator.settings import FED_STATIC, FED_STATION_PATH_XML
from eidaws.federator.utils.misc import append_static_routes
from eidaws.utils.settings import FDSNWS_QUERY_METHOD_TOKEN


FED_STATION_XML_PATH_QUERY = (
    FED_STATION_PATH_XML + "/" + FDSNWS_QUERY_METHOD_TOKEN
)


def setup_routes(app, static=False):

    routes = [web.view(FED_STATION_XML_PATH_QUERY, StationXMLView)]
    if static:
        path_static = pathlib.Path(__file__).parent / FED_STATIC
        append_static_routes(app, routes, FED_STATION_PATH_XML, path_static)

    app.add_routes(routes)
