# -*- coding: utf-8 -*-

import pathlib

from aiohttp import web

from eidaws.federator.eidaws_wfcatalog_json.view import WFCatalogView
from eidaws.federator.settings import FED_STATIC, FED_WFCATALOG_PATH_JSON
from eidaws.federator.utils.misc import append_static_routes
from eidaws.utils.settings import FDSNWS_QUERY_METHOD_TOKEN


FED_WFCATALOG_PATH_QUERY = (
    FED_WFCATALOG_PATH_JSON + "/" + FDSNWS_QUERY_METHOD_TOKEN
)


def setup_routes(app, static=False):

    routes = [web.view(FED_WFCATALOG_PATH_QUERY, WFCatalogView)]
    if static:
        path_static = pathlib.Path(__file__).parent / FED_STATIC
        append_static_routes(app, routes, FED_WFCATALOG_PATH_JSON, path_static)

    app.add_routes(routes)
