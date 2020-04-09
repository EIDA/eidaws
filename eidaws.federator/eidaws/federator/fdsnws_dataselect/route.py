# -*- coding: utf-8 -*-

import pathlib

from aiohttp import web

from eidaws.federator.fdsnws_dataselect.view import DataselectView
from eidaws.federator.settings import FED_STATIC, FED_DATASELECT_PATH_MINISEED
from eidaws.federator.utils.misc import append_static_routes
from eidaws.utils.settings import FDSNWS_QUERY_METHOD_TOKEN


FED_DATASELECT_PATH_QUERY = (
    FED_DATASELECT_PATH_MINISEED + "/" + FDSNWS_QUERY_METHOD_TOKEN
)


def setup_routes(app, static=False):

    routes = [web.view(FED_DATASELECT_PATH_QUERY, DataselectView)]
    if static:
        path_static = pathlib.Path(__file__).parent / FED_STATIC
        append_static_routes(
            app, routes, FED_DATASELECT_PATH_MINISEED, path_static
        )

    app.add_routes(routes)
