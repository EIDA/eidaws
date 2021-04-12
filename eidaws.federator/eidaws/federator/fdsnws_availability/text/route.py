# -*- coding: utf-8 -*-

import pathlib

from aiohttp import web

from eidaws.federator.fdsnws_availability.text.view import (
    AvailabilityQueryView,
    AvailabilityExtentView,
)
from eidaws.federator.settings import FED_STATIC, FED_AVAILABILITY_PATH_TEXT
from eidaws.federator.utils.misc import append_static_routes
from eidaws.utils.settings import (
    FDSNWS_QUERY_METHOD_TOKEN,
    FDSNWS_EXTENT_METHOD_TOKEN,
)


FED_AVAILABILITY_TEXT_PATH_QUERY = (
    FED_AVAILABILITY_PATH_TEXT + "/" + FDSNWS_QUERY_METHOD_TOKEN
)
FED_AVAILABILITY_TEXT_PATH_EXTENT = (
    FED_AVAILABILITY_PATH_TEXT + "/" + FDSNWS_EXTENT_METHOD_TOKEN
)


def setup_routes(app, static=False):

    routes = [
        web.view(FED_AVAILABILITY_TEXT_PATH_QUERY, AvailabilityQueryView),
        web.view(FED_AVAILABILITY_TEXT_PATH_EXTENT, AvailabilityExtentView),
    ]
    if static:
        path_static = pathlib.Path(__file__).parent / FED_STATIC
        append_static_routes(
            app,
            routes,
            FED_AVAILABILITY_PATH_TEXT,
            path_static,
            follow_symlinks=True,
        )

    app.add_routes(routes)
