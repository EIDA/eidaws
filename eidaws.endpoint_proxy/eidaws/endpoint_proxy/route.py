# -*- coding: utf-8 -*-

from aiohttp import web

from eidaws.endpoint_proxy.view import RedirectView
from eidaws.utils.settings import (
    FDSNWS_DATASELECT_PATH_QUERY,
    FDSNWS_STATION_PATH_QUERY,
    FDSNWS_AVAILABILITY_PATH_QUERY,
    EIDAWS_WFCATALOG_PATH_QUERY,
)


def setup_routes(app):
    routes = [
        web.view(FDSNWS_DATASELECT_PATH_QUERY, RedirectView),
        web.view(FDSNWS_STATION_PATH_QUERY, RedirectView),
        web.view(FDSNWS_AVAILABILITY_PATH_QUERY, RedirectView),
        web.view(EIDAWS_WFCATALOG_PATH_QUERY, RedirectView),
    ]

    app.add_routes(routes)
