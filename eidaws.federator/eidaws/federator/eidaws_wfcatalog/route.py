# -*- coding: utf-8 -*-

from aiohttp import web

from eidaws.federator.eidaws_wfcatalog.view import WFCatalogView
from eidaws.federator.settings import FED_WFCATALOG_PATH_JSON
from eidaws.utils.settings import FDSNWS_QUERY_METHOD_TOKEN


FED_WFCATALOG_PATH_QUERY = (
    FED_WFCATALOG_PATH_JSON + "/" + FDSNWS_QUERY_METHOD_TOKEN
)


def setup_routes(app):

    app.add_routes([web.view(FED_WFCATALOG_PATH_QUERY, WFCatalogView)])
