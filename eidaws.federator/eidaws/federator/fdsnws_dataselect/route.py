# -*- coding: utf-8 -*-

from aiohttp import web

from eidaws.federator.fdsnws_dataselect.view import DataselectView
from eidaws.federator.settings import FED_DATASELECT_PATH_MINISEED
from eidaws.utils.settings import FDSNWS_QUERY_METHOD_TOKEN


FED_DATASELECT_PATH_QUERY = (
    FED_DATASELECT_PATH_MINISEED + "/" + FDSNWS_QUERY_METHOD_TOKEN
)


def setup_routes(app):

    app.add_routes([web.view(FED_DATASELECT_PATH_QUERY, DataselectView)])
