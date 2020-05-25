# -*- coding: utf-8 -*-

from flask_restful import Api

from eidaws.stationlite.server.view import (
    StationLiteVersionResource,
    StationLiteWadlResource,
    StationLiteQueryResource,
)
from eidaws.utils.settings import (
    EIDAWS_ROUTING_PATH,
    EIDAWS_ROUTING_PATH_QUERY,
)


def setup_routes(app):

    api = Api(app)

    api.add_resource(
        StationLiteVersionResource, "/".join([EIDAWS_ROUTING_PATH, "version"])
    )
    api.add_resource(
        StationLiteWadlResource,
        "/".join([EIDAWS_ROUTING_PATH, "application.wadl"]),
    )
    api.add_resource(StationLiteQueryResource, EIDAWS_ROUTING_PATH_QUERY)

    return api
