# -*- coding: utf-8 -*-

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

    app.add_url_rule(
        "/".join([EIDAWS_ROUTING_PATH, "version"]),
        view_func=StationLiteVersionResource.as_view("version"),
    )
    app.add_url_rule(
        "/".join([EIDAWS_ROUTING_PATH, "application.wadl"]),
        view_func=StationLiteWadlResource.as_view("application.wadl"),
    )

    app.add_url_rule(
        EIDAWS_ROUTING_PATH_QUERY,
        view_func=StationLiteQueryResource.as_view("query"),
    )

    return app
