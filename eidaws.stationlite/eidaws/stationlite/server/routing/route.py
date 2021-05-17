# -*- coding: utf-8 -*-

from eidaws.stationlite.server.routing.view import (
    RoutingVersionResource,
    RoutingWadlResource,
    RoutingQueryResource,
)
from eidaws.utils.settings import (
    EIDAWS_ROUTING_PATH,
    EIDAWS_ROUTING_PATH_QUERY,
)


def _create_name(name, suffix="routing"):
    return f"{name}-{suffix}"


def setup_routes(app):
    app.add_url_rule(
        "/".join([EIDAWS_ROUTING_PATH, "version"]),
        view_func=RoutingVersionResource.as_view(_create_name("version")),
    )
    app.add_url_rule(
        "/".join([EIDAWS_ROUTING_PATH, "application.wadl"]),
        view_func=RoutingWadlResource.as_view(
            _create_name("application.wadl")
        ),
    )
    app.add_url_rule(
        EIDAWS_ROUTING_PATH_QUERY,
        view_func=RoutingQueryResource.as_view(_create_name("query")),
    )

    return app
