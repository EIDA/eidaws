# -*- coding: utf-8 -*-

from eidaws.stationlite.server.routing.route import (
    setup_routes as setup_routes_routing,
)


def setup_routes(app):
    setup_routes_routing(app)

    return app
