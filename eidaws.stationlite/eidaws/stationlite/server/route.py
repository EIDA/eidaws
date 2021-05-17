# -*- coding: utf-8 -*-

from eidaws.stationlite.server.routing.route import (
    setup_routes as setup_routes_routing,
)
from eidaws.stationlite.server.stationlite.route import (
    setup_routes as setup_routes_stationlite,
)


def setup_routes(app):
    setup_routes_routing(app)
    setup_routes_stationlite(app)

    return app
