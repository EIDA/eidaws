# -*- coding: utf-8 -*-

from eidaws.stationlite.server.stationlite.view import (
    StationLiteVersionResource,
    StationLiteQueryResource,
)
from eidaws.utils.settings import (
    EIDAWS_STATIONLITE_PATH,
    EIDAWS_STATIONLITE_PATH_QUERY,
)


def _create_name(name, suffix="stationlite"):
    return f"{name}-{suffix}"


def setup_routes(app):
    app.add_url_rule(
        "/".join([EIDAWS_STATIONLITE_PATH, "version"]),
        view_func=StationLiteVersionResource.as_view(_create_name("version")),
    )
    app.add_url_rule(
        EIDAWS_STATIONLITE_PATH_QUERY,
        view_func=StationLiteQueryResource.as_view(_create_name("query")),
    )

    return app
