# -*- coding: utf-8 -*-

import aiohttp_cors
import functools

from aiohttp import web

from eidaws.federator.fdsnws_station_text.route import setup_routes
from eidaws.federator.settings import FED_STATION_TEXT_SERVICE_ID
from eidaws.federator.utils.middleware import (
    before_request,
    exception_handling_middleware,
)
from eidaws.federator.utils.misc import (
    setup_endpoint_http_conn_pool,
    setup_routing_http_conn_pool,
    setup_redis,
    setup_response_code_stats,
    setup_cache,
)
from eidaws.federator.utils.parser import setup_parser_error_handler
from eidaws.federator.utils.strict import setup_keywordparser_error_handler
from eidaws.federator.version import __version__


SERVICE_ID = FED_STATION_TEXT_SERVICE_ID


def create_app(config_dict, **kwargs):

    app = web.Application(
        # XXX(damb): The ordering of middlewares matters
        middlewares=[before_request, exception_handling_middleware],
        client_max_size=config_dict["config"][SERVICE_ID]["client_max_size"],
    )

    setup_routes(app)

    on_startup = [
        functools.partial(setup_redis, SERVICE_ID),
        functools.partial(setup_response_code_stats, SERVICE_ID),
        functools.partial(setup_cache, SERVICE_ID),
    ]
    for fn in on_startup:
        app.on_startup.append(fn)

    for k, v in config_dict.items():
        app[k] = v

    cors = aiohttp_cors.setup(
        app,
        defaults={
            "*": aiohttp_cors.ResourceOptions(
                allow_credentials=False,
                expose_headers="*",
                allow_headers="*",
                allow_methods=["POST", "GET"],
            )
        },
    )

    for route in list(app.router.routes()):
        if not isinstance(route.resource, web.StaticResource):  # workaround
            cors.add(route)

    setup_parser_error_handler(service_version=__version__)
    setup_keywordparser_error_handler(service_version=__version__)

    setup_endpoint_http_conn_pool(SERVICE_ID, app)
    setup_routing_http_conn_pool(SERVICE_ID, app)

    return app
