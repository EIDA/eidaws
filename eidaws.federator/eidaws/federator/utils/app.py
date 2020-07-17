# -*- coding: utf-8 -*-

import aiohttp_cors
import functools
import sys

from aiohttp import web

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
    setup_logger,
)
from eidaws.federator.utils.parser import setup_parser_error_handler
from eidaws.federator.utils.remote import XForwardedRelaxed
from eidaws.federator.utils.strict import setup_keywordparser_error_handler
from eidaws.federator.version import __version__


def create_app(service_id, config_dict, setup_routes_callback=None, **kwargs):
    """
    Factory for application creation.
    """

    def make_server_config(service_id, arg_dict):
        return {"config": {service_id: arg_dict}}

    server_config = make_server_config(service_id, config_dict)
    if config_dict["unix_path"] is not None:
        # ignore hostname:port
        config_dict["hostname"] = config_dict["port"] = None

    app = web.Application(
        # XXX(damb): The ordering of middlewares matters
        middlewares=[
            before_request,
            exception_handling_middleware,
            XForwardedRelaxed(num=config_dict["num_forwarded"]).middleware,
        ],
        client_max_size=config_dict["client_max_size"],
    )

    if setup_routes_callback is not None:
        setup_routes_callback(app, static=config_dict["serve_static"])

    on_startup = [
        functools.partial(setup_redis, service_id),
        functools.partial(setup_response_code_stats, service_id),
        functools.partial(setup_cache, service_id),
    ]
    for fn in on_startup:
        app.on_startup.append(fn)

    for k, v in server_config.items():
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

    setup_endpoint_http_conn_pool(service_id, app)
    setup_routing_http_conn_pool(service_id, app)

    return app


def _main(
    service_id, app_factory, parser, argv=sys.argv[1:],
):
    args = parser.parse_args(args=argv)
    args = vars(args)

    # configure logging
    logger = setup_logger(
        service_id, args["path_logging_conf"], capture_warnings=True
    )

    logger.info(f"Version v{__version__}")
    logger.debug(f"Service configuration: {args}")

    app = app_factory(config_dict=args)
    # run standalone app
    web.run_app(
        app, host=args["hostname"], port=args["port"], path=args["unix_path"],
    )
    parser.exit(message="Stopped\n")
