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
from eidaws.federator.settings import (
    FED_DEFAULT_HOSTNAME,
    FED_DEFAULT_PORT,
    FED_DEFAULT_UNIX_PATH,
    FED_DEFAULT_NUM_FORWARDED,
    FED_DEFAULT_SERVE_STATIC,
    FED_DEFAULT_URL_ROUTING,
    FED_DEFAULT_NETLOC_PROXY,
    FED_DEFAULT_ENDPOINT_REQUEST_METHOD,
    FED_DEFAULT_ENDPOINT_CONN_LIMIT,
    FED_DEFAULT_ENDPOINT_CONN_LIMIT_PER_HOST,
    FED_DEFAULT_ENDPOINT_TIMEOUT_CONNECT,
    FED_DEFAULT_ENDPOINT_TIMEOUT_SOCK_CONNECT,
    FED_DEFAULT_ENDPOINT_TIMEOUT_SOCK_READ,
    FED_DEFAULT_ROUTING_CONN_LIMIT,
    FED_DEFAULT_URL_REDIS,
    FED_DEFAULT_REDIS_POOL_MINSIZE,
    FED_DEFAULT_REDIS_POOL_MAXSIZE,
    FED_DEFAULT_REDIS_POOL_TIMEOUT,
    FED_DEFAULT_RETRY_BUDGET_CLIENT_THRES,
    FED_DEFAULT_RETRY_BUDGET_CLIENT_TTL,
    FED_DEFAULT_RETRY_BUDGET_WINDOW_SIZE,
    FED_DEFAULT_POOL_SIZE,
    FED_DEFAULT_CACHE_CONFIG,
    FED_DEFAULT_CLIENT_MAX_SIZE,
    FED_DEFAULT_MAX_STREAM_EPOCH_DURATION,
    FED_DEFAULT_MAX_STREAM_EPOCH_DURATION_TOTAL,
    FED_DEFAULT_STREAMING_TIMEOUT,
)
from eidaws.federator.utils.strict import setup_keywordparser_error_handler
from eidaws.federator.version import __version__


def config():
    """
    Return a default application configuration.
    """

    config = {}
    config.setdefault("hostname", FED_DEFAULT_HOSTNAME)
    config.setdefault("port", FED_DEFAULT_PORT)
    config.setdefault("unix_path", FED_DEFAULT_UNIX_PATH)
    config.setdefault("serve_static", FED_DEFAULT_SERVE_STATIC)
    config.setdefault("url_routing", FED_DEFAULT_URL_ROUTING)
    config.setdefault(
        "routing_connection_limit", FED_DEFAULT_ROUTING_CONN_LIMIT
    )
    config.setdefault(
        "endpoint_request_method", FED_DEFAULT_ENDPOINT_REQUEST_METHOD
    )
    config.setdefault(
        "endpoint_connection_limit", FED_DEFAULT_ENDPOINT_CONN_LIMIT
    )
    config.setdefault(
        "endpoint_connection_limit_per_host",
        FED_DEFAULT_ENDPOINT_CONN_LIMIT_PER_HOST,
    )
    config.setdefault(
        "endpoint_timeout_connect", FED_DEFAULT_ENDPOINT_TIMEOUT_CONNECT
    )
    config.setdefault(
        "endpoint_timeout_sock_connect",
        FED_DEFAULT_ENDPOINT_TIMEOUT_SOCK_CONNECT,
    )
    config.setdefault(
        "endpoint_timeout_sock_read", FED_DEFAULT_ENDPOINT_TIMEOUT_SOCK_READ
    )
    config.setdefault("redis_url", FED_DEFAULT_URL_REDIS)
    config.setdefault("redis_pool_minsize", FED_DEFAULT_REDIS_POOL_MINSIZE)
    config.setdefault("redis_pool_maxsize", FED_DEFAULT_REDIS_POOL_MAXSIZE)
    config.setdefault("redis_pool_timeout", FED_DEFAULT_REDIS_POOL_TIMEOUT)
    config.setdefault(
        "client_retry_budget_threshold", FED_DEFAULT_RETRY_BUDGET_CLIENT_THRES
    )
    config.setdefault(
        "client_retry_budget_ttl", FED_DEFAULT_RETRY_BUDGET_CLIENT_TTL
    )
    config.setdefault(
        "client_retry_budget_window_size", FED_DEFAULT_RETRY_BUDGET_WINDOW_SIZE
    )
    config.setdefault("pool_size", FED_DEFAULT_POOL_SIZE)
    config.setdefault("cache_config", FED_DEFAULT_CACHE_CONFIG)
    config.setdefault("client_max_size", FED_DEFAULT_CLIENT_MAX_SIZE)
    config.setdefault(
        "max_stream_epoch_duration", FED_DEFAULT_MAX_STREAM_EPOCH_DURATION
    )
    config.setdefault(
        "max_total_stream_epoch_duration",
        FED_DEFAULT_MAX_STREAM_EPOCH_DURATION_TOTAL,
    )
    config.setdefault("streaming_timeout", FED_DEFAULT_STREAMING_TIMEOUT)
    config.setdefault("proxy_netloc", FED_DEFAULT_NETLOC_PROXY)
    config.setdefault("num_forwarded", FED_DEFAULT_NUM_FORWARDED)

    return config


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
