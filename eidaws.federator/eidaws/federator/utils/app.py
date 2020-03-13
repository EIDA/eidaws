# -*- coding: utf-8 -*-

import aiohttp_cors
import functools

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
)
from eidaws.federator.utils.parser import setup_parser_error_handler
from eidaws.federator.settings import (
    FED_DEFAULT_URL_ROUTING,
    FED_DEFAULT_NETLOC_PROXY,
    FED_DEFAULT_ENDPOINT_CONN_LIMIT,
    FED_DEFAULT_ENDPOINT_CONN_LIMIT_PER_HOST,
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
)
from eidaws.federator.utils.strict import setup_keywordparser_error_handler
from eidaws.federator.version import __version__


def default_config():
    """
    Return a default application configuration.
    """

    default_config = {}
    default_config.setdefault("url_routing", FED_DEFAULT_URL_ROUTING)
    default_config.setdefault(
        "routing_connection_limit", FED_DEFAULT_ROUTING_CONN_LIMIT
    )
    default_config.setdefault(
        "endpoint_connection_limit", FED_DEFAULT_ENDPOINT_CONN_LIMIT
    )
    default_config.setdefault(
        "endpoint_connection_limit_per_host",
        FED_DEFAULT_ENDPOINT_CONN_LIMIT_PER_HOST,
    )
    default_config.setdefault("redis_url", FED_DEFAULT_URL_REDIS)
    default_config.setdefault(
        "redis_pool_minsize", FED_DEFAULT_REDIS_POOL_MINSIZE
    )
    default_config.setdefault(
        "redis_pool_maxsize", FED_DEFAULT_REDIS_POOL_MAXSIZE
    )
    default_config.setdefault(
        "redis_pool_timeout", FED_DEFAULT_REDIS_POOL_TIMEOUT
    )
    default_config.setdefault(
        "client_retry_budget_threshold", FED_DEFAULT_RETRY_BUDGET_CLIENT_THRES
    )
    default_config.setdefault(
        "client_retry_budget_ttl", FED_DEFAULT_RETRY_BUDGET_CLIENT_TTL
    )
    default_config.setdefault(
        "client_retry_budget_window_size", FED_DEFAULT_RETRY_BUDGET_WINDOW_SIZE
    )
    default_config.setdefault("pool_size", FED_DEFAULT_POOL_SIZE)
    default_config.setdefault("cache_config", FED_DEFAULT_CACHE_CONFIG)
    default_config.setdefault("client_max_size", FED_DEFAULT_CLIENT_MAX_SIZE)
    default_config.setdefault(
        "max_stream_epoch_duration", FED_DEFAULT_MAX_STREAM_EPOCH_DURATION
    )
    default_config.setdefault(
        "max_total_stream_epoch_duration",
        FED_DEFAULT_MAX_STREAM_EPOCH_DURATION_TOTAL,
    )
    # default_config.setdefault("proxy_netloc", FED_DEFAULT_NETLOC_PROXY)

    return default_config


def create_app(service_id, config_dict, setup_routes_callback=None, **kwargs):
    """
    Factory for application creation.
    """

    app = web.Application(
        # XXX(damb): The ordering of middlewares matters
        middlewares=[before_request, exception_handling_middleware],
        client_max_size=config_dict["config"][service_id]["client_max_size"],
    )

    if setup_routes_callback is not None:
        setup_routes_callback(app)

    on_startup = [
        functools.partial(setup_redis, service_id),
        functools.partial(setup_response_code_stats, service_id),
        functools.partial(setup_cache, service_id),
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

    setup_endpoint_http_conn_pool(service_id, app)
    setup_routing_http_conn_pool(service_id, app)

    return app
