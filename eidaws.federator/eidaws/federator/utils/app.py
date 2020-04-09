# -*- coding: utf-8 -*-

import aiohttp_cors
import argparse
import functools
import sys

from aiohttp import web

from eidaws.federator.utils.middleware import (
    before_request,
    exception_handling_middleware,
)
from eidaws.federator.utils.misc import (
    get_config,
    setup_endpoint_http_conn_pool,
    setup_routing_http_conn_pool,
    setup_redis,
    setup_response_code_stats,
    setup_cache,
    setup_logger,
)
from eidaws.federator.utils.parser import setup_parser_error_handler
from eidaws.federator.settings import (
    FED_DEFAULT_CONFIG_BASEDIR,
    FED_DEFAULT_CONFIG_FILE,
    FED_DEFAULT_HOSTNAME,
    FED_DEFAULT_PORT,
    FED_DEFAULT_UNIX_PATH,
    FED_DEFAULT_SERVE_STATIC,
    FED_DEFAULT_URL_ROUTING,
    FED_DEFAULT_NETLOC_PROXY,
    FED_DEFAULT_REQUEST_METHOD,
    FED_DEFAULT_ENDPOINT_CONN_LIMIT,
    FED_DEFAULT_ENDPOINT_CONN_LIMIT_PER_HOST,
    FED_DEFAULT_TIMEOUT_CONNECT,
    FED_DEFAULT_TIMEOUT_SOCK_CONNECT,
    FED_DEFAULT_TIMEOUT_SOCK_READ,
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
from eidaws.utils.error import Error
from eidaws.utils.misc import realpath, real_file_path


config_schema = {
    "type": "object",
    "properties": {
        "hostname": {"type": "string", "format": "ipv4"},
        "port": {"type": "integer", "minimum": 1, "maximum": 65535},
        "unix_path": {
            "oneOf": [
                {"type": "null"},
                {"type": "string", "format": "uri", "pattern": r"^unix:/"},
            ]
        },
        "serve_static": {"type": "boolean"},
        "logging_conf": {"type": "string", "pattern": r"^(\/|~)"},
        "url_routing": {
            "type": "string",
            "format": "uri",
            "pattern": "^https?://",
        },
        "routing_connection_limit": {"type": "integer", "minimum": 1},
        "endpoint_request_method": {
            "type": "string",
            "pattern": "^(GET|POST)$",
        },
        "endpoint_connection_limit": {"type": "integer", "minimum": 1},
        "endpoint_connection_limit_per_host": {
            "type": "integer",
            "minimum": 1,
        },
        "endpoint_timeout_connect": {
            "oneOf": [{"type": "null"}, {"type": "number", "minimum": 0}]
        },
        "endpoint_timeout_sock_connect": {
            "oneOf": [{"type": "null"}, {"type": "number", "minimum": 0}]
        },
        "endpoint_timeout_sock_read": {
            "oneOf": [{"type": "null"}, {"type": "number", "minimum": 0}]
        },
        "redis_url": {
            "type": "string",
            "format": "uri",
            "pattern": "^redis://",
        },
        "redis_pool_minsize": {"type": "integer", "minimum": 1},
        "redis_pool_maxsize": {"type": "integer", "minimum": 1},
        "redis_pool_timeout": {
            "oneOf": [{"type": "number", "minimum": 1}, {"type": "null"}]
        },
        "client_retry_budget_threshold": {
            "type": "number",
            "minimum": 0,
            "maximum": 100,
        },
        "client_retry_budget_ttl": {"type": "number", "minimum": 0},
        "client_retry_budget_window_size": {"type": "integer", "minimum": 1},
        "pool_size": {
            "oneOf": [{"type": "integer", "minimum": 1}, {"type": "null"}]
        },
        "cache_config": {
            "oneOf": [
                {"type": "null"},
                {
                    "type": "object",
                    "properties": {
                        "cache_type": {"type": "string", "pattern": "^null$"},
                    },
                },
                {
                    "type": "object",
                    "properties": {
                        "cache_type": {"type": "string", "pattern": "^redis$"},
                        "cache_kwargs": {
                            "type": "object",
                            "properties": {
                                "url": {
                                    "type": "string",
                                    "format": "uri",
                                    "pattern": "^redis://",
                                },
                                "default_timeout": {
                                    "type": "integer",
                                    "minimum": 0,
                                },
                                "compress": {"type": "boolean"},
                                "minsize": {"type": "integer", "minimum": 1},
                                "maxsize": {"type": "integer", "maximum": 1},
                            },
                        },
                    },
                },
            ]
        },
        "client_max_size": {"type": "integer", "minimum": 0},
        "max_stream_epoch_duration": {
            "oneOf": [{"type": "null"}, {"type": "integer", "minimum": 1}]
        },
        "max_total_stream_epoch_duration": {
            "oneOf": [{"type": "null"}, {"type": "integer", "minimum": 1}]
        },
        "streaming_timeout": {
            "oneOf": [{"type": "null"}, {"type": "integer", "minimum": 1}]
        },
        "proxy_netloc": {
            "oneOf": [
                {"type": "null"},
                {
                    "type": "string",
                    "format": "uri",
                    # allow IPv4(:PORT) or FQDN(:PORT)
                    "pattern": (
                        r"^(((?:[0-9]{1,3}\.){3}[0-9]{1,3})"
                        r"|(([a-zA-Z0-9]+(-[a-zA-Z0-9]+)*\.)+[a-zA-Z]{2,}))"
                        r"(?:\:([1-9][0-9]{0,3}|[1-5][0-9]{4}|6[0-4][0-9]{3}|"
                        r"65[0-4][0-9]{2}|655[0-2][0-9]|6553[0-5]))?$"
                    ),
                },
            ]
        },
    },
    "additionalProperties": False,
}


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
    config.setdefault("endpoint_request_method", FED_DEFAULT_REQUEST_METHOD)
    config.setdefault(
        "endpoint_connection_limit", FED_DEFAULT_ENDPOINT_CONN_LIMIT
    )
    config.setdefault(
        "endpoint_connection_limit_per_host",
        FED_DEFAULT_ENDPOINT_CONN_LIMIT_PER_HOST,
    )
    config.setdefault("endpoint_timeout_connect", FED_DEFAULT_TIMEOUT_CONNECT)
    config.setdefault(
        "endpoint_timeout_sock_connect", FED_DEFAULT_TIMEOUT_SOCK_CONNECT
    )
    config.setdefault(
        "endpoint_timeout_sock_read", FED_DEFAULT_TIMEOUT_SOCK_READ
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

    return config


def create_app(service_id, config_dict, setup_routes_callback=None, **kwargs):
    """
    Factory for application creation.
    """

    config = config_dict["config"][service_id]

    app = web.Application(
        # XXX(damb): The ordering of middlewares matters
        middlewares=[before_request, exception_handling_middleware],
        client_max_size=config["client_max_size"],
    )

    if setup_routes_callback is not None:
        setup_routes_callback(app, static=config["serve_static"])

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


def _main(
    service_id,
    app_factory,
    prog=None,
    argv=sys.argv[1:],
    default_config=config(),
    config_schema=config_schema,
):
    # does all the dirty work

    DEFAULT_PATH_CONFIG = (
        FED_DEFAULT_CONFIG_BASEDIR / "config" / FED_DEFAULT_CONFIG_FILE
    )
    prog = prog or service_id

    parser = argparse.ArgumentParser(
        prog=prog, description=f"Launch federating {prog} web service.",
    )

    parser.add_argument(
        "-c",
        "--config",
        type=real_file_path,
        metavar="PATH",
        default=DEFAULT_PATH_CONFIG,
    )

    args = parser.parse_args(args=argv)

    # load config
    config_dict = get_config(
        service_id,
        path_config=args.config,
        defaults=default_config,
        json_schema=config_schema,
    )

    config = config_dict["config"][service_id]

    # configure logging
    try:
        path_logging_conf = realpath(
            config_dict["config"][service_id]["logging_conf"]
        )
    except (KeyError, TypeError):
        path_logging_conf = None

    logger = setup_logger(service_id, path_logging_conf, capture_warnings=True)

    logger.info(f"{prog}: Version v{__version__}")
    logger.debug(f"Service configuration: {dict(config)}")

    app = app_factory(config_dict=config_dict)
    # run standalone app
    web.run_app(
        app,
        host=config["hostname"],
        port=config["port"],
        path=config["unix_path"],
    )
    parser.exit(message="Stopped\n")


class AppError(Error):
    """Base application error ({})."""


class ConfigurationError(AppError):
    """Configuration errro: {}"""
