# -*- coding: utf-8 -*-
import argparse

from eidaws.federator.version import __version__
from eidaws.federator.fdsnws_station_text import SERVICE_ID, create_app
from eidaws.federator.settings import (
    FED_DEFAULT_CONFIG_BASEDIR,
    FED_DEFAULT_CONFIG_FILE,
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
    FED_DEFAULT_POOL_SIZE,
    FED_DEFAULT_CACHE_CONFIG,
)
from eidaws.federator.utils.misc import get_config, setup_logger
from eidaws.utils.misc import realpath, real_file_path


PROG = "eida-federator-station-text"

DEFAULT_PATH_CONFIG = (
    FED_DEFAULT_CONFIG_BASEDIR / "config" / FED_DEFAULT_CONFIG_FILE
)


# XXX(damb):
# For development purposes start the service with
# $ python -m aiohttp.web -H localhost -P 5000 \
#       eidaws.federator.fdsnws_station_text.app:init_app
#
# NOTE(damb): aiohttp.web
# (https://github.com/aio-libs/aiohttp/blob/master/aiohttp/web.py) provides a
# simple CLI for testing purposes. Though, the parser cannot be used as a
# parent parser since it is wrapped into aiohttp.web:main. Hence, there is no
# way around duplicating the CLI provided.


def default_config():
    """
    Return the application's default configuration.
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
    default_config.setdefault("pool_size", FED_DEFAULT_POOL_SIZE)
    default_config.setdefault("cache_config", FED_DEFAULT_CACHE_CONFIG)
    default_config.setdefault("proxy_netloc", FED_DEFAULT_NETLOC_PROXY)

    return default_config


DEFAULT_CONFIG = default_config()


def init_app(argv):

    parser = argparse.ArgumentParser(
        prog=PROG, description=f"Launch federating {PROG} web service."
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
        SERVICE_ID, path_config=args.config, defaults=DEFAULT_CONFIG,
    )
    app = create_app(config_dict=config_dict)

    try:
        path_logging_conf = realpath(
            config_dict["config"][SERVICE_ID]["logging_conf"]
        )
    except (KeyError, TypeError):
        path_logging_conf = None

    logger = setup_logger(SERVICE_ID, path_logging_conf, capture_warnings=True)

    logger.info(f"{PROG}: Version v{__version__}")
    logger.debug(
        f"Service configuration: {dict(config_dict['config'][SERVICE_ID])}"
    )

    return app
