# -*- coding: utf-8 -*-

import aioredis
import collections
import logging
import logging.config
import logging.handlers  # needed for handlers defined in logging.conf
import warnings
import yaml

from aiohttp import web, TCPConnector
from collections import ChainMap
from jsonschema import validate, ValidationError

from eidaws.federator.settings import (
    FED_BASE_ID,
    FED_CONTENT_TYPE_VERSION,
    FED_CONTENT_TYPE_WADL,
)
from eidaws.federator.utils.cache import Cache
from eidaws.federator.utils.stats import ResponseCodeStats
from eidaws.utils.error import ErrorWithTraceback


class RedisError(ErrorWithTraceback):
    """Base Redis error ({})"""


Route = collections.namedtuple("Route", ["url", "stream_epochs"])


def _callable_or_raise(obj):
    """
    Makes sure an object is callable if it is not ``None``. If not
    callable, a ``ValueError`` is raised.
    """
    if obj and not callable(obj):
        raise ValueError(f"{obj!r} is not callable.")
    else:
        return obj


def get_config(service_id, path_config=None, defaults={}, json_schema=None):

    user_config = {FED_BASE_ID: {}}

    if path_config is None:
        assert defaults, "Using empty default configuration not supported."

        return {"config": {service_id: defaults}}

    try:
        with open(path_config) as ifd:
            _user_config = yaml.safe_load(ifd)

        if isinstance(
            _user_config.get(FED_BASE_ID),
            (collections.abc.Mapping, collections.abc.MutableMapping),
        ):
            user_config = _user_config

    except yaml.parser.ParserError as err:
        warnings.warn(f"Exception while parsing configuration file: {err}")
    except FileNotFoundError as err:
        warnings.warn(f"Configuration file not found ({err}). Using defaults.")

    user_config[FED_BASE_ID].setdefault(service_id, {})
    user_config[FED_BASE_ID].setdefault("common", {})

    config_dict = {
        "config": {
            service_id: ChainMap(
                user_config[FED_BASE_ID][service_id],
                user_config[FED_BASE_ID]["common"],
                defaults,
            )
        }
    }

    if json_schema is not None:
        try:
            validate(
                instance=dict(config_dict["config"][service_id]),
                schema=json_schema,
            )
        except ValidationError as err:
            # avoid circular imports
            from eidaws.federator.utils.app import ConfigurationError

            raise ConfigurationError(str(err))

    return config_dict


def setup_logger(service_id, path_logging_conf=None, capture_warnings=False):
    """
    Initialize the logger of the application.
    """
    logging.basicConfig(level=logging.WARNING)

    LOGGER = FED_BASE_ID + "." + service_id

    if path_logging_conf is not None:
        try:
            logging.config.fileConfig(path_logging_conf)
            logger = logging.getLogger(LOGGER)
            logger.info(
                "Using logging configuration read from "
                f"{path_logging_conf!r}."
            )
        except Exception as err:
            print(
                f"WARNING: Setup logging failed for {path_logging_conf!r} "
                f"with error: {err!r}."
            )
    else:
        logger = logging.getLogger(LOGGER)
        logger.addHandler(logging.NullHandler())

    logging.captureWarnings(bool(capture_warnings))

    return logger


async def setup_redis(service_id, app):
    try:
        pool = await aioredis.create_redis_pool(
            app["config"][service_id]["redis_url"],
            minsize=app["config"][service_id]["redis_pool_minsize"],
            maxsize=app["config"][service_id]["redis_pool_maxsize"],
            timeout=app["config"][service_id]["redis_pool_timeout"],
        )
    except OSError as err:
        raise RedisError(err)

    async def close_redis(app):
        pool.close()
        await pool.wait_closed()

    app.on_cleanup.append(close_redis)
    app["redis_connection_pool"] = pool
    return pool


def setup_endpoint_http_conn_pool(service_id, app):

    conn = TCPConnector(
        limit=app["config"][service_id]["endpoint_connection_limit"],
        limit_per_host=app["config"][service_id][
            "endpoint_connection_limit_per_host"
        ],
    )

    async def close_endpoint_http_conn_pool(app):
        await conn.close()

    app.on_cleanup.append(close_endpoint_http_conn_pool)
    # TODO(damb): Verify if prefix should be added; in particular with several
    # applications.
    app["endpoint_http_conn_pool"] = conn
    return conn


def setup_routing_http_conn_pool(service_id, app):

    conn = TCPConnector(
        limit=app["config"][service_id]["routing_connection_limit"]
    )

    async def close_routing_http_conn_pool(app):
        await conn.close()

    app.on_cleanup.append(close_routing_http_conn_pool)
    app["routing_http_conn_pool"] = conn
    return conn


async def setup_response_code_stats(service_id, app):

    stats = ResponseCodeStats(
        app["redis_connection_pool"],
        ttl=app["config"][service_id]["client_retry_budget_ttl"],
        window_size=app["config"][service_id][
            "client_retry_budget_window_size"
        ],
    )

    app["response_code_statistics"] = stats
    return stats


async def setup_cache(service_id, app):

    cache_config = app["config"][service_id]["cache_config"]

    if cache_config is None:
        app["cache"] = None
        return

    cache = await Cache.create(cache_config)
    app["cache"] = cache
    return cache


async def _on_prepare_static(request, response):
    if request.path.endswith("version"):
        response.headers["Content-Type"] = FED_CONTENT_TYPE_VERSION
    elif request.path.endswith("application.wadl"):
        response.headers["Content-Type"] = FED_CONTENT_TYPE_WADL


def append_static_routes(app, routes, path, path_static):
    app.on_response_prepare.append(_on_prepare_static)
    routes.append(web.static(path, path_static))


# ----------------------------------------------------------------------------
class HelperGETRequest:
    method = "GET"


class HelperPOSTRequest:
    method = "POST"


# ----------------------------------------------------------------------------
def make_context_logger(logger, request):
    return ContextLoggerAdapter(
        logger, {"ctx": request[FED_BASE_ID + ".request_id"]}
    )


class ContextLoggerAdapter(logging.LoggerAdapter):
    """
    Adapter expecting the passed in dict-like object to have a 'ctx' key, whose
    value in brackets is prepended to the log message.
    """

    def process(self, msg, kwargs):
        return f"[{self.extra['ctx']}] {msg}", kwargs
