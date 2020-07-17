# -*- coding: utf-8 -*-

import argparse
import functools
import json
import os
import re

import jsonschema
import yaml

from copy import deepcopy
from pathlib import Path
from types import SimpleNamespace
from urllib.parse import urlparse, urlunparse

from jsonschema import Draft7Validator as jsonvalidator

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
    make_config_file_paths,
)
from eidaws.federator.version import __version__
from eidaws.utils.cli import CustomParser, InterpolatingYAMLConfigFileParser
from eidaws.utils.misc import real_file_path


meta_keys = {"__cwd__", "__path__"}
config_read_mode = "fr"


def between(num, num_type=int, minimum=None, maximum=None):
    try:
        num = num_type(num)
        if minimum is not None and num < minimum:
            raise ValueError
        if maximum is not None and num > maximum:
            raise ValueError
    except ValueError:
        raise argparse.ArgumentError(f"Invalid {num_type.__name__}: {num}")

    return num


def positive_num_or_none(num, num_type=int):
    if num is None:
        return None
    return between(num, num_type, minimum=0)


positive_int = functools.partial(between, num_type=int, minimum=0)
positive_int_exclusive = functools.partial(between, num_type=int, minimum=1)
positive_float = functools.partial(between, num_type=float, minimum=0)
positive_int_or_none = functools.partial(positive_num_or_none, num_type=int)
positive_float_or_none = functools.partial(
    positive_num_or_none, num_type=float
)
percent = functools.partial(between, num_type=float, minimum=0, maximum=100)
port = functools.partial(between, num_type=int, minimum=1, maximum=65535)


def abs_path(path):
    if not os.path.isabs(path):
        raise argparse.ArgumentError(f"Not an absolute path: {path!r}")
    return path


def build_parser(
    service_id,
    prog=None,
    parents=[],
    config_file_parser_class=InterpolatingYAMLConfigFileParser,
):
    def url_routing(url):
        parsed = urlparse(url, scheme="http",)
        if (
            not (all([parsed.scheme, parsed.netloc, parsed.path]))
            or parsed.path != "/eidaws/routing/1/query"
        ):
            raise argparse.ArgumentError(f"Invalid URL: {url!r}")

        return urlunparse(parsed)

    def url_redis(url):
        parsed = urlparse(url, scheme="redis",)
        if (
            not all([parsed.scheme, parsed.netloc, parsed.port])
            and parsed.scheme != "redis"
        ):
            raise argparse.ArgumentError(f"Invalid URL: {url!r}")

        return urlunparse(parsed)

    def proxy_netloc(netloc):
        if netloc is None:
            return None

        # allow IPv4(:PORT) or Hostname(:Port)
        pattern = (
            r"^(((?:[0-9]{1,3}\.){3}[0-9]{1,3})"
            r"|(([a-zA-Z0-9]|[a-zA-Z0-9][a-zA-Z0-9\-]*"
            r"[a-zA-Z0-9])\.)*"
            r"([A-Za-z0-9]|[A-Za-z0-9][A-Za-z0-9\-]*[A-Za-z0-9]))"
            r"(?:\:([1-9][0-9]{0,3}|[1-5][0-9]{4}|6[0-4][0-9]{3}|"
            r"65[0-4][0-9]{2}|655[0-2][0-9]|6553[0-5]))?$"
        )

        if re.match(pattern, netloc) is None:
            raise argparse.ArgumentError(f"Invalid network location: {netloc}")

        return netloc

    cache_config_schema = {
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
                        "required": ["url"],
                        "additionalProperties": False,
                    },
                },
                "required": ["cache_kwargs"],
                "additionalProperties": False,
            },
        ]
    }

    parser = CustomParser(
        prog=prog,
        description=f"Launch federating {prog} web service.",
        parents=parents,
        default_config_files=make_config_file_paths(service_id),
        config_file_parser_class=config_file_parser_class,
        args_for_setting_config_path=["-c", "--config"],
    )
    # optional arguments
    parser.add_argument(
        "-V",
        "--version",
        action="version",
        version="%(prog)s version " + __version__,
    )
    parser.add_argument(
        "-H",
        "--hostname",
        default=FED_DEFAULT_HOSTNAME,
        dest="hostname",
        help="TCP/IP hostname to serve on (default: %(default)s).",
    )
    parser.add_argument(
        "-P",
        "--port",
        type=port,
        dest="port",
        default=FED_DEFAULT_PORT,
        metavar="PORT",
        help="TCP/IP port to serve on (default: %(default)s).",
    )
    parser.add_argument(
        "-U",
        "--unix-path",
        dest="unix_path",
        metavar="PATH",
        default=FED_DEFAULT_UNIX_PATH,
        help="Unix file system path to serve on. Specifying a path will cause "
        "hostname and port arguments to be ignored.",
    )
    parser.add_argument(
        "-w",
        "--worker-pool-size",
        dest="pool_size",
        metavar="NUM",
        type=positive_int_or_none,
        default=FED_DEFAULT_POOL_SIZE,
        help="Number of task workers created in order to process a request. "
        "By default the number of task workers is determined based on the "
        "'--endpoint-connection-limit' configuration parameter.",
    )
    parser.add_argument(
        "--proxy-netloc",
        dest="proxy_netloc",
        type=proxy_netloc,
        metavar="IP[:PORT] or HOSTNAME[:PORT]",
        default=FED_DEFAULT_NETLOC_PROXY,
        help="Proxy network location in case the service is used coupled with "
        "e.g. an HTTP caching proxy.",
    )
    parser.add_argument(
        "--serve-static",
        dest="serve_static",
        action="store_true",
        default=FED_DEFAULT_SERVE_STATIC,
        help="Serve static content (i.e. version and application.wadl). For "
        "development purposes, only. In general, it should be preferred to "
        "serve static files by means of a reverse proxy. (default: "
        "%(default)s).",
    )
    parser.add_argument(
        "--streaming-timeout",
        dest="streaming_timeout",
        type=positive_int_or_none,
        metavar="SEC",
        default=FED_DEFAULT_STREAMING_TIMEOUT,
        help="Streaming timeout in seconds before the first endpoint request "
        "must return with data. If the timeout passed without returing any "
        "data a HTTP 413 (Request too large) response is returned (default: "
        "%(default)s).",
    )
    parser.add_argument(
        "--client-maxsize",
        dest="client_max_size",
        metavar="BYTES",
        type=positive_int_exclusive,
        default=FED_DEFAULT_CLIENT_MAX_SIZE,
        help="Maximum HTTP body size in bytes of POST requests. If the limit "
        "is exceeded a HTTP 413 (Request too large) response is returned ("
        "default: %(default)s bytes).",
    )
    parser.add_argument(
        "--max-stream-epoch-duration",
        dest="max_stream_epoch_duration",
        metavar="DAYS",
        type=positive_int_or_none,
        default=FED_DEFAULT_MAX_STREAM_EPOCH_DURATION,
        help="Maximum per stream epoch duration in days before returning a "
        "HTTP 413 (Request too large) response (default: %(default)s).",
    )
    parser.add_argument(
        "--max-stream-epoch-duration-total",
        dest="max_total_stream_epoch_duration",
        metavar="DAYS",
        type=positive_int_or_none,
        default=FED_DEFAULT_MAX_STREAM_EPOCH_DURATION_TOTAL,
        help="Maximum total stream epoch duration in days before returning a "
        "HTTP 413 (Request too large) response (default: %(default)s).",
    )
    parser.add_argument(
        "--logging-conf",
        dest="path_logging_conf",
        metavar="PATH",
        type=real_file_path,
        help="Path to logging configuration file. The file's syntax must "
        "follow the format specified under https://docs.python.org/3/library/"
        "logging.config.html#logging-config-fileformat.",
    )
    parser.add_argument(
        "-R",
        "--routing-url",
        dest="url_routing",
        metavar="URL",
        type=url_routing,
        default=FED_DEFAULT_URL_ROUTING,
        help="eidaws-stationlite routing URL (default: %(default)s).",
    )
    parser.add_argument(
        "--routing-connection-limit",
        dest="routing_connection_limit",
        type=positive_int_exclusive,
        metavar="NUM",
        default=FED_DEFAULT_ROUTING_CONN_LIMIT,
        help="Maximum number of concurrent HTTP connections to "
        "eidaws-stationlite (default: %(default)s).",
    )
    parser.add_argument(
        "--forwarded",
        dest="num_forwarded",
        type=positive_int,
        metavar="NUM",
        default=FED_DEFAULT_NUM_FORWARDED,
        help="Take the X-Forwarded-For HTTP header field into account "
        "and set the header field accordingly when performing endpoint "
        "requests. This is particularly useful if the service is deployed "
        "behind a reverse proxy and tunneling the remote IP address is "
        "desired. By default forwarding is disabled.",
    )
    parser.add_argument(
        "--endpoint-request-method",
        dest="endpoint_request_method",
        type=str,
        metavar="HTTP_METHOD",
        choices=["GET", "POST"],
        default=FED_DEFAULT_ENDPOINT_REQUEST_METHOD,
        help="HTTP method used when performing endpoint requests (choices: "
        "%(choices)s, default: %(default)s).",
    )
    parser.add_argument(
        "--endpoint-connection-limit",
        type=positive_int_exclusive,
        dest="endpoint_connection_limit",
        metavar="NUM",
        default=FED_DEFAULT_ENDPOINT_CONN_LIMIT,
        help="Overall maximum number of concurrent HTTP connections with "
        "to the federated EIDA endpoint resource (default: %(default)s).",
    )
    parser.add_argument(
        "--endpoint-connection-limit-per-host",
        dest="endpoint_connection_limit_per_host",
        type=positive_int_exclusive,
        metavar="NUM",
        default=FED_DEFAULT_ENDPOINT_CONN_LIMIT_PER_HOST,
        help="Maximum number of concurrent HTTP connections per host with "
        "respect to the federated EIDA endpoint resource (default: "
        "%(default)s).",
    )
    parser.add_argument(
        "--endpoint-timeout-connect",
        dest="endpoint_timeout_connect",
        type=positive_float_or_none,
        metavar="SEC",
        default=FED_DEFAULT_ENDPOINT_TIMEOUT_CONNECT,
        help="Total timeout in seconds for acquiring a connection from the "
        "HTTP connection pool. The time assembles connection establishment "
        "for a new connection or waiting for a free connection from a pool "
        "if pool connection limits are exceeded (default: %(default)s).",
    )
    parser.add_argument(
        "--endpoint-timeout-socket-connect",
        dest="endpoint_timeout_sock_connect",
        type=positive_float_or_none,
        metavar="SEC",
        default=FED_DEFAULT_ENDPOINT_TIMEOUT_SOCK_CONNECT,
        help="Timeout in seconds for connecting to a peer for a new "
        "connection (default: %(default)s).",
    )
    parser.add_argument(
        "--endpoint-timeout-socket-read",
        dest="endpoint_timeout_sock_read",
        type=positive_float_or_none,
        metavar="SEC",
        default=FED_DEFAULT_ENDPOINT_TIMEOUT_SOCK_READ,
        help="Timeout in seconds for reading a portion of data from a peer "
        "(default: %(default)s).",
    )
    parser.add_argument(
        "--redis-url",
        dest="redis_url",
        metavar="URL",
        default=FED_DEFAULT_URL_REDIS,
        help="URL to Redis server (default %(default)s).",
    )
    parser.add_argument(
        "--redis-pool-minsize",
        dest="redis_pool_minsize",
        type=positive_int_exclusive,
        metavar="NUM",
        default=FED_DEFAULT_REDIS_POOL_MINSIZE,
        help="Minimum size of the Redis connection pool (default: "
        "%(default)s).",
    )
    parser.add_argument(
        "--redis-pool-maxsize",
        dest="redis_pool_maxsize",
        type=positive_int_exclusive,
        metavar="NUM",
        default=FED_DEFAULT_REDIS_POOL_MAXSIZE,
        help="Maximum size of the Redis connection pool (default: "
        "%(default)s).",
    )
    parser.add_argument(
        "--redis-pool-timeout",
        dest="redis_pool_timeout",
        type=positive_int_or_none,
        metavar="SEC",
        default=FED_DEFAULT_REDIS_POOL_TIMEOUT,
        help="Timeout for Redis connections (default: %(default)s).",
    )
    parser.add_argument(
        "--retry-budget-threshold",
        dest="client_retry_budget_threshold",
        type=percent,
        metavar="PERCENT",
        default=FED_DEFAULT_RETRY_BUDGET_CLIENT_THRES,
        help="Threshold in percent before endpoint requests are dropped ("
        "default: %(default)s).",
    )
    parser.add_argument(
        "--retry-budget-ttl",
        dest="client_retry_budget_ttl",
        type=positive_int,
        metavar="SEC",
        default=FED_DEFAULT_RETRY_BUDGET_CLIENT_TTL,
        help="TTL in seconds for response codes when performing statistics. "
        "If 0, the TTL is disabled (default: %(default)s).",
    )
    parser.add_argument(
        "--retry-budget-window-size",
        dest="client_retry_budget_window_size",
        type=positive_int,
        metavar="NUM",
        default=FED_DEFAULT_RETRY_BUDGET_WINDOW_SIZE,
        help="Rolling window size with respect to response code time series "
        "(default: %(default)s).",
    )
    parser.add_argument(
        "-C",
        "--cache-config",
        action=ActionJsonSchema(schema=cache_config_schema),
        default=FED_DEFAULT_CACHE_CONFIG,
        help="Cache configuration. Cache keys are computed based on request "
        "parameters (including stream epochs). The cache can be configured "
        "with different caching backends: cache_type='null' (NullCache, "
        "enables response buffering), cache_type: 'redis' (Redis backend). "
        "By default both response buffering and caching is disabled. The "
        "configuration must must obey the following schema: %s",
    )

    return parser


# XXX(damb): Taken from https://github.com/omni-us/jsonargparse
class ActionJsonSchema(argparse.Action):
    """Action to parse option as json validated by a jsonschema."""

    def __init__(self, **kwargs):
        """Initializer for ActionJsonSchema instance.

        Args:
            schema (str or object): Schema to validate values against.
            with_meta (bool): Whether to include metadata (def.=True).

        Raises:
            ValueError: If a parameter is invalid.
            jsonschema.exceptions.SchemaError: If the schema is invalid.
        """
        if "schema" in kwargs:
            _check_unknown_kwargs(kwargs, {"schema", "with_meta"})
            schema = kwargs["schema"]
            if isinstance(schema, str):
                try:
                    schema = yaml.safe_load(schema)
                except Exception as ex:
                    raise type(ex)("Problems parsing schema :: " + str(ex))
            jsonvalidator.check_schema(schema)
            self._validator = self._extend_jsonvalidator_with_default(
                jsonvalidator
            )(schema)
            self._with_meta = kwargs.get("with_meta", True)
        elif "_validator" not in kwargs:
            raise ValueError("Expected schema keyword argument.")
        else:
            self._validator = kwargs.pop("_validator")
            self._with_meta = kwargs.pop("_with_meta")
            kwargs["type"] = str
            super().__init__(**kwargs)

    def __call__(self, *args, **kwargs):
        """Parses an argument validating against the corresponding jsonschema.

        Raises:
            TypeError: If the argument is not valid.
        """
        if len(args) == 0:
            kwargs["_validator"] = self._validator
            kwargs["_with_meta"] = self._with_meta
            if "help" in kwargs and "%s" in kwargs["help"]:
                kwargs["help"] = kwargs["help"] % json.dumps(
                    self._validator.schema, indent=2, sort_keys=True
                )
            return ActionJsonSchema(**kwargs)
        val = self._check_type(args[2])
        if not self._with_meta:
            val = strip_meta(val)
        setattr(args[1], self.dest, val)

    def _check_type(self, value, cfg=None):
        islist = _is_action_value_list(self)
        if not islist:
            value = [value]
        elif not isinstance(value, list):
            raise TypeError(
                "For ActionJsonSchema with "
                f"nargs={self.nargs!r}expected value to be list, received: "
                f"value={value!r}."
            )
        for num, val in enumerate(value):
            try:
                fpath = None
                if isinstance(val, str):
                    val = yaml.safe_load(val)
                if isinstance(val, str):
                    try:
                        fpath = Path(val, mode=config_read_mode)
                    except Exception:
                        pass
                    else:
                        val = yaml.safe_load(fpath.get_content())
                if isinstance(val, SimpleNamespace):
                    val = namespace_to_dict(val)
                path_meta = (
                    val.pop("__path__")
                    if isinstance(val, dict) and "__path__" in val
                    else None
                )
                self._validator.validate(val)
                if path_meta is not None:
                    val["__path__"] = path_meta
                if isinstance(val, dict) and fpath is not None:
                    val["__path__"] = fpath
                value[num] = val
            except (
                TypeError,
                yaml.parser.ParserError,
                jsonschema.exceptions.ValidationError,
            ) as ex:
                elem = "" if not islist else " element " + str(num + 1)
                raise TypeError(
                    'Parser key "' + self.dest + '"' + elem + ": " + str(ex)
                )
        return value if islist else value[0]

    @staticmethod
    def _extend_jsonvalidator_with_default(validator_class):
        """Extends a json schema validator so that it fills in default values."""
        validate_properties = validator_class.VALIDATORS["properties"]

        def set_defaults(validator, properties, instance, schema):
            for property, subschema in properties.items():
                if "default" in subschema:
                    instance.setdefault(property, subschema["default"])

            for error in validate_properties(
                validator, properties, instance, schema
            ):
                yield error

        return jsonschema.validators.extend(
            validator_class, {"properties": set_defaults}
        )


# def _check_unknown_kwargs(kwargs:Dict[str, Any], keys:Set[str]):
def _check_unknown_kwargs(kwargs, keys):
    """Checks whether a kwargs dict has unexpected keys.
    Args:
        kwargs (dict): The keyword arguments dict to check.
        keys (set): The expected keys.
    Raises:
        ValueError: If an unexpected keyword argument is found.
    """
    if len(set(kwargs.keys()) - keys) > 0:
        raise ValueError(
            "Unexpected keyword arguments: "
            + ", ".join(set(kwargs.keys()) - keys)
            + "."
        )


# def _is_action_value_list(action:Action):
def _is_action_value_list(action):
    """Checks whether an action produces a list value.
    Args:
        action: An argparse action to check.
    Returns:
        bool: True if produces list otherwise False.
    """
    return action.nargs in {"*", "+"} or isinstance(action.nargs, int)


def strip_meta(cfg):
    """Removes all metadata keys from a configuration object.
    Args:
        cfg: The configuration object to strip.
    Returns:
        types.SimpleNamespace: The stripped configuration object.
    """
    cfg = deepcopy(cfg)
    if not isinstance(cfg, dict):
        cfg = namespace_to_dict(cfg)

    def strip_keys(cfg, base=None):
        del_keys = []
        for key, val in cfg.items():
            kbase = key if base is None else base + "." + key
            if isinstance(val, dict):
                strip_keys(val, kbase)
            elif key in meta_keys:
                del_keys.append(key)
        for key in del_keys:
            del cfg[key]

    strip_keys(cfg)
    return cfg


# def namespace_to_dict(cfg_ns:SimpleNamespace) -> Dict[str, Any]:
def namespace_to_dict(cfg_ns):
    """Converts a nested namespace into a nested dictionary.
    Args:
        cfg_ns: The configuration to process.
    Returns:
        dict: The nested configuration dictionary.
    """
    cfg_ns = deepcopy(cfg_ns)

    def expand_namespace(cfg):
        cfg = dict(vars(cfg))
        for k, v in cfg.items():
            if isinstance(v, SimpleNamespace):
                cfg[k] = expand_namespace(v)
            elif isinstance(v, list):
                for nn, vv in enumerate(v):
                    if isinstance(vv, SimpleNamespace):
                        cfg[k][nn] = expand_namespace(vv)
        return cfg

    return expand_namespace(cfg_ns)
