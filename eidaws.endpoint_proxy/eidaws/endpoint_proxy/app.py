# -*- coding: utf-8 -*-

import argparse
import sys

from aiohttp import web

from eidaws.endpoint_proxy import create_app
from eidaws.endpoint_proxy.utils import get_config, setup_logger
from eidaws.endpoint_proxy.settings import (
    PROXY_BASE_ID,
    PROXY_DEFAULT_CONFIG_BASEDIR,
    PROXY_DEFAULT_CONFIG_FILE,
    PROXY_DEFAULT_HOSTNAME,
    PROXY_DEFAULT_PORT,
    PROXY_DEFAULT_UNIX_PATH,
    PROXY_DEFAULT_CONN_LIMIT,
    PROXY_DEFAULT_TIMEOUT_CONNECT,
    PROXY_DEFAULT_TIMEOUT_SOCK_CONNECT,
    PROXY_DEFAULT_TIMEOUT_SOCK_READ,
)
from eidaws.endpoint_proxy.version import __version__
from eidaws.utils.app import prepare_cli_config
from eidaws.utils.misc import realpath, real_file_path


PROG = "eida-endpoint-proxy"

config_schema = {
    "type": "object",
    "properties": {
        "hostname": {"type": "string", "format": "ipv4"},
        "port": {"type": "integer", "minimum": 1, "maximum": 65535},
        "unix_path": {
            "oneOf": [{"type": "null"}, {"type": "string", "pattern": r"^/"}]
        },
        "logging_conf": {"type": "string", "pattern": r"^(\/|~)"},
        "endpoint_connection_limit": {"type": "integer", "minimum": 1},
        "endpoint_timeout_connect": {
            "oneOf": [{"type": "null"}, {"type": "number", "minimum": 0}]
        },
        "endpoint_timeout_sock_connect": {
            "oneOf": [{"type": "null"}, {"type": "number", "minimum": 0}]
        },
        "endpoint_timeout_sock_read": {
            "oneOf": [{"type": "null"}, {"type": "number", "minimum": 0}]
        },
    },
}


def config():
    """
    Return a default application configuration.
    """
    config = {}
    config.setdefault("hostname", PROXY_DEFAULT_HOSTNAME)
    config.setdefault("port", PROXY_DEFAULT_PORT)
    config.setdefault("unix_path", PROXY_DEFAULT_UNIX_PATH)
    config.setdefault("endpoint_connection_limit", PROXY_DEFAULT_CONN_LIMIT)
    config.setdefault(
        "endpoint_timeout_connect", PROXY_DEFAULT_TIMEOUT_CONNECT
    )
    config.setdefault(
        "endpoint_timeout_sock_connect", PROXY_DEFAULT_TIMEOUT_SOCK_CONNECT
    )
    config.setdefault(
        "endpoint_timeout_sock_read", PROXY_DEFAULT_TIMEOUT_SOCK_READ
    )

    return config


def main():

    DEFAULT_PATH_CONFIG = (
        PROXY_DEFAULT_CONFIG_BASEDIR / "config" / PROXY_DEFAULT_CONFIG_FILE
    )
    parser = argparse.ArgumentParser(
        prog=PROG, description="Launch endpoint proxy web service.",
    )

    parser.add_argument(
        "-H",
        "--hostname",
        dest="hostname",
        help="TCP/IP hostname to serve on",
    )
    parser.add_argument(
        "-P", "--port", help="TCP/IP port to serve on", dest="port", type=int,
    )
    parser.add_argument(
        "-U",
        "--unix-path",
        dest="unix_path",
        metavar="PATH",
        help="Unix file system path to serve on. Specifying a path will cause "
        "hostname and port arguments to be ignored.",
    )
    parser.add_argument(
        "-c",
        "--config",
        type=real_file_path,
        metavar="PATH",
        default=DEFAULT_PATH_CONFIG,
    )

    args = parser.parse_args(args=sys.argv[1:])
    cli_config = prepare_cli_config(args)

    default_config = config()
    # load config
    config_dict = get_config(
        path_config=args.config,
        cli_config=cli_config,
        defaults=default_config,
        json_schema=config_schema,
    )

    _config = config_dict[PROXY_BASE_ID]["config"]
    # configure logging
    try:
        path_logging_conf = realpath(_config["logging_conf"])
    except (KeyError, TypeError):
        path_logging_conf = None

    logger = setup_logger(path_logging_conf, capture_warnings=True)

    logger.info(f"{PROG}: Version v{__version__}")
    logger.debug(f"Service configuration: {dict(config_dict)}")

    app = create_app(config_dict=config_dict)

    logger.info(f'Application routes: {list(app.router.routes())}')

    # run standalone app
    web.run_app(
        app,
        host=_config["hostname"],
        port=_config["port"],
        path=_config["unix_path"],
    )
    parser.exit(message="Stopped\n")


if __name__ == "__main__":
    main()
