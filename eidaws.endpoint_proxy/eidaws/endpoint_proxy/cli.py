# -*- coding: utf-8 -*-

from eidaws.endpoint_proxy.settings import (
    PROXY_DEFAULT_CONFIG_FILES,
    PROXY_DEFAULT_HOSTNAME,
    PROXY_DEFAULT_PORT,
    PROXY_DEFAULT_UNIX_PATH,
    PROXY_DEFAULT_CONN_LIMIT,
    PROXY_DEFAULT_TIMEOUT_CONNECT,
    PROXY_DEFAULT_TIMEOUT_SOCK_CONNECT,
    PROXY_DEFAULT_TIMEOUT_SOCK_READ,
    PROXY_DEFAULT_NUM_FORWARDED,
)
from eidaws.endpoint_proxy.version import __version__
from eidaws.utils.cli import (
    port,
    positive_int,
    positive_int_exclusive,
    positive_float_or_none,
    CustomParser,
    InterpolatingYAMLConfigFileParser,
)
from eidaws.utils.misc import real_file_path


def build_parser(
    prog=None,
    parents=[],
    config_file_parser_class=InterpolatingYAMLConfigFileParser,
):

    parser = CustomParser(
        prog=prog,
        description=f"Launch {prog} web service.",
        parents=parents,
        default_config_files=PROXY_DEFAULT_CONFIG_FILES,
        config_file_parser_class=config_file_parser_class,
        args_for_setting_config_path=["-c", "--config"],
    )

    parser.add_argument(
        "-V",
        "--version",
        action="version",
        version="%(prog)s version " + __version__,
    )
    parser.add_argument(
        "-H",
        "--hostname",
        default=PROXY_DEFAULT_HOSTNAME,
        dest="hostname",
        help="TCP/IP hostname to serve on (default: %(default)s).",
    )
    parser.add_argument(
        "-P",
        "--port",
        type=port,
        dest="port",
        default=PROXY_DEFAULT_PORT,
        metavar="PORT",
        help="TCP/IP port to serve on (default: %(default)s).",
    )
    parser.add_argument(
        "-U",
        "--unix-path",
        dest="unix_path",
        metavar="PATH",
        default=PROXY_DEFAULT_UNIX_PATH,
        help="Unix file system path to serve on. Specifying a path will cause "
        "hostname and port arguments to be ignored.",
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
        "--connection-limit",
        type=positive_int_exclusive,
        dest="connection_limit",
        metavar="NUM",
        default=PROXY_DEFAULT_CONN_LIMIT,
        help="Overall maximum number of concurrent HTTP connections used "
        "with the proxied host (default: %(default)s).",
    )
    parser.add_argument(
        "--timeout-connect",
        dest="timeout_connect",
        type=positive_float_or_none,
        metavar="SEC",
        default=PROXY_DEFAULT_TIMEOUT_CONNECT,
        help="Total timeout in seconds for acquiring a connection from the "
        "HTTP connection pool. The time assembles connection establishment "
        "for a new connection or waiting for a free connection from a pool "
        "if pool connection limits are exceeded (default: %(default)s).",
    )
    parser.add_argument(
        "--timeout-socket-connect",
        dest="timeout_sock_connect",
        type=positive_float_or_none,
        metavar="SEC",
        default=PROXY_DEFAULT_TIMEOUT_SOCK_CONNECT,
        help="Timeout in seconds for connecting to a peer for a new "
        "connection (default: %(default)s).",
    )
    parser.add_argument(
        "--timeout-socket-read",
        dest="timeout_sock_read",
        type=positive_float_or_none,
        metavar="SEC",
        default=PROXY_DEFAULT_TIMEOUT_SOCK_READ,
        help="Timeout in seconds for reading a portion of data from a peer "
        "(default: %(default)s).",
    )
    parser.add_argument(
        "--forwarded",
        dest="num_forwarded",
        type=positive_int,
        metavar="NUM",
        default=PROXY_DEFAULT_NUM_FORWARDED,
        help="Take the X-Forwarded-For HTTP header field into account "
        "and set the header field accordingly when performing proxied "
        "requests. This is particularly useful if the service is deployed "
        "behind a reverse proxy and tunneling the remote IP address is "
        "desired. By default forwarding is disabled.",
    )

    return parser
