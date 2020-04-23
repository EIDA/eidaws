# -*- coding: utf-8 -*-

import pathlib


PROXY_BASE_ID = "eidaws.endpoint_proxy"

# ----------------------------------------------------------------------------
PROXY_DEFAULT_CONFIG_BASEDIR = pathlib.Path(__file__).parent.parent.parent
PROXY_DEFAULT_CONFIG_FILE = "eidaws_config.yml"

PROXY_DEFAULT_HOSTNAME = "localhost"
PROXY_DEFAULT_PORT = 8080
PROXY_DEFAULT_UNIX_PATH = None

PROXY_DEFAULT_CONN_LIMIT = 20
PROXY_DEFAULT_TIMEOUT_CONNECT = None
PROXY_DEFAULT_TIMEOUT_SOCK_CONNECT = 2
PROXY_DEFAULT_TIMEOUT_SOCK_READ = 30
