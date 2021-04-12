# -*- coding: utf-8 -*-


PROXY_BASE_ID = "eidaws.endpoint_proxy"

# ----------------------------------------------------------------------------
PROXY_DEFAULT_CONFIG_FILES = [
    "/etc/eidaws/eidaws_config.yml",
    "/etc/eidaws/eidaws_proxy_config.yml",
    "~/.eidaws/eidaws_config.yml",
    "~/.eidaws/eidaws_proxy_config.yml",
]

PROXY_DEFAULT_HOSTNAME = "localhost"
PROXY_DEFAULT_PORT = 8080
PROXY_DEFAULT_UNIX_PATH = None

PROXY_DEFAULT_CONN_LIMIT = 20
PROXY_DEFAULT_TIMEOUT_CONNECT = None
PROXY_DEFAULT_TIMEOUT_SOCK_CONNECT = 2
PROXY_DEFAULT_TIMEOUT_SOCK_READ = 30

PROXY_DEFAULT_NUM_FORWARDED = 0
