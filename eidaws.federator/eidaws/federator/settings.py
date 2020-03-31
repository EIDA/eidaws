# -*- coding: utf-8 -*-

import pathlib

# ----------------------------------------------------------------------------
FED_BASE_ID = "eidaws.federator"
FED_STATION_TEXT_SERVICE_ID = "fdsnws.station.text"
FED_STATION_XML_SERVICE_ID = "fdsnws.station.xml"
FED_DATASELECT_MINISEED_SERVICE_ID = "fdsnws.dataselect"

FED_STATION_PATH = "/eidaws/station/"
FED_STATION_PATH_TEXT = FED_STATION_PATH + "text/1"

FED_STATION_PATH = "/eidaws/station/"
FED_STATION_PATH_XML = FED_STATION_PATH + "xml/1"

FED_DATASELECT_PATH = "/eidaws/dataselect/"
FED_DATASELECT_PATH_MINISEED = FED_DATASELECT_PATH + "miniseed/1"

# ----------------------------------------------------------------------------
FED_DEFAULT_CONFIG_BASEDIR = pathlib.Path(__file__).parent.parent.parent
FED_DEFAULT_CONFIG_FILE = "eidaws_config.yml"

FED_DEFAULT_URL_ROUTING = "http://localhost/eidaws/routing/1/query"
FED_DEFAULT_ROUTING_CONN_LIMIT = 100
# NOTE(damb): Current number of EIDA DCs is 12.
FED_DEFAULT_ENDPOINT_CONN_LIMIT = 120
FED_DEFAULT_ENDPOINT_CONN_LIMIT_PER_HOST = 10
FED_DEFAULT_TIMEOUT_CONNECT = None
FED_DEFAULT_TIMEOUT_SOCK_CONNECT = 2
FED_DEFAULT_TIMEOUT_SOCK_READ = 30
FED_DEFAULT_NETLOC_PROXY = None

FED_DEFAULT_URL_REDIS = "redis://localhost:6379"
FED_DEFAULT_REDIS_POOL_MINSIZE = 1
FED_DEFAULT_REDIS_POOL_MAXSIZE = 10
FED_DEFAULT_REDIS_POOL_TIMEOUT = None

FED_DEFAULT_POOL_SIZE = None

# Default request method for endpoint requests
FED_DEFAULT_REQUEST_METHOD = "GET"
# Per client retry-budget cut-off error ratio in percent before requests to
# endpoints are being dropped.
FED_DEFAULT_RETRY_BUDGET_CLIENT_THRES = 1.0
# TTL for response codes when performing statistics
FED_DEFAULT_RETRY_BUDGET_CLIENT_TTL = 3600
# Rolling window size with respect to response code time series
FED_DEFAULT_RETRY_BUDGET_WINDOW_SIZE = 10000

# Frontend cache configuration
# No stream buffering and hence no caching
# In order to enable stream buffering without caching (for e.g. testing
# purposes) configure a NullCache:
#
# FED_DEFAULT_CACHE_CONFIG = {
#   "cache_type": "null"
# }
#
FED_DEFAULT_CACHE_CONFIG = None

# Limit payload size for HTTP POST requests (for clients)
FED_DEFAULT_CLIENT_MAX_SIZE = 1024 ** 2
# Configures HTTP 413 behaviour
FED_DEFAULT_MAX_STREAM_EPOCH_DURATION = None
FED_DEFAULT_MAX_STREAM_EPOCH_DURATION_TOTAL = None
# Configuration with respect to temporary file buffers
FED_DEFAULT_TMPDIR = None
FED_DEFAULT_BUFFER_ROLLOVER_SIZE = 0  # bytes; if 0 rollover is disabled

FED_DEFAULT_STREAMING_TIMEOUT = 600

# Default splitting factor for HTTP status code 413 handling
FED_DEFAULT_SPLITTING_FACTOR = 2
