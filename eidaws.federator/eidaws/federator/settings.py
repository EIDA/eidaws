# -*- coding: utf-8 -*-


def _make_fed_path(base_fed_path, query_format, version="1"):
    return "/".join([base_fed_path, query_format, version])


def make_config_file_paths(service_id):
    ns = service_id.split(".")
    template_etc = "/etc/eidaws/eidaws_federator_{}_config.yml"
    template_home = "~/.eidaws/eidaws_federator_{}_config.yml"

    return [
        "/etc/eidaws/eidaws_config.yml",
        "/etc/eidaws/eidaws_federator_config.yml",
        template_etc.format(ns[1]),
        template_etc.format("_".join(ns[1:])),
        "~/.eidaws/eidaws_config.yml",
        "~/.eidaws/eidaws_federator_config.yml",
        template_home.format(ns[1]),
        template_home.format("_".join(ns[1:])),
    ]


FED_BASE_ID = "eidaws.federator"
FED_STATION_TEXT_SERVICE_ID = "fdsnws.station.text"
FED_STATION_XML_SERVICE_ID = "fdsnws.station.xml"
FED_DATASELECT_MINISEED_SERVICE_ID = "fdsnws.dataselect.miniseed"
FED_AVAILABILITY_TEXT_SERVICE_ID = "fdsnws.availability.text"
FED_AVAILABILITY_JSON_SERVICE_ID = "fdsnws.availability.json"
FED_AVAILABILITY_GEOCSV_SERVICE_ID = "fdsnws.availability.geocsv"
FED_AVAILABILITY_REQUEST_SERVICE_ID = "fdsnws.availability.request"
FED_WFCATALOG_JSON_SERVICE_ID = "eidaws.wfcatalog.json"

FED_STATION_PATH = "/fedws/station"
FED_STATION_TEXT_FORMAT = "text"
FED_STATION_PATH_TEXT = _make_fed_path(
    FED_STATION_PATH, FED_STATION_TEXT_FORMAT
)
FED_STATION_XML_FORMAT = "xml"
FED_STATION_PATH_XML = _make_fed_path(FED_STATION_PATH, FED_STATION_XML_FORMAT)

FED_DATASELECT_PATH = "/fedws/dataselect"
FED_DATASELECT_MINISEED_FORMAT = "miniseed"
FED_DATASELECT_PATH_MINISEED = _make_fed_path(
    FED_DATASELECT_PATH, FED_DATASELECT_MINISEED_FORMAT
)

FED_AVAILABILITY_PATH = "/fedws/availability"
FED_AVAILABILITY_TEXT_FORMAT = "text"
FED_AVAILABILITY_PATH_TEXT = _make_fed_path(
    FED_AVAILABILITY_PATH, FED_AVAILABILITY_TEXT_FORMAT
)
FED_AVAILABILITY_JSON_FORMAT = "json"
FED_AVAILABILITY_PATH_JSON = _make_fed_path(
    FED_AVAILABILITY_PATH, FED_AVAILABILITY_JSON_FORMAT
)
FED_AVAILABILITY_GEOCSV_FORMAT = "geocsv"
FED_AVAILABILITY_PATH_GEOCSV = _make_fed_path(
    FED_AVAILABILITY_PATH, FED_AVAILABILITY_GEOCSV_FORMAT
)
FED_AVAILABILITY_REQUEST_FORMAT = "request"
FED_AVAILABILITY_PATH_REQUEST = _make_fed_path(
    FED_AVAILABILITY_PATH, FED_AVAILABILITY_REQUEST_FORMAT
)

FED_WFCATALOG_PATH = "/fedws/wfcatalog"
FED_WFCATALOG_JSON_FORMAT = "json"
FED_WFCATALOG_PATH_JSON = _make_fed_path(
    FED_WFCATALOG_PATH, FED_WFCATALOG_JSON_FORMAT
)


FED_STATIC = "static"
FED_CONTENT_TYPE_VERSION = "plain/text; charset=utf-8"
FED_CONTENT_TYPE_WADL = "application/xml"


# ----------------------------------------------------------------------------
FED_DEFAULT_HOSTNAME = "localhost"
FED_DEFAULT_PORT = 8080
FED_DEFAULT_UNIX_PATH = None

FED_DEFAULT_SERVE_STATIC = False

FED_DEFAULT_URL_ROUTING = "http://localhost/eidaws/routing/1/query"
FED_DEFAULT_ROUTING_CONN_LIMIT = 100
# NOTE(damb): Current number of EIDA DCs is 12.
FED_DEFAULT_ENDPOINT_CONN_LIMIT = 120
FED_DEFAULT_ENDPOINT_CONN_LIMIT_PER_HOST = 10
FED_DEFAULT_ENDPOINT_TIMEOUT_CONNECT = None
FED_DEFAULT_ENDPOINT_TIMEOUT_SOCK_CONNECT = 2
FED_DEFAULT_ENDPOINT_TIMEOUT_SOCK_READ = 30
FED_DEFAULT_NETLOC_PROXY = None
FED_DEFAULT_NUM_FORWARDED = 0

FED_DEFAULT_URL_REDIS = "redis://localhost:6379"
FED_DEFAULT_REDIS_POOL_MINSIZE = 1
FED_DEFAULT_REDIS_POOL_MAXSIZE = 10
FED_DEFAULT_REDIS_POOL_TIMEOUT = None

FED_DEFAULT_POOL_SIZE = None

# Default request method for endpoint requests
FED_DEFAULT_ENDPOINT_REQUEST_METHOD = "GET"
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
FED_DEFAULT_CLIENT_MAX_SIZE = 1024**2
# Configures HTTP 413 behaviour
FED_DEFAULT_MAX_STREAM_EPOCH_DURATION = None
FED_DEFAULT_MAX_STREAM_EPOCH_DURATION_TOTAL = None
# Configuration with respect to temporary file buffers
FED_DEFAULT_TMPDIR = None
FED_DEFAULT_BUFFER_ROLLOVER_SIZE = 0  # bytes

FED_DEFAULT_STREAMING_TIMEOUT = 600

# Default splitting factor for HTTP status code 413 handling
FED_DEFAULT_SPLITTING_FACTOR = 2

# Fallback miniseed record size in case no blockette 1000 was found
FED_DEFAULT_FALLBACK_MSEED_RECORD_SIZE = 0
