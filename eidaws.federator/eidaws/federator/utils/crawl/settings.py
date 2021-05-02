# -*- coding: utf-8 -*-

import os

from datetime import datetime

# ----------------------------------------------------------------------------
FED_CRAWL_STATION_BASE_ID = "eidaws.crawl.station"

FED_CRAWL_STATION_DEFAULT_CONFIG_FILES = [
    "/etc/eidaws/eidaws_config.yml",
    "/etc/eidaws/eidaws_crawl_config.yml",
    "/etc/eidaws/eidaws_crawl_station_config.yml",
    "~/.eidaws/eidaws_config.yml",
    "~/.eidaws/eidaws_crawl_config.yml",
    "~/.eidaws/eidaws_crawl_station_config.yml",
]

FED_CRAWL_STATION_DEFAULT_URL_FED = "http://localhost:80"
FED_CRAWL_STATION_DEFAULT_URL_STL = "http://localhost:8089"
FED_CRAWL_STATION_DEFAULT_ORIGINAL_EPOCHS = False
FED_CRAWL_STATION_DEFAULT_NETWORK = "*"
FED_CRAWL_STATION_DEFAULT_STATION = "*"
FED_CRAWL_STATION_DEFAULT_LOCATION = "*"
FED_CRAWL_STATION_DEFAULT_CHANNEL = "*"
FED_CRAWL_STATION_DEFAULT_FORMAT = ["text", "xml"]
FED_CRAWL_STATION_DEFAULT_LEVEL = ["network", "station", "channel", "response"]
FED_CRAWL_STATION_DEFAULT_DOMAIN = None
FED_CRAWL_STATION_DEFAULT_NUM_WORKERS = 10
FED_CRAWL_STATION_DEFAULT_TIMEOUT = 10
FED_CRAWL_STATION_DEFAULT_CRAWL_SORTED = False
FED_CRAWL_STATION_DEFAULT_DELAY = None
FED_CRAWL_STATION_DEFAULT_PBAR = False
FED_CRAWL_STATION_DEFAULT_HISTORY_JSON_DUMP = None
FED_CRAWL_STATION_DEFAULT_HISTORY_JSON_LOAD = None
FED_CRAWL_STATION_DEFAULT_HISTORY_INCLUDE_STL = False
FED_CRAWL_STATION_DEFAULT_PATH_PIDFILE = os.path.join(
    "/var/tmp", "eida-crawl-fdsnws-station.pid"
)
FED_CRAWL_STATION_DEFAULT_PATH_LOGGING_CONF = None
