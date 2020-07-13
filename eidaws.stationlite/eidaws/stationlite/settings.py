# -*- coding: utf-8 -*-

import os

# ----------------------------------------------------------------------------
STL_BASE_ID = "eidaws.stationlite"

STL_DEFAULT_CONFIG_FILES = [
    "/etc/eidaws/eidaws_config.yml",
    "/etc/eidaws/eidaws_stationlite_config.yml",
    "/etc/eidaws/eidaws_stationlite_server_config.yml",
    "~/.eidaws_config.yml",
    "~/.eidaws_stationlite_config.yml",
    "~/.eidaws_stationlite_server_config.yml",
]

STL_DEFAULT_CLIENT_MAX_SIZE = 1024 ** 2

# ----------------------------------------------------------------------------
STL_HARVEST_BASE_ID = "eidaws.stationlite.harvest"

STL_HARVEST_DEFAULT_CONFIG_FILES = [
    "/etc/eidaws/eidaws_config.yml",
    "/etc/eidaws/eidaws_stationlite_config.yml",
    "/etc/eidaws/eidaws_stationlite_harvest_config.yml",
    "~/.eidaws_config.yml",
    "~/.eidaws_stationlite_config.yml",
    "~/.eidaws_stationlite_harvest_config.yml",
]

STL_HARVEST_DEFAULT_URL_DB = "postgresql://localhost:5432/stationlite"
STL_HARVEST_DEFAULT_SERVICES = [
    "station",
    "dataselect",
    "availability",
    "wfcatalog",
]
STL_HARVEST_DEFAULT_PATH_PIDFILE = os.path.join(
    "/var/tmp", "eida-stationlite-harvest.pid"
)
STL_HARVEST_DEFAULT_NO_ROUTES = False
STL_HARVEST_DEFAULT_NO_VNETWORKS = False
STL_HARVEST_DEFAULT_STRICT_RESTRICTED = False
STL_HARVEST_DEFAULT_PATH_LOGGING_CONF = None
STL_HARVEST_DEFAULT_TRUNCATE_TIMESTAMP = None
