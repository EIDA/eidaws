# -*- coding: utf-8 -*-

import os
import pathlib


# ----------------------------------------------------------------------------
STL_BASE_ID = "eidaws.stationlite"

STL_DEFAULT_CONFIG_BASEDIR = pathlib.Path(__file__).parent.parent.parent
STL_DEFAULT_CONFIG_FILE = "eidaws_config.yml"

STL_DEFAULT_CLIENT_MAX_SIZE = 1024 ** 2

# ----------------------------------------------------------------------------
STL_HARVEST_BASE_ID = "eidaws.stationlite.harvest"

STL_HARVEST_DEFAULT_URLS_ROUTING = [
    # ODC
    "http://www.orfeus-eu.org/eidaws/routing/1/localconfig",
    # GFZ
    "http://geofon.gfz-potsdam.de/eidaws/routing/1/localconfig",
    # RESIF
    "http://ws.resif.fr/eida_routing.xml",
    # INGV
    "http://webservices.ingv.it/eidaws/routing/1/localconfig",
    # ETHZ
    "http://eida.ethz.ch/eidaws/routing/1/localconfig",
    # BGR
    "http://eida.bgr.de/eidaws/routing/1/localconfig",
    # NIEP
    "http://eida-routing.infp.ro/eidaws/routing/1/routing.xml",
    # KOERI
    "http://eida.koeri.boun.edu.tr/eidaws/routing/1/localconfig",
    # LMU
    "http://erde.geophysik.uni-muenchen.de/eidaws/routing/1/localconfig",
    # NOA
    "http://eida.gein.noa.gr/eidaws/routing/1/localconfig",
    # UIB
    "http://eida.geo.uib.no/eidaws/routing.xml",
]

STL_HARVEST_DEFAULT_URLS_ROUTING_VNET = STL_HARVEST_DEFAULT_URLS_ROUTING
STL_HARVEST_DEFAULT_URL_DB = "postgresql://localhost:5432/stationlite"
STL_HARVEST_DEFAULT_SERVICES = ("station", "dataselect", "wfcatalog")
STL_HARVEST_DEFAULT_PATH_PIDFILE = os.path.join(
    "/var/tmp", "eida-stationlite-harvest.pid"
)
STL_HARVEST_DEFAULT_CONFIG_BASEDIR = STL_DEFAULT_CONFIG_BASEDIR
STL_HARVEST_DEFAULT_CONFIG_FILE = STL_DEFAULT_CONFIG_FILE
STL_HARVEST_DEFAULT_PATH_CONFIG = (
    STL_HARVEST_DEFAULT_CONFIG_BASEDIR
    / "config"
    / STL_HARVEST_DEFAULT_CONFIG_FILE
)
STL_HARVEST_DEFAULT_NO_ROUTES = False
STL_HARVEST_DEFAULT_NO_VNETWORKS = False
STL_HARVEST_DEFAULT_STRICT_RESTRICTED = False
STL_HARVEST_DEFAULT_PATH_LOGGING_CONF = None
STL_HARVEST_DEFAULT_TRUNCATE_TIMESTAMP = None
