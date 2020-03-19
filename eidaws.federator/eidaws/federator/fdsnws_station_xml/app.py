# -*- coding: utf-8 -*-

import argparse

from eidaws.federator.version import __version__
from eidaws.federator.fdsnws_station_xml import SERVICE_ID, create_app
from eidaws.federator.settings import (
    FED_DEFAULT_CONFIG_BASEDIR,
    FED_DEFAULT_CONFIG_FILE,
)
from eidaws.federator.utils.app import (
    default_config,
    config_schema as default_config_schema,
)
from eidaws.federator.utils.misc import get_config, setup_logger
from eidaws.utils.misc import realpath, real_file_path


PROG = "eida-federator-station-xml"

DEFAULT_PATH_CONFIG = (
    FED_DEFAULT_CONFIG_BASEDIR / "config" / FED_DEFAULT_CONFIG_FILE
)


# XXX(damb):
# For development purposes start the service with
# $ python -m aiohttp.web -H localhost -P 5000 \
#       eidaws.federator.fdsnws_station_xml.app:init_app
#
# NOTE(damb): aiohttp.web
# (https://github.com/aio-libs/aiohttp/blob/master/aiohttp/web.py) provides a
# simple CLI for testing purposes. Though, the parser cannot be used as a
# parent parser since it is wrapped into aiohttp.web:main. Hence, there is no
# way around duplicating the CLI provided.


DEFAULT_CONFIG = default_config()


def init_app(argv):

    parser = argparse.ArgumentParser(
        prog=PROG, description=f"Launch federating {PROG} web service."
    )

    parser.add_argument(
        "-c",
        "--config",
        type=real_file_path,
        metavar="PATH",
        default=DEFAULT_PATH_CONFIG,
    )

    args = parser.parse_args(args=argv)

    # load config
    config_dict = get_config(
        SERVICE_ID,
        path_config=args.config,
        defaults=DEFAULT_CONFIG,
        json_schema=default_config_schema,
    )
    app = create_app(config_dict=config_dict)

    try:
        path_logging_conf = realpath(
            config_dict["config"][SERVICE_ID]["logging_conf"]
        )
    except (KeyError, TypeError):
        path_logging_conf = None

    logger = setup_logger(SERVICE_ID, path_logging_conf, capture_warnings=True)

    logger.info(f"{PROG}: Version v{__version__}")
    logger.debug(
        f"Service configuration: {dict(config_dict['config'][SERVICE_ID])}"
    )

    return app
