# -*- coding: utf-8 -*-
"""
eidaws-stationlite implementation.
"""

import datetime
import os
import sys
import traceback
import yaml

from flask import g
from werkzeug.exceptions import HTTPException

from eidaws.stationlite.server.config import Config, ConversionMap
from eidaws.stationlite.server.cors import setup_cors
from eidaws.stationlite.server.db import setup_db
from eidaws.stationlite.server.flask import Flask
from eidaws.stationlite.server.http_error import FDSNHTTPError
from eidaws.stationlite.server.parser import setup_parser_error_handler
from eidaws.stationlite.server.route import setup_routes
from eidaws.stationlite.server.strict import setup_keywordparser_error_handler
from eidaws.stationlite.server.utils import (
    db_init_command,
    db_drop_command,
    setup_logger,
)
from eidaws.stationlite.settings import STL_BASE_ID
from eidaws.stationlite.version import __version__


def create_app(config_dict=None, service_version=__version__):
    """
    Factory function for Flask application.

    :param config_dict: flask configuration object
    :type config_dict: :py:class:`flask.Config`
    :param str service_version: Version string
    """
    app = Flask(__name__)
    app.config.from_object(Config())

    if config_dict is None:
        config_file = os.environ.get("EIDAWS_STATIONLITE_SETTINGS")
        if config_file:
            app.config.from_file(
                config_file, load=yaml.safe_load, converter=ConversionMap()
            )

    else:
        app.config.from_mapping(config_dict)

    logger = setup_logger(app)

    app.cli.add_command(db_init_command)
    app.cli.add_command(db_drop_command)

    @app.before_request
    def before_request():
        g.request_start_time = datetime.datetime.utcnow()

    @app.errorhandler(Exception)
    def handle_error(error):

        # pass through HTTP errors
        if isinstance(error, HTTPException):
            return error

        # handle non-HTTP exceptions
        exc_type, exc_value, exc_traceback = sys.exc_info()
        logger.critical(f"Local Exception: {type(error)}")
        logger.critical(
            "Traceback information: "
            + repr(
                traceback.format_exception(exc_type, exc_value, exc_traceback)
            )
        )
        return FDSNHTTPError.create(500, service_version=service_version)

    setup_routes(app)

    setup_parser_error_handler(service_version=service_version)
    setup_keywordparser_error_handler(service_version=service_version)

    setup_cors(app)
    setup_db(app)

    logger.info(f"{STL_BASE_ID}: Version v{__version__}")
    logger.debug(f"Configuration: {app.config!r}")

    return app
