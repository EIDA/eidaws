# -*- coding: utf-8 -*-
"""
eidaws-stationlite implementation.
"""

import datetime
import logging
import os
import sys
import traceback
import yaml

from flask import g
from werkzeug.exceptions import HTTPException

from eidaws.stationlite.server.config import Config
from eidaws.stationlite.server.db import setup_db
from eidaws.stationlite.server.flask import Flask
from eidaws.stationlite.server.http_error import FDSNHTTPError
from eidaws.stationlite.server.parser import setup_parser_error_handler
from eidaws.stationlite.server.route import setup_routes
from eidaws.stationlite.server.strict import setup_keywordparser_error_handler
from eidaws.stationlite.server.utils import setup_logger
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
        if config_file is not None:
            app.config.from_file(config_file, load=yaml.safe_load)

    else:
        app.config.from_mapping(config_dict)

    logger = setup_logger(app)

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
        print(error)
        return FDSNHTTPError.create(500, service_version=service_version)

    setup_routes(app)

    setup_parser_error_handler(service_version=service_version)
    setup_keywordparser_error_handler(service_version=service_version)

    setup_db(app)

    return app
