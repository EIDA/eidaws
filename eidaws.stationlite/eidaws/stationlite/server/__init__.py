# -*- coding: utf-8 -*-
"""
eidaws-stationlite implementation.
"""

import datetime
import logging
import sys
import traceback

from flask import Flask, g
from flask_sqlalchemy import SQLAlchemy
from webargs.exceptions import HTTPException

from eidaws.stationlite.server.http_error import FDSNHTTPError
from eidaws.stationlite.server.parser import setup_parser_error_handler
from eidaws.stationlite.server.route import setup_routes
from eidaes.stationlite.server.strict import setup_keywordparser_error_handler
from eidaws.stationlite.version import __version__


db = SQLAlchemy()


logger = logging.getLogger("eidaws.stationlite.server")


def create_app(config_dict, service_version=__version__):
    """
    Factory function for Flask application.

    :param config_dict: flask configuration object
    :type config_dict: :py:class:`flask.Config`
    :param str service_version: Version string
    """
    app = Flask(__name__)
    app.config.update(config_dict)

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

    db.init_app(app)

    return app
