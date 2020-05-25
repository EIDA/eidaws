# -*- coding: utf-8 -*-

import logging
import logging.config
import logging.handlers  # needed for handlers defined in logging.conf

import click

from flask.cli import with_appcontext

from eidaws.stationlite.server.db import db
from eidaws.stationlite.engine.orm import ORMBase
from eidaws.stationlite.settings import STL_BASE_ID


@click.command("db-init")
@with_appcontext
def db_init_command():
    """
    Initialize the database.
    """

    ORMBase.metadata.create_all(db.get_engine())
    click.echo("Database initialized.")


@click.command("db-drop")
@with_appcontext
def db_drop_command():
    """
    Remove database tables.
    """

    ORMBase.metadata.drop_all(db.get_engine())
    click.echo("Database tables dropped.")


def setup_logger(app, logger=STL_BASE_ID, capture_warnings=False):
    """
    Initialize the logger of the application.
    """
    logging.basicConfig(level=logging.WARNING)

    path_logging_conf = app.config.get("PATH_LOGGING_CONF")
    if path_logging_conf is not None:
        try:
            logging.config.fileConfig(path_logging_conf)
            _logger = logging.getLogger(logger)
            _logger.info(
                "Using logging configuration read from "
                f"{path_logging_conf!r}."
            )
        except Exception as err:
            print(
                f"WARNING: Setup logging failed for {path_logging_conf!r} "
                f"with error: {err!r}."
            )
            _logger = logging.getLogger(logger)
    else:
        _logger = logging.getLogger(logger)
        _logger.addHandler(logging.NullHandler())

    logging.captureWarnings(bool(capture_warnings))

    return _logger
