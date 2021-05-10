# -*- coding: utf-8 -*-
"""
EIDA NG stationlite utils.

Functions which might be used as *executables*:
    - ``db_init()`` -- create and initialize a stationlite DB
"""

import argparse

import logging
import logging.config
import logging.handlers  # needed for handlers defined in logging.conf
import os
import sys
import traceback

from sqlalchemy import create_engine
from cached_property import cached_property

from eidaws.stationlite.version import __version__
from eidaws.stationlite.engine import orm
from eidaws.stationlite.settings import STL_HARVEST_BASE_ID
from eidaws.utils.app import AppError
from eidaws.utils.cli import CustomParser
from eidaws.utils.error import Error, ExitCodes
from eidaws.utils.misc import realpath


def _url(url):
    """
    check if SQLite URL is absolute.
    """
    if url.startswith("sqlite:") and not (
        url.startswith("////", 7) or url.startswith("///C:", 7)
    ):
        raise argparse.ArgumentTypeError("SQLite URL must be absolute.")
    return url


# ----------------------------------------------------------------------------
class StationLiteDBInitApp:
    """
    Implementation of an utility application to create and initialize an SQLite
    database for EIDA StationLite.
    """

    PROG = "eida-stationlite-db-init"

    class DBAlreadyAvailable(Error):
        """The SQLite database file '{}' is already available."""

    @cached_property
    def config(self):
        args = self._build_cli_parser().parse_args()

        try:
            path_logging_conf = realpath(args.path_logging_conf)
        except (KeyError, TypeError):
            path_logging_conf = None

        self.logger = self._setup_logger(
            path_logging_conf, capture_warnings=True
        )

        return vars(args)

    def _build_cli_parser(self, parents=[]):
        """
        Configure a parser.

        :param list parents: list of parent parsers
        :returns: parser
        :rtype: :py:class:`argparse.ArgumentParser`
        """
        parser = CustomParser(
            prog=self.PROG,
            description="Create and initialize a DB for EIDA StationLite.",
            parents=parents,
        )

        # optional arguments
        parser.add_argument(
            "--version",
            "-V",
            action="version",
            version="%(prog)s version " + __version__,
        )
        parser.add_argument(
            "--sql",
            action="store_true",
            default=False,
            help=(
                "Render the SQL, only; dump the metadata creation "
                "sequence to stdout."
            ),
        )
        parser.add_argument(
            "-o",
            "--overwrite",
            action="store_true",
            default=False,
            help="Overwrite if already existent (SQLite only).",
        )
        parser.add_argument(
            "--logging-conf",
            dest="path_logging_conf",
            metavar="PATH",
            help="Path to logging configuration file.",
        )

        # positional arguments
        parser.add_argument(
            "url_db",
            type=_url,
            metavar="URL",
            help=(
                "DB URL indicating the database dialect and "
                "connection arguments. For SQlite only a "
                "absolute file path is supported."
            ),
        )

        return parser

    def _setup_logger(self, path_logging_conf=None, capture_warnings=False):
        """
        Initialize the logger of the application.
        """
        logging.basicConfig(level=logging.WARNING)

        LOGGER = STL_HARVEST_BASE_ID

        if path_logging_conf is not None:
            try:
                logging.config.fileConfig(path_logging_conf)
                logger = logging.getLogger(LOGGER)
                logger.info(
                    "Using logging configuration read from "
                    f"{path_logging_conf!r}."
                )
            except Exception as err:
                print(
                    f"WARNING: Setup logging failed for {path_logging_conf!r} "
                    f"with error: {err!r}."
                )
                logger = logging.getLogger(LOGGER)
        else:
            logger = logging.getLogger(LOGGER)
            logger.addHandler(logging.NullHandler())

        logging.captureWarnings(bool(capture_warnings))

        return logger

    def run(self):
        """
        Run application.
        """
        # configure SQLAlchemy logging
        # log_level = self.logger.getEffectiveLevel()
        # logging.getLogger('sqlalchemy.engine').setLevel(log_level)
        exit_code = ExitCodes.EXIT_SUCCESS

        self.logger.info(f"{self.PROG}: Version v{__version__}")
        self.logger.debug(f"Configuration: {self.config!r}")

        try:

            if self.config["sql"]:
                # dump sql only
                def dump(sql, *multiparams, **params):
                    print(sql.compile(dialect=engine.dialect))

                idx = self.config["url_db"].find(":")

                engine = create_engine(
                    self.config["url_db"][0:idx] + "://",
                    strategy="mock",
                    executor=dump,
                )
                orm.ORMBase.metadata.create_all(engine, checkfirst=False)
            else:
                if self.config["url_db"].startswith("sqlite"):
                    p = self.config["url_db"][10:]

                    if not self.config["overwrite"] and os.path.isfile(p):
                        raise self.DBAlreadyAvailable(p)

                    if os.path.isfile(p):
                        os.remove(p)

                # create db tables
                engine = create_engine(self.config["url_db"])

                self.logger.debug("Creating database tables ...")
                orm.ORMBase.metadata.create_all(engine)

                self.logger.info(
                    f"DB {self.config['url_db']!r} successfully initialized."
                )

        except Error as err:
            self.logger.error(err)
            exit_code = ExitCodes.EXIT_ERROR
        except Exception as err:
            exc_type, exc_value, exc_traceback = sys.exc_info()
            self.logger.critical("Local Exception: %s" % err)
            self.logger.critical(
                "Traceback information: "
                + repr(
                    traceback.format_exception(
                        exc_type, exc_value, exc_traceback
                    )
                )
            )
            exit_code = ExitCodes.EXIT_ERROR

        sys.exit(exit_code)


# ----------------------------------------------------------------------------
def db_init():
    """
    main function for EIDA stationlite DB initializer
    """

    app = StationLiteDBInitApp()

    try:
        _ = app.config
    except AppError as err:
        # handle errors during the application configuration
        print(
            'ERROR: Application configuration failed "%s".' % err,
            file=sys.stderr,
        )
        sys.exit(ExitCodes.EXIT_ERROR)

    app.run()


# -----------------------------------------------------------------------------
if __name__ == "__main__":
    db_init()
