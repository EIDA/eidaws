# -*- coding: utf-8 -*-
"""
eidaws-stationlite harvesting facilities.
"""

import argparse
import copy
import functools
import logging
import logging.config
import logging.handlers  # needed for handlers defined in logging.conf
import os
import sys
import traceback

from urllib.parse import urlparse, urlunparse

from cached_property import cached_property
from fasteners import InterProcessLock
from lxml import etree
from obspy import UTCDateTime
from sqlalchemy import create_engine
from sqlalchemy.exc import OperationalError

from eidaws.stationlite.engine import db
from eidaws.stationlite.harvest.harvester import (
    Harvester,
    RoutingHarvester,
    VNetHarvester,
)
from eidaws.stationlite.settings import (
    STL_HARVEST_BASE_ID,
    STL_HARVEST_DEFAULT_CONFIG_FILES,
    STL_HARVEST_DEFAULT_NO_ROUTES,
    STL_HARVEST_DEFAULT_NO_VNETWORKS,
    STL_HARVEST_DEFAULT_PATH_PIDFILE,
    STL_HARVEST_DEFAULT_PATH_LOGGING_CONF,
    STL_HARVEST_DEFAULT_SERVICES,
    STL_HARVEST_DEFAULT_STRICT_RESTRICTED,
    STL_HARVEST_DEFAULT_TRUNCATE_TIMESTAMP,
    STL_HARVEST_DEFAULT_URL_DB,
)
from eidaws.stationlite.version import __version__
from eidaws.utils.app import AppError
from eidaws.utils.cli import CustomParser, InterpolatingYAMLConfigFileParser
from eidaws.utils.error import Error, ExitCodes
from eidaws.utils.misc import real_file_path


class NothingToDo(Error):
    """Nothing to do."""


class AlreadyHarvesting(Error):
    """There seems to be a harvesting process already in action ({})."""


class StationLiteHarvestApp:
    """
    Implementation of the harvesting application for EIDA StationLite.
    """

    PROG = "eida-stationlite-harvest"

    DB_PRAGMAS = ["PRAGMA journal_mode=WAL"]

    _POSITIONAL_ARG = "urls-localconfig"

    @cached_property
    def config(self):
        def configure_logging(config_dict):
            try:
                path_logging_conf = real_file_path(
                    config_dict["path_logging_conf"]
                )
            except (KeyError, TypeError):
                path_logging_conf = None

            self.logger = self._setup_logger(
                path_logging_conf, capture_warnings=True
            )

        def parse_positional(dest, remaining_args):

            _remaining_args = copy.deepcopy(remaining_args)

            positionals = []
            for arg in _remaining_args:
                try:
                    key, value = arg.split("=")
                    if dest == key[2:]:
                        positionals.append(value)

                        remaining_args.remove(arg)
                except ValueError:
                    pass

            return positionals, remaining_args

        def error_method(message, orig_error_method=None):
            # skip errors related to missing positional args
            if (
                not message.startswith(
                    "the following arguments are required: "
                )
                and orig_error_method is not None
            ):
                orig_error_method(message)

        # XXX(damb): A dirty workaround is required in order to allow parsing
        # positional arguments from the configuration file.
        parser = self._build_parser()
        _error_method = parser.error
        parser.error = functools.partial(
            error_method, orig_error_method=_error_method
        )
        args, argv = parser.parse_known_args()

        parser.error = _error_method
        positional, remaining_args = parse_positional(
            self._POSITIONAL_ARG, argv
        )

        args = vars(args)
        if args[self._POSITIONAL_ARG] is None:
            if not positional:
                parser.error(
                    "the following arguments are required: "
                    f"{self._POSITIONAL_ARG}"
                )
            if remaining_args:
                parser.error(
                    "unrecognized arguments: {}".format(
                        " ".join(remaining_args)
                    )
                )

            args[self._POSITIONAL_ARG] = positional
        else:
            if argv:
                parser.error(
                    "unrecognized arguments: {}".format(" ".join(argv))
                )

        args[self._POSITIONAL_ARG.replace("-", "_")] = args.pop(
            self._POSITIONAL_ARG
        )

        configure_logging(args)
        return args

    def run(self):
        """
        Run application.
        """
        # configure SQLAlchemy logging
        # log_level = self.logger.getEffectiveLevel()
        # logging.getLogger('sqlalchemy.engine').setLevel(log_level)

        exit_code = ExitCodes.EXIT_SUCCESS

        self.logger.info(f"{self.PROG}: Version v{__version__}")
        self.logger.debug(f"Configuration: {dict(self.config)!r}")

        try:
            path_pidfile = self.config["path_pidfile"]
            pid_lock = InterProcessLock(path_pidfile)
            pid_lock_gotten = pid_lock.acquire(blocking=False)
            if not pid_lock_gotten:
                raise AlreadyHarvesting(path_pidfile)
            self.logger.debug(
                f"Aquired PID lock {self.config['path_pidfile']!r}"
            )

            if (
                self.config["no_routes"]
                and self.config["no_vnetworks"]
                and not self.config["truncate"]
            ):
                raise NothingToDo()

            harvesting = not (
                self.config["no_routes"] and self.config["no_vnetworks"]
            )

            Session = db.ScopedSession()
            engine = create_engine(
                self.config["sqlalchemy_database_uri"], echo=False
            )
            Session.configure(bind=engine)

            if engine.name == "sqlite":
                db.configure_sqlite(self.DB_PRAGMAS)

            # TODO(damb): Implement multithreaded harvesting using a thread
            # pool.
            try:
                if harvesting:
                    self.logger.info("Start harvesting.")

                if not self.config["no_routes"]:
                    self._harvest_routes(Session)
                else:
                    self.logger.info(
                        "Disabled processing <route></route> information."
                    )

                if not self.config["no_vnetworks"]:
                    self._harvest_vnetworks(Session)
                else:
                    self.logger.info(
                        "Disabled processing <vnetwork></vnetwork> "
                        "information."
                    )

                if harvesting:
                    self.logger.info("Finished harvesting successfully.")

                if self.config["truncate"]:
                    self.logger.warning("Removing outdated data.")
                    session = Session()
                    with db.session_guard(session) as _session:
                        num_removed_rows = db.clean(
                            _session,
                            self.config["truncate"],
                        )
                        self.logger.info(
                            f"Number of rows removed: {num_removed_rows}"
                        )

            except OperationalError as err:
                raise db.StationLiteDBEngineError(err)

        # TODO(damb): signal handling
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
        finally:
            try:
                if pid_lock_gotten:
                    pid_lock.release()
            except NameError:
                pass

        sys.exit(exit_code)

    def _build_parser(self, parents=[]):
        """
        Configure a parser.

        :param list parents: list of parent parsers
        :returns: parser
        :rtype: :py:class:`argparse.ArgumentParser`
        """

        def _abs_path(path):
            if not os.path.isabs(path):
                raise argparse.ArgumentError(
                    f"Not an absolute file path: {path!r}"
                )
            return path

        def _sqlalchemy_database_uri(uri):
            parsed = urlparse(uri)
            if not (
                all([parsed.scheme, parsed.path])
                or all([parsed.scheme, parsed.netloc, parsed.path])
            ):
                raise argparse.ArgumentError(f"Invalid database URI: {uri!r}")

            return uri

        def _url(url):
            parsed = urlparse(url)
            if "file" == parsed.scheme:
                if parsed.netloc:
                    raise argparse.ArgumentError(
                        f"Invalid file URI: {url!r}, absolute file path required"
                    )
            else:
                if not (all([parsed.scheme, parsed.netloc])):
                    raise argparse.ArgumentError(f"Invalid URL: {url!r}")

            return urlunparse(parsed)

        def _service(service):
            if service not in STL_HARVEST_DEFAULT_SERVICES:
                raise argparse.ArgumentError(f"Invalid service: {service!r}")
            return service

        def _utcdatetime_or_none(timestamp):
            if timestamp is None:
                return

            try:
                dt = UTCDateTime(timestamp)
            except Exception as err:
                argparse.ArgumentError(f"Invalid UTCDateTime passed: {err}")

            return dt

        parser = CustomParser(
            prog=self.PROG,
            description="Harvest routes for eidaws-stationlite.",
            parents=parents,
            default_config_files=STL_HARVEST_DEFAULT_CONFIG_FILES,
            config_file_parser_class=InterpolatingYAMLConfigFileParser,
            args_for_setting_config_path=["-c", "--config"],
        )
        # optional arguments
        parser.add_argument(
            "-V",
            action="version",
            version="%(prog)s version " + __version__,
        )
        parser.add_argument(
            "-S",
            "--services",
            nargs="+",
            type=_service,
            metavar="SERVICE",
            default=STL_HARVEST_DEFAULT_SERVICES,
            choices=sorted(STL_HARVEST_DEFAULT_SERVICES),
            help=(
                "Whitespace-separated list of services to "
                "be cached. (choices: {%(choices)s}) "
                "By default all services choicable are harvested."
            ),
        )
        parser.add_argument(
            "--strict-restricted",
            action="store_true",
            dest="strict_restricted",
            default=STL_HARVEST_DEFAULT_STRICT_RESTRICTED,
            help=(
                "Perform a strict validation of channel epochs to use the "
                "correct method token depending on their restricted status "
                "property. By default method tokens are adjusted "
                "automatically."
            ),
        )
        parser.add_argument(
            "--no-routes",
            action="store_true",
            dest="no_routes",
            default=STL_HARVEST_DEFAULT_NO_ROUTES,
            help="Do not harvest <route></route> information.",
        )
        parser.add_argument(
            "--no-vnetworks",
            action="store_true",
            dest="no_vnetworks",
            default=STL_HARVEST_DEFAULT_NO_VNETWORKS,
            help="Do not harvest <vnetwork></vnetwork> information.",
        )
        parser.add_argument(
            "-t",
            "--truncate",
            type=_utcdatetime_or_none,
            metavar="TIMESTAMP",
            default=STL_HARVEST_DEFAULT_TRUNCATE_TIMESTAMP,
            help=(
                "Truncate DB (delete outdated information). The format of "
                "TIMESTAMP must agree with formats supported by "
                "obspy.UTCDateTime."
            ),
        )
        parser.add_argument(
            "--database",
            type=_sqlalchemy_database_uri,
            metavar="URL",
            dest="sqlalchemy_database_uri",
            default=STL_HARVEST_DEFAULT_URL_DB,
            help=(
                "DB URL indicating the database dialect and connection "
                "arguments (default: %(default)s)."
            ),
        )
        parser.add_argument(
            "--pid-file",
            "-P",
            type=_abs_path,
            metavar="PATH",
            dest="path_pidfile",
            default=STL_HARVEST_DEFAULT_PATH_PIDFILE,
            help="Absolute path to PID file (default: %(default)s).",
        )
        parser.add_argument(
            "--logging-conf",
            dest="path_logging_conf",
            metavar="PATH",
            default=STL_HARVEST_DEFAULT_PATH_LOGGING_CONF,
            help="Path to logging configuration file.",
        )

        # positional arguments
        parser.add_argument(
            self._POSITIONAL_ARG,
            type=_url,
            metavar="URL",
            nargs="+",
            help=(
                "URL or file URI to eidaws-routing localconfig configuration."
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

    def _harvest_routes(self, Session):
        """
        Harvest the EIDA node's ``<route></route>`` information.

        :param Session: A configured Session class reference
        :type Session: :py:class:`sqlalchemy.orm.session.Session`
        """
        for url in self.config["urls_localconfig"]:
            self.logger.info(f"Processing routes from URL: {url!r}")
            try:
                h = RoutingHarvester(
                    url,
                    services=self.config["services"],
                    force_restricted=not self.config["strict_restricted"],
                )

                session = Session()
                # XXX(damb): Maintain sessions within the scope of a
                # harvesting process.
                with db.session_guard(session) as _session:
                    h.harvest(_session)

            except Harvester.HarvesterError as err:
                self.logger.error(str(err))

    def _harvest_vnetworks(self, Session):
        """
        Harvest the EIDA node's ``<vnetwork></vnetwork>`` information.

        :param Session: A configured Session class reference
        :type Session: :py:class:`sqlalchemy.orm.session.Session`
        """
        for url in self.config["urls_localconfig"]:

            self.logger.info(f"Processing virtual networks from URL: {url!r}")
            try:
                # harvest virtual network configuration
                h = VNetHarvester(url)
                session = Session()
                # XXX(damb): Maintain sessions within the scope of a
                # harvesting process.
                with db.session_guard(session) as _session:
                    h.harvest(_session)

            except Harvester.HarvesterError as err:
                self.logger.error(str(err))


# ----------------------------------------------------------------------------
def main():
    """
    main function for EIDA stationlite harvesting
    """

    app = StationLiteHarvestApp()

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
    main()
