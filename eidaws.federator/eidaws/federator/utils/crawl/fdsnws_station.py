# -*- coding: utf-8 -*-

import aiohttp
import argparse
import asyncio
import datetime
import functools
import json
import logging
import logging.config
import logging.handlers  # needed for handlers defined in logging.conf
import os
import random
import re
import sys
import traceback
import uuid

from collections import defaultdict, Counter
from contextlib import contextmanager
from copy import deepcopy
from itertools import product
from pathlib import Path
from random import randint
from timeit import default_timer as timer
from urllib.parse import urlparse, urlunparse, urljoin

from cached_property import cached_property
from fasteners import InterProcessLock
from tqdm.asyncio import tqdm
from tqdm.contrib.logging import tqdm_logging_redirect

from eidaws.federator.version import __version__
from eidaws.federator.utils.crawl.settings import (
    FED_CRAWL_STATION_BASE_ID,
    FED_CRAWL_STATION_DEFAULT_CONFIG_FILES,
    FED_CRAWL_STATION_DEFAULT_URL_FED,
    FED_CRAWL_STATION_DEFAULT_URL_STL,
    FED_CRAWL_STATION_DEFAULT_ORIGINAL_EPOCHS,
    FED_CRAWL_STATION_DEFAULT_NETWORK,
    FED_CRAWL_STATION_DEFAULT_STATION,
    FED_CRAWL_STATION_DEFAULT_LOCATION,
    FED_CRAWL_STATION_DEFAULT_CHANNEL,
    FED_CRAWL_STATION_DEFAULT_FORMAT,
    FED_CRAWL_STATION_DEFAULT_LEVEL,
    FED_CRAWL_STATION_DEFAULT_DOMAIN,
    FED_CRAWL_STATION_DEFAULT_CONFIG_FILES,
    FED_CRAWL_STATION_DEFAULT_PATH_PIDFILE,
    FED_CRAWL_STATION_DEFAULT_PATH_LOGGING_CONF,
    FED_CRAWL_STATION_DEFAULT_NUM_WORKERS,
    FED_CRAWL_STATION_DEFAULT_TIMEOUT,
    FED_CRAWL_STATION_DEFAULT_CRAWL_SORTED,
    FED_CRAWL_STATION_DEFAULT_DELAY,
    FED_CRAWL_STATION_DEFAULT_PBAR,
    FED_CRAWL_STATION_DEFAULT_HISTORY_JSON_DUMP,
    FED_CRAWL_STATION_DEFAULT_HISTORY_JSON_LOAD,
    FED_CRAWL_STATION_DEFAULT_HISTORY_INCLUDE_STL,
    FED_CRAWL_STATION_DEFAULT_HISTORY_BY_STATUS,
)
from eidaws.federator.utils.pool import Pool
from eidaws.federator.utils.request import FdsnRequestHandler
from eidaws.federator.utils.worker import with_exception_handling
from eidaws.utils.app import AppError
from eidaws.utils.cli import CustomParser, InterpolatingYAMLConfigFileParser
from eidaws.utils.error import Error, ExitCodes
from eidaws.utils.misc import real_file_path
from eidaws.utils.schema import StreamEpochSchema
from eidaws.utils.settings import (
    EIDAWS_ROUTING_PATH_QUERY,
    FDSNWS_STATION_PATH_QUERY,
    FDSNWS_NO_CONTENT_CODES,
)
from eidaws.utils.sncl import StreamEpoch


def _serialize_stream_epoch(stream_epoch):
    serializer = StreamEpochSchema(context={"routing": True})
    return " ".join(serializer.dump(stream_epoch).values())


class AlreadyCrawling(Error):
    """There seems to be a crawler process already in action ({})"""


class RoutingError(Error):
    """Error while requesting routing information ({})"""


class InvalidHistory(Error):
    """Error while loading history ({})"""


class Worker:
    """
    Request worker implementation.
    """

    LOGGER = FED_CRAWL_STATION_BASE_ID + ".worker"

    def __init__(
        self, session, counter, lock, delay=None, pbar=None, history=None
    ):
        self._session = session
        self._counter = counter
        self._lock = lock
        self._delay = delay
        self._history = history

        self.logger = logging.getLogger(self.LOGGER)
        self._pbar = pbar

    @with_exception_handling(ignore_runtime_exception=False)
    async def run(self, url, stream_epoch, query_params, **req_kwargs):
        req_handler = FdsnRequestHandler(
            url,
            stream_epochs=[stream_epoch],
            query_params=query_params,
        )

        resp_status = None
        req = req_handler.get(self._session)
        try:
            if self._delay is not None:
                await asyncio.sleep(self._delay)

            async with req(**req_kwargs) as resp:
                resp.raise_for_status()

                resp_status = resp.status
                self.logger.debug(
                    f"Response: {resp.reason}: resp.status={resp.status}, "
                    f"resp.request_info={resp.request_info}, "
                    f"resp.url={resp.url}, resp.headers={resp.headers}"
                )
        except aiohttp.ClientResponseError as err:
            resp_status = err.status
            msg = (
                f"Error while executing request: {err.message}: "
                f"error={type(err)}, resp.status={resp_status}, "
                f"resp.request_info={err.request_info}, "
                f"resp.headers={err.headers}"
            )

            if resp_status in FDSNWS_NO_CONTENT_CODES:
                self.logger.info(msg)
            else:
                self.logger.warning(msg)

        except (aiohttp.ClientError, asyncio.TimeoutError) as err:
            resp_status = 503
            msg = (
                f"Error while executing request: error={type(err)}, "
                f"req_handler={req_handler!r}"
            )
            if isinstance(err, aiohttp.ClientOSError):
                msg += f", errno={err.errno}"

            self.logger.warning(msg)
        finally:
            history = None
            if self._history is not None:
                history = {
                    "stream": stream_epoch,
                    "params": query_params,
                    "status": resp_status,
                }

            # collect stats
            async with self._lock:
                if resp_status is not None:
                    self._counter[resp_status] += 1

                if history:
                    self._history.append(history)

            if self._pbar is not None:
                self._pbar.update()


class CrawlFDSNWSStationApp:
    """
    Implementation of a crawler application for `fdsnws-station`.
    """

    PROG = "eida-crawl-fdsnws-station"

    _HEADERS = {"User-Agent": "EIDA-Crawler/" + __version__}

    # NOTE(damb): From https://github.com/kvesteri/validators
    _PATTERN_DOMAIN = re.compile(
        r"^(?:[a-zA-Z0-9]"  # First character of the domain
        r"(?:[a-zA-Z0-9-_]{0,61}[A-Za-z0-9])?\.)"  # Sub domain + hostname
        r"+[A-Za-z0-9][A-Za-z0-9-_]{0,61}"  # First 61 characters of the gTLD
        r"[A-Za-z]$"  # Last character of the gTLD
    )

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

        parser = self._build_parser()
        args = vars(parser.parse_args())

        # validate args (parameter dependency)
        if (
            len(args["format"]) == 1
            and args["format"][0] == "text"
            and len(args["level"]) == 1
            and args["level"][0] == "response"
        ):
            parser.error(
                "Invalid configuration: --format text --level response"
            )

        configure_logging(args)
        return args

    async def run(self):
        """
        Run application.
        """

        exit_code = ExitCodes.EXIT_SUCCESS

        self.logger.info(f"{self.PROG}: Version v{__version__}")
        self.logger.debug(f"Configuration: {dict(self.config)!r}")

        try:
            pid_lock, got_pid_lock = self._get_pid_lock(
                self.config["path_pidfile"]
            )

            net_codes = ",".join(self.config["network"])
            sta_codes = ",".join(self.config["station"])
            loc_codes = ",".join(self.config["location"])
            cha_codes = ",".join(self.config["channel"])

            connector = aiohttp.TCPConnector(
                limit=self.config["worker_pool_size"]
            )
            timeout = aiohttp.ClientTimeout(total=self.config["timeout"])

            async with aiohttp.ClientSession(
                connector=connector, headers=self._HEADERS
            ) as session:

                # download stream epochs from eidaws-stationlite
                stream_epoch_dict = {}
                for level in self.config["level"]:
                    self.logger.debug(
                        f"Request stream epochs for level: {level!r}"
                    )

                    stream_epochs = await self._emerge_stream_epochs(
                        session,
                        net_codes,
                        sta_codes,
                        loc_codes,
                        cha_codes,
                        level,
                    )
                    if not stream_epochs:
                        self.logger.debug(
                            f"No stream epochs received for level: {level!r}"
                        )
                        continue

                    stream_epoch_dict[level] = stream_epochs
                    self.logger.debug(
                        f"Received {len(stream_epoch_dict[level])} stream "
                        "epoch(s)."
                    )

                if not stream_epoch_dict:
                    self.logger.info("Nothing to do")
                    return

                _history_dump = None
                if self.config["history_json_dump"]:
                    _history_dump = []

                start = timer()
                stats_counter = Counter()
                lock = asyncio.Lock()
                crawled_total = 0
                with tqdm_logging_redirect(
                    tqdm_class=tqdm,
                    loggers=[logging.root, logging.getLogger("eidaws")],
                    disable=not self.config["progress_bar"],
                ) as pbar:
                    async with Pool(
                        worker_coro=Worker(
                            session,
                            stats_counter,
                            lock,
                            delay=self.config["delay"],
                            pbar=pbar,
                            history=_history_dump,
                        ).run,
                        max_workers=self.config["worker_pool_size"],
                    ) as pool:
                        crawled_total = await self._crawl(
                            pool,
                            stream_epoch_dict,
                            formats=self.config["format"],
                            pbar=pbar,
                            timeout=timeout,
                        )

                    if crawled_total:
                        self.logger.info(
                            "Crawling HTTP response code statistics "
                            f"(total requests: {sum(stats_counter.values())}): "
                            f"{dict(stats_counter)!r}"
                        )
                        self.logger.info(
                            "Finished crawling successfully in "
                            f"{round(timer() - start, 6)}s"
                        )
                    else:
                        self.logger.info("Nothing to do")
                        return

                if _history_dump:
                    self.logger.debug(
                        "Dumping crawling history to {!r}".format(
                            self.config["history_json_dump"]
                            if self.config["history_json_dump"] != "-"
                            else "stdout"
                        )
                    )

                    for entry in _history_dump:
                        entry["stream"] = _serialize_stream_epoch(
                            entry["stream"]
                        )

                    ofd = (
                        open(self.config["history_json_dump"], "w")
                        if self.config["history_json_dump"] != "-"
                        else sys.stdout
                    )
                    json.dump(_history_dump, ofd)
                    if ofd is not sys.stdout:
                        ofd.close()

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
                if got_pid_lock:
                    pid_lock.release()
            except NameError:
                pass

        sys.exit(exit_code)

    async def _emerge_stream_epochs(
        self,
        session,
        net_codes,
        sta_codes,
        loc_codes,
        cha_codes,
        level,
        domains=None,
        domains_exclude=None,
    ):
        """
        Emerge stream epochs using eidaws-stationlite.
        """

        async def _request(url, params):
            def _parse_stream_epochs(
                text, domains=None, excluded_domains=None
            ):
                # compute domains to be crawled
                _excluded_domains = None
                if excluded_domains:
                    _excluded_domains = excluded_domains[:]
                    if excluded_domains and domains:
                        explicitly_included = [
                            d for d in excluded_domains if d in domains
                        ]
                        for d in explicitly_included:
                            _excluded_domains.remove(d)

                stream_epochs = []
                url = None
                skip_url = False
                for line in text.split("\n"):
                    if not url:
                        url = line.strip()
                        if excluded_domains or domains:
                            parsed = urlparse(url)
                            if (
                                _excluded_domains
                                and parsed.netloc in _excluded_domains
                                or domains
                                and parsed.netloc not in domains
                            ):
                                skip_url = True

                    elif not line.strip():
                        url = None
                        skip_url = False

                    else:
                        if skip_url:
                            continue

                        se = StreamEpoch.from_snclline(line)
                        stream_epochs.append(se)

                return stream_epochs

            try:
                async with session.get(url, params=params) as resp:
                    resp.raise_for_status()
                    resp_status = resp.status

                    msg = (
                        f"Response: {resp.reason}: resp.status={resp.status}, "
                        f"resp.request_info={resp.request_info}, "
                        f"resp.url={resp.url}, resp.headers={resp.headers}"
                    )

                    if resp_status in FDSNWS_NO_CONTENT_CODES:
                        self.logger.info(msg)
                        return []
                    elif resp_status != 200:
                        raise RoutingError(f"{resp}")

                    self.logger.debug(msg)
                    return _parse_stream_epochs(
                        await resp.text(),
                        domains=self.config["domain"],
                        excluded_domains=self.config["exclude_domain"],
                    )

            except aiohttp.ClientResponseError as err:
                resp_status = err.status
                msg = (
                    f"Error while executing request: {err.message}: "
                    f"error={type(err)}, resp.status={resp_status}, "
                    f"resp.request_info={err.request_info}, "
                    f"resp.headers={err.headers}"
                )

                if resp_status in FDSNWS_NO_CONTENT_CODES:
                    self.logger.info(msg)
                else:
                    self.logger.warning(msg)
            except (aiohttp.ClientError, asyncio.TimeoutError) as err:
                resp_status = 503
                msg = (
                    f"Error while executing request: error={type(err)}, "
                    f"url={url!r}, params={params!r}"
                )
                if isinstance(err, aiohttp.ClientOSError):
                    msg += f", errno={err.errno}"

                self.logger.warning(msg)

            return None

        url_routing = urljoin(
            self.config["routing_url"], EIDAWS_ROUTING_PATH_QUERY
        )
        params = {
            "network": net_codes,
            "station": sta_codes,
            "location": loc_codes,
            "channel": cha_codes,
            "level": level,
            "service": "station",
        }
        stream_epochs = await _request(url_routing, params)

        if stream_epochs is not None:
            # remove duplicates - maintain order
            retval = list(dict.fromkeys(stream_epochs))
            if not self.config["sorted"]:
                random.shuffle(retval)

            return retval

        return None

    async def _crawl(
        self,
        pool,
        stream_epoch_dict,
        formats,
        pbar=None,
        **req_kwargs,
    ):
        """
        Dispatch crawling request tasks based on ``stream_epochs_dict`` onto
        ``pool``.
        """
        url_federator = urljoin(
            self.config["federator_url"], FDSNWS_STATION_PATH_QUERY
        )

        async def _crawl_from_dict(stream_epoch_dict, formats):
            for level, stream_epochs in stream_epoch_dict.items():
                for stream_epoch in stream_epochs:
                    for f in formats:
                        if f == "text" and level == "response":
                            continue

                        query_params = {"format": f, "level": level}
                        self.logger.debug(
                            f"Creating task: stream_epoch={stream_epoch!r}, "
                            f"query_params={query_params!r}"
                        )
                        await pool.submit(
                            url_federator,
                            stream_epoch,
                            query_params,
                            **req_kwargs,
                        )

        def _total(stream_epoch_dict):
            retval = 0
            for level, stream_epochs in stream_epoch_dict.items():
                for f in self.config["format"]:
                    if level == "response" and f == "text":
                        continue
                    retval += len(stream_epochs)

            return retval

        if self.config["history_json_load"] is None:
            total = _total(stream_epoch_dict)
            pbar.reset(total=total)

            await _crawl_from_dict(stream_epoch_dict, formats)
            return total

        self.logger.debug(
            "Loading history from {!r}".format(
                self.config["history_json_load"]
                if self.config["history_json_load"] != "-"
                else "stdin"
            )
        )
        _history = None
        try:
            _history = json.load(self.config["history_json_load"])
        except json.JSONDecodeError as err:
            raise InvalidHistory(err)

        if _history is not None:

            def _prepare_history(history, stream_epoch_dict):
                # synchronize history with eidaws-stationlite stream epochs
                idx = {
                    level: set(stream_epochs)
                    for level, stream_epochs in stream_epoch_dict.items()
                }

                seen = defaultdict(set)
                total = 0
                from_history = []
                try:
                    for entry in history:
                        stream_epoch = entry["stream"]
                        query_params = entry["params"]
                        if not stream_epoch or not query_params:
                            continue
                        l = query_params["level"]
                        f = query_params["format"]
                        if not l or not f:
                            continue

                        if (
                            self.config["history_by_status"]
                            and entry["status"]
                            not in self.config["history_by_status"]
                        ):
                            continue

                        stream_epoch = StreamEpoch.from_snclline(stream_epoch)
                        if l in idx and stream_epoch in idx[l]:
                            from_history.append(
                                (stream_epoch, {"format": f, "level": l})
                            )

                            seen[l].add(stream_epoch)
                            total += 1

                except (KeyError, TypeError) as err:
                    raise InvalidHistory(err)

                supplementary = {}
                if self.config["history_include_stl"]:
                    self.logger.debug(
                        "Checking for supplementary stream epochs"
                    )
                    supplementary = {}
                    for level, stream_epochs in stream_epoch_dict.items():
                        supplementary[level] = list(
                            set(stream_epochs) - seen.get("level", set())
                        )

                    self.logger.debug(
                        "Found {} supplementary stream epochs to be crawled".format(
                            sum(
                                len(se_lst)
                                for l, se_lst in supplementary.items()
                            )
                        )
                    )
                    total += _total(stream_epoch_dict)

                return from_history, supplementary, total

            from_history, supplementary, total = _prepare_history(
                _history, stream_epoch_dict
            )

            pbar.reset(total=total)

            for stream_epoch, query_params in from_history:
                self.logger.debug(
                    f"Creating task: stream_epoch={stream_epoch!r}, "
                    f"query_params={query_params!r}"
                )
                await pool.submit(
                    url_federator,
                    stream_epoch,
                    query_params,
                    **req_kwargs,
                )

            await _crawl_from_dict(supplementary, formats)

            return total

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

        def _url(url):
            parsed = urlparse(url)
            if not (all([parsed.scheme, parsed.netloc])):
                raise argparse.ArgumentError(f"Invalid URL: {url!r}")

            return urlunparse(parsed)

        # NOTE(damb): Refer to
        # http://docs.fdsn.org/projects/source-identifiers/en/v1.0/definition.html
        def _net_code(code):
            if code and re.match("[A-Z0-9*?]{1,8}$", code):
                return code

            raise argparse.ArgumentError(f"Invalid network code: {code!r}")

        def _sta_code(code):
            if code and re.match("[A-Z0-9*?-]{1,8}$", code):
                return code

            raise argparse.ArgumentError(f"Invalid station code: {code!r}")

        def _loc_code(code):
            if code and re.match("[A-Z0-9*?-]{1,8}$", code):
                return code

            raise argparse.ArgumentError(f"Invalid location code: {code!r}")

        def _cha_code(code):
            if code and re.match("[A-Z0-9*?]{1,3}$", code):
                return code

            raise argparse.ArgumentError(f"Invalid channel code: {code!r}")

        def _positive(i, ret_type):
            try:
                i = ret_type(i)
                if i <= 0:
                    raise ValueError
            except Exception as err:
                raise argparse.ArgumentError(f"Invalid value: must be >= 0")

            return i

        _positive_float = functools.partial(_positive, ret_type=float)
        _positive_int = functools.partial(_positive, ret_type=int)

        _timeout = _positive_float
        _worker_pool_size = _positive_int

        def _delay(i):
            if i is None:
                return None

            return _positive_float(i)

        def _domain_or_none(domain):
            def to_unicode(obj, charset="utf-8", errors="strict"):
                if obj is None:
                    return None
                if not isinstance(obj, bytes):
                    return str(obj)
                return obj.decode(charset, errors)

            if domain is None:
                return None

            try:
                if self._PATTERN_DOMAIN.match(
                    to_unicode(domain).encode("idna").decode("ascii")
                ):
                    return domain
            except (UnicodeError, AttributeError):
                raise argparse.ArgumentError(
                    f"Invalid domain name: {domain!r}"
                )

            raise argparse.ArgumentError(f"Invalid domain name: {domain!r}")

        parser = CustomParser(
            prog=self.PROG,
            description=(
                "Crawl fdsnws-station with eidaws-stationlite / "
                "eidaws-federator and keep caches hot."
            ),
            parents=parents,
            default_config_files=FED_CRAWL_STATION_DEFAULT_CONFIG_FILES,
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
            "-R",
            "--routing-url",
            type=_url,
            metavar="URL",
            dest="routing_url",
            default=FED_CRAWL_STATION_DEFAULT_URL_STL,
            help=("eidaws-stationlite URL (default: %(default)s)."),
        )
        parser.add_argument(
            "-F",
            "--federator-url",
            type=_url,
            metavar="URL",
            dest="federator_url",
            default=FED_CRAWL_STATION_DEFAULT_URL_FED,
            help=("eidaws-federator URL (default: %(default)s)."),
        )
        parser.add_argument(
            "--domain",
            nargs="+",
            metavar="DOMAIN",
            type=_domain_or_none,
            default=FED_CRAWL_STATION_DEFAULT_DOMAIN,
            help=(
                "Whitespace-separated list of domains crawling is restricted "
                "to. By default all domains are crawled."
            ),
        )
        parser.add_argument(
            "--exclude-domain",
            nargs="+",
            metavar="DOMAIN",
            dest="exclude_domain",
            type=_domain_or_none,
            help=(
                "Whitespace-separated list of domains to be excluded "
                "crawling. Exclusions are calculated first, inclusions later. "
                "By default all domains are crawled."
            ),
        )
        parser.add_argument(
            "--network",
            nargs="+",
            metavar="CODE",
            type=_net_code,
            default=FED_CRAWL_STATION_DEFAULT_NETWORK,
            help=(
                "Whitespace-separated list of network codes crawling "
                "is restricted to. Allows FDSNWS wildcard characters to be "
                "used (default: %(default)s)."
            ),
        )
        parser.add_argument(
            "--station",
            nargs="+",
            metavar="CODE",
            type=_sta_code,
            default=FED_CRAWL_STATION_DEFAULT_STATION,
            help=(
                "Whitespace-separated list of station codes crawling "
                "is restricted to. Allows FDSNWS wildcard characters to be "
                "used (default: %(default)s)."
            ),
        )
        parser.add_argument(
            "--location",
            nargs="+",
            metavar="CODE",
            type=_loc_code,
            default=FED_CRAWL_STATION_DEFAULT_LOCATION,
            help=(
                "Whitespace-separated list of location codes crawling "
                "is restricted to. Allows FDSNWS wildcard characters to be "
                "used (default: %(default)s)."
            ),
        )
        parser.add_argument(
            "--channel",
            nargs="+",
            metavar="CODE",
            type=_cha_code,
            default=FED_CRAWL_STATION_DEFAULT_CHANNEL,
            help=(
                "Whitespace-separated list of channel codes crawling "
                "is restricted to. Allows FDSNWS wildcard characters to be "
                "used (default: %(default)s)."
            ),
        )
        parser.add_argument(
            "--format",
            nargs="+",
            metavar="FORMAT",
            default=FED_CRAWL_STATION_DEFAULT_FORMAT,
            choices=sorted(FED_CRAWL_STATION_DEFAULT_FORMAT),
            help=(
                "Whitespace-separated list of formats to "
                "be crawled (choices: {%(choices)s}). "
                "By default all formats choicable are crawled."
            ),
        )
        parser.add_argument(
            "--level",
            nargs="+",
            metavar="LEVEL",
            default=FED_CRAWL_STATION_DEFAULT_LEVEL,
            choices=sorted(FED_CRAWL_STATION_DEFAULT_LEVEL),
            help=(
                "Whitespace-separated list of levels to "
                "be crawled (choices: {%(choices)s}). "
                "By default all levels choicable are crawled."
            ),
        )
        parser.add_argument(
            "-w",
            "--worker-pool-size",
            type=_worker_pool_size,
            metavar="NUM",
            dest="worker_pool_size",
            default=FED_CRAWL_STATION_DEFAULT_NUM_WORKERS,
            help=(
                "Number of concurrently crawling request workers "
                "(default: %(default)s)."
            ),
        )
        parser.add_argument(
            "--timeout",
            type=_timeout,
            metavar="SEC",
            default=FED_CRAWL_STATION_DEFAULT_TIMEOUT,
            help="Total request timeout in seconds for a single request "
            "(including connection establishment, request sending and "
            "response reading) while crawling (default: %(default)s).",
        )
        parser.add_argument(
            "--delay",
            type=_delay,
            metavar="SEC",
            default=FED_CRAWL_STATION_DEFAULT_DELAY,
            help="Delay requests by SEC.",
        )
        parser.add_argument(
            "--sorted",
            action="store_true",
            default=FED_CRAWL_STATION_DEFAULT_CRAWL_SORTED,
            help="Keep stream epochs alphanumerically sorted when crawling "
            "(default: %(default)s).",
        )
        parser.add_argument(
            "--progress-bar",
            action="store_true",
            dest="progress_bar",
            default=FED_CRAWL_STATION_DEFAULT_PBAR,
            help="Display progress bar.",
        )
        parser.add_argument(
            "--history-json-dump",
            type=str,
            dest="history_json_dump",
            metavar="PATH",
            default=FED_CRAWL_STATION_DEFAULT_HISTORY_JSON_DUMP,
            help="Dump the crawling history to the PATH specified. Specify "
            "PATH as '-' if you want the crawler to dump the history to "
            "stdout.",
        )
        parser.add_argument(
            "--history-json-load",
            type=argparse.FileType("r"),
            dest="history_json_load",
            metavar="PATH",
            default=FED_CRAWL_STATION_DEFAULT_HISTORY_JSON_LOAD,
            help="Load the crawling history from the PATH specified. Specify "
            "PATH as '-' if you want the crawler to load the history from "
            "stdin. If loading the crawling history is enabled, crawling "
            "is performed based on the history provided syncronized with "
            "eidaws-stationlite. Therefore, using the '--format' CLI argument "
            "has no effect. Note that crawling will be executed in the "
            "sorting order provided by the history.",
        )
        parser.add_argument(
            "--history-include-supplementary-epochs",
            action="store_true",
            dest="history_include_stl",
            default=FED_CRAWL_STATION_DEFAULT_HISTORY_INCLUDE_STL,
            help="Crawl supplementary stream epochs from eidaws-stationlite, "
            "too, while crawling based on a history. Note that for "
            "supplementary epochs crawling is performed for the formats "
            "specified by the '--format' CLI argument.",
        )
        parser.add_argument(
            "--history-by-status",
            nargs="+",
            dest="history_by_status",
            type=_positive_int,
            metavar="CODE",
            default=FED_CRAWL_STATION_DEFAULT_HISTORY_BY_STATUS,
            help="Whitespace separated list of HTTP status codes. Perform a "
            "history based crawling run, but restrict the stream epochs to be "
            "crawled to the HTTP status codes defined.",
        )
        parser.add_argument(
            "-P",
            "--pid-file",
            type=_abs_path,
            metavar="PATH",
            dest="path_pidfile",
            default=FED_CRAWL_STATION_DEFAULT_PATH_PIDFILE,
            help="Absolute path to PID file (default: %(default)s).",
        )
        parser.add_argument(
            "--logging-conf",
            dest="path_logging_conf",
            metavar="PATH",
            default=FED_CRAWL_STATION_DEFAULT_PATH_LOGGING_CONF,
            help="Path to logging configuration file.",
        )

        return parser

    def _setup_logger(self, path_logging_conf=None, capture_warnings=False):
        """
        Initialize the logger of the application.
        """
        logging.basicConfig(level=logging.WARNING)

        LOGGER = FED_CRAWL_STATION_BASE_ID

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

    def _get_pid_lock(self, path_pidfile):
        pid_lock = InterProcessLock(path_pidfile)
        got_pid_lock = pid_lock.acquire(blocking=False)
        if not got_pid_lock:
            raise AlreadyCrawling(path_pidfile)

        self.logger.debug(f"Aquired PID lock {self.config['path_pidfile']!r}")

        return pid_lock, got_pid_lock


# ----------------------------------------------------------------------------
def main():
    """
    main function for EIDA stationlite harvesting
    """

    app = CrawlFDSNWSStationApp()

    try:
        _ = app.config
    except AppError as err:
        # handle errors during the application configuration
        print(
            'ERROR: Application configuration failed "%s".' % err,
            file=sys.stderr,
        )
        sys.exit(ExitCodes.EXIT_ERROR)

    loop = asyncio.get_event_loop()
    loop.run_until_complete(app.run())


# -----------------------------------------------------------------------------
if __name__ == "__main__":
    main()
