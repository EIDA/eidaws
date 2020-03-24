# -*- coding: utf-8 -*-

import abc
import aiohttp
import asyncio
import datetime
import functools
import logging
import sys
import traceback

from aiohttp import web

from eidaws.federator.settings import FED_BASE_ID, FED_DEFAULT_NETLOC_PROXY
from eidaws.federator.utils.httperror import FDSNHTTPError
from eidaws.federator.utils.misc import (
    make_context_logger,
    Route,
)
from eidaws.federator.utils.mixin import ClientRetryBudgetMixin, ConfigMixin
from eidaws.federator.utils.request import RoutingRequestHandler
from eidaws.federator.version import __version__
from eidaws.utils.error import ErrorWithTraceback
from eidaws.utils.settings import (
    FDSNWS_DEFAULT_NO_CONTENT_ERROR_CODE,
    FDSNWS_NO_CONTENT_CODES,
)
from eidaws.utils.sncl import StreamEpoch


def _duration_to_timedelta(*args, **kwargs):
    try:
        return datetime.timedelta(*args, **kwargs)
    except TypeError:
        return None


def cached(func):
    """
    Method decorator providing caching facilities.
    """

    @functools.wraps(func)
    async def wrapper(self, *args, **kwargs):
        cache_key = self.make_cache_key(
            self.query_params, self.stream_epochs, key_prefix=type(self)
        )

        cached, found = await self.get_cache(cache_key)

        self._await_on_close.insert(
            0, functools.partial(self.set_cache, cache_key)
        )

        if found:
            return web.Response(
                content_type=self.content_type,
                charset=self.charset,
                body=cached,
            )

        return await func(self, *args, **kwargs)

    return wrapper


def with_exception_handling(coro):
    """
    Method decorator providing general purpose exception handling for worker
    tasks.
    """

    @functools.wraps(coro)
    async def wrapper(self, *args, **kwargs):

        try:
            await coro(self, *args, **kwargs)
        except asyncio.CancelledError:
            raise
        except Exception as err:
            exc_type, exc_value, exc_traceback = sys.exc_info()
            self.logger.critical(f"Local TaskWorker exception: {type(err)}")
            self.logger.critical(
                "Traceback information: "
                + repr(
                    traceback.format_exception(
                        exc_type, exc_value, exc_traceback
                    )
                )
            )
            self._queue.task_done()

    return wrapper


class BaseAsyncWorker(abc.ABC, ConfigMixin):
    """
    Abstract base class for worker implementations.
    """

    LOGGER = FED_BASE_ID + ".worker"

    def __init__(self, request):

        self.request = request

        self._logger = logging.getLogger(self.LOGGER)
        self.logger = make_context_logger(self._logger, self.request)

    @abc.abstractmethod
    async def run(self, req_method="GET", **kwargs):
        pass

    async def _handle_error(self, error=None, **kwargs):
        msg = kwargs.get("msg", error)
        if msg is not None:
            self.logger.warning(str(msg))

    async def _handle_413(self, url, stream_epoch, **kwargs):
        raise RequestProcessorError("HTTP code 413 handling not implemented.")


class RequestProcessorError(ErrorWithTraceback):
    """Base RequestProcessor error ({})."""


class BaseRequestProcessor(ClientRetryBudgetMixin, ConfigMixin):
    """
    Abstract base class for request processors.
    """

    LOGGER = FED_BASE_ID + ".process"

    ACCESS = "any"

    def __init__(self, request, url_routing, **kwargs):
        """
        :param float retry_budget_client: Per client retry-budget in percent.
            The value defines the cut-off error ratio above requests to
            datacenters (DC) are dropped.
        """

        self.request = request

        self._url_routing = url_routing

        self._proxy_netloc = kwargs.get(
            "proxy_netloc", FED_DEFAULT_NETLOC_PROXY
        )

        self._default_endtime = datetime.datetime.utcnow()
        self._post = False

        self._routed_urls = None
        self._tasks = []
        self._await_on_close = [
            self._gc_response_code_stats,
            self._teardown_tasks,
        ]

        self._logger = logging.getLogger(self.LOGGER)
        self.logger = make_context_logger(self._logger, self.request)

    @property
    def query_params(self):
        return self.request[FED_BASE_ID + ".query_params"]

    @property
    def stream_epochs(self):
        return self.request[FED_BASE_ID + ".stream_epochs"]

    @property
    def request_submitted(self):
        return self.request[FED_BASE_ID + ".request_starttime"]

    @property
    def nodata(self):
        return int(
            self.query_params.get(
                "nodata", FDSNWS_DEFAULT_NO_CONTENT_ERROR_CODE
            )
        )

    @property
    def post(self):
        return self._post

    @post.setter
    def post(self, val):
        self._post = bool(val)

    @property
    def content_type(self):
        raise NotImplementedError

    @property
    def charset(self):
        return None

    @property
    def pool_size(self):
        return self.config["pool_size"]

    @property
    def max_stream_epoch_duration(self):
        return _duration_to_timedelta(
            days=self.config["max_stream_epoch_duration"]
        )

    @property
    def max_total_stream_epoch_duration(self):
        return _duration_to_timedelta(
            days=self.config["max_total_stream_epoch_duration"]
        )

    @property
    def client_retry_budget_threshold(self):
        return self.config["client_retry_budget_threshold"]

    def _handle_error(self, error=None, **kwargs):
        msg = kwargs.get("msg", error)
        if msg is not None:
            self.logger.warning(str(msg))

    _handle_413 = _handle_error

    async def _route(self, timeout=aiohttp.ClientTimeout(total=2 * 60)):
        req_handler = RoutingRequestHandler(
            self._url_routing,
            self.stream_epochs,
            self.query_params,
            proxy_netloc=self._proxy_netloc,
            access=self.ACCESS,
        )

        async with aiohttp.ClientSession(
            connector=self.request.app["routing_http_conn_pool"],
            timeout=timeout,
            connector_owner=False,
        ) as session:
            req = (
                req_handler.post(session)
                if self._post
                else req_handler.get(session)
            )

            async with req() as resp:
                self.logger.debug(
                    f"Response: {resp.reason}: resp.status={resp.status}, "
                    f"resp.request_info={resp.request_info}, "
                    f"resp.url={resp.url}, resp.headers={resp.headers}"
                )

                if resp.status in FDSNWS_NO_CONTENT_CODES:
                    raise FDSNHTTPError.create(
                        self.nodata,
                        self.request,
                        request_submitted=self.request_submitted,
                        service_version=__version__,
                    )

                try:
                    resp.raise_for_status()
                except aiohttp.ClientResponseError as err:
                    self.logger.exception(err)
                    raise FDSNHTTPError.create(
                        500,
                        self.request,
                        request_submitted=self.request_submitted,
                        service_version=__version__,
                    )

                if resp.status != 200:
                    self.logger.error(f"Error while routing: {resp}")
                    raise FDSNHTTPError.create(
                        500,
                        self.request,
                        request_submitted=self.request_submitted,
                        service_version=__version__,
                    )

                return await self._emerge_routes(
                    await resp.text(),
                    post=self._post,
                    default_endtime=self._default_endtime,
                )

    @cached
    async def federate(self, timeout=aiohttp.ClientTimeout(total=60)):
        try:
            self._routed_urls, routes = await self._route()
        except asyncio.TimeoutError as err:
            self.logger.warning(f"TimeoutError while routing: {type(err)}")
            raise FDSNHTTPError.create(
                500,
                self.request,
                request_submitted=self.request_submitted,
                service_version=__version__,
            )

        if not routes:
            raise FDSNHTTPError.create(
                self.nodata,
                self.request,
                request_submitted=self.request_submitted,
                service_version=__version__,
            )

        self.logger.debug(
            f"Number of (demuxed) routes received: {len(routes)}"
        )

        response = await self._make_response(routes, timeout=timeout)

        await asyncio.shield(self.finalize())

        return response

    async def _make_response(
        self,
        routes,
        req_method="GET",
        timeout=aiohttp.ClientTimeout(total=60),
    ):
        """
        Template method to be implemented by concrete processor
        implementations.
        """

        raise NotImplementedError

    async def finalize(self, **kwargs):
        """
        Finalize the response.
        """

        for coro in self._await_on_close:
            await coro(**kwargs)

    async def _join_with_exception_handling(self, queue, response):
        try:
            await asyncio.wait_for(
                queue.join(), self.config["streaming_timeout"]
            )
        except asyncio.TimeoutError:
            if not response.prepared:
                self.logger.warning(
                    "No valid results to be federated within streaming "
                    f"timeout: {self.config['streaming_timeout']}s"
                )
                raise FDSNHTTPError.create(
                    413,
                    self.request,
                    request_submitted=self.request_submitted,
                    service_version=__version__,
                )

        if not response.prepared:
            raise FDSNHTTPError.create(
                self.nodata,
                self.request,
                request_submitted=self.request_submitted,
                service_version=__version__,
            )

    async def _teardown_tasks(self, **kwargs):

        return_exceptions = kwargs.get("return_exceptions", True)

        self.logger.debug("Teardown worker tasks ...")
        for task in self._tasks:
            task.cancel()

        await asyncio.gather(*self._tasks, return_exceptions=return_exceptions)

    async def _gc_response_code_stats(self):

        self.logger.debug("Garbage collect response code statistics ...")

        for url in self._routed_urls:
            await self.gc_cretry_budget(url)

    async def _emerge_routes(self, text, post, default_endtime):
        """
        Default implementation parsing the routing service's output stream and
        create fully demultiplexed routes. Note that routes with an exceeded
        per client retry-budget are dropped.
        """

        def validate_stream_durations(stream_duration, total_stream_duration):
            if (
                self.max_stream_epoch_duration is not None
                and stream_duration > self.max_stream_epoch_duration
            ) or (
                self.max_total_stream_epoch_duration is not None
                and total_stream_duration
                > self.max_total_stream_epoch_duration
            ):
                raise FDSNHTTPError.create(
                    413,
                    self.request,
                    request_submitted=self.request_submitted,
                    service_version=__version__,
                )

        url = None
        skip_url = False

        urls = set([])
        routes = []
        total_stream_duration = datetime.timedelta()

        for line in text.split("\n"):
            if not url:
                url = line.strip()

                try:
                    e_ratio = await self.get_cretry_budget_error_ratio(url)
                except Exception:
                    pass
                else:
                    if e_ratio > self.client_retry_budget_threshold:
                        self.logger.warning(
                            f"Exceeded per client retry-budget for {url}: "
                            f"(e_ratio={e_ratio})."
                        )
                        skip_url = True

            elif not line.strip():
                urls.add(url)

                url = None
                skip_url = False

            else:
                if skip_url:
                    continue

                # XXX(damb): Do not substitute an empty endtime when
                # performing HTTP GET requests in order to guarantee
                # more cache hits (if eida-federator is coupled with
                # HTTP caching proxy).
                se = StreamEpoch.from_snclline(
                    line,
                    default_endtime=(self._default_endtime if post else None),
                )

                stream_duration = se.duration
                try:
                    total_stream_duration += stream_duration
                except OverflowError:
                    total_stream_duration = datetime.timedelta.max

                validate_stream_durations(
                    stream_duration, total_stream_duration
                )

                routes.append(Route(url=url, stream_epochs=[se]))

        return urls, routes
