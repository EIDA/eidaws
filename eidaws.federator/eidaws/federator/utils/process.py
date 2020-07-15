import aiohttp
import asyncio
import collections
import datetime
import functools
import heapq
import logging

from dataclasses import dataclass, field
from typing import Any

from aiohttp import web

from eidaws.federator.settings import FED_BASE_ID
from eidaws.federator.utils.httperror import FDSNHTTPError
from eidaws.federator.utils.misc import create_job_context
from eidaws.federator.utils.mixin import (
    CachingMixin,
    ClientRetryBudgetMixin,
    ConfigMixin,
)
from eidaws.federator.utils.pool import Pool
from eidaws.federator.utils.request import RoutingRequestHandler
from eidaws.federator.utils.worker import ReponseDrain, QueueDrain
from eidaws.federator.version import __version__
from eidaws.utils.error import ErrorWithTraceback
from eidaws.utils.misc import (
    get_req_config,
    make_context_logger,
    Route,
)
from eidaws.utils.settings import (
    FDSNWS_DEFAULT_NO_CONTENT_ERROR_CODE,
    FDSNWS_NO_CONTENT_CODES,
    KEY_REQUEST_QUERY_PARAMS,
    KEY_REQUEST_STARTTIME,
    KEY_REQUEST_STREAM_EPOCHS,
)
from eidaws.utils.sncl import StreamEpoch


def _duration_to_timedelta(*args, **kwargs):
    try:
        return datetime.timedelta(*args, **kwargs)
    except TypeError:
        return None


def cached(coro):
    """
    Method decorator providing caching facilities.
    """
    ENCODING = "gzip"

    @functools.wraps(coro)
    async def wrapper(self, *args, **kwargs):
        async def set_cache(cache_key):
            if self._response_sent and self.cache_buffer:
                self.logger.debug(f"Set cache (cache_key={cache_key!r}).")
                await self.set_cache(cache_key)

        cache_key = self.make_cache_key(
            self.query_params, self.stream_epochs, key_prefix=type(self)
        )

        # use compressed cache content if available; qvalues are not
        # taken into account
        accept_encoding = self.request.headers.get(
            "Accept-Encoding", ""
        ).lower()

        cache_config = self.config["cache_config"]
        compressed_cache = bool(
            cache_config
            and cache_config.get("cache_type") == "redis"
            and cache_config.get("cache_kwargs")
            and cache_config["cache_kwargs"].get("compress", True)
        )
        decompress = (
            False
            if not compressed_cache
            or ENCODING in accept_encoding
            and compressed_cache
            else True
        )

        cached, found = await self.get_cache(cache_key, decompress=decompress)

        self._await_on_close.insert(0, functools.partial(set_cache, cache_key))

        if found:
            resp = web.Response(
                content_type=self.content_type,
                charset=self.charset,
                body=cached,
            )
            if decompress:
                resp.enable_compression()
            elif compressed_cache:
                resp.headers["Content-Encoding"] = ENCODING

            return resp

        return await coro(self, *args, **kwargs)

    return wrapper


def group_routes_by(routes, key="network"):
    """
    Group routes by a certain :py:class:`~eidaws.utils.sncl.Stream` keyword.
    Combined keywords are also possible e.g. ``network.station``. When
    combining keys the seperating character is ``.``.

    :param dict routing_table: Routing table
    :param str key: Key used for grouping.
    """
    SEP = "."

    retval = collections.defaultdict(list)

    for route in routes:
        try:
            _key = getattr(route.stream_epochs[0].stream, key)
        except AttributeError:
            if SEP in key:
                # combined key
                _key = SEP.join(
                    getattr(route.stream_epochs[0].stream, k)
                    for k in key.split(SEP)
                )
            else:
                raise KeyError(f"Invalid separator. Must be {SEP!r}.")

        retval[_key].append(route)

    return retval


class RequestProcessorError(ErrorWithTraceback):
    """Base RequestProcessor error ({})."""


class BaseRequestProcessor(CachingMixin, ClientRetryBudgetMixin, ConfigMixin):
    """
    Abstract base class for request processors.
    """

    LOGGER = FED_BASE_ID + ".process"

    ACCESS = "any"

    RESOURCE_METHOD = None

    def __init__(self, request, **kwargs):
        self.request = request

        self._default_endtime = datetime.datetime.utcnow()
        self._post = False

        self._routed_urls = None
        self._response_sent = False
        self._await_on_close = [
            self._gc_response_code_stats,
        ]

        self._logger = logging.getLogger(self.LOGGER)
        self.logger = make_context_logger(self._logger, self.request)

    @property
    def query_params(self):
        return get_req_config(self.request, KEY_REQUEST_QUERY_PARAMS)

    @property
    def stream_epochs(self):
        return get_req_config(self.request, KEY_REQUEST_STREAM_EPOCHS)

    @property
    def request_submitted(self):
        return get_req_config(self.request, KEY_REQUEST_STARTTIME)

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
    def proxy(self):
        proxy_netloc = self.config.get("proxy_netloc")
        return f"http://{proxy_netloc}" if proxy_netloc else None

    @property
    def pool_size(self):
        return (
            self.config["pool_size"]
            or self.config["endpoint_connection_limit"]
        )

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

    async def _route(self, timeout=aiohttp.ClientTimeout(total=2 * 60)):
        req_handler = RoutingRequestHandler(
            self.config["url_routing"],
            self.stream_epochs,
            self.query_params,
            access=self.ACCESS,
            method=self.RESOURCE_METHOD,
        )

        async with aiohttp.ClientSession(
            connector=self.request.app["routing_http_conn_pool"],
            timeout=timeout,
            connector_owner=False,
        ) as session:
            req = (
                req_handler.post(session)
                if self.post
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
                    self.logger.error(err)
                    raise FDSNHTTPError.create(
                        500,
                        self.request,
                        request_submitted=self.request_submitted,
                        service_version=__version__,
                        error_desc_long=f"Error while routing: {err}",
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
                    post=self.post,
                    default_endtime=self._default_endtime,
                )

    @cached
    async def federate(self, timeout=aiohttp.ClientTimeout(total=60)):
        try:
            self._routed_urls, routes = await self._route()
        except (aiohttp.ClientError, asyncio.TimeoutError) as err:
            if isinstance(err, asyncio.TimeoutError):
                msg = f"TimeoutError: {type(err)}"
            else:
                msg = f"{type(err)}: {err}"

            msg = f"Error while routing: {msg}"
            self.logger.error(msg)
            raise FDSNHTTPError.create(
                500,
                self.request,
                request_submitted=self.request_submitted,
                error_desc_long=msg,
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
        self.logger.debug(f"Routes received: {routes}")

        try:
            # XXX(damb): Handle exceptions within middleware.
            return await self._make_response(
                routes,
                req_method=self.config["endpoint_request_method"],
                timeout=timeout,
                proxy=self.proxy,
            )
        finally:
            # TODO(damb): Finalization prevents access logs being output
            await asyncio.shield(self.finalize())

    def create_job_context(self, *routes):
        return create_job_context(self.request, *routes)

    def make_stream_response(self, *args, **kwargs):
        """
        Factory for a :py:class:`aiohttp.web.StreamResponse`.
        """

        response = web.StreamResponse(*args, **kwargs)

        response_write = response.write

        async def write(*args, **kwargs):
            await response_write(*args, **kwargs)
            self.dump_to_cache_buffer(*args, **kwargs)

        response.write = write

        return response

    async def _make_response(
        self,
        routes,
        req_method="GET",
        timeout=aiohttp.ClientTimeout(total=60),
        **req_kwargs,
    ):
        """
        Return a federated response.
        """
        raise NotImplementedError

    async def _dispatch(self, pool, routes, req_method, **req_kwargs):
        """
        Dispatch jobs onto ``pool``.
        """
        raise NotImplementedError

    async def _prepare_response(self, response):
        """
        Template method preparing the response.
        """
        response.content_type = self.content_type
        response.charset = self.charset
        await response.prepare(self.request)

    async def _write_response_footer(self, response):
        """
        Template method to be implemented in case writing a response footer is
        required.
        """

    def _create_worker_drain(self, *args, **kwargs):
        """
        Template method returning an instance of a worker drain.
        """
        raise NotImplementedError

    def _create_worker(self, session, drain, **kwargs):
        """
        Template method returning the processor's worker object.
        """
        raise NotImplementedError

    async def finalize(self):
        """
        Finalize the response.
        """

        for coro_or_func in self._await_on_close:
            if asyncio.iscoroutine(coro_or_func):
                await coro_or_func
            elif asyncio.iscoroutinefunction(coro_or_func) or (
                isinstance(coro_or_func, functools.partial)
                and asyncio.iscoroutinefunction(coro_or_func.func)
            ):
                await coro_or_func()
            elif callable(coro_or_func):
                coro_or_func()
            else:
                raise TypeError("Unknown type: {type(coro_or_func)}")

    async def _gc_response_code_stats(self):

        self.logger.debug("Garbage collect response code statistics ...")

        for url in self._routed_urls:
            await self.gc_cretry_budget(url)

    async def _emerge_routes(
        self, text, post, default_endtime,
    ):
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
                self.logger.debug(
                    "Exceeded configured limits: {}{}".format(
                        "stream_duration="
                        f"{stream_duration.total_seconds()}s (configured="
                        f"{self.max_stream_epoch_duration.total_seconds()}s), "
                        if self.max_stream_epoch_duration
                        else "",
                        "total_stream_duration: "
                        f"{total_stream_duration.total_seconds()}s "
                        "(configured="
                        f"{self.max_total_stream_epoch_duration.total_seconds()}s"
                        ")"
                        if self.max_total_stream_epoch_duration
                        else "",
                    )
                )
                raise FDSNHTTPError.create(
                    413,
                    self.request,
                    request_submitted=self.request_submitted,
                    service_version=__version__,
                    error_desc_long=(
                        "Exceeded configured stream epoch limits: "
                        "({}{})".format(
                            "limit per requested stream epoch="
                            f"{self.max_stream_epoch_duration.days} days, "
                            if self.max_stream_epoch_duration
                            else "",
                            f"total={self.max_total_stream_epoch_duration.days}"
                            " days"
                            if self.max_total_stream_epoch_duration
                            else "",
                        )
                    ),
                )

        url = None
        skip_url = False

        urls = set()
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
                    line, default_endtime=default_endtime if post else None,
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


class UnsortedResponse(BaseRequestProcessor):
    def _create_worker_drain(self, *args, **kwargs):
        return ReponseDrain(*args, **kwargs)

    async def _dispatch(self, pool, routes, req_method, **req_kwargs):
        """
        Dispatch jobs onto ``pool``.
        """
        for route in routes:
            ctx = self.create_job_context(route)
            self.logger.debug(
                f"Creating job: context={ctx!r}, route={route!r}"
            )
            await pool.submit(
                route, req_method=req_method, context=ctx, **req_kwargs,
            )

    async def _make_response(
        self,
        routes,
        req_method="GET",
        timeout=aiohttp.ClientTimeout(total=60),
        **req_kwargs,
    ):
        """
        Return a federated response.
        """

        def make_worker(response, session, lock):
            drain = self._create_worker_drain(
                self.request, response, self._prepare_response,
            )
            return self._create_worker(self.request, session, drain, lock=lock)

        async with aiohttp.ClientSession(
            connector=self.request.config_dict["endpoint_http_conn_pool"],
            timeout=timeout,
            connector_owner=False,
        ) as session:

            response = self.make_stream_response()
            lock = asyncio.Lock()
            worker = make_worker(response, session, lock)

            try:

                async with Pool(
                    worker_coro=worker.run,
                    max_workers=self.pool_size,
                    timeout=self.config["streaming_timeout"],
                ) as pool:

                    await self._dispatch(
                        pool, routes, req_method, **req_kwargs
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

            await self._write_response_footer(response)
            await response.write_eof()
            self._response_sent = True
            return response


class SortedResponse(BaseRequestProcessor):
    @dataclass(order=True)
    class PrioritizedItem:
        priority: int
        item: Any = field(compare=False)

    def __init__(self, request, **kwargs):
        super().__init__(request, **kwargs)

        self._current_priority = 0
        self._buf = []  # heap

    def _create_worker_drain(self, *args, **kwargs):
        return QueueDrain(*args, **kwargs)

    async def _make_response(
        self,
        routes,
        req_method="GET",
        timeout=aiohttp.ClientTimeout(total=60),
        **req_kwargs,
    ):
        """
        Return a federated response.
        """

        def create_result_processor(*args, **kwargs):
            t = self.request.loop.create_task(
                self._process_results(*args, **kwargs)
            )
            self._await_on_close.append(self._teardown_tasks(t))
            return t

        async with aiohttp.ClientSession(
            connector=self.request.config_dict["endpoint_http_conn_pool"],
            timeout=timeout,
            connector_owner=False,
        ) as session:

            result_queue = asyncio.Queue()
            response = self.make_stream_response()
            drain = self._create_worker_drain(result_queue)
            worker = self._create_worker(self.request, session, drain)

            # TODO(damb): Configure timeout for dropping an expected result
            _ = create_result_processor(result_queue, response)

            try:

                async with Pool(
                    worker_coro=worker.run,
                    max_workers=self.pool_size,
                    timeout=self.config["streaming_timeout"],
                ) as pool:

                    await self._dispatch(
                        pool, routes, req_method, **req_kwargs
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

            # finish processing if previously no streaming_timeout was raised
            await result_queue.join()
            await self._write_buffered(response, append=response.prepared)

            if not response.prepared:
                raise FDSNHTTPError.create(
                    self.nodata,
                    self.request,
                    request_submitted=self.request_submitted,
                    service_version=__version__,
                )

            await self._write_response_footer(response)
            await response.write_eof()
            self._response_sent = True
            return response

    async def _process_results(self, queue, response, timeout=30):
        """
        Template method consuming results from a ``queue`` in order to
        write them to a ``response``.

        :param queue: Queue results are consumed from
        :param response: Response instance results are written to
        :param float timeout: Timeout in seconds an expected result is
            dropped.
        """
        # TODO(damb): Implement timeout in order to drop an expected result
        while True:
            result_received = False
            try:
                priority, result = await asyncio.wait_for(queue.get(), 0.1)
                result_received = True
                self.logger.debug(
                    f"Processing result (priority={priority}) ..."
                )

                if self._current_priority < priority:
                    item = self.PrioritizedItem(priority, result)
                    heapq.heappush(self._buf, item)
                    continue
                elif self._current_priority > priority:
                    continue
                elif self._current_priority == priority and not result:
                    self._current_priority += 1
                    continue

                if not response.prepared:
                    await self._prepare_response(response)
                else:
                    await self._write_separator(response)

                await response.write(result)

                self._current_priority += 1

            except asyncio.TimeoutError:
                pass
            finally:
                if result_received:
                    queue.task_done()

            await self._write_buffered(response, append=response.prepared)

    async def _write_separator(self, response):
        """
        Template method if a chunk separator is required.
        """

    async def _write_buffered(self, response, append=True):
        try:
            while self._current_priority == min(self._buf):
                buffered = heapq.heappop(self._buf)
                if buffered.item:
                    if append:
                        await self._write_separator(response, append)
                    else:
                        await self._prepare_response(response)

                    await response.write(buffered.item)

                self._current_priority += 1
        except ValueError:
            pass

    async def _teardown_tasks(self, *tasks):
        self.logger.debug("Teardown background tasks ...")

        for t in tasks:
            t.cancel()

        results = await asyncio.gather(*tasks, return_exceptions=True)
        for result in results:
            if isinstance(result, asyncio.CancelledError):
                continue
            if isinstance(result, RuntimeError):
                self.logger.debug(
                    f"RuntimeError while tearing down tasks: {result}"
                )
            elif isinstance(result, Exception):
                self.logger.error(
                    f"Error while tearing down tasks: {type(result)}"
                )
