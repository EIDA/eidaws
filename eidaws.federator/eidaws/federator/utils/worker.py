# -*- coding: utf-8 -*-

import aiohttp
import asyncio
import copy
import datetime
import functools
import logging
import sys
import traceback

from concurrent.futures import ThreadPoolExecutor
from cached_property import cached_property

from eidaws.federator.settings import FED_BASE_ID
from eidaws.federator.utils.mixin import ClientRetryBudgetMixin, ConfigMixin
from eidaws.federator.utils.misc import _serialize_query_params
from eidaws.federator.utils.request import FdsnRequestHandler
from eidaws.federator.utils.tempfile import AioSpooledTemporaryFile
from eidaws.federator.utils.misc import route_to_uuid
from eidaws.utils.error import ErrorWithTraceback
from eidaws.utils.misc import (
    _callable_or_raise,
    get_req_config,
    make_context_logger,
)
from eidaws.utils.settings import (
    FDSNWS_NO_CONTENT_CODES,
    KEY_REQUEST_QUERY_PARAMS,
    KEY_REQUEST_ID,
)


def _split_stream_epoch(stream_epoch, num, default_endtime):
    return stream_epoch.slice(num=num, default_endtime=default_endtime)


def with_exception_handling(ignore_runtime_exception=False):
    """
    Method decorator providing general purpose exception handling for worker
    tasks.
    """

    def decorator(coro):
        @functools.wraps(coro)
        async def wrapper(self, *args, **kwargs):

            try:
                await coro(self, *args, **kwargs)
            except asyncio.CancelledError:
                raise
            except ConnectionResetError as err:
                self.logger.debug(f"TaskWorker exception: {type(err)}")
            except RuntimeError as err:
                exc_type, exc_value, exc_traceback = sys.exc_info()
                log = getattr(
                    self.logger,
                    "debug" if ignore_runtime_exception else "error",
                )
                log(f"TaskWorker RuntimeError: {err}")
                log(
                    "Traceback information: "
                    + repr(
                        traceback.format_exception(
                            exc_type, exc_value, exc_traceback
                        )
                    )
                )

        return wrapper

    return decorator


def with_context_logging():
    """
    Method decorator for :py:func:`BaseWorker.run` coros providing providing
    context logging facilities.
    """

    def decorator(coro):
        @functools.wraps(coro)
        async def wrapper(self, *args, **kwargs):
            try:
                ctx_args = self.create_job_context(args[0])
            except IndexError:
                ctx_args = [self.request]

            self.logger = make_context_logger(self._logger, *ctx_args)
            await coro(self, *args, **kwargs)

        return wrapper

    return decorator


class Drain:
    """
    Abstract base class for drain implementations.
    """

    @property
    def prepared(self):
        return False

    async def drain(self, chunk):
        raise NotImplementedError


class ReponseDrain(Drain):
    def __init__(self, request, response, prepare_callback=None):
        self.request = request
        self._response = response
        self._prepare_callback = _callable_or_raise(prepare_callback)

    @property
    def prepared(self):
        return self._response.prepared

    async def drain(self, chunk):
        if not self.prepared:
            if self._prepare_callback is not None:
                await self._prepare_callback(self._response)
            else:
                await self._response.prepare(self.request)

        await self._response.write(chunk)


class QueueDrain(Drain):
    def __init__(self, queue):
        self._queue = queue

    @property
    def prepared(self):
        return True

    async def drain(self, chunk):
        await self._queue.put(chunk)


class WorkerError(ErrorWithTraceback):
    """Base Worker error ({})."""


class BaseWorker(ClientRetryBudgetMixin, ConfigMixin):
    """
    Abstract base class for worker implementations.
    """

    REQUEST_HANDLER_CLS = FdsnRequestHandler
    QUERY_PARAM_SERIALIZER = None
    LOGGER = FED_BASE_ID + ".worker"

    def __init__(
        self, request, session, drain, lock=None, **kwargs,
    ):
        self.request = request
        self._session = session
        self._drain = drain
        self._lock = lock

        self._logger = logging.getLogger(self.LOGGER)
        self.logger = make_context_logger(self._logger, self.request)

    @cached_property
    def query_params(self):
        """
        Return serialized query parameters.
        """
        return _serialize_query_params(
            get_req_config(self.request, KEY_REQUEST_QUERY_PARAMS),
            self.QUERY_PARAM_SERIALIZER,
        )

    @property
    def format(self):
        return self.query_params["format"]

    @property
    def request_headers(self):
        headers = copy.deepcopy(self.REQUEST_HANDLER_CLS.DEFAULT_HEADERS)
        if not self.config["num_forwarded"]:
            return headers

        headers["X-Forwarded-For"] = self.request.remote
        return headers

    async def run(self, route, req_method="GET", **req_kwargs):
        raise NotImplementedError

    async def _handle_error(self, error=None, logger=None, **kwargs):
        logger = logger or self.logger
        msg = kwargs.get("msg", error)

        if msg is not None:
            logger.warning(str(msg))

    async def _handle_413(self, url=None, stream_epoch=None, **kwargs):
        raise WorkerError("HTTP code 413 handling not implemented.")

    async def finalize(self):
        """
        Template coro intented to be called when finializing a job.
        """

    def _log_request(self, req_handler, method):
        self.logger.debug(f"Request ({method}): {req_handler!r}")

    def create_job_context(self, route):
        return [
            self.request,
            route_to_uuid(route),
        ]


class BaseSplitAlignWorker(BaseWorker):
    """
    Abstract base class for worker implementations providing splitting and
    aligning facilities.
    """

    _CHUNK_SIZE = 4096

    def __init__(
        self, request, session, drain, lock=None, **kwargs,
    ):
        super().__init__(
            request, session, drain, lock=lock, **kwargs,
        )

        self._endtime = kwargs.get("endtime", datetime.datetime.utcnow())

        self._chunk_size = self._CHUNK_SIZE
        self._stream_epochs = []

        assert self._lock is not None, "Lock not assigned"

    @with_context_logging()
    @with_exception_handling(ignore_runtime_exception=True)
    async def run(self, route, req_method="GET", **req_kwargs):
        def route_with_single_stream(route):
            streams = set()

            for se in route.stream_epochs:
                streams.add(se.id())

            return len(streams) == 1

        with ThreadPoolExecutor(max_workers=1) as executor:
            assert route_with_single_stream(
                route
            ), "Cannot handle multiple streams within a single route."

            req_id = get_req_config(self.request, KEY_REQUEST_ID)
            async with AioSpooledTemporaryFile(
                max_size=self.config["buffer_rollover_size"],
                prefix=str(req_id) + ".",
                dir=self.config["tempdir"],
                executor=executor,
            ) as buf:

                url = route.url
                _sorted = sorted(route.stream_epochs)
                self._stream_epochs = copy.deepcopy(_sorted)

                await self._run(
                    url,
                    _sorted,
                    req_method=req_method,
                    buf=buf,
                    splitting_factor=self.config["splitting_factor"],
                    **req_kwargs,
                )

                if await buf.tell():
                    async with self._lock:
                        append = True if self._drain.prepared else False
                        await self._write_buffer_to_drain(
                            buf, self._drain, append=append,
                        )

        await self.finalize()

    async def _run(
        self,
        url,
        stream_epochs,
        req_method,
        buf,
        splitting_factor,
        **req_kwargs,
    ):
        for se in stream_epochs:

            req_handler = self.REQUEST_HANDLER_CLS(
                url=url,
                stream_epochs=[se],
                query_params=self.query_params,
                headers=self.request_headers,
            )
            req_handler.format = self.format

            req = getattr(req_handler, req_method.lower())(self._session)
            self._log_request(req_handler, req_method)
            try:
                resp = await req(**req_kwargs)
            except (aiohttp.ClientError, asyncio.TimeoutError) as err:
                self.logger.warning(
                    f"Error while executing request: error={type(err)}, "
                    f"req_handler={req_handler!r}, method={req_method}"
                )

                await self.update_cretry_budget(req_handler.url, 503)
                break

            msg = (
                f"Response: {resp.reason}: resp.status={resp.status}, "
                f"resp.request_info={resp.request_info}, "
                f"resp.url={resp.url}, resp.headers={resp.headers}"
            )

            try:
                resp.raise_for_status()
            except aiohttp.ClientResponseError:
                if resp.status == 413:
                    await self._handle_413(
                        url,
                        se,
                        splitting_factor=splitting_factor,
                        req_method=req_method,
                        req_kwargs=req_kwargs,
                        buf=buf,
                    )
                    continue
                else:
                    await self._handle_error(msg=msg)
                    break
            else:
                if resp.status != 200:
                    if resp.status in FDSNWS_NO_CONTENT_CODES:
                        self.logger.info(msg)
                        continue
                    else:
                        await self._handle_error(msg=msg)
                        break
            finally:
                await self.update_cretry_budget(req_handler.url, resp.status)

            self.logger.debug(msg)
            await self._write_response_to_buffer(resp, buf)

    async def _handle_413(self, url, stream_epoch, **kwargs):

        assert (
            "splitting_factor" in kwargs
            and "req_method" in kwargs
            and "req_kwargs" in kwargs
            and "buf" in kwargs
        ), "Missing kwarg."

        splitting_factor = kwargs["splitting_factor"]
        buf = kwargs["buf"]
        req_kwargs = kwargs["req_kwargs"]

        splitted = sorted(
            _split_stream_epoch(
                stream_epoch,
                num=splitting_factor,
                default_endtime=self._endtime,
            )
        )
        # keep track of stream epochs attempting to download
        idx = self._stream_epochs.index(stream_epoch)
        self._stream_epochs.pop(idx)
        for i in range(len(splitted)):
            self._stream_epochs.insert(i + idx, splitted[i])

        self.logger.debug(
            f"Splitting {stream_epoch!r} "
            f"(splitting_factor={splitting_factor}). "
            f"Stream epochs after splitting: {self._stream_epochs!r}"
        )

        await self._run(
            url,
            splitted,
            req_method=kwargs["req_method"],
            buf=buf,
            splitting_factor=splitting_factor,
            **req_kwargs,
        )

    async def _write_response_to_buffer(self, resp, buf):
        """
        Template coro.
        """
        raise NotImplementedError

    async def _write_buffer_to_drain(self, buf, drain, append=True):
        await buf.seek(0)

        while True:
            chunk = await buf.read(self._chunk_size)

            if not chunk:
                break

            await drain.drain(chunk)


class NetworkLevelMixin:
    """
    Mixin providing facilities for worker implementations operating at network
    level granularity
    """

    async def _fetch(self, route, req_method="GET", parent_ctx=None, **kwargs):
        # context logging
        logger = self.logger
        if parent_ctx is not None:
            logger = make_context_logger(
                self._logger, *parent_ctx, route_to_uuid(route)
            )

        req_handler = self.REQUEST_HANDLER_CLS(
            **route._asdict(),
            query_params=self.query_params,
            headers=self.request_headers,
        )
        req_handler.format = self.format

        req = getattr(req_handler, req_method.lower())(self._session)
        self._log_request(req_handler, req_method)
        try:
            resp = await req(**kwargs)
        except (aiohttp.ClientError, asyncio.TimeoutError) as err:
            msg = (
                f"Error while executing request: error={type(err)}, "
                f"req_handler={req_handler!r}, method={req_method}"
            )
            await self._handle_error(msg=msg, logger=logger)
            await self.update_cretry_budget(req_handler.url, 503)

            return route, None

        msg = (
            f"Response: {resp.reason}: resp.status={resp.status}, "
            f"resp.request_info={resp.request_info}, "
            f"resp.url={resp.url}, resp.headers={resp.headers}"
        )

        try:
            resp.raise_for_status()
        except aiohttp.ClientResponseError:
            if resp.status == 413:
                await self._handle_413()
            else:
                await self._handle_error(msg=msg, logger=logger)

            return route, None
        else:
            if resp.status != 200:
                if resp.status in FDSNWS_NO_CONTENT_CODES:
                    logger.info(msg)
                else:
                    await self._handle_error(msg=msg, logger=logger)

                return route, None

        logger.debug(msg)

        await self.update_cretry_budget(req_handler.url, resp.status)
        return route, resp
