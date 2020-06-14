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
from functools import cached_property

from eidaws.federator.settings import FED_BASE_ID
from eidaws.federator.utils.mixin import ClientRetryBudgetMixin, ConfigMixin
from eidaws.federator.utils.misc import (
    _serialize_query_params,
    make_context_logger,
)
from eidaws.federator.utils.request import FdsnRequestHandler
from eidaws.federator.utils.tempfile import AioSpooledTemporaryFile
from eidaws.utils.error import ErrorWithTraceback
from eidaws.utils.misc import _callable_or_raise
from eidaws.utils.settings import FDSNWS_NO_CONTENT_CODES


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


class Drain:
    """
    Abstract base class for consumer implementations.
    """

    async def drain(self, chunk):
        raise NotImplementedError


class ReponseDrain(Drain):
    def __init__(self, request, response, prepare_callback=None):
        self.request = request
        self._response = response
        self._prepare_callback = _callable_or_raise(prepare_callback)

    @property
    def response(self):
        return self._response

    async def drain(self, chunk):
        if not self._response.prepared:

            if self._prepare_callback is not None:
                await self._prepare_callback(self._response)
            else:
                await self._response.prepare(self.request)

        await self._response.write(chunk)


class QueueDrain(Drain):
    def __init__(self, queue):
        self._queue = queue

    async def drain(self, chunk):
        await self._queue.put(chunk)


class WorkerError(ErrorWithTraceback):
    """Base Worker error ({})."""


class BaseAsyncWorker(ClientRetryBudgetMixin, ConfigMixin):
    """
    Abstract base class for worker implementations.
    """

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
            self.request[FED_BASE_ID + ".query_params"],
            self.QUERY_PARAM_SERIALIZER,
        )

    @property
    def format(self):
        return self.query_params["format"]

    async def run(self, route, req_method="GET", **req_kwargs):
        raise NotImplementedError

    async def _handle_error(self, error=None, **kwargs):
        msg = kwargs.get("msg", error)
        if msg is not None:
            self.logger.warning(str(msg))

    async def _handle_413(self, url=None, stream_epoch=None, **kwargs):
        raise WorkerError("HTTP code 413 handling not implemented.")

    async def finalize(self):
        """
        Template coro intented to be called when finializing a job.
        """


class BaseSplitAlignAsyncWorker(BaseAsyncWorker):
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

    @with_exception_handling(ignore_runtime_exception=True)
    async def run(self, route, req_method="GET", **req_kwargs):
        def route_with_single_stream(route):
            streams = set([])

            for se in route.stream_epochs:
                streams.add(se.id())

            return len(streams) == 1

        with ThreadPoolExecutor(max_workers=1) as executor:
            assert route_with_single_stream(
                route
            ), "Cannot handle multiple streams within a single route."

            req_id = self.request[FED_BASE_ID + ".request_id"]
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
                        append = (
                            True if self._drain.response.prepared else False
                        )
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

            req_handler = FdsnRequestHandler(
                url=url, stream_epochs=[se], query_params=self.query_params
            )
            req_handler.format = self.format

            req = getattr(req_handler, req_method.lower())(self._session)
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
            await self._write_response_to_buffer(buf, resp)

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

    async def _write_response_to_buffer(self, buf, resp):
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
