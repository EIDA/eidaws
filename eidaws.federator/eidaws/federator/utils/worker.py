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

from eidaws.federator.settings import FED_BASE_ID
from eidaws.federator.utils.misc import _callable_or_raise
from eidaws.federator.utils.mixin import ClientRetryBudgetMixin, ConfigMixin
from eidaws.federator.utils.misc import make_context_logger
from eidaws.federator.utils.request import FdsnRequestHandler
from eidaws.federator.utils.tempfile import AioSpooledTemporaryFile
from eidaws.utils.error import ErrorWithTraceback
from eidaws.utils.settings import FDSNWS_NO_CONTENT_CODES


def _split_stream_epoch(stream_epoch, num, default_endtime):
    return stream_epoch.slice(num=num, default_endtime=default_endtime)


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
            await self.finalize()

    return wrapper


class WorkerError(ErrorWithTraceback):
    """Base Worker error ({})."""


class BaseAsyncWorker(ClientRetryBudgetMixin, ConfigMixin):
    """
    Abstract base class for worker implementations.
    """

    LOGGER = FED_BASE_ID + ".worker"

    def __init__(
        self,
        request,
        queue,
        session,
        response,
        write_lock,
        prepare_callback=None,
        **kwargs,
    ):
        self.request = request
        self._queue = queue
        self._session = session
        self._response = response

        self._lock = write_lock
        self._prepare_callback = _callable_or_raise(prepare_callback)

        self._logger = logging.getLogger(self.LOGGER)
        self.logger = make_context_logger(self._logger, self.request)

    async def run(self, req_method="GET", **kwargs):
        raise NotImplementedError

    async def _handle_error(self, error=None, **kwargs):
        msg = kwargs.get("msg", error)
        if msg is not None:
            self.logger.warning(str(msg))

    async def _handle_413(self, url=None, stream_epoch=None, **kwargs):
        raise WorkerError("HTTP code 413 handling not implemented.")

    async def finalize(self):
        self._queue.task_done()


class BaseSplitAlignAsyncWorker(BaseAsyncWorker):
    """
    Abstract base class for worker implementations providing splitting and
    aligning facilities.
    """

    QUERY_FORMAT = None

    _CHUNK_SIZE = 4096

    def __init__(
        self,
        request,
        queue,
        session,
        response,
        write_lock,
        query_format=None,
        prepare_callback=None,
        **kwargs,
    ):
        super().__init__(
            request,
            queue,
            session,
            response,
            write_lock,
            prepare_callback=prepare_callback,
            **kwargs,
        )

        self._query_format = query_format or self.QUERY_FORMAT
        self._endtime = kwargs.get("endtime", datetime.datetime.utcnow())

        self._chunk_size = self._CHUNK_SIZE
        self._stream_epochs = []

        assert self._query_format is not None, 'Undefined "query_format"'

    @with_exception_handling
    async def run(self, req_method="GET", **kwargs):
        def route_with_single_stream(route):
            streams = set([])

            for se in route.stream_epochs:
                streams.add(se.id())

            return len(streams) == 1

        with ThreadPoolExecutor(max_workers=1) as executor:
            while True:
                route, query_params = await self._queue.get()

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
                        query_params=query_params,
                        req_method=req_method,
                        buf=buf,
                        splitting_factor=self.config["splitting_factor"],
                    )

                    if await buf.tell():

                        async with self._lock:
                            if not self._response.prepared:

                                if self._prepare_callback is not None:
                                    await self._prepare_callback(
                                        self._response
                                    )
                                else:
                                    await self._response.prepare(self.request)

                            await self._write_buffer_to_response(
                                buf, self._response, executor=executor
                            )

                await self.finalize()

    async def _run(
        self,
        url,
        stream_epochs,
        query_params,
        req_method,
        buf,
        splitting_factor,
        executor=None,
        **kwargs,
    ):
        for se in stream_epochs:

            req_handler = FdsnRequestHandler(
                url=url, stream_epochs=[se], query_params=query_params
            )
            req_handler.format = self._query_format

            req = getattr(req_handler, req_method.lower())(self._session)
            try:
                resp = await req(**kwargs)
            except (aiohttp.ClientError, asyncio.TimeoutError) as err:
                self.logger.warning(
                    f"Error while executing request: error={type(err)}, "
                    f"url={req_handler.url}, method={req_method}"
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
                        query_params=query_params,
                        req_method=req_method,
                        buf=buf,
                    )
                    continue
                else:
                    self._handle_error(msg=msg)
                    break
            else:
                if resp.status != 200:
                    if resp.status in FDSNWS_NO_CONTENT_CODES:
                        self.logger.info(msg)
                    else:
                        self._handle_error(msg=msg)
                        break
            finally:
                await self.update_cretry_budget(req_handler.url, resp.status)

            self.logger.debug(msg)
            await self._write_response_to_buffer(
                buf, resp, executor=executor,
            )

    async def _handle_413(self, url, stream_epoch, **kwargs):

        assert (
            "splitting_factor" in kwargs
            and "query_params" in kwargs
            and "req_method" in kwargs
            and "buf" in kwargs
        ), "Missing kwarg."

        splitting_factor = kwargs["splitting_factor"]
        buf = kwargs["buf"]

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
            query_params=kwargs["query_params"],
            req_method=kwargs["req_method"],
            buf=buf,
            splitting_factor=splitting_factor,
        )

    async def _write_response_to_buffer(self, buf, resp, executor):
        """
        Template coro.
        """
        raise NotImplementedError

    async def _write_buffer_to_response(self, buf, resp, executor):
        await buf.seek(0)

        while True:
            chunk = await buf.read(self._chunk_size)

            if not chunk:
                break

            await resp.write(chunk)
