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
from eidaws.federator.utils.misc import (
    _coroutine_or_raise,
    _serialize_query_params,
)
from eidaws.federator.utils.request import FdsnRequestHandler
from eidaws.federator.utils.tempfile import AioSpooledTemporaryFile
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
        self,
        request,
        session,
        drain,
        lock=None,
        **kwargs,
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

    async def run(self, route, req_method="GET", context=None, **req_kwargs):
        raise NotImplementedError

    async def handle_error(self, error=None, context=None, **kwargs):
        context = context or {}
        logger = context.get("logger", self.logger)

        msg = kwargs.get("msg", error)
        if msg is not None:
            logger.warning(str(msg))

    async def handle_413(
        self, url=None, stream_epoch=None, context=None, **kwargs
    ):
        raise WorkerError("HTTP code 413 handling not implemented.")

    async def finalize(self):
        """
        Template coro intented to be called when finializing a job.
        """

    def _log_request(self, req_handler, method, logger=None):
        logger = logger or self.logger
        logger.debug(f"Request ({method}): {req_handler!r}")


class BaseSplitAlignWorker(BaseWorker):
    """
    Abstract base class for worker implementations providing splitting and
    aligning facilities.
    """

    _CHUNK_SIZE = 4096

    def __init__(
        self,
        request,
        session,
        drain,
        lock=None,
        **kwargs,
    ):
        super().__init__(
            request,
            session,
            drain,
            lock=lock,
            **kwargs,
        )

        self._endtime = kwargs.get("endtime", datetime.datetime.utcnow())

        assert self._lock is not None, "Lock not assigned"

    @with_exception_handling(ignore_runtime_exception=True)
    async def run(self, route, req_method="GET", context=None, **req_kwargs):
        def route_with_single_stream(route):
            streams = set()

            for se in route.stream_epochs:
                streams.add(se.id())

            return len(streams) == 1

        url = route.url
        _sorted = sorted(route.stream_epochs)

        context = context or {}
        context["chunk_size"] = self._CHUNK_SIZE
        context["stream_epochs_record"] = copy.deepcopy(_sorted)

        # context logging
        try:
            logger = make_context_logger(self._logger, *context["logger_ctx"])
        except (TypeError, KeyError):
            logger = self.logger
        finally:
            context["logger"] = logger

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

                await self._run(
                    url,
                    _sorted,
                    req_method=req_method,
                    buf=buf,
                    splitting_factor=self.config["splitting_factor"],
                    context=context,
                    **req_kwargs,
                )

                if await buf.tell():
                    async with self._lock:
                        append = self._drain.prepared or False
                        await self._flush(
                            buf,
                            self._drain,
                            context,
                            append=append,
                        )

        await self.finalize()

    async def _run(
        self,
        url,
        stream_epochs,
        req_method,
        buf,
        splitting_factor,
        context,
        **req_kwargs,
    ):
        logger = context.get("logger") or self.logger

        for se in stream_epochs:
            req_handler = self.REQUEST_HANDLER_CLS(
                url=url,
                stream_epochs=[se],
                query_params=self.query_params,
                headers=self.request_headers,
            )
            req_handler.format = self.format
            req = getattr(req_handler, req_method.lower())(self._session)

            self._log_request(req_handler, req_method, logger=logger)
            resp_status = None
            try:
                async with req(**req_kwargs) as resp:
                    resp.raise_for_status()

                    resp_status = resp.status
                    msg = (
                        f"Response: {resp.reason}: resp.status={resp_status}, "
                        f"resp.request_info={resp.request_info}, "
                        f"resp.url={resp.url}, resp.headers={resp.headers}"
                    )
                    if resp_status == 200:
                        logger.debug(msg)
                        await self._buffer_response(resp, buf, context=context)
                    elif resp_status in FDSNWS_NO_CONTENT_CODES:
                        logger.info(msg)
                    else:
                        await self.handle_error(msg=msg, context=context)
                        break

            except aiohttp.ClientResponseError as err:
                resp_status = err.status
                msg = (
                    f"Error while executing request: {err.message}: "
                    f"error={type(err)}, resp.status={resp_status}, "
                    f"resp.request_info={err.request_info}, "
                    f"resp.headers={err.headers}"
                )

                if resp_status == 413:
                    await self.handle_413(
                        url,
                        se,
                        splitting_factor=splitting_factor,
                        req_method=req_method,
                        req_kwargs=req_kwargs,
                        buf=buf,
                        context=context,
                    )
                elif resp_status in FDSNWS_NO_CONTENT_CODES:
                    logger.info(msg)
                # https://github.com/aio-libs/aiohttp/issues/3641
                elif (
                    resp_status == 400
                    and "invalid constant string" == err.message
                ):
                    resp_status = 204
                    logger.info(
                        "Excess found in read (reset status code to "
                        f"{resp_status}). Original aiohttp error: {msg}"
                    )
                else:
                    await self.handle_error(msg=msg, context=context)
                    break

            except (aiohttp.ClientError, asyncio.TimeoutError) as err:
                resp_status = 503
                msg = (
                    f"Error while executing request: error={type(err)}, "
                    f"req_handler={req_handler!r}, method={req_method}"
                )
                if isinstance(err, aiohttp.ClientOSError):
                    msg += f", errno={err.errno}"
                await self.handle_error(msg=msg, context=context)
                break

            finally:
                if resp_status is not None:
                    await self.update_cretry_budget(
                        req_handler.url, resp_status
                    )

    async def handle_413(self, url, stream_epoch, context=None, **kwargs):

        assert (
            "splitting_factor" in kwargs
            and "req_method" in kwargs
            and "req_kwargs" in kwargs
            and "buf" in kwargs
        ), "Missing kwarg."

        splitting_factor = kwargs["splitting_factor"]
        buf = kwargs["buf"]
        req_kwargs = kwargs["req_kwargs"]

        context = context or {}
        logger = context.get("logger", self._logger)
        stream_epochs_record = context.get("stream_epochs_record")

        splitted = sorted(
            _split_stream_epoch(
                stream_epoch,
                num=splitting_factor,
                default_endtime=self._endtime,
            )
        )
        if stream_epochs_record:
            # keep track of stream epochs attempting to download
            idx = stream_epochs_record.index(stream_epoch)
            stream_epochs_record.pop(idx)
            for i in range(len(splitted)):
                stream_epochs_record.insert(i + idx, splitted[i])

            logger.debug(
                f"Splitting {stream_epoch!r} "
                f"(splitting_factor={splitting_factor}). "
                f"Stream epochs after splitting: {stream_epochs_record!r}"
            )

        await self._run(
            url,
            splitted,
            req_method=kwargs["req_method"],
            buf=buf,
            splitting_factor=splitting_factor,
            context=context,
            **req_kwargs,
        )

    async def _buffer_response(self, resp, buf, context, **kwargs):
        """
        Template coro.
        """
        raise NotImplementedError

    async def _flush(self, buf, drain, context, append=True):
        """
        Write ``buf`` to ``drain``.
        """
        await buf.seek(0)

        while True:
            chunk = await buf.read(context.get("chunk_size", -1))

            if not chunk:
                break

            await drain.drain(chunk)


class NetworkLevelMixin:
    """
    Mixin providing facilities for worker implementations operating at network
    level granularity
    """

    async def _fetch(
        self,
        route,
        parser_cb=None,
        req_method="GET",
        context=None,
        **kwargs,
    ):
        parser_cb = _coroutine_or_raise(parser_cb)
        # context logging
        try:
            logger = make_context_logger(self._logger, *context["logger_ctx"])
        except (TypeError, KeyError):
            logger = self.logger

        req_handler = self.REQUEST_HANDLER_CLS(
            **route._asdict(),
            query_params=self.query_params,
            headers=self.request_headers,
        )
        req_handler.format = self.format

        req = getattr(req_handler, req_method.lower())(self._session)

        self._log_request(req_handler, req_method, logger=logger)
        resp_status = None

        try:
            async with req(**kwargs) as resp:
                resp_status = resp.status
                resp.raise_for_status()

                msg = (
                    f"Response: {resp.reason}: resp.status={resp_status}, "
                    f"resp.request_info={resp.request_info}, "
                    f"resp.url={resp.url}, resp.headers={resp.headers}"
                )
                if resp_status != 200:
                    if resp_status in FDSNWS_NO_CONTENT_CODES:
                        logger.info(msg)
                    else:
                        await self.handle_error(msg=msg, context=context)

                    return route, None

                logger.debug(msg)
                if parser_cb is None:
                    return route, await resp.read()

                return route, await parser_cb(resp)

        except aiohttp.ClientResponseError as err:
            resp_status = err.status
            msg = (
                f"Error while executing request: {err.message}: "
                f"error={type(err)}, resp.status={resp_status}, "
                f"resp.request_info={err.request_info}, "
                f"resp.headers={err.headers}"
            )

            if resp_status == 413:
                await self.handle_413(context=context)
            elif resp_status in FDSNWS_NO_CONTENT_CODES:
                logger.info(msg)
            # https://github.com/aio-libs/aiohttp/issues/3641
            elif (
                resp_status == 400 and "invalid constant string" == err.message
            ):
                resp_status = 204
                logger.info(
                    "Excess found in read (reset status code to "
                    f"{resp_status}). Original aiohttp error: {msg}"
                )
            else:
                await self.handle_error(msg=msg, context=context)

            return route, None

        except (aiohttp.ClientError, asyncio.TimeoutError) as err:
            msg = (
                f"Error while executing request: error={type(err)}, "
                f"req_handler={req_handler!r}, method={req_method}"
            )
            if isinstance(err, aiohttp.ClientOSError):
                msg += f", errno={err.errno}"
            await self.handle_error(msg=msg, context=context)

            resp_status = 503
            return route, None
        finally:
            if resp_status is not None:
                await self.update_cretry_budget(req_handler.url, resp_status)
