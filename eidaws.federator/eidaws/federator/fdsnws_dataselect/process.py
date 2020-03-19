# -*- coding: utf-8 -*-

import aiohttp
import asyncio
import datetime

from aiohttp import web
from concurrent.futures import ThreadPoolExecutor

from eidaws.federator.settings import (
    FED_BASE_ID,
    FED_DATASELECT_MINISEED_SERVICE_ID,
)
from eidaws.federator.utils.httperror import FDSNHTTPError
from eidaws.federator.utils.misc import _callable_or_raise
from eidaws.federator.utils.mixin import (
    CachingMixin,
    ClientRetryBudgetMixin,
)
from eidaws.federator.utils.process import (
    BaseRequestProcessor,
    RequestProcessorError,
    BaseAsyncWorker,
)
from eidaws.federator.utils.tempfile import AioSpooledTemporaryFile
from eidaws.federator.utils.request import FdsnRequestHandler
from eidaws.federator.version import __version__
from eidaws.utils.settings import FDSNWS_NO_CONTENT_CODES


_QUERY_FORMAT = "miniseed"


class _DataselectAsyncWorker(BaseAsyncWorker, ClientRetryBudgetMixin):
    """
    A worker task implementation operating on `StationXML
    <https://www.fdsn.org/xml/station/>`_ ``NetworkType`` ``BaseNodeType``
    element granularity.
    """

    SERVICE_ID = FED_DATASELECT_MINISEED_SERVICE_ID

    LOGGER = ".".join(
        [FED_BASE_ID, FED_DATASELECT_MINISEED_SERVICE_ID, "worker"]
    )

    MSEED_RECORD_SIZE = 512
    CHUNK_SIZE = MSEED_RECORD_SIZE * 8

    def __init__(
        self,
        request,
        queue,
        session,
        response,
        write_lock,
        prepare_callback=None,
        write_callback=None,
        **kwargs,
    ):
        super().__init__(request)

        self._queue = queue
        self._session = session
        self._response = response

        self._lock = write_lock
        self._prepare_callback = _callable_or_raise(prepare_callback)
        self._write_callback = _callable_or_raise(write_callback)

        self._endtime = kwargs.get("endtime", datetime.datetime.utcnow())

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

                req_handler = FdsnRequestHandler(
                    **route._asdict(), query_params=query_params
                )
                req_handler.format = _QUERY_FORMAT

                await self._run(
                    **route._asdict(),
                    query_params=query_params,
                    req_method=req_method,
                    executor=executor,
                )

    async def _run(
        self,
        url,
        stream_epochs,
        query_params,
        req_method,
        last_chunk=None,
        splitting_const=2,
        executor=None,
        **kwargs,
    ):

        for se in stream_epochs:

            req_handler = FdsnRequestHandler(
                url=url, stream_epochs=[se], query_params=query_params
            )
            req_handler.format = _QUERY_FORMAT

            req = (
                req_handler.get(self._session)
                if req_method == "GET"
                else req_handler.post(self._session)
            )

            try:
                resp = await req(**kwargs)
            except (aiohttp.ClientError, asyncio.TimeoutError) as err:
                self.logger.warning(
                    f"Error while executing request: error={type(err)}, "
                    f"url={req_handler.url}, method={req_method}"
                )

                try:
                    await self.update_cretry_budget(req_handler.url, 503)
                except Exception:
                    pass
                finally:
                    self._queue.task_done()

                continue

            msg = (
                f"Response: {resp.reason}: resp.status={resp.status}, "
                f"resp.request_info={resp.request_info}, "
                f"resp.url={resp.url}, resp.headers={resp.headers}"
            )

            try:
                resp.raise_for_status()
            except aiohttp.ClientResponseError:
                if resp.status == 413:
                    raise RequestProcessorError(
                        "HTTP code 413 handling not implemented."
                    )

                self.logger.warning(msg)
                self._queue.task_done()
                continue
            else:
                if resp.status != 200:
                    if resp.status in FDSNWS_NO_CONTENT_CODES:
                        self.logger.info(msg)
                    else:
                        self.logger.warning(msg)

                    self._queue.task_done()
                    continue

            req_id = self.request[FED_BASE_ID + ".request_id"]

            async with AioSpooledTemporaryFile(
                max_size=self.config["buffer_rollover_size"],
                prefix=str(req_id) + ".",
                dir=self.config["tempdir"],
                executor=executor,
            ) as buf:

                await self._write_response_to_buffer(
                    buf, resp, executor=executor
                )

                async with self._lock:
                    if not self._response.prepared:

                        if self._prepare_callback is not None:
                            await self._prepare_callback(self._response)
                        else:
                            await self._response.prepare(self.request)

                    await self._write_buffer_to_response(
                        buf, self._response, executor=executor
                    )

            try:
                await self.update_cretry_budget(req_handler.url, resp.status)
            except Exception:
                pass

        self._queue.task_done()

    async def _write_response_to_buffer(self, buf, resp, executor):
        while True:
            try:
                chunk = await resp.content.read(self.CHUNK_SIZE)
            except asyncio.TimeoutError as err:
                self.logger.warning(f"Socket read timeout: {type(err)}")
                break

            if not chunk:
                break

            await buf.write(chunk)

    async def _write_buffer_to_response(self, buf, resp, executor):
        await buf.seek(0)

        while True:
            chunk = await buf.read(self.CHUNK_SIZE)

            if not chunk:
                break

            await resp.write(chunk)

            if self._write_callback is not None:
                self._write_callback(chunk)


BaseAsyncWorker.register(_DataselectAsyncWorker)


class DataselectRequestProcessor(BaseRequestProcessor, CachingMixin):

    SERVICE_ID = FED_DATASELECT_MINISEED_SERVICE_ID

    LOGGER = ".".join([FED_BASE_ID, SERVICE_ID, "process"])

    def __init__(self, request, url_routing, **kwargs):
        super().__init__(
            request, url_routing, **kwargs,
        )

    @property
    def content_type(self):
        return "application/vnd.fdsn.mseed"

    async def _prepare_response(self, response):
        response.content_type = self.content_type
        response.headers["Content-Disposition"] = (
            'attachment; filename="'
            + FED_BASE_ID.replace(".", "-")
            + "-"
            + datetime.datetime.utcnow().isoformat()
            + '.mseed"'
        )
        await response.prepare(self.request)

    async def _dispatch(self, queue, routes, **kwargs):
        """
        Dispatch jobs.
        """

        # granular request strategy
        for route in routes:
            self.logger.debug(f"Creating job for route: {route!r}")

            job = (route, self.query_params)
            await queue.put(job)

    async def _make_response(
        self,
        routes,
        req_method="GET",
        timeout=aiohttp.ClientTimeout(
            connect=None, sock_connect=2, sock_read=30
        ),
        **kwargs,
    ):
        """
        Return a federated response.
        """

        queue = asyncio.Queue()
        response = web.StreamResponse()

        lock = asyncio.Lock()

        await self._dispatch(queue, routes)

        async with aiohttp.ClientSession(
            connector=self.request.config_dict["endpoint_http_conn_pool"],
            timeout=timeout,
            connector_owner=False,
        ) as session:

            pool_size = (
                self.pool_size or self.config["endpoint_connection_limit"]
            )
            for _ in range(pool_size):
                worker = _DataselectAsyncWorker(
                    self.request,
                    queue,
                    session,
                    response,
                    lock,
                    endtime=self._default_endtime,
                    prepare_callback=self._prepare_response,
                    # avoid gzip encoding when writing data
                    write_callback=self.dump_to_cache_buffer,
                )

                task = asyncio.create_task(
                    worker.run(req_method=req_method, **kwargs)
                )
                self._tasks.append(task)

            await queue.join()

            if not response.prepared:
                raise FDSNHTTPError.create(
                    self.nodata,
                    self.request,
                    request_submitted=self.request_submitted,
                    service_version=__version__,
                )

            await response.write_eof()

            return response
