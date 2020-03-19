# -*- coding: utf-8 -*-

import aiohttp
import asyncio
import copy
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
    BaseAsyncWorker,
)
from eidaws.federator.utils.tempfile import AioSpooledTemporaryFile
from eidaws.federator.utils.request import FdsnRequestHandler
from eidaws.federator.version import __version__
from eidaws.utils.settings import FDSNWS_NO_CONTENT_CODES


_QUERY_FORMAT = "miniseed"


def _split_stream_epoch(stream_epoch, num, default_endtime):
    return stream_epoch.slice(num=num, default_endtime=default_endtime)


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

        self._stream_epochs = []

        # TODO(damb): Check if chunk_size is a multiple of record_size.
        self._record_size = self.MSEED_RECORD_SIZE

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
                # TODO(damb): Check if there is enough space left on device.
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
                    )

                    if _sorted[-1].endtime == self._stream_epochs[-1].endtime:

                        if await buf.tell():

                            async with self._lock:
                                if not self._response.prepared:

                                    if self._prepare_callback is not None:
                                        await self._prepare_callback(
                                            self._response
                                        )
                                    else:
                                        await self._response.prepare(
                                            self.request
                                        )

                                await self._write_buffer_to_response(
                                    buf, self._response, executor=executor
                                )

                        self._queue.task_done()

    async def _run(
        self,
        url,
        stream_epochs,
        query_params,
        req_method,
        buf,
        splitting_const=2,
        last_record=None,
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
                        splitting_const=splitting_const,
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
                buf, resp, executor=executor, last_record=last_record,
            )

    async def _handle_413(self, url, stream_epoch, **kwargs):

        assert (
            "splitting_const" in kwargs
            and "query_params" in kwargs
            and "req_method" in kwargs
            and "buf" in kwargs
        ), "Missing kwarg."

        splitting_const = kwargs["splitting_const"]
        buf = kwargs["buf"]

        splitted = sorted(
            _split_stream_epoch(
                stream_epoch,
                num=splitting_const,
                default_endtime=self._endtime,
            )
        )
        # keep track of stream epochs attempting to download
        idx = self._stream_epochs.index(stream_epoch)
        self._stream_epochs.pop(idx)
        for i in range(len(splitted)):
            self._stream_epochs.insert(i + idx, splitted[i])

        self.logger.debug(
            f"Splitting {stream_epoch!r} (splitting_const={splitting_const}). "
            f"Stream epochs after splitting: {self._stream_epochs!r}"
        )

        last_record = None
        await buf.seek(0, 2)
        if await buf.tell() > self._record_size:
            await buf.seek(-self._record_size, 2)
            last_record = await buf.read(self._record_size)

        await self._run(
            url,
            splitted,
            query_params=kwargs["query_params"],
            req_method=kwargs["req_method"],
            buf=buf,
            last_record=last_record,
        )

    async def _write_response_to_buffer(
        self, buf, resp, executor, last_record=None
    ):
        while True:
            try:
                chunk = await resp.content.read(self.CHUNK_SIZE)
            except asyncio.TimeoutError as err:
                self.logger.warning(f"Socket read timeout: {type(err)}")
                break

            if not chunk:
                break

            # FIXME(damb): This might not work if record_size != chunk_size;
            # Note, that chunk_size must be a multiple of mseed record size.
            if last_record is not None and last_record in chunk:
                chunk = chunk[: -self._record_size]

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

    async def update_cretry_budget(self, url, code):
        try:
            await super().update_cretry_budget(url, code)
        except Exception:
            pass


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
