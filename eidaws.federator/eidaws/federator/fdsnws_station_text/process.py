# -*- coding: utf-8 -*-

import asyncio
import aiohttp

from aiohttp import web

from eidaws.federator.settings import FED_BASE_ID, FED_STATION_TEXT_SERVICE_ID
from eidaws.federator.utils.request import FdsnRequestHandler
from eidaws.federator.utils.httperror import FDSNHTTPError
from eidaws.federator.utils.misc import _callable_or_raise, Route
from eidaws.federator.utils.mixin import CachingMixin, ClientRetryBudgetMixin
from eidaws.federator.utils.process import (
    _duration_to_timedelta,
    BaseRequestProcessor,
    RequestProcessorError,
    BaseAsyncWorker,
)
from eidaws.federator.version import __version__
from eidaws.utils.settings import FDSNWS_NO_CONTENT_CODES


_QUERY_FORMAT = "text"


class _StationTextAsyncWorker(BaseAsyncWorker, ClientRetryBudgetMixin):
    """
    A worker task which fetches data and writes the results to the ``response``
    object.
    """

    LOGGER = ".".join([FED_BASE_ID, FED_STATION_TEXT_SERVICE_ID, "worker"])

    def __init__(
        self,
        request,
        queue,
        session,
        response,
        write_lock,
        prepare_callback=None,
        write_callback=None,
    ):
        super().__init__(request)

        self._queue = queue
        self._session = session
        self._response = response

        self._lock = write_lock
        self._prepare_callback = _callable_or_raise(prepare_callback)
        self._write_callback = _callable_or_raise(write_callback)

    async def run(self, req_method="GET", **kwargs):

        while True:
            route, query_params = await self._queue.get()
            req_handler = FdsnRequestHandler(
                **route._asdict(), query_params=query_params
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

            self.logger.debug(msg)
            data = await self._parse_resp(resp)

            if data is not None:
                async with self._lock:
                    if not self._response.prepared:

                        if self._prepare_callback is not None:
                            await self._prepare_callback(self._response)
                        else:
                            await self._response.prepare(self.request)

                    await self._response.write(data)

                    if self._write_callback is not None:
                        self._write_callback(data)

            try:
                await self.update_cretry_budget(req_handler.url, resp.status)
            except Exception:
                pass

            self._queue.task_done()

    async def _parse_resp(self, resp):
        # XXX(damb): Read the entire response into memory
        try:
            text = await resp.read()
        except asyncio.TimeoutError as err:
            self.logger.warning(f"Socket read timeout: {type(err)}")
            return None
        else:
            # strip header
            return text[(text.find(b"\n") + 1) :]


BaseAsyncWorker.register(_StationTextAsyncWorker)


class StationTextRequestProcessor(BaseRequestProcessor, CachingMixin):

    LOGGER = ".".join([FED_BASE_ID, FED_STATION_TEXT_SERVICE_ID, "process"])

    _HEADER_MAP = {
        "network": b"#Network|Description|StartTime|EndTime|TotalStations",
        "station": (
            b"#Network|Station|Latitude|Longitude|"
            b"Elevation|SiteName|StartTime|EndTime"
        ),
        "channel": (
            b"#Network|Station|Location|Channel|Latitude|"
            b"Longitude|Elevation|Depth|Azimuth|Dip|"
            b"SensorDescription|Scale|ScaleFreq|ScaleUnits|"
            b"SampleRate|StartTime|EndTime"
        ),
    }

    def __init__(self, request, url_routing, **kwargs):
        super().__init__(
            request, url_routing, **kwargs,
        )

        self._config = self.request.app["config"][FED_STATION_TEXT_SERVICE_ID]
        self._level = self.query_params.get("level", "station")

    @property
    def content_type(self):
        return "text/plain"

    @property
    def charset(self):
        return "utf-8"

    @property
    def pool_size(self):
        return self._config["pool_size"]

    @property
    def max_stream_epoch_duration(self):
        return _duration_to_timedelta(
            days=self._config["max_stream_epoch_duration"]
        )

    @property
    def max_total_stream_epoch_duration(self):
        return _duration_to_timedelta(
            days=self._config["max_total_stream_epoch_duration"]
        )

    @property
    def client_retry_budget_threshold(self):
        return self._config["client_retry_budget_threshold"]

    async def _prepare_response(self, response):
        response.content_type = self.content_type
        response.charset = self.charset
        await response.prepare(self.request)

        header = self._HEADER_MAP[self._level]
        await response.write(header + b"\n")
        self.dump_to_cache_buffer(header + b"\n")

    async def _dispatch(self, queue, routing_table, **kwargs):
        """
        Dispatch jobs.
        """

        for url, stream_epochs in routing_table.items():
            # granular request strategy
            for se in stream_epochs:
                self.logger.debug(
                    f"Creating job: URL={url}, stream_epochs={se!r}"
                )

                job = (
                    Route(url=url, stream_epochs=[se]),
                    self.query_params,
                )
                await queue.put(job)

    async def _make_response(
        self,
        routing_table,
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

        await self._dispatch(queue, routing_table)

        async with aiohttp.ClientSession(
            connector=self.request.app["endpoint_http_conn_pool"],
            timeout=timeout,
            connector_owner=False,
        ) as session:

            # create worker tasks
            pool_size = (
                self.pool_size
                or self._config["endpoint_connection_limit"]
                or queue.qsize()
            )

            for _ in range(pool_size):
                worker = _StationTextAsyncWorker(
                    self.request,
                    queue,
                    session,
                    response,
                    lock,
                    prepare_callback=self._prepare_response,
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
