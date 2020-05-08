# -*- coding: utf-8 -*-

import asyncio
import aiohttp

from eidaws.federator.settings import (
    FED_BASE_ID,
    FED_STATION_TEXT_FORMAT,
    FED_STATION_TEXT_SERVICE_ID,
)
from eidaws.federator.utils.request import FdsnRequestHandler
from eidaws.federator.utils.process import BaseRequestProcessor
from eidaws.federator.utils.worker import (
    with_exception_handling,
    BaseAsyncWorker,
)
from eidaws.utils.settings import FDSNWS_NO_CONTENT_CODES


class _StationTextAsyncWorker(BaseAsyncWorker):
    """
    A worker task which fetches data and writes the results to the ``response``
    object.
    """

    SERVICE_ID = FED_STATION_TEXT_SERVICE_ID

    LOGGER = ".".join([FED_BASE_ID, SERVICE_ID, "worker"])

    QUERY_FORMAT = FED_STATION_TEXT_FORMAT

    @with_exception_handling
    async def run(self, req_method="GET", **kwargs):

        while True:
            route, query_params = await self._queue.get()

            req_handler = FdsnRequestHandler(
                **route._asdict(), query_params=query_params
            )
            req_handler.format = self.QUERY_FORMAT

            req = getattr(req_handler, req_method.lower())(self._session)
            try:
                resp = await req(**kwargs)
            except (aiohttp.ClientError, asyncio.TimeoutError) as err:
                msg = (
                    f"Error while executing request: error={type(err)}, "
                    f"url={req_handler.url}, method={req_method}"
                )
                await self._handle_error(msg=msg)

                await self.update_cretry_budget(req_handler.url, 503)
                await self.finalize()
                continue

            data = await self._parse_resp(resp)

            if data is not None:
                async with self._lock:
                    if not self._response.prepared:

                        if self._prepare_callback is not None:
                            await self._prepare_callback(self._response)
                        else:
                            await self._response.prepare(self.request)

                    await self._response.write(data)

            await self.update_cretry_budget(req_handler.url, resp.status)
            await self.finalize()

    async def _parse_resp(self, resp):
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
                await self._handle_error(msg=msg)

            return None
        else:
            if resp.status != 200:
                if resp.status in FDSNWS_NO_CONTENT_CODES:
                    self.logger.info(msg)
                else:
                    await self._handle_error(msg=msg)

                return None
            else:
                self.logger.debug(msg)

        # XXX(damb): Read the entire response into memory
        try:
            text = await resp.read()
        except asyncio.TimeoutError as err:
            self.logger.warning(f"Socket read timeout: {type(err)}")
            return None
        else:
            # strip header
            return text[(text.find(b"\n") + 1) :]


class StationTextRequestProcessor(BaseRequestProcessor):

    SERVICE_ID = FED_STATION_TEXT_SERVICE_ID

    LOGGER = ".".join([FED_BASE_ID, SERVICE_ID, "process"])

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

    def __init__(self, request, **kwargs):
        super().__init__(
            request, **kwargs,
        )

        self._level = self.query_params.get("level", "station")

    @property
    def content_type(self):
        return "text/plain"

    @property
    def charset(self):
        return "utf-8"

    async def _prepare_response(self, response):
        response.content_type = self.content_type
        response.charset = self.charset
        await response.prepare(self.request)

        header = self._HEADER_MAP[self._level]
        await response.write(header + b"\n")

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

        async def dispatch(queue, routes, **kwargs):
            """
            Dispatch jobs.
            """

            # granular request strategy
            for route in routes:
                self.logger.debug(f"Creating job for route: {route!r}")

                job = (route, self.query_params)
                await queue.put(job)

        queue = asyncio.Queue()
        response = self.make_stream_response()
        lock = asyncio.Lock()

        await dispatch(queue, routes)

        async with aiohttp.ClientSession(
            connector=self.request.config_dict["endpoint_http_conn_pool"],
            timeout=timeout,
            connector_owner=False,
        ) as session:

            # create worker tasks
            for _ in range(self.pool_size):
                worker = _StationTextAsyncWorker(
                    self.request,
                    queue,
                    session,
                    response,
                    lock,
                    prepare_callback=self._prepare_response,
                )

                task = asyncio.create_task(
                    worker.run(req_method=req_method, **kwargs)
                )
                self._tasks.append(task)

            await self._join_with_exception_handling(queue, response)

            await response.write_eof()

            return response
