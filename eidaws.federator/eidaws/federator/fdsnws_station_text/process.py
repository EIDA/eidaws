# -*- coding: utf-8 -*-
import asyncio
import aiohttp

from aiohttp import web

from eidaws.federator.settings import FED_BASE_ID, FED_STATION_TEXT_SERVICE_ID
from eidaws.federator.utils.request import FdsnRequestHandler
from eidaws.federator.utils.httperror import FDSNHTTPError
from eidaws.federator.utils.misc import _callable_or_raise
from eidaws.federator.utils.mixin import CachingMixin
from eidaws.federator.utils.process import (
    cached,
    BaseRequestProcessor,
    RequestProcessorError,
)
from eidaws.federator.version import __version__
from eidaws.utils.settings import FDSNWS_NO_CONTENT_CODES


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

    QUERY_FORMAT = "text"

    def __init__(self, request, url_routing, **kwargs):
        super().__init__(
            request, url_routing, **kwargs,
        )

        self._config = self.request.app["config"][FED_STATION_TEXT_SERVICE_ID]
        self._level = self.query_params.get("level", "station")

        self._lock = asyncio.Lock()

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
    def client_retry_budget_threshold(self):
        return self._config["client_retry_budget_threshold"]

    async def _fetch(self, queue, session, response, method="GET", **kwargs):
        """
        A worker task which both fetches data and writes the results to the
        ``response`` object.
        """

        while True:
            req_handler = await queue.get()

            req = (
                req_handler.get(session)
                if method == "GET"
                else req_handler.post(session)
            )

            try:
                resp = await req(**kwargs)
            except (aiohttp.ClientError, asyncio.TimeoutError) as err:
                self.logger.warning(
                    f"Error while executing request: error={type(err)}, "
                    f"url={req_handler.url}, method={method}"
                )

                try:
                    await self.update_cretry_budget(req_handler.url, 503)
                except Exception:
                    pass
                finally:
                    queue.task_done()

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
                queue.task_done()
                continue
            else:
                if resp.status != 200:
                    if resp.status in FDSNWS_NO_CONTENT_CODES:
                        self.logger.info(msg)
                    else:
                        self.logger.warning(msg)

                    queue.task_done()
                    continue

            self.logger.debug(msg)
            data = await self._parse_resp(resp)

            if data is not None:
                async with self._lock:
                    if not response.prepared:
                        response.content_type = self.content_type
                        response.charset = self.charset
                        await response.prepare(self.request)

                        header = self._HEADER_MAP[self._level]
                        await response.write(header)
                        await response.write(b"\n")
                        self.dump_to_cache_buffer(header + b"\n")

                    await response.write(data)

                    self.dump_to_cache_buffer(data)

            try:
                await self.update_cretry_budget(req_handler.url, resp.status)
            except Exception:
                pass

            queue.task_done()

    async def _dispatch(self, queue, routing_table, **kwargs):
        """
        Dispatch requests.
        """

        for url, stream_epochs in routing_table.items():
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
                    continue

            # granular request strategy
            for se in stream_epochs:
                self.logger.debug(
                    f"Creating job: URL={url}, stream_epochs={se!r}"
                )

                req_handler = FdsnRequestHandler(url, [se], self.query_params)
                req_handler.format = self.QUERY_FORMAT

                await queue.put(req_handler)

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

    async def _make_response(
        self,
        routing_table,
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
                task = asyncio.create_task(
                    self._fetch(queue, session, response, **kwargs)
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
