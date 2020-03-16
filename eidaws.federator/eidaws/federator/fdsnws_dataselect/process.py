# -*- coding: utf-8 -*-

import aiohttp
import asyncio
import datetime

from aiohttp import web

from eidaws.federator.settings import (
    FED_BASE_ID,
    FED_DATASELECT_MINISEED_SERVICE_ID,
)
from eidaws.federator.utils.httperror import FDSNHTTPError
from eidaws.federator.utils.misc import _callable_or_raise, Route
from eidaws.federator.utils.mixin import CachingMixin, ClientRetryBudgetMixin
from eidaws.federator.utils.process import (
    _duration_to_timedelta,
    BaseRequestProcessor,
    RequestProcessorError,
    # BaseWorker,
)
from eidaws.federator.utils.request import FdsnRequestHandler
from eidaws.federator.version import __version__
from eidaws.utils.settings import FDSNWS_NO_CONTENT_CODES


_QUERY_FORMAT = "miniseed"


# class _DataselectWorker(BaseWorker, ClientRetryBudgetMixin):
#     """
#     A worker task implementation operating on `StationXML
#     <https://www.fdsn.org/xml/station/>`_ ``NetworkType`` ``BaseNodeType``
#     element granularity.
#     """

#     LOGGER = ".".join(
#         [FED_BASE_ID, FED_DATASELECT_MINISEED_SERVICE_ID, "worker"]
#     )

#     def __init__(
#         self,
#         request,
#         queue,
#         session,
#         response,
#         write_lock,
#         prepare_callback=None,
#         write_callback=None,
#     ):
#         super().__init__(request)

#         self._queue = queue
#         self._session = session
#         self._response = response

#         self._lock = write_lock
#         self._prepare_callback = _callable_or_raise(prepare_callback)
#         self._write_callback = _callable_or_raise(write_callback)

#     async def run(self, req_method="GET", **kwargs):

#         while True:
#             pass


# BaseWorker.register(_DataselectWorker)


class DataselectRequestProcessor(BaseRequestProcessor, CachingMixin):

    LOGGER = ".".join(
        [FED_BASE_ID, FED_DATASELECT_MINISEED_SERVICE_ID, "process"]
    )

    def __init__(self, request, url_routing, **kwargs):
        super().__init__(
            request, url_routing, **kwargs,
        )

        self._config = self.request.app["config"][
            FED_DATASELECT_MINISEED_SERVICE_ID
        ]

    @property
    def content_type(self):
        return "application/vnd.fdsn.mseed"

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
        # await response.prepare(self.request)

        # header = self.STATIONXML_HEADER.format(
        #     self.STATIONXML_SOURCE, datetime.datetime.utcnow().isoformat()
        # )
        # header = header.encode("utf-8")
        # await response.write(header)
        # self.dump_to_cache_buffer(header)

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

                # TODO TODO TODO
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
        pool_size = self.pool_size or self._config["endpoint_connection_limit"]

        # TODO TODO TODO
        # pool = DataselectThreadPoolExecutor(max_workers=pool_size)
        response = web.StreamResponse()
        await response.prepare(self.request)

        # lock = asyncio.Lock()

        # await self._dispatch(pool, routing_table)

        # async with aiohttp.ClientSession(
        #     connector=self.request.app["endpoint_http_conn_pool"],
        #     timeout=timeout,
        #     connector_owner=False,
        # ) as session:

        #     # create worker tasks
        #     pool_size = (
        #         self.pool_size
        #         or self._config["endpoint_connection_limit"]
        #         or queue.qsize()
        #     )

        #     for _ in range(pool_size):
        #         worker = _StationXMLAsyncWorker(
        #             self.request,
        #             queue,
        #             session,
        #             response,
        #             lock,
        #             prepare_callback=self._prepare_response,
        #             write_callback=self.dump_to_cache_buffer,
        #             level=self._level,
        #         )

        #         task = asyncio.create_task(
        #             worker.run(req_method=req_method, **kwargs)
        #         )
        #         self._tasks.append(task)

        #     await queue.join()

        #     if not response.prepared:
        #         raise FDSNHTTPError.create(
        #             self.nodata,
        #             self.request,
        #             request_submitted=self.request_submitted,
        #             service_version=__version__,
        #         )

        #     footer = self.STATIONXML_FOOTER.encode("utf-8")
        #     await response.write(footer)
        #     self.dump_to_cache_buffer(footer)

        await response.write_eof()

        return response
