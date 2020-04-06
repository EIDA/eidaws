# -*- coding: utf-8 -*-

import aiohttp
import asyncio
import datetime
import errno
import json

from aiohttp import web

from eidaws.federator.settings import (
    FED_BASE_ID,
    FED_WFCATALOG_JSON_SERVICE_ID,
)
from eidaws.federator.utils.process import (
    _patch_response_write,
    BaseRequestProcessor,
)
from eidaws.federator.utils.worker import BaseSplitAlignAsyncWorker


_QUERY_FORMAT = "json"

_JSON_ARRAY_START = b"["
_JSON_ARRAY_END = b"]"
_JSON_SEP = b","


class _WFCatalogAsyncWorker(BaseSplitAlignAsyncWorker):
    """
    A worker task implementation for ``eidaws-wfcatalog`` ``format=json``.
    The worker implements splitting and aligning facilities.

    When splitting and aligning (i.e. merging potentially occurring overlaps)
    data is downloaded sequentially. Note, that a worker assumes JSON
    objects to be shipped ordered within the array.
    """

    SERVICE_ID = FED_WFCATALOG_JSON_SERVICE_ID

    LOGGER = ".".join([FED_BASE_ID, SERVICE_ID, "worker"])

    QUERY_FORMAT = _QUERY_FORMAT

    _CHUNK_SIZE = 8192

    async def _write_response_to_buffer(self, buf, resp):
        last_obj = None
        last_obj_length = 0

        await buf.seek(0, 2)
        if await buf.tell():
            # deserialize the last JSON object from buffer
            # XXX(damb): Assume that self._chunk_size >= last_obj_length
            try:
                await buf.seek(-self._chunk_size, 2)
            except OSError as err:
                if err.errno == errno.EINVAL:
                    await buf.seek(0)
                else:
                    raise

            stack = []
            chunk = await buf.read()

            for i, c in enumerate(reversed(chunk), start=1):
                if c == 125:  # b'}'
                    stack.append(c)
                elif c == 123:  # b'{'
                    stack.pop()

                last_obj_length = i

                if not stack:
                    break

            last_obj = json.loads(chunk[-last_obj_length:])

        first_chunk = True
        while True:
            try:
                chunk = await resp.content.read(self._chunk_size)
            except asyncio.TimeoutError as err:
                self.logger.warning(f"Socket read timeout: {type(err)}")
                break

            if not chunk:
                # chop off b']'
                await buf.truncate(await buf.tell() - 1)
                break

            if first_chunk:
                if last_obj is not None:
                    # deserialize the first JSON object from the chunk
                    try:
                        obj = json.loads(chunk[1 : last_obj_length + 1])
                    except json.JSONDecodeError:
                        obj = None

                    if obj is not None and last_obj == obj:
                        # chop off b'[' + first JSON object + b','
                        chunk = chunk[1 + last_obj_length + 1 :]

                    last_obj = None
                else:
                    # chop off b'['
                    chunk = chunk[1:]

                if await buf.tell():
                    await buf.write(_JSON_SEP)

                first_chunk = False

            await buf.write(chunk)

    async def _write_buffer_to_response(self, buf, resp, append=True):
        await buf.seek(0)

        if append:
            await resp.write(_JSON_SEP)

        while True:
            chunk = await buf.read(self._chunk_size)

            if not chunk:
                break

            await resp.write(chunk)


class WFCatalogRequestProcessor(BaseRequestProcessor):

    SERVICE_ID = FED_WFCATALOG_JSON_SERVICE_ID

    LOGGER = ".".join([FED_BASE_ID, SERVICE_ID, "process"])

    def __init__(self, request, url_routing, **kwargs):
        super().__init__(
            request, url_routing, **kwargs,
        )

    @property
    def content_type(self):
        return "application/json"

    async def _prepare_response(self, response):
        response.content_type = self.content_type
        response.headers["Content-Disposition"] = (
            'attachment; filename="'
            + FED_BASE_ID.replace(".", "-")
            + "-"
            + datetime.datetime.utcnow().isoformat()
            + '.json"'
        )
        await response.prepare(self.request)

        await response.write(_JSON_ARRAY_START)

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
        response = web.StreamResponse()
        _patch_response_write(response, self.dump_to_cache_buffer)

        lock = asyncio.Lock()

        await dispatch(queue, routes)

        async with aiohttp.ClientSession(
            connector=self.request.config_dict["endpoint_http_conn_pool"],
            timeout=timeout,
            connector_owner=False,
        ) as session:

            pool_size = (
                self.pool_size or self.config["endpoint_connection_limit"]
            )
            for _ in range(pool_size):
                worker = _WFCatalogAsyncWorker(
                    self.request,
                    queue,
                    session,
                    response,
                    lock,
                    endtime=self._default_endtime,
                    prepare_callback=self._prepare_response,
                    write_callback=self.dump_to_cache_buffer,
                )

                task = asyncio.create_task(
                    worker.run(req_method=req_method, **kwargs)
                )
                self._tasks.append(task)

            await self._join_with_exception_handling(queue, response)

            await response.write(_JSON_ARRAY_END)
            await response.write_eof()

            return response
