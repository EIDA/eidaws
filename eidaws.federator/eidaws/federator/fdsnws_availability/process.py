# -*- coding: utf-8 -*-

import asyncio
import aiohttp

from eidaws.federator.utils.httperror import FDSNHTTPError
from eidaws.federator.utils.process import group_routes_by, SortedResponse
from eidaws.federator.utils.request import FdsnRequestHandler
from eidaws.federator.version import __version__
from eidaws.federator.utils.worker import (
    with_context_logging,
    with_exception_handling,
    BaseWorker,
    NetworkLevelMixin,
)
from eidaws.utils.misc import Route
from eidaws.utils.settings import FDSNWS_NO_CONTENT_CODES
from eidaws.utils.sncl import none_as_max, max_as_none, StreamEpoch


# TODO(damb): Implement on-the-fly merging of physically distributed data.


class AvailabilityWorker(NetworkLevelMixin, BaseWorker):
    def __init__(
        self, request, session, drain, lock=None, **kwargs,
    ):
        super().__init__(
            request, session, drain, lock=lock, **kwargs,
        )

        self._buf = {}

    @with_context_logging()
    @with_exception_handling(ignore_runtime_exception=True)
    async def run(self, route, net, priority, req_method="GET", **req_kwargs):

        self.logger.debug(f"Fetching data for network: {net}")
        job_ctx = self.create_job_context(route)

        # granular request strategy
        tasks = [
            self._fetch(
                _route, req_method=req_method, parent_ctx=job_ctx, **req_kwargs
            )
            for _route in route
        ]

        results = await asyncio.gather(*tasks, return_exceptions=False)

        for _route, resp in results:
            data = await self._parse_response(resp)

            if not data:
                continue

            se = _route.stream_epochs[0]
            self._buf[se.id()] = data

        if self._buf:
            serialized = self._dump(self._buf)
            await self._drain.drain((priority, serialized))

        await self.finalize()

    async def finalize(self):
        self._buf = {}

    async def _parse_response(self, resp):
        if resp is None:
            return None

        try:
            data = await resp.read()
        except asyncio.TimeoutError as err:
            self.logger.warning(f"Socket read timeout: {type(err)}")
            return None
        else:
            return self._load(data)

    def _load(self, data, **kwargs):
        raise NotImplementedError

    def _dump(self, obj, **kwargs):
        raise NotImplementedError


class AvailabilityRequestProcessor(SortedResponse):
    @property
    def merge(self):
        return self.query_params.get("merge", [])

    async def _dispatch(self, pool, routes, req_method, **req_kwargs):
        """
        Dispatch jobs onto ``pool``.

        .. note::
            Routes are post-processed i.e. reduced to their extent w.r.t. time
            contstraints.
        """

        # XXX(damb): Currently, orderby=nslc_time_quality_samplerate (default)
        # is the only sort order implemented

        def reduce_to_extent(routes):

            grouped = group_routes_by(
                routes, key="network.station.location.channel"
            )

            reduced = []
            for group_key, routes in grouped.items():

                urls = set()
                _stream = None
                ts = set()
                for r in routes:
                    assert (
                        len(r.stream_epochs) == 1
                    ), "granular routes required"

                    urls.add(r.url)
                    se_orig = r.stream_epochs[0]
                    _stream = se_orig.stream
                    with none_as_max(se_orig.endtime) as end:
                        ts.add(se_orig.starttime)
                        ts.add(end)

                with max_as_none(max(ts)) as end:
                    se = StreamEpoch(_stream, starttime=min(ts), endtime=end)
                    reduced.append(Route(url=r.url, stream_epochs=[se]))

                # do not allow distributed stream epochs; would require
                # on-the-fly de-/serialization
                if len(urls) != 1:
                    raise ValueError("Distributed stream epochs not allowed.")

            return reduced

        grouped_routes = group_routes_by(routes, key="network")
        for net, _routes in grouped_routes.items():
            try:
                grouped_routes[net] = reduce_to_extent(_routes)
            except ValueError:
                raise FDSNHTTPError.create(
                    self.nodata,
                    self.request,
                    request_submitted=self.request_submitted,
                    service_version=__version__,
                )

        _sorted = sorted(grouped_routes)
        for priority, net in enumerate(_sorted):
            _routes = grouped_routes[net]

            self.logger.debug(
                f"Creating job: priority={priority}, network={net}, "
                f"route={_routes!r}"
            )

            await pool.submit(
                _routes, net, priority, req_method=req_method, **req_kwargs,
            )
