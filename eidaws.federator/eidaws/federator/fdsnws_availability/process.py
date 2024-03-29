# -*- coding: utf-8 -*-

import asyncio
from eidaws.federator.utils.httperror import FDSNHTTPError
from eidaws.federator.utils.misc import create_job_context
from eidaws.federator.utils.process import group_routes_by, SortedResponse
from eidaws.federator.version import __version__
from eidaws.federator.utils.worker import (
    with_exception_handling,
    BaseWorker,
    NetworkLevelMixin,
)
from eidaws.utils.misc import make_context_logger, Route
from eidaws.utils.sncl import none_as_max, max_as_none, StreamEpoch


# TODO(damb): Implement on-the-fly merging of physically distributed data.


class AvailabilityWorker(NetworkLevelMixin, BaseWorker):
    @with_exception_handling(ignore_runtime_exception=True)
    async def run(
        self,
        route,
        net,
        priority,
        req_method="GET",
        context=None,
        **req_kwargs,
    ):
        context = context or {}
        # context logging
        try:
            logger = make_context_logger(self._logger, *context["logger_ctx"])
        except (TypeError, KeyError):
            logger = self.logger
        finally:
            context["logger"] = logger

        _buffer = {}

        logger.debug(f"Fetching data for network: {net!r}")
        # granular request strategy
        tasks = [
            self._fetch(
                _route,
                parser_cb=self._parse_response,
                req_method=req_method,
                context={
                    "logger_ctx": create_job_context(
                        self.request, parent_ctx=context.get("logger_ctx")
                    )
                },
                **req_kwargs,
            )
            for _route in route
        ]
        results = await asyncio.gather(*tasks, return_exceptions=False)

        logger.debug(f"Processing data for network: {net!r}")
        for _route, data in results:
            if not data:
                continue

            se = _route.stream_epochs[0]
            _buffer[se.id()] = data

        if _buffer:
            serialized = self._dump(_buffer)
            await self._drain.drain((priority, serialized))

        await self.finalize()

    async def _parse_response(self, resp):
        if resp is None:
            return None

        return self._load(await resp.read())

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
            ctx = {"logger_ctx": create_job_context(self.request)}
            self.logger.debug(
                f"Creating job: context={ctx!r}, priority={priority}, "
                f"network={net!r}, route={_routes!r}"
            )
            await pool.submit(
                _routes,
                net,
                priority,
                req_method=req_method,
                context=ctx,
                **req_kwargs,
            )
