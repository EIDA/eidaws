# -*- coding: utf-8 -*-

import collections
import logging
import socket

from cached_property import cached_property
from flask import request, make_response, render_template
from flask.views import MethodView
from webargs.flaskparser import use_args

from eidaws.stationlite.server.parser import (
    use_fdsnws_args,
    use_fdsnws_kwargs,
    StreamEpochSchema,
    ManyStreamEpochSchema,
)
from eidaws.stationlite.server.routing.parser import RoutingSchema
from eidaws.stationlite.server.routing.stream import OutputStream
from eidaws.stationlite.server.strict import with_strict_args
from eidaws.stationlite.version import __version__
from eidaws.utils.sncl import (
    generate_stream_epochs,
    StreamEpochsHandler,
    StreamEpoch,
)
from eidaws.utils.settings import (
    EIDAWS_ROUTING_PATH_QUERY,
    FDSNWS_DEFAULT_NO_CONTENT_ERROR_CODE,
)
from eidaws.utils.misc import Route

from eidaws.stationlite.core.query import (
    resolve_vnetwork,
    query_routes,
)
from eidaws.stationlite.server.db import db
from eidaws.stationlite.server.http_error import FDSNHTTPError


class RoutingVersionResource(MethodView):
    """
    ``version`` resource implementation for eidaws-stationlite *routing*
    """

    def get(self):
        return make_response(
            __version__, {"Content-Type": "text/plain; charset=utf-8"}
        )

    post = get


class RoutingWadlResource(MethodView):
    """
    ``application.wadl`` resource implementation for eidaws-stationlite
    *routing*
    """

    @cached_property
    def wadl(self):
        return render_template(
            "routing.wadl",
            url=f"http://{socket.getfqdn()}{EIDAWS_ROUTING_PATH_QUERY}",
        )

    def get(self):
        return make_response(self.wadl, {"Content-Type": "application/xml"})

    post = get


class RoutingQueryResource(MethodView):
    """
    ``query`` resource implementation for eidaws-stationlite *routing*
    """

    LOGGER = "eidaws.stationlite.routing_resource"

    def __init__(self):
        super().__init__()
        self.logger = logging.getLogger(self.LOGGER)

    @use_args(RoutingSchema(), locations=("query",))
    @use_fdsnws_kwargs(
        ManyStreamEpochSchema(context={"request": request}),
        locations=("query",),
    )
    @with_strict_args(
        (StreamEpochSchema, RoutingSchema),
        locations=("query",),
    )
    def get(self, args, stream_epochs):
        """
        Process an eidaws-stationlite *routing* HTTP GET request.
        """
        return self._make_response(args, stream_epochs)

    @use_fdsnws_args(RoutingSchema(), locations=("form",))
    @use_fdsnws_kwargs(
        ManyStreamEpochSchema(context={"request": request}),
        locations=("form",),
    )
    @with_strict_args(RoutingSchema, locations=("form",))
    def post(self, args, stream_epochs):
        """
        Process an eidaws-stationlite *routing* HTTP POST request.
        """
        return self._make_response(args, stream_epochs)

    def _make_response(self, args, stream_epochs):
        self.logger.debug(f"RoutingSchema: {args}")
        self.logger.debug(f"StreamEpoch objects: {stream_epochs}")

        payload = self._process_request(
            args,
            stream_epochs,
        )

        if not payload:
            raise FDSNHTTPError.create(
                int(args.get("nodata", FDSNWS_DEFAULT_NO_CONTENT_ERROR_CODE))
            )

        return make_response(
            payload, {"Content-Type": "text/plain; charset=utf-8"}
        )

    def _process_request(self, args, stream_epochs):
        # resolve virtual network stream epochs
        vnet_stream_epochs_found = []
        vnet_stream_epochs_resolved = []
        for stream_epoch in stream_epochs:
            self.logger.debug(f"Resolving {stream_epoch!r} regarding VNET.")
            resolved = resolve_vnetwork(db.session, stream_epoch)
            if resolved:
                vnet_stream_epochs_resolved.extend(resolved)
                vnet_stream_epochs_found.append(stream_epoch)

        self.logger.debug(
            f"Stream epochs from VNETs: {vnet_stream_epochs_resolved!r}"
        )

        for vnet_stream_epoch in vnet_stream_epochs_found:
            stream_epochs.remove(vnet_stream_epoch)

        stream_epochs.extend(vnet_stream_epochs_resolved)

        # NOTE(damb): Do neither merge epochs nor trim to query epoch if
        # service == "station"
        merge_epochs = trim_to_stream_epoch = args["service"] != "station"
        canonicalize_epochs = args["service"] == "station"

        # collect results for each stream epoch
        routes = []
        for stream_epoch in stream_epochs:
            self.logger.debug(f"Processing request for {stream_epoch!r}")
            # query
            _routes = query_routes(
                db.session,
                stream_epoch,
                args["service"],
                level=args["level"],
                access=args["access"],
                method=args["method"],
                minlat=args["minlatitude"],
                maxlat=args["maxlatitude"],
                minlon=args["minlongitude"],
                maxlon=args["maxlongitude"],
                trim_to_stream_epoch=trim_to_stream_epoch,
            )

            if trim_to_stream_epoch:
                # adjust stream epochs regarding time constraints
                for url, stream_epochs_handler in _routes:
                    stream_epochs_handler.modify_with_temporal_constraints(
                        start=stream_epoch.starttime, end=stream_epoch.endtime
                    )
            elif canonicalize_epochs:
                # canonicalize epochs
                for url, stream_epochs_handler in _routes:
                    stream_epochs_handler.canonicalize_epochs(
                        start=stream_epoch.starttime, end=stream_epoch.endtime
                    )

            routes.extend(_routes)

        self.logger.debug(f"StationLite routes: {routes}")

        # merge routes
        processed_routes = collections.defaultdict(StreamEpochsHandler)
        for url, stream_epochs_handler in routes:
            for stream_epochs in generate_stream_epochs(
                stream_epochs_handler, merge_epochs=merge_epochs
            ):
                for se in stream_epochs:
                    processed_routes[url].add(se)

        self.logger.debug(
            f"StationLite routes (processed): {processed_routes}"
        )
        # demux
        for url, stream_epochs_handler in processed_routes.items():
            if args["level"] in ("network", "station"):
                processed_routes[url] = [
                    StreamEpoch.from_streamepochs(stream_epochs)
                    for stream_epochs in stream_epochs_handler
                ]
            else:
                processed_routes[url] = [
                    stream_epoch
                    for stream_epochs in generate_stream_epochs(
                        stream_epochs_handler, merge_epochs=merge_epochs
                    )
                    for stream_epoch in stream_epochs
                ]

        # sort response
        routes = [
            Route(url=url, stream_epochs=sorted(stream_epochs))
            for url, stream_epochs in processed_routes.items()
        ]

        # sort additionally by URL
        routes.sort()

        ostream = OutputStream.create(
            args["format"],
            routes=routes,
        )
        return str(ostream)
