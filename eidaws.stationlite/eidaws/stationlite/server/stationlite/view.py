# -*- coding: utf-8 -*-

import logging

from flask import request, make_response, Response
from flask.views import MethodView
from webargs.flaskparser import use_args

from eidaws.stationlite.core.query import query_stationlite
from eidaws.stationlite.core.utils import ChannelEpochsHandler
from eidaws.stationlite.server.db import db
from eidaws.stationlite.server.http_error import FDSNHTTPError
from eidaws.stationlite.server.parser import (
    use_fdsnws_args,
    use_fdsnws_kwargs,
    StreamEpochSchema,
    ManyStreamEpochSchema,
)
from eidaws.stationlite.server.stationlite.parser import StationLiteSchema
from eidaws.stationlite.server.strict import with_strict_args
from eidaws.stationlite.version import __version__
from eidaws.utils.settings import FDSNWS_DEFAULT_NO_CONTENT_ERROR_CODE


class StationLiteVersionResource(MethodView):
    """
    ``version`` resource implementation for eidaws-stationlite *stationlite*
    """

    def get(self):
        return make_response(
            __version__, {"Content-Type": "text/plain; charset=utf-8"}
        )

    post = get


class StationLiteQueryResource(MethodView):
    """
    ``query`` resource implementation for eidaws-stationlite *stationlite*
    """

    LOGGER = "eidaws.stationlite.stationlite_resource"

    def __init__(self):
        super().__init__()
        self.logger = logging.getLogger(self.LOGGER)

    @use_args(StationLiteSchema(), locations=("query",))
    @use_fdsnws_kwargs(
        ManyStreamEpochSchema(context={"request": request}),
        locations=("query",),
    )
    @with_strict_args(
        (StreamEpochSchema, StationLiteSchema),
        locations=("query",),
    )
    def get(self, args, stream_epochs):
        """
        Process an eidaws-stationlite *stationlite* HTTP GET request.
        """
        return self._make_response(args, stream_epochs)

    @use_fdsnws_args(StationLiteSchema(), locations=("form",))
    @use_fdsnws_kwargs(
        ManyStreamEpochSchema(context={"request": request}),
        locations=("form",),
    )
    @with_strict_args(StationLiteSchema, locations=("form",))
    def post(self, args, stream_epochs):
        """
        Process an eidaws-stationlite *stationlite* HTTP POST request.
        """
        return self._make_response(args, stream_epochs)

    def _make_response(self, args, stream_epochs):
        self.logger.debug(f"StationLiteSchema: {args}")
        self.logger.debug(f"StreamEpoch objects: {stream_epochs}")

        cha_epochs_handler = self._process_request(
            args,
            stream_epochs,
        )

        if not cha_epochs_handler:
            raise FDSNHTTPError.create(
                int(args.get("nodata", FDSNWS_DEFAULT_NO_CONTENT_ERROR_CODE))
            )

        def generate_resp(cha_epochs_handler):
            first = True
            yield "["
            for cha_epoch in cha_epochs_handler:
                if first:
                    first = False
                else:
                    yield ", "
                yield cha_epoch.jsonify()
            yield "]"

        return Response(
            generate_resp(cha_epochs_handler), content_type="application/json"
        )

    def _process_request(self, args, stream_epochs):
        cha_epochs_handler = ChannelEpochsHandler()
        for stream_epoch in stream_epochs:
            query_stationlite(
                db.session,
                stream_epoch,
                cha_epochs_handler,
                merge=args["merge"],
            )

        return cha_epochs_handler
