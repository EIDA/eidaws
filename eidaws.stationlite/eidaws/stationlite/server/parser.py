# -*- coding: utf-8 -*-

import datetime

from marshmallow import (
    validates_schema,
    ValidationError,
)
from webargs import core
from webargs.flaskparser import parser, FlaskParser

from eidaws.stationlite.server.http_error import FDSNHTTPError
from eidaws.stationlite.settings import STL_DEFAULT_CLIENT_MAX_SIZE
from eidaws.stationlite.version import __version__
from eidaws.utils.parser import FDSNWSParserMixin
from eidaws.utils.schema import (
    StreamEpochSchema as _StreamEpochSchema,
    _ManyStreamEpochSchema,
)


class StreamEpochSchema(_StreamEpochSchema):
    @validates_schema
    def validate_temporal_constraints(self, data, **kwargs):
        """
        Validation of temporal constraints.
        """
        # NOTE(damb): context dependent validation
        if self.context.get("request"):

            starttime = data.get("starttime")
            endtime = data.get("endtime")
            now = datetime.datetime.utcnow()

            if self.context.get("request").method == "GET":

                if not endtime:
                    endtime = now
                elif endtime > now:
                    endtime = now
                    # silently reset endtime
                    data["endtime"] = now

            elif self.context.get("request").method == "POST":
                if starttime is None or endtime is None:
                    raise ValidationError("missing temporal constraints")

            if starttime:
                if starttime > now:
                    raise ValidationError("starttime in future")
                elif starttime >= endtime:
                    raise ValidationError(
                        "endtime must be greater than starttime"
                    )


ManyStreamEpochSchema = _ManyStreamEpochSchema(
    stream_epoch_schema=StreamEpochSchema
)


# ----------------------------------------------------------------------------
def setup_parser_error_handler(service_version=None):
    @parser.error_handler
    @fdsnws_parser.error_handler
    def handle_parser_error(error, req, schema, status_code, headers):

        raise FDSNHTTPError.create(
            400,
            service_version=service_version,
            error_desc_long=str(error),
        )

    return handle_parser_error


class FDSNWSFlaskParser(FDSNWSParserMixin, FlaskParser):
    def parse_querystring(self, req, name, field):
        return core.get_value(
            self._parse_streamepochs_from_argdict(req.args), name, field
        )

    def parse_form(self, req, name, field):
        """Pull a form value from the request."""
        try:
            return core.get_value(
                self._parse_postfile(self._get_data(req)), name, field
            )
        except AttributeError:
            pass
        return core.missing

    def _get_data(
        self, req, as_text=True, max_content_length=STL_DEFAULT_CLIENT_MAX_SIZE
    ):
        """
        Savely reads the buffered incoming data from the client.

        :param req: Request the raw data is read from
        :type req: :py:class:`flask.Request`
        :param bool as_text: If set to :code:`True` the return value will be a
            decoded unicode string.
        :param int max_content_length: Max bytes accepted
        :returns: Byte string or rather unicode string, respectively. Depending
            on the :code:`as_text` parameter.
        """
        validate_content_length(req, max_content_length)

        return req.get_data(cache=True, as_text=as_text)


def validate_content_length(req, max_content_length):
    if req.content_length is None:
        raise FDSNHTTPError.create(
            400,
            error_desc_long="'Content-Length' not specified.",
            service_version=__version__,
        )
    if req.content_length > max_content_length:
        raise FDSNHTTPError.create(413, service_version=__version__)


fdsnws_parser = FDSNWSFlaskParser()
use_fdsnws_args = fdsnws_parser.use_args
use_fdsnws_kwargs = fdsnws_parser.use_kwargs
