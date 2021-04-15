# -*- coding: utf-8 -*-

import datetime

from marshmallow import (
    Schema,
    fields,
    validate,
    validates_schema,
    pre_load,
    ValidationError,
)
from webargs import core
from webargs.fields import DelimitedList
from webargs.flaskparser import parser, FlaskParser

from eidaws.stationlite.version import __version__
from eidaws.stationlite.server.http_error import FDSNHTTPError
from eidaws.stationlite.settings import STL_DEFAULT_CLIENT_MAX_SIZE
from eidaws.utils.parser import FDSNWSParserMixin
from eidaws.utils.schema import (
    FDSNWSBool,
    Latitude,
    Longitude,
    NoData,
    StreamEpochSchema as _StreamEpochSchema,
    _ManyStreamEpochSchema,
)
from eidaws.utils.settings import (
    FDSNWS_QUERY_METHOD_TOKEN,
    FDSNWS_QUERYAUTH_METHOD_TOKEN,
    FDSNWS_EXTENT_METHOD_TOKEN,
    FDSNWS_EXTENTAUTH_METHOD_TOKEN,
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


class StationLiteSchema(Schema):
    """
    Stationlite webservice schema definition.

    The parameters defined correspond to the definition
    `https://www.orfeus-eu.org/data/eida/webservices/routing/`
    """

    format = fields.Str(
        # NOTE(damb): formats different from 'post' are not implemented yet.
        # missing='xml'
        missing="post",
        # validate=validate.OneOf(['xml', 'json', 'get', 'post'])
        validate=validate.OneOf(["post", "get"]),
    )
    service = fields.Str(
        missing="dataselect",
        validate=validate.OneOf(
            ["availability", "dataselect", "station", "wfcatalog"]
        ),
    )

    nodata = NoData()
    alternative = FDSNWSBool(missing="false")
    access = fields.Str(
        missing="any", validate=validate.OneOf(["open", "closed", "any"])
    )
    level = fields.Str(
        missing="channel",
        validate=validate.OneOf(["network", "station", "channel", "response"]),
    )
    method = DelimitedList(
        fields.Str(
            validate=validate.OneOf(
                [
                    FDSNWS_QUERY_METHOD_TOKEN,
                    FDSNWS_QUERYAUTH_METHOD_TOKEN,
                    FDSNWS_EXTENT_METHOD_TOKEN,
                    FDSNWS_EXTENTAUTH_METHOD_TOKEN,
                ]
            ),
        ),
        missing=None,
        allow_none=True,
    )
    merge = FDSNWSBool(missing="true")
    # geographic (rectangular spatial) options
    # XXX(damb): Default values are defined and assigned within merge_keys ()
    minlatitude = Latitude(missing=-90.0)
    minlat = Latitude(load_only=True)
    maxlatitude = Latitude(missing=90.0)
    maxlat = Latitude(load_only=True)
    minlongitude = Longitude(missing=-180.0)
    minlon = Latitude(load_only=True)
    maxlongitude = Longitude(missing=180.0)
    maxlon = Latitude(load_only=True)

    @pre_load
    def merge_keys(self, data, **kwargs):
        """
        Merge both alternative field parameter values and assign default
        values.

        .. note::
            The default :py:module:`webargs` parser does not provide this
            feature by default such that :code:`load_only` field parameters are
            exclusively parsed.

        :param dict data: data
        """
        _mappings = [
            ("minlat", "minlatitude"),
            ("maxlat", "maxlatitude"),
            ("minlon", "minlongitude"),
            ("maxlon", "maxlongitude"),
        ]

        for alt_key, key in _mappings:
            if alt_key in data and key in data:
                data.pop(alt_key)
            elif alt_key in data and key not in data:
                data[key] = data[alt_key]
                data.pop(alt_key)

        return data

    @validates_schema
    def validate_spatial(self, data, **kwargs):
        if (
            data["minlatitude"] >= data["maxlatitude"]
            or data["minlongitude"] >= data["maxlongitude"]
        ):
            raise ValidationError("Bad Request: Invalid spatial constraints.")

    @validates_schema
    def validate_level(self, data, **kwargs):
        if data["level"] != "channel" and data["service"] != "station":
            raise ValidationError(
                f"Bad Request: Invalid 'level' value {data['level']!r} "
                f"for service {data['service']!r}."
            )

    @validates_schema
    def validate_merge(self, data, **kwargs):
        if not data["merge"] and data["service"] != "station":
            raise ValidationError(
                f"Bad Request: Invalid 'merge' value {data['merge']!r} "
                f"for service {data['service']!r}."
            )

    @validates_schema
    def validate_access(self, data, **kwargs):
        if data["access"] != "any" and data["service"] not in (
            "dataselect",
            "availability",
        ):
            raise ValidationError(
                f"Bad Request: Invalid 'access' value {data['access']!r} "
                f"for service {data['service']!r}"
            )

    class Meta:
        strict = True


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
