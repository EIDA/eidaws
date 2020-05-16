# -*- coding: utf-8 -*-

from marshmallow import (
    Schema,
    fields,
    validate,
    validates_schema,
    pre_load,
    ValidationError,
)
from webargs import core
from webargs.flaskparser import parser, FlaskParser

from eidaws.stationlite.version import __version__
from eidaws.stationlite.server.http_error import FDSNHTTPError
from eidaws.stationlite.settings import STL_DEFAULT_CLIENT_MAX_SIZE
from eidaws.utils.parser import FDSNWSParserMixin
from eidaws.utils.schema import FDSNWSBool, Latitude, Longitude, NoData


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
        validate=validate.OneOf(["dataselect", "station", "wfcatalog"]),
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
                "Bad Request: Invalid 'level' value {!r} for service "
                "{!r}.".format(data["level"], data["service"])
            )

    @validates_schema
    def validate_access(self, data, **kwargs):
        if data["access"] != "any" and data["service"] != "dataselect":
            raise ValidationError(
                "Bad Request: Invalid 'access' value {!r} for service "
                "{!r}".format(data["access"], data["service"])
            )

    class Meta:
        strict = True


# ----------------------------------------------------------------------------
def setup_parser_error_handler(service_version=None):
    @parser.error_handler
    @fdsnws_parser.error_handler
    def handle_parser_error(error, req, schema, status_code, headers):

        raise FDSNHTTPError.create(
            400, service_version=service_version, error_desc_long=str(error),
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
        if req.content_length > max_content_length:
            raise FDSNHTTPError(413, service_version=__version__)

        return req.get_data(cache=True, as_text=as_text)


fdsnws_parser = FDSNWSFlaskParser()
use_fdsnws_args = fdsnws_parser.use_args
use_fdsnws_kwargs = fdsnws_parser.use_kwargs
