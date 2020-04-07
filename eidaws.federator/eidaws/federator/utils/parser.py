# -*- coding: utf-8 -*-
"""
Federator schema definitions
"""
from webargs import core
from webargs.aiohttpparser import parser, AIOHTTPParser

from marshmallow import (
    fields,
    validate,
    ValidationError,
    pre_load,
    validates_schema,
    Schema,
    SchemaOpts,
    post_load,
)

from eidaws.federator.settings import FED_BASE_ID
from eidaws.federator.utils.httperror import FDSNHTTPError
from eidaws.utils.parser import FDSNWSParserMixin
from eidaws.utils.schema import (
    _merge_fields,
    FDSNWSDateTime,
    Latitude,
    Longitude,
    Radius,
    FDSNWSBool,
    NoData,
)


class ServiceOpts(SchemaOpts):
    """
    Same as the default class Meta options, but adds the *service* option.
    """

    def __init__(self, meta, **kwargs):
        SchemaOpts.__init__(self, meta, **kwargs)
        self.service = getattr(meta, "service", "dataselect")


class ServiceSchema(Schema):
    """
    Base class for webservice schema definitions.
    """

    OPTIONS_CLASS = ServiceOpts

    # read-only (the parameter is not parsed by webargs)
    service = fields.Str(dump_only=True)

    @post_load
    def set_service(self, data, **kwargs):
        data["service"] = self.opts.service
        return data

    class Meta:
        strict = True


class StationSchema(ServiceSchema):
    """
    Station webservice schema definition

    The parameters defined correspond to the definition
    `<http://www.orfeus-eu.org/data/eida/webservices/station/>`_ except the
    ``format`` query parameter.
    """

    nodata = NoData()

    # temporal options
    startbefore = FDSNWSDateTime(format="fdsnws")
    startafter = FDSNWSDateTime(format="fdsnws")
    endbefore = FDSNWSDateTime(format="fdsnws")
    endafter = FDSNWSDateTime(format="fdsnws")

    # geographic (rectangular spatial) options
    minlatitude = Latitude()
    minlat = Latitude(load_only=True)
    maxlatitude = Latitude()
    maxlat = Latitude(load_only=True)
    minlongitude = Longitude()
    minlon = Latitude(load_only=True)
    maxlongitude = Longitude()
    maxlon = Latitude(load_only=True)

    # geographic (circular spatial) options
    latitude = Latitude()
    lat = Latitude(load_only=True)
    longitude = Longitude()
    lon = Latitude(load_only=True)
    minradius = Radius()
    maxradius = Radius()

    # request options
    level = fields.Str(
        missing="station",
        validate=validate.OneOf(["network", "station", "channel", "response"]),
    )
    includerestricted = FDSNWSBool(missing="true")

    @pre_load
    def merge_keys(self, data, **kwargs):
        """
        Merge alternative field parameter values.

        .. note::
            The default webargs parser does not provide this feature by
            default such that `load_from` fields parameters are exclusively
            parsed.
        """
        _mappings = [
            ("minlat", "minlatitude"),
            ("maxlat", "maxlatitude"),
            ("minlon", "minlongitude"),
            ("maxlon", "maxlongitude"),
            ("lat", "latitude"),
            ("lon", "longitude"),
        ]

        _merge_fields(data, _mappings)
        return data

    @validates_schema
    def validate_level(self, data, **kwargs):
        if data["format"] == "text" and data["level"] == "response":
            raise ValidationError("Invalid level for format 'text'.")

    @validates_schema
    def validate_spatial_params(self, data, **kwargs):
        # NOTE(damb): Allow either rectangular or circular spatial parameters
        rectangular_spatial = (
            "minlatitude",
            "maxlatitude",
            "minlongitude",
            "maxlongitude",
        )
        circular_spatial = ("latitude", "longitude", "minradius", "maxradius")

        if any(k in data for k in rectangular_spatial) and any(
            k in data for k in circular_spatial
        ):
            raise ValidationError(
                "Bad Request: Both rectangular spatial and circular spatial"
                + " parameters defined."
            )
            # TODO(damb): check if min values are smaller than max values;
            # no default values are set

    class Meta:
        service = "station"
        strict = True
        ordered = True


# -----------------------------------------------------------------------------
def setup_parser_error_handler(service_version=None):
    @parser.error_handler
    @fdsnws_parser.error_handler
    def handle_parser_error(error, req, schema, status_code, headers):

        raise FDSNHTTPError.create(
            400,
            req,
            request_submitted=req[FED_BASE_ID + ".request_starttime"],
            service_version=service_version,
            error_desc_long=str(error),
        )

    return handle_parser_error


class FDSNWSAIOHTTPParser(FDSNWSParserMixin, AIOHTTPParser):
    def parse_querystring(self, req, name, field):
        return core.get_value(
            self._parse_streamepochs_from_argdict(req.query), name, field
        )

    async def parse_form(self, req, name, field):
        return core.get_value(
            self._parse_postfile(await req.text()), name, field
        )


fdsnws_parser = FDSNWSAIOHTTPParser()
use_fdsnws_args = fdsnws_parser.use_args
use_fdsnws_kwargs = fdsnws_parser.use_kwargs
