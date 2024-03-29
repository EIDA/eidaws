# -*- coding: utf-8 -*-

from marshmallow import (
    fields,
    validate,
    ValidationError,
    pre_load,
    validates_schema,
)

from eidaws.federator.utils.parser import ServiceSchema
from eidaws.utils.schema import (
    _merge_fields,
    FDSNWSDateTime,
    Latitude,
    Longitude,
    Radius,
    FDSNWSBool,
    NoData,
)


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
    def validate_time_constraints(self, data, **kwargs):
        start_after = data.get("startafter")
        end_before = data.get("endbefore")

        if start_after and end_before and start_after >= end_before:
            raise ValidationError(
                "Invalid time constraints specified: 'startafter' >= "
                "'endbefore'"
            )

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

        has_rectangular_spatial = any(k in data for k in rectangular_spatial)
        has_circular_spatial = any(k in data for k in circular_spatial)

        if has_rectangular_spatial and has_circular_spatial:
            raise ValidationError(
                "Both rectangular spatial and circular spatial "
                "parameters defined."
            )

        if (
            has_rectangular_spatial
            and (
                data.get("minlatitude", -90.0) >= data.get("maxlatitude", 90.0)
                or data.get("minlongitude", -180.0)
                >= data.get("maxlongitude", 180.0)
            )
        ) or (
            has_circular_spatial
            and (data.get("minradius", 0.0) >= data.get("maxradius", 180.0))
        ):
            raise ValidationError("Invalid spatial constraints.")

    class Meta:
        service = "station"
        strict = True
        ordered = True
