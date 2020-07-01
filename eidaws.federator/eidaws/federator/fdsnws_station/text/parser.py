# -*- coding: utf-8 -*-
"""
Federator schema definitions
"""

from marshmallow import validates_schema, validate, fields, ValidationError

from eidaws.federator.fdsnws_station.parser import StationSchema


# -----------------------------------------------------------------------------
class StationTextSchema(StationSchema):
    """
    Station webservice schema definition

    The parameters defined correspond to the definition
    `<http://www.orfeus-eu.org/data/eida/webservices/station/>`_ except of the
    ``format`` query parameter.
    """

    format = fields.String(missing="text", validate=validate.OneOf(["text"]))

    @validates_schema
    def validate_level(self, data, **kwargs):
        if data["level"] == "response":
            raise ValidationError("Invalid level for format: 'text'.")
