# -*- coding: utf-8 -*-

from marshmallow import validate, validates_schema, fields, ValidationError

from eidaws.federator.fdsnws_availability.parser import (
    _AvailabilitySchema as _AvailabilityBaseSchema,
    AvailabilityQuerySchema as _AvailabilityQuerySchema,
    AvailabilityExtentSchema as _AvailabilityExtentSchema,
)


class _AvailabilitySchema(_AvailabilityBaseSchema):
    format = fields.Str(
        missing="geocsv",
        validate=validate.OneOf(["geocsv"]),
    )


AvailabilityQuerySchema = _AvailabilityQuerySchema(_AvailabilitySchema)
AvailabilityExtentSchema = _AvailabilityExtentSchema(_AvailabilitySchema)
