# -*- coding: utf-8 -*-

from marshmallow import validate, validates_schema, fields, ValidationError

from eidaws.federator.fdsnws_availability.parser import (
    _AvailabilitySchema as _AvailabilityBaseSchema,
    AvailabilityQuerySchema as _AvailabilityQuerySchema,
    AvailabilityExtentSchema as _AvailabilityExtentSchema,
)


class _AvailabilitySchema(_AvailabilityBaseSchema):
    format = fields.Str(
        missing="request",
        validate=validate.OneOf(["request"]),
    )

    @validates_schema
    def validate_show(self, data, **kwargs):
        if "show" in data:
            raise ValidationError(
                f"Invalid parameter 'show' for format={data['format']}."
            )


AvailabilityQuerySchema = _AvailabilityQuerySchema(_AvailabilitySchema)
AvailabilityExtentSchema = _AvailabilityExtentSchema(_AvailabilitySchema)
