# -*- coding: utf-8 -*-

from marshmallow import validate, fields

from eidaws.federator.fdsnws_availability.parser import (
    _AvailabilitySchema as _AvailabilityBaseSchema,
    AvailabilityQuerySchema as _AvailabilityQuerySchema,
    AvailabilityExtentSchema as _AvailabilityExtentSchema,
)


class _AvailabilitySchema(_AvailabilityBaseSchema):
    format = fields.Str(missing="json", validate=validate.OneOf(["json"]),)


AvailabilityQuerySchema = _AvailabilityQuerySchema(_AvailabilitySchema)
AvailabilityExtentSchema = _AvailabilityExtentSchema(_AvailabilitySchema)
