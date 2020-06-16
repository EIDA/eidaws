# -*- coding: utf-8 -*-
"""
Federator schema definitions for ``fdsnws-availability``.
"""

import functools

from marshmallow import validate, fields
from webargs.fields import DelimitedList

from eidaws.federator.utils.parser import ServiceSchema
from eidaws.utils.schema import FDSNWSBool, NoData, NotEmptyFloat
from eidaws.utils.settings import FDSNWS_QUERY_LIST_SEPARATOR_CHAR


Quality = functools.partial(
    fields.Str, validate=validate.OneOf(["D", "R", "Q", "M", "*"])
)


def Merge(*valid_values, **kwargs):
    return DelimitedList(
        fields.Str(validate=validate.OneOf(valid_values)),
        delimiter=FDSNWS_QUERY_LIST_SEPARATOR_CHAR,
        **kwargs
    )


def OrderBy(*valid_values, **kwargs):
    return fields.Str(validate=validate.OneOf(valid_values), **kwargs)


class _AvailabilitySchema(ServiceSchema):
    """
    Availability webservice schema definition

    The parameters defined correspond to the definition
    `<http://www.fdsn.org/webservices/fdsnws-availability-1.0.pdf>`_.
    """

    nodata = NoData()

    format = fields.Str(
        missing="text",
        validate=validate.OneOf(["text", "geocsv", "json", "request"]),
    )
    quality = Quality(missing="*")
    limit = fields.Int(validate=validate.Range(min=1))
    includerestricted = FDSNWSBool(missing="false")

    class Meta:
        service = "availability"
        strict = True
        ordered = True


class AvailabilityQuerySchema(_AvailabilitySchema):
    merge = Merge("samplerate", "quality", "overlap")
    mergegaps = NotEmptyFloat(validate=validate.Range(min=0))
    orderby = OrderBy(
        "nslc_time_quality_samplerate",
        "latestupdate",
        "latestupdate_desc",
        missing="nslc_time_quality_samplerate",
    )
    show = fields.Str(validate=validate.OneOf(["latestupdate"]),)


class AvailabilityExtentSchema(_AvailabilitySchema):
    merge = Merge("samplerate", "quality")
    orderby = OrderBy(
        "nslc_time_quality_samplerate",
        "timespancount",
        "timespancount_desc",
        missing="nslc_time_quality_samplerate",
    )
