# -*- coding: utf-8 -*-
"""
Federator schema definitions for ``fdsnws-availability``.
"""

from marshmallow import validate, fields
from webargs.fields import DelimitedList

from eidaws.federator.utils.parser import ServiceSchema
from eidaws.utils.schema import FDSNWSBool, NoData, NotEmptyFloat, Quality
from eidaws.utils.settings import FDSNWS_QUERY_LIST_SEPARATOR_CHAR


def MergeField(valid_values, **kwargs):
    return DelimitedList(
        fields.Str(validate=validate.OneOf(valid_values)),
        delimiter=FDSNWS_QUERY_LIST_SEPARATOR_CHAR,
        **kwargs
    )


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
    quality = Quality()
    limit = fields.Int(validate=validate.Range(min=1))
    includerestricted = FDSNWSBool(missing="false")
    orderby = fields.Str(
        missing="nslc_time_quality_samplerate",
        validate=validate.OneOf(
            [
                "nslc_time_quality_samplerate",
                "latestupdate",
                "latestupdate_desc",
                "timespancount",
                "timespancount_desc",
            ]
        ),
    )

    class Meta:
        service = "availability"
        strict = True
        ordered = True


class AvailabilityQuerySchema(_AvailabilitySchema):
    merge = MergeField(["samplerate", "quality", "overlap"])
    mergegaps = NotEmptyFloat(validate=validate.Range(min=0))
    show = fields.Str(
        missing=None,
        allow_none=True,
        validate=validate.OneOf(["latestupdate"]),
    )


class AvailabilityExtentSchema(_AvailabilitySchema):
    merge = MergeField(["samplerate", "quality"])
