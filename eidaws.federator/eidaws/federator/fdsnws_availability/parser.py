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

    quality = Quality(missing="*")
    # TODO(damb): To be implemented.
    # limit = fields.Int(validate=validate.Range(min=1))
    includerestricted = FDSNWSBool(missing="false")

    class Meta:
        service = "availability"
        strict = True
        ordered = True


def AvailabilityQuerySchema(base_cls=_AvailabilitySchema):
    class _AvailabilityQuerySchema(base_cls):
        merge = Merge("samplerate", "quality", "overlap", as_string=True)
        mergegaps = NotEmptyFloat(validate=validate.Range(min=0))
        # orderby = OrderBy(
        #     "nslc_time_quality_samplerate",
        #     "latestupdate",
        #     "latestupdate_desc",
        #     missing="nslc_time_quality_samplerate",
        # )
        show = fields.Str(validate=validate.OneOf(["latestupdate"]),)

    return _AvailabilityQuerySchema


def AvailabilityExtentSchema(base_cls=_AvailabilitySchema):
    class _AvailabilityExtentSchema(base_cls):
        merge = Merge("samplerate", "quality", as_string=True)
        # orderby = OrderBy(
        #     "nslc_time_quality_samplerate",
        #     "latestupdate",
        #     "latestupdate_desc",
        #     "timespancount",
        #     "timespancount_desc",
        #     missing="nslc_time_quality_samplerate",
        # )

    return _AvailabilityExtentSchema
