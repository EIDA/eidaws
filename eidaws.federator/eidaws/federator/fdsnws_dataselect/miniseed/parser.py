# -*- coding: utf-8 -*-
"""
Federator schema definitions for ``fdsnws-dataselect``.
"""

from marshmallow import validate, fields

from eidaws.federator.utils.parser import ServiceSchema
from eidaws.utils.schema import NoData


class DataselectSchema(ServiceSchema):
    """
    Dataselect webservice schema definition

    The parameters defined correspond to the definition
    `<https://www.fdsn.org/webservices/fdsnws-dataselect-1.1.pdf>`_.
    """

    format = fields.Str(
        missing="miniseed", validate=validate.OneOf(["miniseed"])
    )
    nodata = NoData()

    class Meta:
        service = "dataselect"
        strict = True
        ordered = True
