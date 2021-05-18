# -*- coding: utf-8 -*-

from marshmallow import Schema

from eidaws.utils.schema import (
    FDSNWSBool,
    NoData,
)


class StationLiteSchema(Schema):
    """
    StationLite *stationlite* webservice schema definition.
    """

    nodata = NoData()
    # merge epochs
    merge = FDSNWSBool(missing="true")
