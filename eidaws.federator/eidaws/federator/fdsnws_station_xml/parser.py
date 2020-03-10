# -*- coding: utf-8 -*-
"""
Federator schema definitions for ``fdsnws-station-xml``.
"""

from marshmallow import validate, fields

from eidaws.federator.utils.parser import StationSchema


# -----------------------------------------------------------------------------
class StationXMLSchema(StationSchema):
    """
    Station XML webservice schema definition

    The parameters defined correspond to the definition
    `<http://www.orfeus-eu.org/data/eida/webservices/station/>`_ except of the
    ``format`` query parameter.
    """

    format = fields.String(missing="xml", validate=validate.OneOf(["xml"]))
