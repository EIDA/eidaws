# -*- coding: utf-8 -*-

from eidaws.utils.settings import FDSNWS_DOCUMENTATION_URI

# Error <CODE>: <SIMPLE ERROR DESCRIPTION>
# <MORE DETAILED ERROR DESCRIPTION>
# Usage details are available from <SERVICE DOCUMENTATION URI>
# Request:
# <SUBMITTED URL>
# Request Submitted:
# <UTC DATE TIME>
# Service version:
# <3-LEVEL VERSION>

ERROR_MESSAGE_TEMPLATE = """
Error %s: %s

%s

Usage details are available from %s

Request:
%s

Request Submitted:
%s

Service version:
%s
"""


def make_error_message(
    status_code,
    description_short,
    description_long,
    documentation_uri,
    request_url,
    request_time,
    service_version,
):
    """Return text of error message."""

    return ERROR_MESSAGE_TEMPLATE % (
        status_code,
        description_short,
        description_long,
        documentation_uri,
        request_url,
        request_time,
        service_version,
    )


class FDSNHTTPError(Exception):
    """
    General HTTP error class for 5xx and 4xx errors for FDSN web services,
    with error message according to standards. Needs to be subclassed for
    individual error types.
    """

    error_desc_short = ""

    DEFAULT_DOCUMENTATION_URI = FDSNWS_DOCUMENTATION_URI
    DEFAULT_SERVICE_VERSION = ""

    CONTENT_TYPE = "text/plain"

    def __init__(self, **kwargs):
        super().__init__()
