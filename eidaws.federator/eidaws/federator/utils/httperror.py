# -*- coding: utf-8 -*-
"""
Facilites for FDSNWS conform HTTP error definitions

See also: http://www.fdsn.org/webservices/FDSN-WS-Specifications-1.1.pdf
"""

import datetime

from aiohttp.web import HTTPException

from eidaws.utils.settings import FDSNWS_SERVICE_DOCUMENTATION_URI


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


# -----------------------------------------------------------------------------
class FDSNHTTPError(HTTPException):
    """
    General HTTP error class for 5xx and 4xx errors for FDSN web services,
    with error message according to standards. Needs to be subclassed for
    individual error types.
    """

    status_code = 0
    error_desc_short = ""

    DEFAULT_DOCUMENTATION_URI = FDSNWS_SERVICE_DOCUMENTATION_URI
    DEFAULT_SERVICE_VERSION = ""

    CONTENT_TYPE = "text/plain"

    @staticmethod
    def create(status_code, *args, **kwargs):
        """
        Factory method for concrete FDSN error implementations.
        """
        if status_code == 204:
            return HTTPNoContent(*args, **kwargs)
        elif status_code == 404:
            return HTTPNotFound(*args, **kwargs)
        elif status_code == 400:
            return HTTPBadRequest(*args, **kwargs)
        elif status_code == 413:
            return HTTPRequestEntityTooLarge(*args, **kwargs)
        elif status_code == 414:
            return HTTPRequestURITooLong(*args, **kwargs)
        elif status_code == 500:
            return HTTPInternalServerError(*args, **kwargs)
        elif status_code == 503:
            return HTTPServiceUnavailable(*args, **kwargs)
        else:
            return HTTPInternalServerError(*args, **kwargs)

    def __init__(
        self,
        request,
        request_submitted=None,
        documentation_uri=None,
        service_version=None,
        error_desc_long=None,
    ):

        documentation_uri = documentation_uri or self.DEFAULT_DOCUMENTATION_URI
        service_version = service_version or self.DEFAULT_SERVICE_VERSION
        error_desc_long = error_desc_long or self.error_desc_short
        request_submitted = request_submitted or datetime.datetime.utcnow()

        text = self.make_error_message(
            self.status_code,
            self.error_desc_short,
            error_desc_long,
            documentation_uri,
            request.url,
            request_submitted.isoformat(),
            service_version,
        )

        super().__init__(text=text, content_type=self.CONTENT_TYPE)

    @staticmethod
    def make_error_message(
        status_code,
        description_short,
        description_long,
        documentation_uri,
        request_url,
        request_time,
        service_version,
    ):
        """
        Return text of error message.
        """

        return ERROR_MESSAGE_TEMPLATE % (
            status_code,
            description_short,
            description_long,
            documentation_uri,
            request_url,
            request_time,
            service_version,
        )


class HTTPNoContent(FDSNHTTPError):
    status_code = 204
    error_desc_short = ""


class HTTPNotFound(FDSNHTTPError):
    status_code = 404
    error_desc_short = ""


class HTTPBadRequest(FDSNHTTPError):
    status_code = 400
    error_desc_short = "Bad request"


class HTTPRequestEntityTooLarge(FDSNHTTPError):
    status_code = 413
    error_desc_short = "Request too large"


class HTTPRequestURITooLong(FDSNHTTPError):
    status_code = 414
    error_desc_short = "Request URI too large"


class HTTPInternalServerError(FDSNHTTPError):
    status_code = 500
    error_desc_short = "Internal server error"


class HTTPServiceUnavailable(FDSNHTTPError):
    status_code = 503
    error_desc_short = "Service temporarily unavailable"
