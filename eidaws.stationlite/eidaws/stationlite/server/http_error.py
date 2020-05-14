# -*- coding: utf-8 -*-
"""
FDSNWS conform HTTP error definitions.

See also: http://www.fdsn.org/webservices/FDSN-WS-Specifications-1.1.pdf
"""

from flask import request, g, make_response
from werkzeug.exceptions import HTTPException

from eidaws.utils.http_error import (
    make_error_message,
    FDSNHTTPError as _FDSNHTTPError,
)
from eidaws.utils.settings import FDSNWS_NO_CONTENT_CODES


class FDSNHTTPError(HTTPException, _FDSNHTTPError):
    """
    General HTTP error class for 5xx and 4xx errors for FDSN web services,
    with error message according to standards. Needs to be subclassed for
    individual error types.
    """

    code = -1

    @staticmethod
    def create(status_code, *args, **kwargs):
        """
        Factory method for concrete FDSN error implementations.
        """
        if status_code in FDSNWS_NO_CONTENT_CODES:
            return NoDataError(status_code)
        elif status_code == 400:
            return BadRequestError(*args, **kwargs)
        elif status_code == 413:
            return RequestTooLargeError(*args, **kwargs)
        elif status_code == 414:
            return RequestURITooLargeError(*args, **kwargs)
        elif status_code == 500:
            return InternalServerError(*args, **kwargs)
        elif status_code == 503:
            return TemporarilyUnavailableError(*args, **kwargs)
        else:
            return InternalServerError(*args, **kwargs)

    def __init__(
        self,
        documentation_uri=None,
        service_version=None,
        error_desc_long=None,
    ):
        documentation_uri = documentation_uri or self.DEFAULT_DOCUMENTATION_URI
        service_version = service_version or self.DEFAULT_SERVICE_VERSION
        error_desc_long = error_desc_long or self.error_desc_short

        description = make_error_message(
            self.status_code,
            self.error_desc_short,
            error_desc_long,
            documentation_uri,
            request.url,
            g.request_start_time.isoformat(),
            service_version,
        )

        response = make_response(
            description,
            self.code,
            {"Content-Type": f"{self.CONTENT_TYPE}; charset=utf-8"},
        )

        super().__init__(description=description, response=response)


class NoDataError(HTTPException):
    def __init__(self, status_code=204):
        description = ""
        response = make_response(description, self.code)
        response.headers["Content-Type"] = _FDSNHTTPError.CONTENT_TYPE

        super().__init__(description=description, response=response)


class BadRequestError(FDSNHTTPError):
    code = 400
    error_desc_short = "Bad request"


class RequestTooLargeError(FDSNHTTPError):
    code = 413
    error_desc_short = "Request too large"


class RequestURITooLargeError(FDSNHTTPError):
    code = 414
    error_desc_short = "Request URI too large"


class InternalServerError(FDSNHTTPError):
    code = 500
    error_desc_short = "Internal server error"


class TemporarilyUnavailableError(FDSNHTTPError):
    code = 503
    error_desc_short = "Service temporarily unavailable"
