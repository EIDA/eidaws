# -*- coding: utf-8 -*-

import contextlib
import io
import logging

import requests

from eidaws.stationlite.settings import STL_HARVEST_BASE_ID
from eidaws.utils.error import Error
from eidaws.utils.settings import FDSNWS_NO_CONTENT_CODES


logger = logging.getLogger(".".join([STL_HARVEST_BASE_ID, __name__]))


class RequestsError(requests.exceptions.RequestException, Error):
    """Base request error ({})."""

    def __init__(self, *args, **kwargs):
        super().__init__(*args, **kwargs)


class ClientError(RequestsError):
    """Response code not OK ({})."""


class NoContent(RequestsError):
    """The request '{}' is returning no content ({})."""


@contextlib.contextmanager
def binary_request(request, logger=logger, **kwargs):
    """
    Make a request.
    :param request: Request object to be used
    :type request: :py:class:`requests.Request`
    :param float timeout: Timeout in seconds
    :param logger: Logger instance to be used for logging
    :rtype: io.BytesIO
    """

    try:
        with request(**kwargs) as r:

            logger.debug(f"Request URL (absolute, encoded): {r.url!r}")
            logger.debug(f"Response headers: {r.headers!r}")

            if r.status_code in FDSNWS_NO_CONTENT_CODES:
                raise NoContent(r.url, r.status_code, response=r)

            r.raise_for_status()
            if r.status_code != 200:
                raise ClientError(r.status_code, response=r)

            yield io.BytesIO(r.content)

    except (NoContent, ClientError) as err:
        raise err
    except requests.exceptions.RequestException as err:
        raise RequestsError(err, response=err.response)
