# -*- coding: utf-8 -*-

import functools

import requests

from urllib.parse import urlparse, urljoin

from eidaws.stationlite.engine.utils import RestrictedStatus
from eidaws.stationlite.harvest.request import binary_request
from eidaws.stationlite.settings import STL_SERVICES
from eidaws.utils.error import Error
from eidaws.utils.settings import (
    FDSNWS_QUERY_METHOD_TOKEN,
    FDSNWS_QUERYAUTH_METHOD_TOKEN,
    FDSNWS_EXTENT_METHOD_TOKEN,
    FDSNWS_EXTENTAUTH_METHOD_TOKEN,
    FDSNWS_VERSION_METHOD_TOKEN,
    FDSNWS_AVAILABILITY_MAJORVERSION,
    FDSNWS_DATASELECT_MAJORVERSION,
    FDSNWS_STATION_MAJORVERSION,
    EIDAWS_WFCATALOG_MAJORVERSION,
)


class ValidationError(Error):
    """Base validation error ({})"""


class PathError(ValidationError):
    """Invalid URL path {} for URL {}"""


class VersionError(ValidationError):
    """Invalid service version {} for URL {}"""


class ServiceError(ValidationError):
    """Invalid service identifier tag {}"""


def _get_method_token(url):
    """
    Utility function returning the method token from the URL's path.

    :param str url: URL
    :returns: Method token
    :retval: str
    """
    token = urlparse(url).path.split("/")[-1]

    try:
        float(token)
    except ValueError:
        return token
    else:
        return None


def _validate_method_token(url, restricted_status=RestrictedStatus.OPEN):
    token = _get_method_token(url)

    if token is None or token != FDSNWS_QUERY_METHOD_TOKEN:
        raise ValidationError(
            f"Invalid method token {token!r} for URL {url!r}"
        )


validate_station_method_token = _validate_method_token
validate_wfcatalog_method_token = _validate_method_token


def validate_dataselect_method_token(url,
        restricted_status=RestrictedStatus.OPEN):
    token = _get_method_token(url)
    if (
        token is None
        or (restricted_status == RestrictedStatus.OPEN and token != FDSNWS_QUERY_METHOD_TOKEN)
        or (
            restricted_status == RestrictedStatus.CLOSED
            and token != FDSNWS_QUERYAUTH_METHOD_TOKEN
        )
    ):
        raise ValidationError(
            f"Invalid method token {token!r} for URL {url!r}"
        )


def validate_availability_method_token(url,
        restricted_status=RestrictedStatus.OPEN):
    token = _get_method_token(url)
    if (
        token is None
        or (
            restricted_status == RestrictedStatus.OPEN
            and token
            not in (FDSNWS_QUERY_METHOD_TOKEN, FDSNWS_EXTENT_METHOD_TOKEN)
        )
        or (
            restricted_status == RestrictedStatus.CLOSED
            and token
            not in (
                FDSNWS_QUERYAUTH_METHOD_TOKEN,
                FDSNWS_EXTENTAUTH_METHOD_TOKEN,
            )
        )
    ):
        raise ValidationError(
            f"Invalid method token {token!r} for URL {url!r}"
        )


def validate_method_token(url, service, restricted_status=RestrictedStatus.OPEN):
    """
    Validates the *service method token* AKA the *service resource*.
    """

    if service == "station":
        validate_station_method_token(url, restricted_status)
    elif service == "wfcatalog":
        validate_wfcatalog_method_token(url, restricted_status)
    elif service == "dataselect":
        validate_dataselect_method_token(url, restricted_status)
    elif service == "availability":
        validate_availability_method_token(url, restricted_status)


def validate_major_version(url, service):
    """
    Validates the service *major version* by means of querying the service
    version from `url`.
    """

    def _get_major_version(url):
        req = functools.partial(
            requests.get, urljoin(url, FDSNWS_VERSION_METHOD_TOKEN)
        )
        with binary_request(req, timeout=60) as resp:
            return resp.read().strip().split(b".")[0]

    major_version = _get_major_version(url)
    try:
        int(major_version)
    except ValueError:
        raise VersionError(major_version, f"{url!r}")

    major_version = major_version.decode("utf-8")

    if (
        service == "station"
        and major_version != FDSNWS_STATION_MAJORVERSION
        or service == "wfcatalog"
        and major_version != EIDAWS_WFCATALOG_MAJORVERSION
        or service == "dataselect"
        and major_version != FDSNWS_DATASELECT_MAJORVERSION
        or service == "availability"
        and major_version != FDSNWS_AVAILABILITY_MAJORVERSION
    ):
        raise VersionError(major_version, f"{url!r}")


def validate_service(service):
    if service not in STL_SERVICES:
        raise ServiceError(f"{service!r}")
