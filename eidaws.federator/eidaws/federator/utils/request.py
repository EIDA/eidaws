# -*- coding: utf-8 -*-

import datetime
import functools

from collections import OrderedDict
from copy import deepcopy
from urllib.parse import urlparse, urlunparse

from eidaws.federator.utils.misc import HelperGETRequest
from eidaws.federator.version import __version__
from eidaws.utils.misc import convert_sncl_dicts_to_query_params
from eidaws.utils.schema import StreamEpochSchema


def _query_params_from_stream_epochs(stream_epochs):

    serializer = StreamEpochSchema(
        many=True, context={"request": HelperGETRequest}
    )

    return convert_sncl_dicts_to_query_params(serializer.dump(stream_epochs))


def _serialize_stream_epochs_post(stream_epochs):
    serializer = StreamEpochSchema(many=True)
    serialized = serializer.dump(stream_epochs)
    now = datetime.datetime.utcnow().isoformat()

    # set endtime if not specified
    se_maps = []
    for _map in serialized:
        if _map["endtime"] is None:
            _map["endtime"] = now

        se_maps.append(" ".join(str(v) for v in _map.values()))

    return "\n".join(m for m in se_maps)


# -----------------------------------------------------------------------------
class RequestHandlerBase:
    """
    RequestHandler base class implementation.
    """

    DEFAULT_HEADERS = {
        "User-Agent": "EIDA-Federator/" + __version__,
    }

    def __init__(self, url, stream_epochs=[], query_params={}, headers={}):
        """
        :param url: URL
        :type url: str or bytes
        :param list stream_epochs: List of
            :py:class:`eidaws.utils.sncl.StreamEpoch` objects
        :param dict query_params: Dictionary of query parameters
        :param dict headers: Dictionary of request header parameters
        """

        if isinstance(url, bytes):
            url = url.decode("utf-8")
        url = urlparse(url)
        self._scheme = url.scheme
        self._netloc = url.netloc
        self._path = url.path.rstrip("/")

        self._query_params = OrderedDict(
            (p, v)
            for p, v in query_params.items()
            if self._filter_query_params(p, v)
        )
        self._stream_epochs = stream_epochs

        self._headers = headers or self.DEFAULT_HEADERS

    @property
    def url(self):
        """
        Returns request URL without query parameters.
        """
        return urlunparse(
            (self._scheme, self._netloc, self._path, "", "", "",)
        )

    @property
    def stream_epochs(self):
        return self._stream_epochs

    @property
    def payload_post(self):
        raise NotImplementedError

    @property
    def payload_get(self):
        raise NotImplementedError

    def post(self, session):
        """
        :param session: Session the request will be bound to
        :type session: :py:class:`aiohttp.ClientSession`
        """
        raise NotImplementedError

    def get(self, session):
        """
        :param session: Session the request will be bound to
        :type session: :py:class:`aiohttp.ClientSession`
        """
        raise NotImplementedError

    def __str__(self):
        return ", ".join(
            [
                f"scheme={self._scheme}",
                f"netloc={self._netloc}",
                f"path={self._path}",
                f"headers={self._headers}",
                f"qp={self._query_params}",
                "streams={}".format(
                    ", ".join(str(se) for se in self._stream_epochs)
                ),
            ]
        )

    def __repr__(self):
        return f"<{type(self).__name__}: {self}>"

    def _filter_query_params(self, param, value):
        return True


class RoutingRequestHandler(RequestHandlerBase):
    """
    Representation of a `eidaws-routing` (*StationLite*) request handler.
    """

    QUERY_PARAMS = set(
        (
            "service",
            "level",
            "minlatitude",
            "minlat",
            "maxlatitude",
            "maxlat",
            "minlongitude",
            "minlon",
            "maxlongitude",
            "maxlon",
        )
    )

    def __init__(
        self, url, stream_epochs=[], query_params={}, headers={}, **kwargs
    ):
        """
        :param method: Specifies the ``method`` query filter parameter when
            requesting data from StationLite
        :param str access: Specifies the ``access`` query parameter when
            requesting data from StationLite
        """

        super().__init__(url, stream_epochs, query_params, headers)

        self._query_params["format"] = "post"
        self._query_params["access"] = kwargs.get("access", "any")

        method = kwargs.get("method")
        if method:
            self._query_params["method"] = method

    @property
    def payload_post(self):
        data = "\n".join(
            "{}={}".format(p, v) for p, v in self._query_params.items()
        )

        return "{}\n{}".format(
            data, _serialize_stream_epochs_post(self._stream_epochs)
        )

    @property
    def payload_get(self):
        qp = {p: f"{v}" for p, v in self._query_params.items()}
        qp.update(_query_params_from_stream_epochs(self._stream_epochs))
        return qp

    def post(self, session):
        return functools.partial(
            session.post,
            self.url,
            data=self.payload_post,
            headers=self._headers,
        )

    def get(self, session):
        return functools.partial(
            session.get,
            self.url,
            params=self.payload_get,
            headers=self._headers,
        )

    def _filter_query_params(self, param, value):
        return param in self.QUERY_PARAMS


class FdsnRequestHandler(RequestHandlerBase):

    QUERY_PARAMS = set(
        (
            "service",
            "nodata",
            "minlatitude",
            "minlat",
            "maxlatitude",
            "maxlat",
            "minlongitude",
            "minlon",
            "maxlongitude",
            "maxlon",
        )
    )

    @property
    def format(self):
        try:
            return self._query_params["format"]
        except KeyError:
            return None

    @format.setter
    def format(self, value):
        self._query_params["format"] = value

    @property
    def payload_post(self):
        data = "\n".join(
            "{}={}".format(p, v) for p, v in self._query_params.items()
        )

        return "{}\n{}".format(
            data, _serialize_stream_epochs_post(self._stream_epochs)
        )

    @property
    def payload_get(self):
        qp = {p: f"{v}" for p, v in self._query_params.items()}
        qp.update(_query_params_from_stream_epochs(self._stream_epochs))
        return qp

    def get(self, session):
        return functools.partial(
            session.get,
            self.url,
            params=self.payload_get,
            headers=self._headers,
        )

    def post(self, session):
        return functools.partial(
            session.post,
            self.url,
            data=self.payload_post,
            headers=self._headers,
        )

    def _filter_query_params(self, param, value):
        return param not in self.QUERY_PARAMS
