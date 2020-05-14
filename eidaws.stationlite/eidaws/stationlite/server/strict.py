# -*- coding: utf-8 -*-

import functools

from flask import request

from eidaws.stationlite.http_error import FDSNHTTPError
from eidaws.stationlite.settings import STL_DEFAULT_CLIENT_MAX_SIZE
from eidaws.stationlite.version import __version__
from eidaws.utils.strict import KeywordParser


def setup_keywordparser_error_handler(service_version=None):
    @keyword_parser.error_handler
    def handle_parser_error(error, req):

        raise FDSNHTTPError.create(
            400, service_version=service_version, error_desc_long=str(error),
        )

    return handle_parser_error


class FlaskKeywordParser(KeywordParser):

    LOGGER = "eidaws.stationlite.strict.keyword_parser"

    def with_strict_args(self, schemas, locations=None):
        def decorator(func):
            @functools.wraps(func)
            def wrapper(*args, **kwargs):
                self.parse(schemas, request, locations)
                return func(*args, **kwargs)

            wrapper.__wrapped__ = func
            return wrapper

        return decorator

    def _get_args(self, req):
        return req.args

    def _get_data(
        self, req, as_text=True, max_content_length=STL_DEFAULT_CLIENT_MAX_SIZE
    ):
        """
        Savely reads the buffered incoming data from the client.

        :param req: Request the raw data is read from
        :type req: :py:class:`flask.Request`
        :param bool as_text: If set to :code:`True` the return value will be a
            decoded unicode string.
        :param int max_content_length: Max bytes accepted
        :returns: Byte string or rather unicode string, respectively. Depending
            on the :code:`as_text` parameter.
        """
        if req.content_length > max_content_length:
            raise FDSNHTTPError(413, service_version=__version__)

        return req.get_data(cache=True, as_text=as_text)


keyword_parser = FlaskKeywordParser()
with_strict_args = keyword_parser.with_strict_args
