# -*- coding: utf-8 -*-

import asyncio
import inspect
import logging

from marshmallow import Schema, exceptions

from eidaws.federator.settings import FED_BASE_ID
from eidaws.federator.utils.httperror import FDSNHTTPError
from eidaws.federator.utils.misc import _callable_or_raise
from eidaws.utils.error import Error
from eidaws.utils.settings import FDSNWS_QUERY_VALUE_SEPARATOR_CHAR


class KeywordParserError(Error):
    """Base KeywordParser error ({})."""


class ValidationError(KeywordParserError, exceptions.ValidationError):
    """ValidationError: {}."""


class AsyncKeywordParser:
    """
    Base class for async keyword parsers.
    """

    LOGGER = "strict.keywordparser"

    __location_map__ = {"query": "parse_querystring", "form": "parse_form"}

    def __init__(self, error_handler=None):
        self.error_callback = _callable_or_raise(error_handler)
        self.logger = logging.getLogger(self.LOGGER)

    @staticmethod
    def _parse_arg_keys(arg_dict):
        """
        :param dict arg_dict: Dictionary like structure to be parsed

        :returns: Tuple with argument keys
        :rtype: tuple
        """

        return tuple(arg_dict.keys())

    @staticmethod
    def _parse_postfile(postfile):
        """
        Parse all argument keys from a POST request file.

        :param str postfile: Postfile content

        :returns: Tuple with parsed keys.
        :rtype: tuple
        """
        argmap = {}

        for line in postfile.split("\n"):
            _line = line.split(FDSNWS_QUERY_VALUE_SEPARATOR_CHAR)
            if len(_line) != 2:
                continue

            if all(w == "" for w in _line):
                raise ValidationError("RTFM :)")

            argmap[_line[0]] = _line[1]

        return tuple(argmap.keys())

    def parse_querystring(self, req):
        """
        Parse argument keys from ``req``.

        :param req: Request object to be parsed
        :type req: :py:class:`flask.Request`
        """

        return self._parse_arg_keys(self._get_args(req))

    def _get_args(self, req):
        """
        Template method returning the query args from ``req``.
        """

        raise NotImplementedError

    async def parse_form(self, req):
        """
        :param req: Request object to be parsed
        :type req: :py:class:`flask.Request`
        """
        try:
            parsed_list = self._parse_postfile(await self._get_data(req))
        except ValidationError as err:
            if self.error_callback:
                self.error_callback(err, req)
            else:
                self.handle_error(err, req)

        return parsed_list

    async def _get_data(self, req, as_text=True):
        """
        Savely reads the buffered incoming data from the client.

        :param req: Request the raw data is read from
        :param bool as_text: If set to ``True`` the return value will be a
            decoded unicode string.
        :returns: Byte string or rather unicode string, respectively. Depending
            on the ``as_text`` parameter.
        """

        raise NotImplementedError

    async def parse(self, schemas, req, locations):
        """
        Validate request query parameters.

        :param schemas: Marshmallow Schemas used for request validation
        :type schemas: tuple/list of :py:class:`marshmallow.Schema`
            or :py:class:`marshmallow.Schema`
        :param locations: Locations where to load data from
        :type locations: tuple of str

        Calls `handle_error` with :py:class:`ValidationError`.
        """
        if inspect.isclass(schemas):
            schemas = [schemas()]
        elif isinstance(schemas, Schema):
            schemas = [schemas]

        valid_fields = set()
        for schema in [s() if inspect.isclass(s) else s for s in schemas]:
            valid_fields.update(schema.fields.keys())

        parsers = []
        for l in locations:
            try:
                fn = self.__location_map__[l]
                if inspect.isfunction(fn) or asyncio.iscoroutinefunction(fn):
                    function = fn
                else:
                    function = getattr(self, fn)
                parsers.append(function)
            except KeyError:
                raise ValueError(f"Invalid location: {l!r}")

        req_args = set()

        for fn in parsers:
            parsed = (
                await fn(req) if asyncio.iscoroutinefunction(fn) else fn(req)
            )
            req_args.update(parsed)

        invalid_args = req_args.difference(valid_fields)
        if invalid_args:
            err = ValidationError(
                f"Invalid request query parameters: {invalid_args}"
            )

            if self.error_callback:
                self.error_callback(err, req)
            else:
                self.handle_error(err, req)

    def error_handler(self, func):
        """
        Decorator that registers a custom error handling function. The
        function should received the raised error, request object used
        to parse the request. Overrides the parser's ``handle_error``
        method.

        Example: 

        .. code ::

            from eidaws.federator.utils.strict import keyword_parser

            class CustomError(Exception):
                pass


            @keyword_parser.error_handler
            def handle_error(error, req):
                raise CustomError(error.messages)

        :param callable func: The error callback to register.
        """
        self.error_callback = func
        return func

    def handle_error(self, error, req):
        """
        Called if an error occurs while parsing strict args.
        By default, just logs and raises ``error``.

        :param Exception error: an Error to be handled
        :param Request req: request object

        :raises: Exception
        """
        self.logger.error(error)
        raise error


# ----------------------------------------------------------------------------
def setup_keywordparser_error_handler(service_version=None):
    @keyword_parser.error_handler
    def handle_parser_error(error, req):

        raise FDSNHTTPError.create(
            400,
            req,
            request_submitted=req[FED_BASE_ID + ".request_starttime"],
            service_version=service_version,
            error_desc_long=str(error),
        )

    return handle_parser_error


class AIOHTTPKeywordParser(AsyncKeywordParser):
    """
    aiohttp implementation of :py:class:`AsyncKeywordParser`.
    """

    def _get_args(self, req):
        return req.query

    async def _get_data(self, req, as_text=True):
        if as_text:
            return await req.text()

        return await req.read()


keyword_parser = AIOHTTPKeywordParser()
