# -*- coding: utf-8 -*-

import asyncio
import inspect

from marshmallow import Schema

from eidaws.federator.utils.httperror import FDSNHTTPError
from eidaws.utils.strict import (
    KeywordParser,
    ValidationError,
)
from eidaws.utils.misc import get_req_config
from eidaws.utils.settings import KEY_REQUEST_STARTTIME


class AsyncKeywordParser(KeywordParser):
    """
    Base class for async keyword parsers.
    """

    LOGGER = "eidaws.federator.utils.strict.keyword_parser"

    # TODO(damb): Lots of duplicate code here from BaseKeywordParser. Rethink.
    async def parse_form(self, req):
        """
        Parse argument keys from the ``req``'s form.

        :param req: Request object to be parsed
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
        for loc in locations:
            try:
                fn = self.__location_map__[loc]
                if inspect.isfunction(fn) or asyncio.iscoroutinefunction(fn):
                    function = fn
                else:
                    function = getattr(self, fn)
                parsers.append(function)
            except KeyError:
                raise ValueError(f"Invalid location: {loc!r}")

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


# ----------------------------------------------------------------------------
def setup_keywordparser_error_handler(service_version=None):
    @keyword_parser.error_handler
    def handle_parser_error(error, req):

        raise FDSNHTTPError.create(
            400,
            req,
            request_submitted=get_req_config(req, KEY_REQUEST_STARTTIME),
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
