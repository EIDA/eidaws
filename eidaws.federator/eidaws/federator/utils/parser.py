# -*- coding: utf-8 -*-
"""
Federator schema definitions
"""
from webargs import core
from webargs.aiohttpparser import parser, AIOHTTPParser

from marshmallow import (
    fields,
    Schema,
    SchemaOpts,
    post_load,
)

from eidaws.federator.utils.httperror import FDSNHTTPError
from eidaws.utils.parser import FDSNWSParserMixin
from eidaws.utils.misc import get_req_config
from eidaws.utils.settings import KEY_REQUEST_STARTTIME


class ServiceOpts(SchemaOpts):
    """
    Same as the default class Meta options, but adds the *service* option.
    """

    def __init__(self, meta, **kwargs):
        SchemaOpts.__init__(self, meta, **kwargs)
        self.service = getattr(meta, "service", "dataselect")


class ServiceSchema(Schema):
    """
    Base class for webservice schema definitions.
    """

    OPTIONS_CLASS = ServiceOpts

    # read-only (the parameter is not parsed by webargs)
    service = fields.Str(dump_only=True)

    @post_load
    def set_service(self, data, **kwargs):
        data["service"] = self.opts.service
        return data

    class Meta:
        strict = True


# -----------------------------------------------------------------------------
def setup_parser_error_handler(service_version=None):
    @parser.error_handler
    @fdsnws_parser.error_handler
    def handle_parser_error(error, req, schema, status_code, headers):

        raise FDSNHTTPError.create(
            400,
            req,
            request_submitted=get_req_config(req, KEY_REQUEST_STARTTIME),
            service_version=service_version,
            error_desc_long=str(error),
        )

    return handle_parser_error


class FDSNWSAIOHTTPParser(FDSNWSParserMixin, AIOHTTPParser):
    def parse_querystring(self, req, name, field):
        return core.get_value(
            self._parse_streamepochs_from_argdict(req.query), name, field
        )

    async def parse_form(self, req, name, field):
        return core.get_value(
            self._parse_postfile(await req.text()), name, field
        )


fdsnws_parser = FDSNWSAIOHTTPParser()
use_fdsnws_args = fdsnws_parser.use_args
use_fdsnws_kwargs = fdsnws_parser.use_kwargs
