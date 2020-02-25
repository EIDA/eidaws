# -*- coding: utf-8 -*-
from webargs import core
from webargs.aiohttpparser import AIOHTTPParser


from eidaws.utils.parser import FDSNWSParserMixin

# TODO(damb): Move to eidaws.federator/eidaws/federator/utils/parser.py


# -----------------------------------------------------------------------------
class FDSNWSAIOHTTPParser(FDSNWSParserMixin, AIOHTTPParser):

    def parse_querystring(self, req, name, field):
        return core.get_value(
            self._parse_streamepochs_from_argdict(req.query),
            name, field)

    async def parse_form(self, req, name, field):
        post_data = self._cache.get('post')
        if post_data is None:
            self._cache['post'] = await req.post()

        return core.get_value(self._parse_postfile(
            self._cache['post'], name, field))


fdsnws_parser = FDSNWSAIOHTTPParser()
use_fdsnws_args = fdsnws_parser.use_args
use_fdsnws_kwargs = fdsnws_parser.use_kwargs
