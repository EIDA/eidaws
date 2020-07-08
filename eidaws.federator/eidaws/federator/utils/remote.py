# -*- coding: utf-8 -*-

from aiohttp import web
from aiohttp_remotes import XForwardedRelaxed as _XForwardedRelaxed

from eidaws.federator.utils.httperror import FDSNHTTPError
from eidaws.federator.version import __version__
from eidaws.utils.misc import get_req_config
from eidaws.utils.settings import KEY_REQUEST_STARTTIME


class XForwardedRelaxed(_XForwardedRelaxed):
    @web.middleware
    async def middleware(self, request, handler):
        try:
            return await super().middleware(request, handler)
        except (IndexError, ValueError):
            await self.raise_error(request)

    def raise_error(self, request):
        raise FDSNHTTPError.create(
            400,
            request,
            request_submitted=get_req_config(request, KEY_REQUEST_STARTTIME),
            service_version=__version__,
            error_desc_long="Invalid HTTP header configuration.",
        )
