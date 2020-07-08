# -*- coding: utf-8 -*-

from aiohttp import web
from aiohttp_remotes import XForwardedRelaxed as _XForwardedRelaxed


class XForwardedRelaxed(_XForwardedRelaxed):
    @web.middleware
    async def middleware(self, request, handler):
        try:
            return await super().middleware(request, handler)
        except (IndexError, ValueError):
            await self.raise_error(request)
