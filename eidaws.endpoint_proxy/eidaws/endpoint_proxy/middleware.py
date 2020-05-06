# -*- coding: utf-8 -*-

import asyncio
import logging
import sys
import traceback

from aiohttp import web

from eidaws.endpoint_proxy.settings import PROXY_BASE_ID


logger = logging.getLogger(PROXY_BASE_ID + ".middleware")


@web.middleware
async def exception_handling_middleware(request, handler):
    try:
        return await handler(request)
    except (
        web.HTTPBadRequest,
        web.HTTPNotFound,
        web.HTTPForbidden,
        web.HTTPRequestEntityTooLarge,
        web.HTTPServiceUnavailable,
        asyncio.CancelledError,
    ) as err:
        print(err)
        raise err
    except Exception as err:
        exc_type, exc_value, exc_traceback = sys.exc_info()
        logger.critical(f"Local Exception: {type(err)}")
        logger.critical(
            "Traceback information: "
            + repr(
                traceback.format_exception(exc_type, exc_value, exc_traceback)
            )
        )
        raise err
