# -*- coding: utf-8 -*-

import asyncio
import logging
import sys
import traceback
import uuid

from aiohttp import web

from eidaws.endpoint_proxy.settings import PROXY_BASE_ID
from eidaws.endpoint_proxy.utils import make_context_logger


logger = logging.getLogger(PROXY_BASE_ID + ".middleware")


@web.middleware
async def before_request(request, handler):
    request["request_id"] = uuid.uuid4()
    return await handler(request)


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
        web.HTTPGatewayTimeout,
        asyncio.CancelledError,
    ) as err:
        raise err
    except Exception as err:
        exc_type, exc_value, exc_traceback = sys.exc_info()
        _logger = make_context_logger(logger, request)
        _logger.critical(f"Local Exception: {type(err)}")
        _logger.critical(
            "Traceback information: "
            + repr(
                traceback.format_exception(exc_type, exc_value, exc_traceback)
            )
        )
        raise err
