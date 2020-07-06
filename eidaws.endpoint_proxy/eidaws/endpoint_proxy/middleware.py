# -*- coding: utf-8 -*-

import asyncio
import datetime
import logging
import sys
import traceback
import uuid

from aiohttp import web

from eidaws.endpoint_proxy.settings import PROXY_BASE_ID
from eidaws.utils.misc import log_access, make_context_logger
from eidaws.utils.settings import (
    REQUEST_CONFIG_KEY,
    KEY_REQUEST_ID,
    KEY_REQUEST_STARTTIME,
)


logger = logging.getLogger(PROXY_BASE_ID + ".middleware")


@web.middleware
async def before_request(request, handler):
    # set up config dict
    request[REQUEST_CONFIG_KEY] = dict()
    request[REQUEST_CONFIG_KEY][
        KEY_REQUEST_STARTTIME
    ] = datetime.datetime.utcnow()
    request[REQUEST_CONFIG_KEY][KEY_REQUEST_ID] = uuid.uuid4()

    log_access(logger, request)

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
        _logger.critical(
            f"Local Exception: error={type(err)}, "
            f"url={request.url!r}, method={request.method!r}"
        )
        _logger.critical(
            "Traceback information: "
            + repr(
                traceback.format_exception(exc_type, exc_value, exc_traceback)
            )
        )
        raise err
