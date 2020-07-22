# -*- coding: utf-8 -*-
import asyncio
import datetime
import logging
import uuid
import sys
import traceback

from aiohttp import web

from eidaws.federator.settings import FED_BASE_ID
from eidaws.federator.utils.httperror import FDSNHTTPError
from eidaws.federator.version import __version__
from eidaws.utils.settings import (
    REQUEST_CONFIG_KEY,
    KEY_REQUEST_ID,
    KEY_REQUEST_STARTTIME,
)
from eidaws.utils.misc import (
    get_req_config,
    log_access,
    make_context_logger,
)


logger = logging.getLogger(FED_BASE_ID + ".middleware")


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
        web.HTTPNotFound,
        web.HTTPForbidden,
        web.HTTPMethodNotAllowed,
        asyncio.CancelledError,
        FDSNHTTPError,
    ) as err:
        raise err
    except web.HTTPRequestEntityTooLarge as err:
        raise FDSNHTTPError.create(
            413,
            request,
            request_submitted=get_req_config(request, KEY_REQUEST_STARTTIME),
            error_desc_long=str(err),
            service_version=__version__,
        )
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
        raise FDSNHTTPError.create(
            500,
            request,
            request_submitted=get_req_config(request, KEY_REQUEST_STARTTIME),
            service_version=__version__,
        )
