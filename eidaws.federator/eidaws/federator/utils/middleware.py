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


logger = logging.getLogger(FED_BASE_ID + ".middleware")


@web.middleware
async def before_request(request, handler):

    request[FED_BASE_ID + ".request_starttime"] = datetime.datetime.utcnow()
    request[FED_BASE_ID + ".request_id"] = uuid.uuid4()

    return await handler(request)


@web.middleware
async def exception_handling_middleware(request, handler):
    try:
        return await handler(request)
    except (web.HTTPNotFound, asyncio.CancelledError, FDSNHTTPError) as err:
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
        raise FDSNHTTPError.create(
            500,
            request,
            request_submitted=request[FED_BASE_ID + ".request_starttime"],
            service_version=__version__,
        )
