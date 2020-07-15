# -*- coding: utf-8 -*-

import asyncio
import aiohttp

from eidaws.federator.fdsnws_station.text.parser import StationTextSchema
from eidaws.federator.settings import (
    FED_BASE_ID,
    FED_STATION_TEXT_SERVICE_ID,
)
from eidaws.federator.utils.process import UnsortedResponse
from eidaws.federator.utils.worker import (
    with_exception_handling,
    BaseWorker,
)
from eidaws.utils.misc import make_context_logger
from eidaws.utils.settings import FDSNWS_NO_CONTENT_CODES


class _StationTextWorker(BaseWorker):
    """
    A worker task which fetches data and writes the results to the ``response``
    object.
    """

    SERVICE_ID = FED_STATION_TEXT_SERVICE_ID
    QUERY_PARAM_SERIALIZER = StationTextSchema

    LOGGER = ".".join([FED_BASE_ID, SERVICE_ID, "worker"])

    @with_exception_handling(ignore_runtime_exception=True)
    async def run(self, route, req_method="GET", context=None, **req_kwargs):
        # context logging
        try:
            logger = make_context_logger(self._logger, *context["logger_ctx"])
        except (TypeError, KeyError):
            logger = self.logger
        finally:
            context["logger"] = logger

        req_handler = self.REQUEST_HANDLER_CLS(
            **route._asdict(),
            query_params=self.query_params,
            headers=self.request_headers,
        )

        req_handler.format = self.format
        req = getattr(req_handler, req_method.lower())(self._session)

        self._log_request(req_handler, req_method, logger=logger)
        resp_status = None
        try:
            async with req(**req_kwargs) as resp:
                resp.raise_for_status()

                resp_status = resp.status
                msg = (
                    f"Response: {resp.reason}: resp.status={resp_status}, "
                    f"resp.request_info={resp.request_info}, "
                    f"resp.url={resp.url}, resp.headers={resp.headers}"
                )
                if resp_status == 200:
                    logger.debug(msg)
                    # XXX(damb): Read the entire response into memory
                    text = await resp.read()
                    # strip header
                    data = text[(text.find(b"\n") + 1) :]
                    if data:
                        async with self._lock:
                            await self._drain.drain(data)

                elif resp_status in FDSNWS_NO_CONTENT_CODES:
                    logger.info(msg)
                else:
                    await self._handle_error(msg=msg, context=context)

        except aiohttp.ClientResponseError as err:
            resp_status = err.status
            msg = (
                f"Error while executing request: {err.message}: "
                f"error={type(err)}, resp.status={resp_status}, "
                f"resp.request_info={err.request_info}, "
                f"resp.headers={err.headers}"
            )

            if resp_status == 413:
                await self._handle_413()
            elif resp_status in FDSNWS_NO_CONTENT_CODES:
                logger.info(msg)
            else:
                await self._handle_error(msg=msg, context=context)

        except (aiohttp.ClientError, asyncio.TimeoutError) as err:
            resp_status = 503
            msg = (
                f"Error while executing request: error={type(err)}, "
                f"req_handler={req_handler!r}, method={req_method}"
            )
            await self._handle_error(msg=msg, context=context)

        finally:
            if resp_status is not None:
                await self.update_cretry_budget(req_handler.url, resp_status)

            await self.finalize()


class StationTextRequestProcessor(UnsortedResponse):

    SERVICE_ID = FED_STATION_TEXT_SERVICE_ID

    LOGGER = ".".join([FED_BASE_ID, SERVICE_ID, "process"])

    _HEADER_MAP = {
        "network": b"#Network|Description|StartTime|EndTime|TotalStations",
        "station": (
            b"#Network|Station|Latitude|Longitude|"
            b"Elevation|SiteName|StartTime|EndTime"
        ),
        "channel": (
            b"#Network|Station|Location|Channel|Latitude|"
            b"Longitude|Elevation|Depth|Azimuth|Dip|"
            b"SensorDescription|Scale|ScaleFreq|ScaleUnits|"
            b"SampleRate|StartTime|EndTime"
        ),
    }

    @property
    def content_type(self):
        return "text/plain"

    @property
    def charset(self):
        return "utf-8"

    async def _prepare_response(self, response):
        await super()._prepare_response(response)
        header = self._HEADER_MAP[self.query_params["level"]]
        await response.write(header + b"\n")

    def _create_worker(self, request, session, drain, lock=None, **kwargs):
        return _StationTextWorker(request, session, drain, lock=lock, **kwargs)
