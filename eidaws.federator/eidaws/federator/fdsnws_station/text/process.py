# -*- coding: utf-8 -*-

import asyncio
import aiohttp

from eidaws.federator.fdsnws_station.text.parser import StationTextSchema
from eidaws.federator.settings import (
    FED_BASE_ID,
    FED_STATION_TEXT_SERVICE_ID,
)
from eidaws.federator.utils.request import FdsnRequestHandler
from eidaws.federator.utils.process import UnsortedResponse
from eidaws.federator.utils.worker import (
    with_context_logging,
    with_exception_handling,
    BaseWorker,
)
from eidaws.utils.settings import FDSNWS_NO_CONTENT_CODES


class _StationTextWorker(BaseWorker):
    """
    A worker task which fetches data and writes the results to the ``response``
    object.
    """

    SERVICE_ID = FED_STATION_TEXT_SERVICE_ID
    QUERY_PARAM_SERIALIZER = StationTextSchema

    LOGGER = ".".join([FED_BASE_ID, SERVICE_ID, "worker"])

    @with_context_logging()
    @with_exception_handling(ignore_runtime_exception=True)
    async def run(self, route, req_method="GET", **req_kwargs):
        req_handler = FdsnRequestHandler(
            **route._asdict(), query_params=self.query_params
        )
        req_handler.format = self.format

        req = getattr(req_handler, req_method.lower())(self._session)

        resp_status = None
        try:
            resp = await req(**req_kwargs)
        except (aiohttp.ClientError, asyncio.TimeoutError) as err:
            msg = (
                f"Error while executing request: error={type(err)}, "
                f"req_handler={req_handler!r}, method={req_method}"
            )
            await self._handle_error(msg=msg)
            resp_status = 503

        else:

            data = await self._parse_resp(resp)

            if data is not None:
                async with self._lock:
                    await self._drain.drain(data)

            resp_status = resp.status
        finally:
            if resp_status is not None:
                await self.update_cretry_budget(req_handler.url, resp_status)

            await self.finalize()

    async def _parse_resp(self, resp):
        msg = (
            f"Response: {resp.reason}: resp.status={resp.status}, "
            f"resp.request_info={resp.request_info}, "
            f"resp.url={resp.url}, resp.headers={resp.headers}"
        )

        try:
            resp.raise_for_status()
        except aiohttp.ClientResponseError:
            if resp.status == 413:
                await self._handle_413()
            else:
                await self._handle_error(msg=msg)

            return None
        else:
            if resp.status != 200:
                if resp.status in FDSNWS_NO_CONTENT_CODES:
                    self.logger.info(msg)
                else:
                    await self._handle_error(msg=msg)

                return None
            else:
                self.logger.debug(msg)

        # XXX(damb): Read the entire response into memory
        try:
            text = await resp.read()
        except asyncio.TimeoutError as err:
            self.logger.warning(f"Socket read timeout: {type(err)}")
            return None
        else:
            # strip header
            return text[(text.find(b"\n") + 1) :]


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
        return _StationTextWorker(
            request, session, drain, lock=lock, **kwargs
        )
