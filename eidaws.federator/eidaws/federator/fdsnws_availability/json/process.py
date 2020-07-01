# -*- coding: utf-8 -*-

import datetime
import io

from eidaws.federator.fdsnws_availability.json.parser import (
    AvailabilityQuerySchema,
    AvailabilityExtentSchema,
)
from eidaws.federator.fdsnws_availability.process import (
    AvailabilityWorker,
    AvailabilityRequestProcessor as _AvailabilityRequestProcessor,
)
from eidaws.federator.settings import (
    FED_BASE_ID,
    FED_AVAILABILITY_JSON_SERVICE_ID,
)
from eidaws.utils.settings import (
    FDSNWS_EXTENT_METHOD_TOKEN,
    FDSNWS_QUERY_METHOD_TOKEN,
)


class _AvailablityWorker(AvailabilityWorker):

    SERVICE_ID = FED_AVAILABILITY_JSON_SERVICE_ID

    LOGGER = ".".join([FED_BASE_ID, SERVICE_ID, "worker"])

    def _load(self, data):

        if not data:
            return None

        # extract "datasources" array
        try:
            return data[(data.index(b"[") + 1) : data.rindex(b"]")]
        except ValueError:
            return None

    def _dump(self, obj):
        _sorted = sorted(obj)
        return b",".join(obj[i] for i in _sorted)


class _AvailablityQueryWorker(_AvailablityWorker):
    QUERY_PARAM_SERIALIZER = AvailabilityQuerySchema


class _AvailablityExtentWorker(_AvailablityWorker):
    QUERY_PARAM_SERIALIZER = AvailabilityExtentSchema


class AvailabilityRequestProcessor(_AvailabilityRequestProcessor):

    SERVICE_ID = FED_AVAILABILITY_JSON_SERVICE_ID
    LOGGER = ".".join([FED_BASE_ID, SERVICE_ID, "process"])

    @property
    def content_type(self):
        return "application/json"

    @property
    def header(self):
        return (
            b'{"version":1.0,"created":"'
            + self._default_endtime.isoformat().encode("utf-8")
            + b'Z","datasources":['
        )

    async def _prepare_response(self, response):
        response.content_type = self.content_type
        response.charset = self.charset
        response.headers["Content-Disposition"] = (
            'inline; filename="'
            + FED_BASE_ID.replace(".", "-")
            + "-"
            + datetime.datetime.utcnow().isoformat()
            + '.json"'
        )
        await response.prepare(self.request)
        await response.write(self.header)

    async def _write_separator(self, response):
        await response.write(b",")

    async def _write_response_footer(self, response):
        await response.write(b"]}")

    def _create_worker(self, request, session, drain, lock=None, **kwargs):
        return _AvailablityWorker(
            self.request, session, drain, lock=lock,
        )


class AvailabilityQueryRequestProcessor(AvailabilityRequestProcessor):
    RESOURCE_METHOD = FDSNWS_QUERY_METHOD_TOKEN

    def _create_worker(self, request, session, drain, lock=None, **kwargs):
        return _AvailablityQueryWorker(
            self.request, session, drain, lock=lock,
        )


class AvailabilityExtentRequestProcessor(AvailabilityRequestProcessor):
    RESOURCE_METHOD = FDSNWS_EXTENT_METHOD_TOKEN

    def _create_worker(self, request, session, drain, lock=None, **kwargs):
        return _AvailablityExtentWorker(
            self.request, session, drain, lock=lock,
        )
