# -*- coding: utf-8 -*-

import datetime

from eidaws.federator.fdsnws_availability.process import (
    AvailabilityAsyncWorker,
    AvailabilityRequestProcessor as _AvailabilityRequestProcessor,
)
from eidaws.federator.fdsnws_availability.request.parser import (
    AvailabilityQuerySchema,
    AvailabilityExtentSchema,
)
from eidaws.federator.settings import (
    FED_BASE_ID,
    FED_AVAILABILITY_REQUEST_SERVICE_ID,
)
from eidaws.utils.settings import (
    FDSNWS_EXTENT_METHOD_TOKEN,
    FDSNWS_QUERY_METHOD_TOKEN,
)


class _AvailablityAsyncWorker(AvailabilityAsyncWorker):

    SERVICE_ID = FED_AVAILABILITY_REQUEST_SERVICE_ID

    LOGGER = ".".join([FED_BASE_ID, SERVICE_ID, "worker"])

    def _load(self, data):
        if not data:
            return None
        return data

    def _dump(self, obj):
        _sorted = sorted(obj)
        return b"".join(obj[i] for i in _sorted)


class _AvailablityQueryAsyncWorker(_AvailablityAsyncWorker):
    QUERY_PARAM_SERIALIZER = AvailabilityQuerySchema


class _AvailablityExtentAsyncWorker(_AvailablityAsyncWorker):
    QUERY_PARAM_SERIALIZER = AvailabilityExtentSchema


class AvailabilityRequestProcessor(_AvailabilityRequestProcessor):

    SERVICE_ID = FED_AVAILABILITY_REQUEST_SERVICE_ID
    LOGGER = ".".join([FED_BASE_ID, SERVICE_ID, "process"])

    @property
    def content_type(self):
        return "text/plain"

    @property
    def charset(self):
        return "utf-8"

    async def _prepare_response(self, response):
        response.content_type = self.content_type
        response.charset = self.charset
        response.headers["Content-Disposition"] = (
            'inline; filename="'
            + FED_BASE_ID.replace(".", "-")
            + "-"
            + datetime.datetime.utcnow().isoformat()
            + '.txt"'
        )
        await response.prepare(self.request)

    def _create_worker(self, request, session, drain, lock=None, **kwargs):
        return _AvailablityAsyncWorker(
            self.request, session, drain, lock=lock,
        )


class AvailabilityQueryRequestProcessor(AvailabilityRequestProcessor):
    RESOURCE_METHOD = FDSNWS_QUERY_METHOD_TOKEN

    def _create_worker(self, request, session, drain, lock=None, **kwargs):
        return _AvailablityQueryAsyncWorker(
            self.request, session, drain, lock=lock,
        )


class AvailabilityExtentRequestProcessor(AvailabilityRequestProcessor):
    RESOURCE_METHOD = FDSNWS_EXTENT_METHOD_TOKEN

    def _create_worker(self, request, session, drain, lock=None, **kwargs):
        return _AvailablityExtentAsyncWorker(
            self.request, session, drain, lock=lock,
        )
