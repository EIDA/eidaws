# -*- coding: utf-8 -*-

import datetime

from eidaws.federator.fdsnws_availability.text.parser import (
    AvailabilityQuerySchema,
    AvailabilityExtentSchema,
)
from eidaws.federator.fdsnws_availability.process import (
    AvailabilityWorker,
    AvailabilityRequestProcessor as _AvailabilityRequestProcessor,
)
from eidaws.federator.settings import (
    FED_BASE_ID,
    FED_AVAILABILITY_TEXT_SERVICE_ID,
)
from eidaws.utils.settings import (
    FDSNWS_EXTENT_METHOD_TOKEN,
    FDSNWS_QUERY_METHOD_TOKEN,
)


class _AvailablityWorker(AvailabilityWorker):

    SERVICE_ID = FED_AVAILABILITY_TEXT_SERVICE_ID

    LOGGER = ".".join([FED_BASE_ID, SERVICE_ID, "worker"])

    def _load(self, data):
        if not data:
            return None

        if data[0] == 35:  # b'#'
            # strip off header
            return data[(data.find(b"\n") + 1) :]
        return data

    def _dump(self, obj):
        _sorted = sorted(obj)
        return b"".join(obj[i] for i in _sorted)


class _AvailablityQueryWorker(_AvailablityWorker):
    QUERY_PARAM_SERIALIZER = AvailabilityQuerySchema


class _AvailablityExtentWorker(_AvailablityWorker):
    QUERY_PARAM_SERIALIZER = AvailabilityExtentSchema


class AvailabilityRequestProcessor(_AvailabilityRequestProcessor):

    SERVICE_ID = FED_AVAILABILITY_TEXT_SERVICE_ID
    LOGGER = ".".join([FED_BASE_ID, SERVICE_ID, "process"])

    @property
    def content_type(self):
        return "text/plain"

    @property
    def charset(self):
        return "utf-8"

    @property
    def header(self):
        header = [b"#Network", b"Station", b"Location", b"Channel"]
        if "quality" not in self.merge:
            header.append(b"Quality")
        if "samplerate" not in self.merge:
            header.append(b"SampleRate")
        header.append(b"Earliest")
        header.append(b"Latest".rjust(25))
        if self.RESOURCE_METHOD == FDSNWS_QUERY_METHOD_TOKEN:
            if self.query_params.get("show") == "latestupdate":
                header.append(b"Updated".rjust(28))
        elif self.RESOURCE_METHOD == FDSNWS_EXTENT_METHOD_TOKEN:
            header.append(b"Updated".rjust(28))
            header.append(b"TimeSpans".rjust(29))
            header.append(b"Restriction")
        return b" ".join(header)

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
        await response.write(self.header)
        await response.write(b"\n")

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
