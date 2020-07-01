# -*- coding: utf-8 -*-

import datetime

from eidaws.federator.fdsnws_availability.process import (
    AvailabilityWorker,
    AvailabilityRequestProcessor as _AvailabilityRequestProcessor,
)
from eidaws.federator.fdsnws_availability.geocsv.parser import (
    AvailabilityQuerySchema,
    AvailabilityExtentSchema,
)
from eidaws.federator.settings import (
    FED_BASE_ID,
    FED_AVAILABILITY_GEOCSV_SERVICE_ID,
)
from eidaws.utils.settings import (
    FDSNWS_EXTENT_METHOD_TOKEN,
    FDSNWS_QUERY_METHOD_TOKEN,
)


def _find_nth(string, substr, n):
    if n == 0:
        raise ValueError
    elif n == 1:
        return string.find(substr)
    else:
        return string.find(substr, _find_nth(string, substr, n - 1) + 1)


class _AvailablityWorker(AvailabilityWorker):

    SERVICE_ID = FED_AVAILABILITY_GEOCSV_SERVICE_ID

    LOGGER = ".".join([FED_BASE_ID, SERVICE_ID, "worker"])

    def _load(self, data):
        if not data:
            return None
        # strip off header
        return data[_find_nth(data, b"\n", 5) + 1 :]

    def _dump(self, obj):
        _sorted = sorted(obj)
        return b"".join(obj[i] for i in _sorted)


class _AvailablityQueryWorker(_AvailablityWorker):
    QUERY_PARAM_SERIALIZER = AvailabilityQuerySchema


class _AvailablityExtentWorker(_AvailablityWorker):
    QUERY_PARAM_SERIALIZER = AvailabilityExtentSchema


class AvailabilityRequestProcessor(_AvailabilityRequestProcessor):

    SERVICE_ID = FED_AVAILABILITY_GEOCSV_SERVICE_ID
    LOGGER = ".".join([FED_BASE_ID, SERVICE_ID, "process"])

    @property
    def content_type(self):
        return "text/csv"

    @property
    def charset(self):
        return "utf-8"

    @property
    def header(self):
        def to_string(l):
            return b"|".join(l)

        header_fields = [
            (b"#field_unit: unitless", b"#field_type: string", b"Network"),
            (b"unitless", b"string", b"Station"),
            (b"unitless", b"string", b"Location"),
            (b"unitless", b"string", b"Channel"),
        ]
        if "quality" not in self.merge:
            header_fields.append((b"unitless", b"string", b"Quality"))
        if "samplerate" not in self.merge:
            header_fields.append((b"hertz", b"float", b"SampleRate"))
        header_fields.append((b"ISO_8601", b"datetime", b"Earliest"))
        header_fields.append((b"ISO_8601", b"datetime", b"Latest"))
        if self.RESOURCE_METHOD == FDSNWS_QUERY_METHOD_TOKEN:
            if self.query_params.get("show") == "latestupdate":
                header_fields.append((b"ISO_8601", b"datetime", b"Updated"))
        elif self.RESOURCE_METHOD == FDSNWS_EXTENT_METHOD_TOKEN:
            header_fields.append((b"ISO_8601", b"datetime", b"Updated"))
            header_fields.append((b"unitless", b"integer", b"TimeSpans"))
            header_fields.append((b"unitless", b"string", b"Restriction"))

        header_units, header_types, header_names = zip(*header_fields)
        header_units = to_string(header_units)
        header_types = to_string(header_types)
        header_names = to_string(header_names)

        return b"\n".join(
            [
                b"#dataset: GeoCSV 2.0",
                b"#delimiter: |",
                header_units,
                header_types,
                header_names,
            ]
        )

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
