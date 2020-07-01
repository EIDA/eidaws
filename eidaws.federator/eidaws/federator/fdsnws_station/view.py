# -*- coding: utf-8 -*-

from eidaws.federator.settings import FED_BASE_ID
from eidaws.federator.utils.view import BaseView


def StationView(service_id, schema, processor_cls):
    class _StationView(BaseView):

        LOGGER = ".".join((FED_BASE_ID, service_id, "view"))

        SERVICE_ID = service_id

        def __init__(self, request):
            super().__init__(
                request, schema=schema, processor_cls=processor_cls,
            )

    return _StationView
