# -*- coding: utf-8 -*-

from eidaws.federator.settings import FED_AVAILABILITY_TEXT_SERVICE_ID
from eidaws.federator.fdsnws_availability.parser import (
    AvailabilityExtentSchema,
    AvailabilityQuerySchema,
)
from eidaws.federator.fdsnws_availability.view import AvailabilityView
from eidaws.federator.fdsnws_availability.text.process import (
    AvailabilityQueryRequestProcessor,
    AvailabilityExtentRequestProcessor,
)

AvailabilityQueryView = AvailabilityView(
    FED_AVAILABILITY_TEXT_SERVICE_ID,
    AvailabilityQuerySchema,
    AvailabilityQueryRequestProcessor,
)

AvailabilityExtentView = AvailabilityView(
    FED_AVAILABILITY_TEXT_SERVICE_ID,
    AvailabilityExtentSchema,
    AvailabilityExtentRequestProcessor,
)
