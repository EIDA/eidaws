# -*- coding: utf-8 -*-

import abc
import logging

from eidaws.federator.settings import FED_BASE_ID
from eidaws.federator.utils.mixin import ClientRetryBudgetMixin, ConfigMixin
from eidaws.utils.error import ErrorWithTraceback
from eidaws.federator.utils.misc import make_context_logger


def _split_stream_epoch(stream_epoch, num, default_endtime):
    return stream_epoch.slice(num=num, default_endtime=default_endtime)


class WorkerError(ErrorWithTraceback):
    """Base Worker error ({})."""


class BaseAsyncWorker(abc.ABC, ClientRetryBudgetMixin, ConfigMixin):
    """
    Abstract base class for worker implementations.
    """

    LOGGER = FED_BASE_ID + ".worker"

    def __init__(self, request):

        self.request = request

        self._logger = logging.getLogger(self.LOGGER)
        self.logger = make_context_logger(self._logger, self.request)

    @abc.abstractmethod
    async def run(self, req_method="GET", **kwargs):
        pass

    async def _handle_error(self, error=None, **kwargs):
        msg = kwargs.get("msg", error)
        if msg is not None:
            self.logger.warning(str(msg))

    async def _handle_413(self, url=None, stream_epoch=None, **kwargs):
        raise WorkerError("HTTP code 413 handling not implemented.")
