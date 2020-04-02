# -*- coding: utf-8 -*-

import asyncio
import functools
import logging
import sys
import traceback

from eidaws.federator.settings import FED_BASE_ID
from eidaws.federator.utils.mixin import ClientRetryBudgetMixin, ConfigMixin
from eidaws.utils.error import ErrorWithTraceback
from eidaws.federator.utils.misc import make_context_logger


def _split_stream_epoch(stream_epoch, num, default_endtime):
    return stream_epoch.slice(num=num, default_endtime=default_endtime)


def with_exception_handling(coro):
    """
    Method decorator providing general purpose exception handling for worker
    tasks.
    """

    @functools.wraps(coro)
    async def wrapper(self, *args, **kwargs):

        try:
            await coro(self, *args, **kwargs)
        except asyncio.CancelledError:
            raise
        except Exception as err:
            exc_type, exc_value, exc_traceback = sys.exc_info()
            self.logger.critical(f"Local TaskWorker exception: {type(err)}")
            self.logger.critical(
                "Traceback information: "
                + repr(
                    traceback.format_exception(
                        exc_type, exc_value, exc_traceback
                    )
                )
            )
            self._queue.task_done()

    return wrapper


class WorkerError(ErrorWithTraceback):
    """Base Worker error ({})."""


class BaseAsyncWorker(ClientRetryBudgetMixin, ConfigMixin):
    """
    Abstract base class for worker implementations.
    """

    LOGGER = FED_BASE_ID + ".worker"

    def __init__(self, request):

        self.request = request

        self._logger = logging.getLogger(self.LOGGER)
        self.logger = make_context_logger(self._logger, self.request)

    async def run(self, req_method="GET", **kwargs):
        raise NotImplementedError

    async def _handle_error(self, error=None, **kwargs):
        msg = kwargs.get("msg", error)
        if msg is not None:
            self.logger.warning(str(msg))

    async def _handle_413(self, url=None, stream_epoch=None, **kwargs):
        raise WorkerError("HTTP code 413 handling not implemented.")
