# -*- coding: utf-8 -*-

import asyncio
import logging
import sys
import traceback

from collections import deque

from eidaws.federator.settings import FED_BASE_ID
from eidaws.utils.error import ErrorWithTraceback


# NOTE(damb): Based on https://github.com/CaliDog/asyncpool with some minor
# modifications.

logger = logging.getLogger(FED_BASE_ID + ".pool")


def _coroutine_or_raise(obj):
    """Makes sure an object is callable if it is not ``None``. If not
    a coroutine, a ``ValueError`` is raised.
    """
    if obj and not any(
        [asyncio.iscoroutine(obj), asyncio.iscoroutinefunction(obj)]
    ):

        raise ValueError(f"{obj!r} is not a coroutine.")
    return obj


class PoolError(ErrorWithTraceback):
    """Base Pool error ({})."""


class Pool:

    DEFAULT_NUM_WORKERS = 32

    QUEUE_CLS = asyncio.Queue

    LOGGER = FED_BASE_ID + ".pool"

    def __init__(
        self, worker_coro=None, max_workers=None, timeout=None,
    ):

        if max_workers is None:
            max_workers = self.DEFAULT_NUM_WORKERS
        if max_workers < 1:
            raise ValueError("Number of processes must be at least 1")

        self._size = max_workers
        self._loop = asyncio.get_event_loop()

        self._queue = self.QUEUE_CLS()
        self._exceptions = False

        self._worker_coro = _coroutine_or_raise(worker_coro)
        self._worker_tasks = deque()

        self._timeout = timeout

    @property
    def exceptions(self):
        return self._exceptions

    async def __aenter__(self):
        self.start()
        return self

    async def __aexit__(self, ext_type, exc, tb):
        await self.join()

    def start(self):
        assert not self._worker_tasks
        self._worker_coro = _coroutine_or_raise(self._worker_coro)
        self._exceptions = False

        for _ in range(self._size):
            worker_coro = self._wrap_worker_coro(self._worker_coro)
            self._worker_tasks.append(asyncio.create_task(worker_coro))

    async def submit(self, *args, return_future=False, **kwargs):
        fut = self._loop.create_future() if return_future else None
        await self._queue.put((fut, args, kwargs))
        return fut

    async def join(self, timeout=None):

        if not self._worker_tasks:
            return True

        timeout = timeout or self._timeout
        try:
            await asyncio.wait_for(self._queue.join(), timeout)
        except BaseException:
            raise
        else:
            return True
        finally:
            for worker in self._worker_tasks:
                worker.cancel()

            results = await asyncio.gather(
                *self._worker_tasks, return_exceptions=True
            )

            while results:
                result = results.pop()

                if isinstance(result, asyncio.CancelledError):
                    continue
                elif isinstance(result, Exception):
                    self._exceptions = True
                    raise PoolError(result)

    def worker(self, coro):
        """
        Decorator that registers a worker coroutine.
        """
        self._worker_coro = coro
        return coro

    async def _wrap_worker_coro(self, coro):

        while True:
            fut = None
            task_received = False
            try:
                obj = await self._queue.get()
                task_received = True

                fut, args, kwargs = obj
                result = await coro(*args, **kwargs)

                if fut:
                    fut.set_result(result)
            except asyncio.CancelledError:
                raise
            except Exception as err:
                if fut:
                    fut.set_exception(err)
                self._exception = True

                exc_type, exc_value, exc_traceback = sys.exc_info()
                logger.critical(
                    f"PoolWorker Exception: {type(err)}; Traceback information: "
                    + repr(
                        traceback.format_exception(
                            exc_type, exc_value, exc_traceback
                        )
                    )
                )
                raise PoolError(err)

            finally:
                if task_received:
                    self._queue.task_done()
