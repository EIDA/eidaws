# -*- coding: utf-8 -*-
"""
Facilities related with statistics.
"""

import abc
import asyncio
import os
import time
import uuid

import aioredis

from copy import deepcopy
from urllib.parse import urlsplit

from eidaws.utils.error import ErrorWithTraceback


class StatsError(ErrorWithTraceback):
    """Base Stats error ({})."""


class RedisCollection(metaclass=abc.ABCMeta):
    """
    Abstract class providing backend functionality for Redis collections.
    """

    ENCODING = "utf-8"

    def __init__(self, redis, key=None, **kwargs):
        self.redis = redis

        self.key = key or self._create_key()

    async def _transaction(self, fn, *extra_keys, **kwargs):
        """
        Helper simplifying code within a watched transaction.

        Takes *fn*, function treated as a transaction. Returns whatever
        *fn* returns. :code:`self.key` is watched. `fn` takes `tr` as
        the only argument.

        :param fn: Closure treated as a transaction.
        :type fn: function `fn(tr)`
        :param extra_keys: Optional list of additional keys to watch.
        :type extra_keys: list
        :rtype: whatever *fn* returns
        """
        results = []

        watch_delay = kwargs.pop("watch_delay", None)
        while True:
            await self.redis.watch(self.key, *extra_keys)
            # XXX(damb): A aioredis.Pipline like object can only once used for
            # execution.
            tr = self.redis.multi_exec()
            try:
                fn(tr)

                results = await tr.execute(return_exceptions=True)
                break
            except aioredis.MultiExecError:
                if watch_delay is not None and watch_delay > 0:
                    await asyncio.sleep(watch_delay)
                continue

        return results

    @abc.abstractmethod
    async def _data(self, **kwargs):
        """
        Helper for getting the time series data within a transaction.
        """

    async def _clear(self):
        """
        Helper for clear operations.
        """

        await self.redis.delete(self.key)

    @staticmethod
    def _create_key():
        """
        Creates a random Redis key for storing this collection's data.

        :rtype: string

        .. note::
            :py:func:`uuid.uuid4` is used. If you are not satisfied with its
            `collision probability
            <http://stackoverflow.com/a/786541/325365>`_, make your own
            implementation by subclassing and overriding this method.
        """

        return uuid.uuid4().hex


class ResponseCodeTimeSeries(RedisCollection):
    """
    Distributed collection implementing a response code time series. The
    timeseries is implemented based on Redis' `sorted set
    <https://redis.io/topics/data-types>`_ following the pattern described at
    `redislabs.com
    <https://redislabs.com/redis-best-practices/time-series/sorted-set-time-series/>`_.

    ..warning::
        The ``window_size`` of the collection can't be enforced when multiple
        processes are accessing its corresponding Redis collection.
    """

    KEY_DELIMITER = b":"
    _DEFAULT_TTL = 3600  # seconds
    _DEFAULT_WINDOW_SIZE = 10000

    ERROR_CODES = (500, 503)

    def __init__(self, redis, key=None, **kwargs):
        super().__init__(redis, key, **kwargs)

        self.ttl, self.window_size = self._validate_ctor_args(
            kwargs.get("ttl", self._DEFAULT_TTL),
            kwargs.get("window_size", self._DEFAULT_WINDOW_SIZE),
        )

        self._buffer = None

    async def get_error_ratio(self):
        """
        Returns the error ratio of the response code time series. Values are
        between ``0`` (no errors) and ``1`` (errors only).
        """
        data = await self._data(ttl=self.ttl)
        num_errors = len(
            [code for code, t in data if int(code) in self.ERROR_CODES]
        )

        if not data:
            return 0

        return num_errors / len(data)

    async def _len(self, **kwargs):
        return len(await self._data(**kwargs))

    def __aiter__(self):
        return self

    async def __anext__(self):
        # buffered iteration; similar to the example given in
        # https://www.python.org/dev/peps/pep-0492/
        if self._buffer is None:
            self._buffer = await self._data(ttl=self.ttl)

        if not self._buffer:
            self._buffer = None
            raise StopAsyncIteration

        return self._buffer.pop(0)

    async def gc(self, **kwargs):
        """
        Discard deprecated values from the time series.
        """
        ttl = kwargs.get("ttl") or self.ttl
        thres = time.time() - ttl

        await self.redis.zremrangebyscore(
            self.key, min=float("-inf"), max=thres
        )

    async def clear(self, **kwargs):
        await self._clear()

    async def append(self, value):
        """
        Append *value* to the time series.

        :param int value: Response code to be appended
        """
        hash(value)

        num_items = await self.redis.zcount(
            self.key, min=float("-inf"), max=float("inf")
        )

        def append_trans(tr):
            if (self.window_size is not None) and (
                num_items >= self.window_size
            ):
                idx = num_items - self.window_size
                tr.zremrangebyrank(self.key, 0, idx)

            score = time.time()
            member = self._serialize(value, score)
            tr.zadd(self.key, score, member)

        await self._transaction(append_trans, watch_delay=0.005)

    async def _data(self, **kwargs):
        """
        Helper for getting the time series data within a transaction.
        """
        ttl = kwargs.get("ttl") or self.ttl

        now = time.time()
        items = await self.redis.zrevrangebyscore(
            self.key, now, now - ttl, withscores=True
        )

        if not items:
            return []

        return [(self._deserialize(member), score) for member, score in items]

    def _deserialize(self, value, **kwargs):
        retval = value.split(self.KEY_DELIMITER)[0]
        return retval.decode(self.ENCODING)

    def _serialize(self, value, score, **kwargs):
        return (
            str(value).encode(self.ENCODING)
            + self.KEY_DELIMITER
            + str(score).encode(self.ENCODING)
            +
            # add 8 random bytes
            os.urandom(8)
        )

    @staticmethod
    def _validate_ctor_args(ttl, window_size):
        if ttl < 0 or window_size < 0:
            raise ValueError("Negative value specified.")
        return ttl, window_size


class ResponseCodeStats:
    """
    Container for datacenter response code statistics handling.
    """

    DEFAULT_PREFIX = b"stats:response-codes"

    def __init__(self, redis, prefix=None, **kwargs):

        self.redis = redis
        self.kwargs_series = kwargs

        self._prefix = prefix or self.DEFAULT_PREFIX
        if isinstance(self._prefix, str):
            self._prefix = self._prefix.encode(RedisCollection.ENCODING)

        self._map = {}

    async def add(self, url, code, **kwargs):
        """
        Add ``code`` to a response code time series specified by ``url``.
        """
        kwargs_series = deepcopy(self.kwargs_series)
        kwargs_series.update(kwargs)

        key = self._create_key_from_url(url, prefix=self._prefix)

        if key not in self._map:
            self._map[key] = ResponseCodeTimeSeries(
                redis=self.redis, key=key, **kwargs_series
            )

        await self._map[key].append(code)

    async def gc(self, url, lazy_load=True):
        """
        Discard deprecated values from a response code time series specified by
        ``url``.

        :param bool lazy_load: Lazily load the response code time series to be
            garbage collected
        """
        key = self._create_key_from_url(url, prefix=self._prefix)

        if lazy_load and key not in self._map:
            # lazy loading
            self._map[key] = ResponseCodeTimeSeries(
                redis=self.redis, key=key, **self.kwargs_series
            )

        await self._map[key].gc()

    async def clear(self, url):
        key = self._create_key_from_url(url, prefix=self._prefix)

        try:
            await self._map[key].clear()
        except KeyError as err:
            raise StatsError(err)

    async def get_error_ratio(self, url, lazy_load=True):
        """
        Return the error ratio of a response code time series specified by
        ``url``.

        :param bool lazy_load: Lazily load the response code time series the
            error ratio is computed from
        """

        key = self._create_key_from_url(url, prefix=self._prefix)

        if lazy_load and key not in self._map:
            # lazy loading
            self._map[key] = ResponseCodeTimeSeries(
                redis=self.redis, key=key, **self.kwargs_series
            )

        return await self._map[key].get_error_ratio()

    def __contains__(self, url):
        return self._create_key_from_url(url) in self._map

    def __getitem__(self, key):
        return self._map[key]

    @staticmethod
    def _create_key_from_url(url, prefix=None):
        delimiter = ResponseCodeTimeSeries.KEY_DELIMITER

        if isinstance(url, str):
            url = url.encode(RedisCollection.ENCODING)

        split_result = urlsplit(url)
        args = [split_result.path, split_result.netloc]

        if prefix:
            args.insert(0, prefix)

        return delimiter.join(args)
