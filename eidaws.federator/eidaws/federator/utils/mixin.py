# -*- coding: utf-8 -*-

import asyncio
import aioredis
import base64
import functools
import hashlib

from eidaws.federator.utils.cache import null_control


def with_redis_exception_handling(propagate_exceptions=False):
    """
    Returns a method decorator providing Redis exception handling facilities.
    """

    def propagate_exception(err):
        if propagate_exceptions:
            raise err

    def decorator(func):
        @functools.wraps(func)
        async def wrapper(self, *args, **kwargs):
            try:
                return await func(self, *args, **kwargs)
            except asyncio.TimeoutError as err:
                self.logger.warning(
                    f"Timout while connecting Redis: {type(err)}"
                )
                propagate_exception(err)
            except (OSError, aioredis.RedisError) as err:
                self.logger.warning(f"Error while connecting Redis: {err}")
                propagate_exception(err)

        return wrapper

    return decorator


class CachingMixin:
    """
    Adds caching facilities to a
    :py:class:`~eidaws.federator.utils.process.BaseRequestProcessor` or any
    other object with a ``request`` property.
    """

    @property
    def cache(self):
        return self.request.app["cache"]

    @property
    def cache_buffer(self):
        if not hasattr(self, "_cache_buffer"):
            self._cache_buffer = []

        return self._cache_buffer

    def make_cache_key(
        self,
        query_params,
        stream_epochs,
        key_prefix=None,
        sort_args=True,
        hash_method=hashlib.md5,
        exclude_params=("nodata", "service",),
    ):
        """
        Create a cache key from ``query_params`` and ``stream_epochs``.

        :param query_params: Mapping with requested query parameters
        :param stream_epochs: List of
            :py:class:`~eidaws.utils.sncl.StreamEpoch` objects.
        :param key_prefix: Caching key prefix
        :param bool sort_args: Sort caching key components before creating the
            key.
        :param hash_method: Hash method used for key generation. Default is
            ``hashlib.md5``.
        :param exclude_params: Keys to be excluded from the ``query_params``
            mapping while generating the key.
        :type exclude_params: tuple of str
        """
        if sort_args:
            query_params = [
                (k, v)
                for k, v in query_params.items()
                if k not in exclude_params
            ]
            query_params = sorted(query_params)
            stream_epochs = sorted(stream_epochs)

        updated = "{0}{1}{2}".format(
            key_prefix or "", query_params, stream_epochs
        )
        updated.translate(*null_control)

        cache_key = hash_method()
        cache_key.update(updated.encode("utf-8"))
        cache_key = base64.b64encode(cache_key.digest())[:16]
        cache_key = cache_key.decode("utf-8")

        return cache_key

    def dump_to_cache_buffer(self, data):
        if self.cache is not None:
            self.cache_buffer.append(data)

    async def set_cache(self, cache_key, timeout=None, **kwargs):
        await self.cache.set(
            cache_key, b"".join(self.cache_buffer), timeout=timeout, **kwargs
        )

    async def get_cache(self, cache_key, **kwargs):
        """
        Lookup ``cache_key`` from the cache.
        """

        try:
            retval = await self.cache.get(cache_key, **kwargs)
            found = True

            # If the value returned by cache.get() is None, it might be
            # because the key is not found in the cache or because the
            # cached value is actually None
            if retval is None:
                found = await self.cache.exists(cache_key)
        except Exception:
            found = False
            return None, found
        else:
            return retval, found


class ClientRetryBudgetMixin:
    """
    Adds facilities with respect to the client retry budget extension to a
    :py:class:`~eidaws.federator.utils.process.BaseRequestProcessor` or any
    other object with a ``request`` property.
    """

    @property
    def stats_retry_budget_client(self):
        return self.request.app["response_code_statistics"]

    @with_redis_exception_handling(propagate_exceptions=True)
    async def get_cretry_budget_error_ratio(self, url):
        """
        Return the error ratio of a response code time series referenced by
        ``url``.

        :param str url: URL indicating the response code time series to be
            garbage collected

        :returns: Error ratio in percent
        :rtype: float
        """
        return 100 * await self.stats_retry_budget_client.get_error_ratio(url)

    @with_redis_exception_handling()
    async def update_cretry_budget(self, url, code):
        """
        Add ``code`` to the response code time series referenced by
        ``url``.

        :param str url: URL indicating the response code time series to be
            updated
        :param int code: HTTP status code to be appended
        """
        await self.stats_retry_budget_client.add(url, code)

    @with_redis_exception_handling()
    async def gc_cretry_budget(self, url):
        """
        Garbage collect the response code time series referenced by ``url``.

        :param str url: URL indicating the response code time series to be
            garbage collected
        """
        await self.stats_retry_budget_client.gc(url)


class ConfigMixin:
    """
    Simplifies configuration handling for any object with a ``request`` and a ``SERVICE_ID``
    property.
    """

    SERVICE_ID = None

    @property
    def config(self):
        assert self.SERVICE_ID, f"Undefined property: SERVICE_ID."

        return self.request.config_dict["config"][self.SERVICE_ID]
