# -*- coding: utf-8 -*-
"""
Caching facilities

The module provides a similar functionality as implemented by `pallets/cachelib
<https://github.com/pallets/cachelib>`_ and `sh4nks/flask-caching
<https://github.com/sh4nks/flask-caching>`_.
"""

import abc
import aioredis
import gzip
import string

from eidaws.utils.error import ErrorWithTraceback

# Used to remove control characters and whitespace from cache keys.
valid_chars = set(string.ascii_letters + string.digits + "_.")
delchars = "".join(c for c in map(chr, range(256)) if c not in valid_chars)
null_control = (dict((k, None) for k in delchars),)


# -----------------------------------------------------------------------------
class CacheError(ErrorWithTraceback):
    """Base cache error ({})."""


# -----------------------------------------------------------------------------
class CachingBackend(abc.ABC):
    """
    Base class for cache backend implementations.
    """

    def _init(self, default_timeout=300, **kwargs):
        """
        :param default_timeout: The default timeout (in seconds) that is used
        if no timeout is specified in :py:meth:`set`. A timeout of 0 indicates
        that the cache never expires.
        """
        self._default_timeout = default_timeout

    def _normalize_timeout(self, timeout):
        if timeout is None:
            return self._default_timeout
        return timeout

    async def get(self, key, **kwargs):
        """
        Look up ``key`` in the cache and return the value for it.

        :param key: The key to be looked up
        :returns: The value if it exists and is readable, else ``None``.
        """

        return None

    async def delete(self, key):
        """
        Delete ``key`` from the cache.

        :param key: The key to delete
        :returns: Whether the key existed and has been deleted.
        :rtype: boolean
        """

        return True

    async def set(self, key, value, timeout=None, **kwargs):
        """
        Add a new ``key: value`` to the cache. The value is overwritten in case
        the ``key`` is already cached.

        :param key: The key to be set
        :param value: The value to be cached
        :param timeout: The cache timeout for the key in seconds. If not
            specified the default timeout is used. A timeout of 0 indicates
            that the cache never expires.

        :returns: ``True`` if the key has been updated and ``False`` for
            backend errors.
        rtype: boolean
        """

        return True

    async def close(self):
        """
        Gracefully shutdown a caching backend.
        """

    @abc.abstractmethod
    async def exists(self, key):
        """
        Validate if a key exists in the cache without returning it. The data is
        neither loaded nor deserialized.

        :param key: Key to validate
        """

    @abc.abstractmethod
    async def flush_all(self):
        """
        Delete all cache entries.
        """


class NullCache(CachingBackend):
    """
    A cache that doesn't cache.
    """

    async def exists(self, key):
        return False

    async def flush_all(self):
        pass


class RedisCache(CachingBackend):
    """
    Implementation of a `Redis <https://redis.io/>`_ caching backend.
    """

    @classmethod
    async def create(
        cls, url, default_timeout=300, compress=True, key_prefix=None, **kwargs
    ):
        self = cls()
        super(cls, self)._init(default_timeout)
        self.key_prefix = key_prefix or ""
        self.redis = await aioredis.create_redis_pool(url, **kwargs)

        self._compress = compress

        return self

    def _create_key_prefix(self):
        if isinstance(self.key_prefix, str):
            return self.key_prefix
        return self.key_prefix()

    async def get(self, key, **kwargs):
        return self._deserialize(
            await self.redis.get(self._create_key_prefix() + key), **kwargs
        )

    async def delete(self, key):
        return await self.redis.delete(self._create_key_prefix() + key)

    async def set(self, key, value, timeout=None, **kwargs):
        key = self._create_key_prefix() + key
        value = self._serialize(value, **kwargs)

        timeout = self._normalize_timeout(timeout)
        return await self.redis.set(key, value, expire=timeout)

    async def exists(self, key):
        return await self.redis.exists(self._create_key_prefix() + key)

    async def flush_all(self, **kwargs):
        await self.redis.flushall(*kwargs)

    async def close(self):
        self.redis.close()
        await self.redis.wait_closed()

    def _serialize(self, value, compress=None):
        compress = self._compress if compress is None else bool(compress)
        if compress:
            return gzip.compress(value)

        return value

    def _deserialize(self, value, decompress=None):
        """
        The complementary method of :py:meth:`_serialize`. Can be called with
        ``None``.
        """

        if value is None:
            return None

        decompress = self._compress if decompress is None else bool(decompress)
        if decompress:
            return gzip.decompress(value)

        return value


CachingBackend.register(NullCache)
CachingBackend.register(RedisCache)


# -----------------------------------------------------------------------------
class Cache:
    """
    Generic API for cache objects.
    """

    CACHE_MAP = {
        "null": NullCache,
        "redis": RedisCache,
    }

    @classmethod
    async def create(cls, config={}):

        config.setdefault("cache_type", "null")
        config.setdefault("cache_kwargs", {})

        self = cls()
        await self._set_cache(config)

        return self

    async def _set_cache(self, config):
        cache_obj = self.CACHE_MAP[config["cache_type"]]
        self._cache = await cache_obj.create(**config["cache_kwargs"])

    async def get(self, *args, **kwargs):
        return await self._cache.get(*args, **kwargs)

    async def set(self, *args, **kwargs):
        return await self._cache.set(*args, **kwargs)

    async def delete(self, *args, **kwargs):
        return await self._cache.delete(*args, **kwargs)

    async def exists(self, *args, **kwargs):
        return await self._cache.__contains__(*args, **kwargs)

    async def close(self, *args, **kwargs):
        return await self._cache.close(*args, **kwargs)

    async def flush_all(self, *args, **kwargs):
        return await self._cache.flush_all(*args, **kwargs)
