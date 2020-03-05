# -*- coding: utf-8 -*-
"""
Cache related test facilities.
"""

import aioredis
import asyncio
import pytest

from eidaws.federator.utils.cache import RedisCache


@pytest.fixture
async def redis_cache():

    DB = 15

    try:
        cache = await RedisCache.create(
            "redis://localhost:6379", db=DB, timeout=1
        )
    except (OSError, aioredis.RedisError) as err:
        pytest.skip(str(err))

    if await cache.redis.dbsize():
        raise EnvironmentError(
            f"Redis database number {DB} is not empty, tests could harm "
            f"your data."
        )

    yield cache

    await cache.redis.flushdb()
    cache.redis.close()
    await cache.redis.wait_closed()


class TestRedisCache:
    @pytest.mark.asyncio
    async def test_init(self, redis_cache):

        assert await redis_cache.redis.dbsize() == 0

    @pytest.mark.asyncio
    async def test_set_get(self, redis_cache):
        cache_key = "cache_key"
        cache_value = b"foo"
        await redis_cache.set(cache_key, cache_value)

        assert await redis_cache.get(cache_key) == cache_value

    @pytest.mark.asyncio
    async def test_expired(self, redis_cache):
        cache_key = "cache_key"
        cache_value = b"foo"
        await redis_cache.set(cache_key, cache_value, timeout=1)
        assert await redis_cache.exists(cache_key)

        await asyncio.sleep(1)

        assert not await redis_cache.exists(cache_key)

    @pytest.mark.asyncio
    async def test_delete(self, redis_cache):
        cache_key = "cache_key"
        cache_value = b"foo"

        await redis_cache.set(cache_key, cache_value)
        assert await redis_cache.exists(cache_key)

        await redis_cache.delete(cache_key)

        assert not await redis_cache.exists(cache_key)
