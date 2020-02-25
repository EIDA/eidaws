# -*- coding: utf-8 -*-
"""
Statistics related test facilities.
"""


import aioredis
import asyncio
import pytest

from eidaws.federator.utils.stats import ResponseCodeTimeSeries


@pytest.fixture
async def redis_connection():

    DB = 15

    try:
        redis = await aioredis.create_redis(
            "redis://localhost:6379", db=DB, timeout=1
        )
    except (OSError, aioredis.RedisError) as err:
        pytest.skip(str(err))

    if await redis.dbsize():
        raise EnvironmentError(
            f"Redis database number {DB} is not empty, tests could harm "
            f"your data."
        )

    yield redis

    await redis.flushdb()
    redis.close()
    await redis.wait_closed()


class TestResponseCodeTimeSeries:
    @staticmethod
    def create_timeseries(*args, **kwargs):
        return ResponseCodeTimeSeries(*args, **kwargs)

    @pytest.mark.asyncio
    async def test_init(self, redis_connection):
        ts = self.create_timeseries(redis_connection)
        assert await ts._len() == 0

    @pytest.mark.asyncio
    async def test_append(self, redis_connection):
        ts = self.create_timeseries(redis_connection)

        status_codes = [200, 500, 503, 204]
        for c in status_codes:
            await ts.append(c)

        assert [c async for c, score in ts] == [
            str(c) for c in reversed(status_codes)
        ]

    @pytest.mark.asyncio
    async def test_ttl(self, redis_connection):
        ttl = 0.1
        ts = self.create_timeseries(redis_connection, ttl=ttl)

        status_codes = [200, 500, 503, 204]
        for c in status_codes:
            await ts.append(c)

        await asyncio.sleep(ttl)
        assert [c async for c, score in ts] == []

    @pytest.mark.asyncio
    async def test_window_size(self, redis_connection):
        size = 3
        ts = self.create_timeseries(redis_connection, window_size=size)

        status_codes = [200, 500, 503, 204]
        for c in status_codes:
            await ts.append(c)

        assert [c async for c, score in ts] == [
            str(c) for c in reversed(status_codes[1:])
        ]

        assert len(await redis_connection.zrange(ts.key, 0, -1)) == 3

    @pytest.mark.asyncio
    async def test_gc(self, redis_connection):
        ttl = 0.4
        ts = self.create_timeseries(redis_connection, ttl=ttl)

        status_codes = [200, 500, 503, 204]
        for c in status_codes:
            await ts.append(c)
            await asyncio.sleep(ttl / len(status_codes))

        await ts.gc()

        assert len(await redis_connection.zrange(ts.key, 0, -1)) == 3

    @pytest.mark.asyncio
    async def test_error_ratio(self, redis_connection):
        ts = self.create_timeseries(redis_connection)

        status_codes = [200, 500, 503, 204]
        for c in status_codes:
            await ts.append(c)

        assert await ts.get_error_ratio() == 0.5
