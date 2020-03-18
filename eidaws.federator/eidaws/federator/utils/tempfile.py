# -*- coding: utf-8 -*-

import asyncio
import functools

from aiofiles.base import AsyncBase
from aiofiles.threadpool.utils import (
    delegate_to_executor,
    proxy_property_directly,
)
from contextlib import asynccontextmanager
from functools import partial
from tempfile import SpooledTemporaryFile as _SpooledTemporaryFile


@asynccontextmanager
async def AioSpooledTemporaryFile(
    max_size=0,
    mode="w+b",
    buffering=-1,
    encoding=None,
    newline=None,
    suffix=None,
    prefix=None,
    dir=None,
    loop=None,
    executor=None,
):
    """
    Async open a spooled temporary file
    """

    if loop is None:
        loop = asyncio.get_event_loop()

    cb = partial(
        _SpooledTemporaryFile,
        mode=mode,
        buffering=buffering,
        encoding=encoding,
        newline=newline,
        suffix=suffix,
        prefix=prefix,
        dir=dir,
        max_size=max_size,
    )

    f = await loop.run_in_executor(executor, cb)
    spooled = AsyncSpooledTemporaryFile(f, loop=loop, executor=executor)
    try:
        yield spooled
    finally:
        await spooled.close()


# ----------------------------------------------------------------------------
def cond_delegate_to_executor(*attrs):
    def cls_builder(cls):
        for attr_name in attrs:
            setattr(cls, attr_name, _make_cond_delegate_method(attr_name))
        return cls

    return cls_builder


def _make_cond_delegate_method(attr_name):
    """For spooled temp files, delegate only if rolled to file object"""

    async def method(self, *args, **kwargs):
        if self._file._rolled:
            cb = functools.partial(
                getattr(self._file, attr_name), *args, **kwargs
            )
            return await self._loop.run_in_executor(self._executor, cb)
        else:
            return getattr(self._file, attr_name)(*args, **kwargs)

    return method


@delegate_to_executor("fileno", "rollover")
@cond_delegate_to_executor(
    "close",
    "flush",
    "isatty",
    "newlines",
    "read",
    "readline",
    "readlines",
    "seek",
    "tell",
    "truncate",
)
@proxy_property_directly("closed", "encoding", "mode", "name", "softspace")
class AsyncSpooledTemporaryFile(AsyncBase):
    """Async wrapper for SpooledTemporaryFile class"""

    async def _check(self):
        if self._file._rolled:
            return
        max_size = self._file._max_size
        if max_size and await self.tell() > max_size:
            await self.rollover()

    async def write(self, s):
        """Implementation to anticipate rollover"""
        if self._file._rolled:
            cb = partial(self._file.write, s)
            return await self._loop.run_in_executor(self._executor, cb)
        else:
            file = self._file._file  # reference underlying base IO object
            rv = file.write(s)
            await self._check()
            return rv

    async def writelines(self, iterable):
        """Implementation to anticipate rollover"""
        if self._file._rolled:
            cb = partial(self._file.writelines, iterable)
            return await self._loop.run_in_executor(self._executor, cb)
        else:
            file = self._file._file  # reference underlying base IO object
            rv = file.writelines(iterable)
            await self._check()
            return rv
