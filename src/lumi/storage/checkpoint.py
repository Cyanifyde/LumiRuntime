from __future__ import annotations

import asyncio
import logging
import weakref
from contextlib import asynccontextmanager
from contextvars import ContextVar

import asyncmy

_GATES: weakref.WeakKeyDictionary = weakref.WeakKeyDictionary()
LOGGER = logging.getLogger(__name__)


class DatabaseBarrier:
    def __init__(self):
        self.open = asyncio.Event()
        self.open.set()
        self.empty = asyncio.Event()
        self.empty.set()
        self.holders = {}
        self.roots = {}
        self.chain = ContextVar("lumi_database_lease_chain", default=None)

    async def enter(self):
        task = asyncio.current_task()


        root = self.chain.get()
        if root not in self.roots:
            await self.open.wait()
            root = object()
            self.chain.set(root)
        depth = self.holders.get(task, (root, 0))[1]
        self.holders[task] = root, depth + 1
        self.roots[root] = self.roots.get(root, 0) + 1
        self.empty.clear()
        return task

    def leave(self, task):
        root, depth = self.holders[task]
        depth -= 1
        if depth:
            self.holders[task] = root, depth
        else:
            del self.holders[task]
        self.roots[root] -= 1
        if not self.roots[root]:
            del self.roots[root]
        if not self.holders:
            self.empty.set()

    async def freeze(self):
        self.open.clear()
        empty = asyncio.create_task(self.empty.wait())
        resumed = asyncio.create_task(self.open.wait())
        try:
            await asyncio.wait({empty, resumed}, return_when=asyncio.FIRST_COMPLETED)
            return self.empty.is_set() and not self.open.is_set()
        finally:
            for task in (empty, resumed):
                task.cancel()
            await asyncio.gather(empty, resumed, return_exceptions=True)

    def resume(self):
        self.open.set()


def barrier() -> DatabaseBarrier:
    loop = asyncio.get_running_loop()
    gate = _GATES.get(loop)
    if gate is None:
        gate = _GATES[loop] = DatabaseBarrier()
    return gate


class Lease:
    def __init__(self, pool):
        self.pool = pool
        self.connection = None

    async def acquire(self):
        owner = await self.pool.gate.enter()
        try:
            self.connection = await self.pool.raw.acquire()
            self.pool.owners[self.connection] = owner
            return self.connection
        except BaseException:
            self.pool.gate.leave(owner)
            raise

    def __await__(self):
        return self.acquire().__await__()

    async def __aenter__(self):
        return await self.acquire()

    async def __aexit__(self, *_):
        await self.pool.release(self.connection)
        self.connection = None


class Pool:
    def __init__(self, raw, gate):
        self.raw, self.gate, self.owners = raw, gate, {}

    def __getattr__(self, name):
        return getattr(self.raw, name)

    def acquire(self):
        return Lease(self)

    def release(self, connection):


        owner = self.owners.pop(connection)
        try:
            if getattr(connection, "connected", False) and connection.get_transaction_status():
                LOGGER.warning("database_pool_discard")
            return self.raw.release(connection)
        finally:
            self.gate.leave(owner)


async def create_pool(**settings):
    return Pool(await asyncmy.create_pool(**settings), barrier())


@asynccontextmanager
async def read_cursor(pool):

    async with pool.acquire() as connection:
        try:
            async with connection.cursor() as cursor:
                yield cursor
        finally:
            await connection.rollback()
