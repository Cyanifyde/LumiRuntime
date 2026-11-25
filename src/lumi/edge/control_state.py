import asyncio
import secrets
import time

import msgspec
from redis.exceptions import RedisError

from lumi.contracts.runtime_control import (
    BLOCK_RECEIPTS,
    MAX_PAYLOAD,
    REPLIES,
    REQUEST_MS,
    REQUESTS,
    STATE,
    VERSION,
    BlockReceipt,
    ControlProjection,
    ControlReply,
    ControlRequest,
)


async def read_block_receipt(redis, job):
    try:
        async with asyncio.timeout(2):
            wire = await redis.get(BLOCK_RECEIPTS + job.job_id)
        if not wire or len(wire) > 1024:
            return None
        receipt = msgspec.json.decode(wire, type=BlockReceipt)
    except (TimeoutError, RedisError, msgspec.DecodeError):
        return None
    now = time.time_ns() // 1_000_000
    if receipt.matches(job.job_id, now) and receipt.deadline == job.expires_at_ms:
        return receipt
    return None


class RedisControl:
    def __init__(self, redis, owner, *, clock=None):
        self.redis, self.owner = redis, owner
        self.clock = clock or (lambda: time.time_ns() // 1_000_000)
        self.snapshot = None
        self._writes = asyncio.Lock()
        self.on_change = None

    async def refresh(self):
        if self.snapshot is not None and await self.redis.get(VERSION) == self.snapshot.version():
            return
        raw = await self.redis.get(STATE)
        if raw is None or len(raw) > MAX_PAYLOAD:
            return
        try:
            snapshot = msgspec.json.decode(raw, type=ControlProjection)
        except msgspec.DecodeError:
            return
        previous = self.snapshot
        if snapshot.valid() and (
            previous is None
            or (snapshot.generation, snapshot.serial) > (previous.generation, previous.serial)
        ):
            self.snapshot = snapshot
            if self.on_change is not None:
                self.on_change(snapshot)

    async def wait_ready(self):
        while self.snapshot is None:
            try:
                await self.refresh()
            except RedisError:
                pass
            if self.snapshot is None:
                await asyncio.sleep(1)

    async def run(self):
        while True:
            try:
                await self.refresh()
            except RedisError:
                pass
            await asyncio.sleep(1)

    async def request(self, operation, **values):
        try:
            async with self._writes, asyncio.timeout(REQUEST_MS / 1000):
                await self.refresh()
                now = self.clock()
                if self.snapshot is None:
                    raise RuntimeError("Runtime control unavailable")
                request = ControlRequest(
                    secrets.token_hex(16),
                    self.snapshot.generation,
                    operation,
                    now + REQUEST_MS,
                    **values,
                )
                if not request.valid(now, self.owner):
                    raise ValueError("Invalid runtime control")
                async with self.redis.pipeline(transaction=True) as transaction:
                    transaction.rpush(REQUESTS, msgspec.json.encode(request)).pexpire(
                        REQUESTS, REQUEST_MS
                    )
                    await transaction.execute()
                item = await self.redis.blpop(REPLIES + request.identifier)
                if not item or len(item[1]) > 1024:
                    raise RuntimeError("Runtime control outcome unconfirmed")
                reply = msgspec.json.decode(item[1], type=ControlReply)
                if (
                    reply.identifier != request.identifier
                    or reply.generation != request.generation
                    or reply.serial <= 0
                ):
                    raise RuntimeError("Runtime control outcome unconfirmed")
                await self.refresh()
                if (self.snapshot.generation, self.snapshot.serial) < (
                    reply.generation,
                    reply.serial,
                ):
                    raise RuntimeError("Runtime control outcome unconfirmed")
                return reply.changed
        except (TimeoutError, RedisError, msgspec.DecodeError) as exc:
            raise RuntimeError("Runtime control outcome unconfirmed") from exc

    async def get_maintenance(self):
        return self.snapshot.maintenance

    async def load_drains(self):
        return self.snapshot.drains

    async def set_maintenance(self, enabled):
        await self.request("maintenance", owner=self.owner, enabled=enabled)

    async def set_drain(self, group, enabled):
        await self.request("drain", owner=self.owner, group=group, enabled=enabled)


class ProjectedBlocklist:
    def __init__(self, control):
        self.control = control
        self._ingress_gate = None

    @property
    def ready(self):
        return self.control.snapshot is not None

    def bind_ingress_gate(self, gate):
        self._ingress_gate = gate

    def contains(self, user):
        if self._ingress_gate is not None and self._ingress_gate.blocks_identity(user):
            return True
        snapshot = self.control.snapshot
        if snapshot is None:
            raise RuntimeError("Block projection is not loaded")
        if user == self.control.owner:
            return False
        return user in snapshot.permanent or snapshot.temporary.get(user, 0) > self.control.clock()

    async def block(self, user, actor_id):
        return user != self.control.owner and await self.control.request(
            "block", subject=user, owner=actor_id
        )

    async def unblock(self, user):
        return user != self.control.owner and await self.control.request(
            "unblock", subject=user, owner=self.control.owner
        )
