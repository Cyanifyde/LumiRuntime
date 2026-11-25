import asyncio
import logging
import time

import msgspec
from redis.exceptions import WatchError

from lumi.contracts.runtime_control import (
    ACTOR_ADMISSIONS,
    BLOCK_RECEIPTS,
    BLOCK_REPLIES,
    BLOCK_REQUESTS,
    GENERATION,
    MAX_PAYLOAD,
    REPLIES,
    REQUESTS,
    STATE,
    VERSION,
    ActorAdmission,
    BlockReceipt,
    BlockRequest,
    ControlProjection,
    ControlReply,
    ControlRequest,
    identifier,
)

LOGGER = logging.getLogger(__name__)


AUTOMATIC_BLOCKS = {("tools", "translate", "translate.translate"): 20 * 60}


class PlatformControl:
    def __init__(self, redis, store, generation, owner, *, clock=None):
        self.redis, self.store, self.generation, self.owner = redis, store, generation, owner
        self.clock = clock or (lambda: time.time_ns() // 1_000_000)
        self.serial = 0
        self.task = None
        self.active = False
        self.dirty = True
        self.next_cleanup = 0

    async def start(self):


        if not await self.current():
            raise RuntimeError("Platform generation changed")
        await self.store.remove_block(self.owner)
        await self.publish()
        self.task = asyncio.create_task(self.run(), name="platform-runtime-control")

    async def close(self):
        if self.task is not None:
            self.task.cancel()
            await asyncio.gather(self.task, return_exceptions=True)
        await self.store.close()

    def drain_ready(self):


        return not self.active

    async def current(self):
        return int(await self.redis.get(GENERATION) or 0) == self.generation

    async def publish(self, receipt=None):
        clock = self.clock()
        permanent = (await self.store.load_blocks()) - {self.owner}
        temporary = {
            uid: expiry
            for uid, expiry in (await self.store.load_temporary_blocks()).items()
            if uid != self.owner and expiry > clock
        }
        snapshot = ControlProjection(
            self.generation,
            self.serial + 1,
            permanent,
            temporary,
            await self.store.get_maintenance(),
            await self.store.load_drains(),
        )
        payload = msgspec.json.encode(snapshot)
        if len(payload) > MAX_PAYLOAD or not snapshot.valid():
            raise ValueError("Invalid or oversized control projection")
        try:
            async with self.redis.pipeline(transaction=True) as transaction:
                await transaction.watch(GENERATION)
                if int(await transaction.get(GENERATION) or 0) != self.generation:
                    raise RuntimeError("Platform generation changed")
                transaction.multi()
                transaction.set(STATE, payload).set(VERSION, snapshot.version())
                if receipt is not None:
                    remaining = receipt.deadline - self.clock()
                    if remaining > 0:
                        transaction.set(
                            BLOCK_RECEIPTS + receipt.job,
                            msgspec.json.encode(receipt),
                            px=remaining,
                        )
                await transaction.execute()
        except WatchError as exc:
            raise RuntimeError("Platform generation changed") from exc
        self.serial = snapshot.serial
        self.dirty = False

    async def process(self, raw):
        if not raw or len(raw) > 1024:
            return
        try:
            request = msgspec.json.decode(raw, type=ControlRequest)
        except msgspec.DecodeError:
            return
        if request.generation != self.generation or not request.valid(self.clock(), self.owner):
            return
        if not await self.current() or not request.valid(self.clock(), self.owner):
            return
        self.active = self.dirty = True
        try:
            if request.operation == "block":
                changed = await self.store.add_block(request.subject, request.owner)
            elif request.operation == "unblock":
                changed = await self.store.remove_block(request.subject)
            elif request.operation == "maintenance":
                await self.store.set_maintenance(request.enabled)
                changed = True
            else:
                await self.store.set_drain(request.group, request.enabled)
                changed = True


            await self.publish()
            remaining = request.deadline - self.clock()
            if remaining > 0:
                response = ControlReply(request.identifier, self.generation, self.serial, changed)
                key = REPLIES + request.identifier
                async with self.redis.pipeline(transaction=True) as transaction:
                    transaction.rpush(key, msgspec.json.encode(response)).pexpire(key, remaining)
                    await transaction.execute()
        finally:
            self.active = False

    async def process_actor_request(self, raw):
        if not raw or len(raw) > 1024:
            return
        try:
            request = msgspec.json.decode(raw, type=BlockRequest)
        except msgspec.DecodeError:
            return
        if not request.valid(self.clock()) or not await self.current():
            return

        result = await self._apply_actor_request(request)
        remaining = request.deadline - self.clock()
        if remaining > 0:
            key = BLOCK_REPLIES + request.reply
            async with self.redis.pipeline(transaction=True) as transaction:
                transaction.rpush(key, b"applied" if result else b"declined").pexpire(
                    key, remaining
                )
                await transaction.execute()

    async def _apply_actor_request(self, request):
        wire = await self.redis.get(ACTOR_ADMISSIONS + request.job)
        if not wire or len(wire) > 1024:
            return False
        try:
            ticket = msgspec.json.decode(wire, type=ActorAdmission)
        except msgspec.DecodeError:
            return False
        seconds = AUTOMATIC_BLOCKS.get((ticket.group, ticket.module, ticket.route))
        if (
            not seconds
            or ticket.job != request.job
            or not identifier(ticket.actor)
            or ticket.actor == self.owner
            or ticket.deadline < request.deadline
            or not identifier(ticket.generation)
            or int(await self.redis.get(f"lumi:generation:{ticket.group}") or 0)
            != ticket.generation
        ):
            return False
        now = self.clock()
        if not request.valid(now):
            return False
        key = BLOCK_RECEIPTS + request.job
        existing = await self.redis.get(key)
        if existing is not None:
            try:
                return msgspec.json.decode(existing, type=BlockReceipt).matches(request.job, now)
            except msgspec.DecodeError:
                return False


        if not await self.redis.set(key, "pending", nx=True, px=ticket.deadline - now):
            return False
        if not await self.current() or not request.valid(self.clock()):
            return False
        self.active = self.dirty = True
        try:
            expires = now + seconds * 1000
            changed = await self.store.add_temporary_block(ticket.actor, expires, ticket.module)
            await self.publish(
                BlockReceipt(request.job, ticket.deadline, expires) if changed else None
            )
            return changed
        finally:
            self.active = False

    async def run(self):
        while True:
            try:
                if not await self.current():
                    return
                self.active = True
                if self.clock() >= self.next_cleanup:
                    if await self.store.delete_expired_temporary_blocks(self.clock()):
                        self.dirty = True
                    self.next_cleanup = self.clock() + 60_000
                if self.dirty or await self.redis.exists(STATE, VERSION) != 2:
                    await self.publish()
                self.active = False
                item = await self.redis.blpop((REQUESTS, BLOCK_REQUESTS), timeout=1)
                if item:
                    processor = (
                        self.process
                        if item[0] in (REQUESTS, REQUESTS.encode())
                        else self.process_actor_request
                    )
                    await processor(item[1])
            except Exception:
                LOGGER.warning("platform_runtime_control_unavailable")
                self.active = False
                await asyncio.sleep(1)
