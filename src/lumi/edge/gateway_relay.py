from __future__ import annotations

import asyncio
import logging
import secrets
import time

import discord
import msgspec
from redis.exceptions import RedisError

from lumi.contracts.native_mutations import MUTATION_ATTEMPT, DeleteMessages, RecordedMutation
from lumi.shared.capture_admission import CaptureAdmissionIndex
from lumi.shared.capture_packets import (
    CaptureSignal,
    decode_batch,
    decode_coverage,
    encode_batch,
    encode_coverage,
)
from lumi.shared.capture_relay import CaptureRelay
from lumi.shared.discord_assets import profile_image
from lumi.shared.discord_observations import Observation, identifier, project

from .capture_spool import CaptureSpool
from .gateway_baselines import cached_baselines
from .gateway_observations import cached_role_name, observation

LOGGER = logging.getLogger(__name__)


class GatewayRelays:


    def __init__(self, edge):
        self.edge = edge
        self.relays = {}
        self.task = None

    async def start(self):
        await self.sync()
        self.task = asyncio.create_task(self.run(), name="gateway-capture-registry")

    async def sync(self):
        declared = {
            (entry.manifest.group, entry.manifest.module)
            for entry in self.edge.registry.snapshot.modules.values()
            if entry.manifest.gateway_capture is not None
        }


        for owner in self.relays.keys() - declared:
            await self._stop(owner)
        for owner in sorted(declared - self.relays.keys()):
            try:
                relay = GatewayRelay(self.edge, owner)
                self.relays[owner] = relay
                await relay.start()
            except (RedisError, OSError, ValueError, RuntimeError):
                LOGGER.warning("gateway_capture_unavailable")
                if owner in self.relays:
                    await self._stop(owner)

    async def _stop(self, owner):
        relay = self.relays.pop(owner)
        try:
            await relay.close()
        except (RedisError, OSError, ValueError, RuntimeError):
            LOGGER.warning("gateway_capture_unavailable")

    async def run(self):
        while True:
            await asyncio.sleep(1)
            try:
                await self.sync()
            except (RedisError, OSError, ValueError, RuntimeError):
                LOGGER.warning("gateway_capture_unavailable")

    async def close(self):
        if self.task is not None:
            self.task.cancel()
            await asyncio.gather(self.task, return_exceptions=True)
        await asyncio.gather(*(self._stop(owner) for owner in tuple(self.relays)))


class GatewayRelay:
    def __init__(self, edge, owner, *, clock=None):
        self.edge, self.owner = edge, owner
        self.clock = clock or (lambda: int(time.time() * 1000))
        self.spool = CaptureSpool(edge.queue_redis, owner)
        self.capture_relay = CaptureRelay(edge.cache_redis, *owner)
        self.coverage_relay = CaptureRelay(edge.cache_redis, *owner, coverage=True)
        self.admission = CaptureAdmissionIndex(edge.queue_redis, owner)
        self.task = None
        self.tasks = set()
        self._capture_lock = asyncio.Lock()
        self.capture_signals = {}
        self.bootstrapped = {}
        self.last_order = time.time_ns()

    def new_session(self):
        self.bootstrapped.clear()
        for guild in self.admission.guilds:
            if self.edge.gateway_captures.enabled(self.owner, guild):
                self.report_capture_signal(guild, "session_replaced")

    def report_capture_signal(self, guild, kind="capture_error"):
        key = guild, kind
        if guild and key not in self.capture_signals:
            self.capture_signals[key] = CaptureSignal(
                guild, kind, self.clock(), secrets.token_hex(16),
            )

    async def flush_capture_signals(self):
        pending = tuple(self.capture_signals.items())
        await self.spool.record_signals(tuple(signal for _, signal in pending))
        for key, signal in pending:
            if self.capture_signals.get(key) == signal:
                self.capture_signals.pop(key)

    async def start(self):
        await self.refresh_admission()
        self.bind()
        for guild in self.admission.guilds:
            if self.edge.gateway_captures.enabled(self.owner, guild):
                self.report_capture_signal(guild, "gateway_started")
        await self.flush_capture_signals()
        self.task = asyncio.create_task(self.run(), name="gateway-capture")

    def bind(self):
        self.edge.gateway_captures.bind(
            self.owner, self.observe_gateway, self.new_session, refresh_rules=self.refresh_revision,
            mutation=self.observe_mutation, fault=self.report_capture_signal,
        )

    async def close(self):
        self.edge.gateway_captures.unbind(self.owner, self.observe_gateway)
        if self.task:
            self.task.cancel()
            await asyncio.gather(self.task, return_exceptions=True)
        await asyncio.gather(*self.tasks, return_exceptions=True)
        await self.flush_capture_signals()

    async def observe_mutation(self, attempt):
        application_id = getattr(self.edge.user, "id", None)
        if not application_id:
            raise ValueError("Discord mutation has no local actor.")
        self.last_order = max(self.last_order + 1, time.time_ns())
        deletion = isinstance(attempt, DeleteMessages)
        obs = Observation(
            secrets.token_hex(16), attempt.guild, MUTATION_ATTEMPT,
            attempt.messages[0] if deletion else attempt.member,
            self.clock(), self.last_order, attempt.channel if deletion else 0,
            attempt.actor, msgspec.to_builtins(RecordedMutation(attempt, application_id)),
        )
        ingress = getattr(self.edge, "private_ingress", None)
        if ingress is None:
            await self.spool.append(obs)
        else:
            async with ingress.gate.track(obs, internal=True) as permitted:
                if permitted and not ingress.gate.fence.matches(obs):
                    await self.spool.append(obs)

    def observe_gateway(self, guild, name, data):
        self.last_order = max(self.last_order + 1, time.time_ns())
        obs = observation(
            self.edge, self.admission, guild, name, data, order=self.last_order, now=self.clock(),
        )
        if obs:
            self.enqueue(obs)

    def enqueue(self, obs):
        ingress = getattr(self.edge, "private_ingress", None)
        if ingress is not None:
            if ingress.gate.fence.matches(obs) or (
                ingress.blocked(0) and asyncio.current_task() not in ingress.gate.active
            ):
                return
        if len(self.tasks) >= 256:
            self.report_capture_signal(obs.guild)
            return

        async def persist():
            try:


                async with self._capture_lock:
                    if obs.kind in {"MESSAGE_UPDATE", "MESSAGE_DELETE"} and not obs.data.get("subject"):
                        known = await self.spool.identity(obs.guild, obs.resource)
                        if known:
                            if self.edge.blocklist.contains(known[0]) or (
                                known[1] and not self.admission.permits(obs.guild, automated=True)
                            ):
                                return
                            obs.data["subject"] = known[0]
                            obs.data.setdefault("bot", bool(known[1]))
                    await self.spool.append(obs)
            except (RedisError, OSError, TypeError, ValueError, RuntimeError):
                self.report_capture_signal(obs.guild)

        task = asyncio.create_task(persist(), name="gateway-capture")
        self.tasks.add(task)
        task.add_done_callback(self.tasks.discard)
        if ingress is not None:
            ingress.gate.watch(task)

    async def capture_pending(self):
        ingress = getattr(self.edge, "private_ingress", None)
        if ingress is not None:
            async with ingress.gate.track(0) as permitted:
                if permitted:
                    await self._ordinary_capture_pending()
            return
        await self._ordinary_capture_pending()

    async def _ordinary_capture_pending(self):
        ingress = getattr(self.edge, "private_ingress", None)
        frame, settled = await self.capture_relay.read()
        if frame is not None:
            batch = decode_batch(frame)
            if ingress is not None and any(
                ingress.gate.fence.matches(obs) for obs in batch.records
            ):
                await self.capture_relay.discard(frame)
            elif settled:
                await self.spool.ack_observations(tuple(obs.id for obs in batch.records))
                await self.capture_relay.discard(frame)
            else:
                return

        pending = []
        for seq, obs in await self.spool.batch():
            if ingress is not None and ingress.gate.fence.matches(obs):
                await self.spool.ack(seq)
                continue
            pending.append(obs)
        if pending:
            frame = encode_batch(pending, getattr(self.edge.user, "id", 0))
            if ingress is None or not any(ingress.gate.fence.matches(obs) for obs in pending):
                await self.capture_relay.offer(frame)
                if ingress is not None and any(ingress.gate.fence.matches(obs) for obs in pending):
                    await self.capture_relay.discard(frame)

    async def relay_capture_signals(self):
        await self.flush_capture_signals()
        frame, settled = await self.coverage_relay.read()
        if frame is not None:
            if not settled:
                return
            await self.spool.ack_signals(decode_coverage(frame).signals)
            await self.coverage_relay.discard(frame)
        signals = await self.spool.signal_batch()
        if signals:
            await self.coverage_relay.offer(encode_coverage(signals))

    async def run(self):
        tick = 0
        while True:
            try:
                ingress = getattr(self.edge, "private_ingress", None)
                if ingress is None:
                    tick = await self._ordinary_run_tick(tick)
                else:
                    async with ingress.gate.track(0) as permitted:
                        if permitted:
                            tick = await self._ordinary_run_tick(tick)
            except asyncio.CancelledError:
                raise
            except Exception:
                LOGGER.warning("gateway_capture_unavailable")
            await asyncio.sleep(1)

    async def _ordinary_run_tick(self, tick):
        if tick % 5 == 0:
            await self.refresh_admission()
        await self.relay_capture_signals()
        await self.capture_pending()
        for guild_id in self.admission.guilds:
            scope = self.edge.gateway_captures.scope(self.owner, guild_id)
            if scope is None:
                self.bootstrapped.pop(guild_id, None)
                continue
            requested = frozenset(scope.baseline_events)
            outstanding = requested - self.bootstrapped.get(guild_id, frozenset())
            guild = self.edge.get_guild(guild_id) if outstanding else None
            if guild is not None:
                self.bootstrapped[guild_id] = requested
                await self.bootstrap(guild, outstanding)
            elif not outstanding:
                self.bootstrapped[guild_id] = requested
        tick += 1
        return tick

    async def refresh_admission(self, guild_id=None):
        try:
            if guild_id is None:
                await self.admission.refresh()
            else:
                await self.admission.refresh_guild(guild_id)
        except RedisError:


            LOGGER.warning("gateway_capture_unavailable")

    async def refresh_revision(self, guild_id, revision):
        if self.admission.revision(guild_id) < revision:
            self.admission.require_revision(guild_id, revision)
            await self.refresh_admission(guild_id)

    async def bootstrap(self, guild, requested):


        cutoff = self.last_order

        def allows_channel(channel):
            return (
                guild.me is not None and channel.permissions_for(guild.me).view_channel
                and self.admission.permits(guild.id, channels=(
                    channel.id, getattr(channel, "category_id", None),
                ))
            )

        baselines = cached_baselines(guild, requested, allows_channel=allows_channel)
        for event, resource, data in baselines:
            if self.edge.blocklist.contains(resource):
                continue
            payload = project(event, data, role_name=lambda role: cached_role_name(guild, role))
            payload["_baseline"] = True
            payload["_baseline_order"] = cutoff
            self.enqueue(
                Observation(
                    identifier("baseline", guild.id, event, resource, self.clock()),
                    guild.id,
                    event,
                    resource,
                    self.clock(),
                    0,
                    data.get("id", 0),
                    data=payload,
                )
            )
        if "GUILD_MEMBER_UPDATE" not in requested:
            return
        try:
            members = await self.edge._guild_members(guild)
            if not self._member_baseline_allowed(guild.id):
                return
            for member in members:
                if self.edge.blocklist.contains(member.id):
                    continue
                payload = dict(
                    subject=member.id,
                    name=member.display_name,
                    username=member.name,
                    nick=member.nick,
                    bot=member.bot,
                    avatar=profile_image(str(member.display_avatar.url)),
                    user_avatar=str(member.avatar.url) if member.avatar else None,
                    guild_avatar=str(member.guild_avatar.url) if member.guild_avatar else None,
                    global_name=member.global_name,
                    role_ids=[r.id for r in member.roles if r.id != guild.id],
                    roles={str(r.id): r.name for r in member.roles if r.id != guild.id},
                    joined_at=member.joined_at.isoformat() if member.joined_at else None,
                    _baseline=True,
                    _baseline_order=cutoff,
                    until=int(member.timed_out_until.timestamp() * 1000)
                    if member.timed_out_until
                    else 0,
                )
                self.enqueue(
                    Observation(
                        identifier("baseline-member", guild.id, member.id, self.clock()),
                        guild.id,
                        "GUILD_MEMBER_UPDATE",
                        member.id,
                        self.clock(),
                        0,
                        data=payload,
                    )
                )
                if len(self.tasks) > 200:
                    await asyncio.gather(*self.tasks, return_exceptions=True)
                    if not self._member_baseline_allowed(guild.id):
                        return
        except (discord.HTTPException, ValueError, TimeoutError):
            self.report_capture_signal(guild.id, "baseline_error")

    def _member_baseline_allowed(self, guild_id):
        scope = self.edge.gateway_captures.scope(self.owner, guild_id)
        return (
            scope is not None and "GUILD_MEMBER_UPDATE" in scope.baseline_events
            and self.admission.permits(guild_id)
        )
