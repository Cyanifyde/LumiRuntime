from __future__ import annotations

import contextlib
from collections.abc import Callable
from dataclasses import dataclass

from discord.state import ConnectionState

from lumi.contracts.gateway import GUILD_CAPTURE_EVENTS
from lumi.contracts.native_mutations import mutation_kind
from lumi.shared.capture_relay import route_scope

from .members import MEMBER_EVENTS

CACHE_GATEWAY = {
    "USER_UPDATE",
    "PRESENCE_UPDATE",
    "GUILD_MEMBERS_CHUNK",
    "THREAD_MEMBERS_UPDATE",
    "MESSAGE_REACTION_ADD",
    "MESSAGE_REACTION_REMOVE",
    "MESSAGE_REACTION_REMOVE_ALL",
    "MESSAGE_REACTION_REMOVE_EMOJI",
}
PROTECTED_GATEWAY = GUILD_CAPTURE_EVENTS | CACHE_GATEWAY
HOOKS = PROTECTED_GATEWAY | MEMBER_EVENTS | {"READY", "RESUMED", "GUILD_CREATE", "GUILD_DELETE"}


@dataclass(frozen=True, slots=True)
class CaptureSink:
    observe: Callable
    new_session: Callable
    refresh_rules: Callable | None = None
    mutation: Callable | None = None
    fault: Callable | None = None


class GatewayCaptures:


    def __init__(self, edge):
        self.edge = edge
        self._sinks = {}
        self._snapshot = None
        self._events = {}

    def bind(self, owner, observe, new_session, *, refresh_rules=None, mutation=None, fault=None):
        route_scope(*owner)
        if owner in self._sinks:
            raise ValueError("Gateway capture relay already bound.")
        self._sinks[owner] = CaptureSink(observe, new_session, refresh_rules, mutation, fault)
        self._snapshot = None

    def unbind(self, owner, observe):
        sink = self._sinks.get(owner)
        if sink is not None and sink.observe == observe:
            del self._sinks[owner]
            self._snapshot = None

    @staticmethod
    def declared(snapshot, owner):
        group, module = owner
        route = snapshot.modules.get(module)
        return (
            route.manifest.gateway_capture
            if route is not None and route.manifest.group == group else None
        )

    def scope(self, owner, guild):
        if self.edge.policy.module_enabled(guild, owner[1]):
            return self.declared(self.edge.registry.snapshot, owner)
        return None

    def enabled(self, owner, guild):
        return self.scope(owner, guild) is not None

    def _routes(self, event):
        snapshot = self.edge.registry.snapshot
        if snapshot is not self._snapshot:
            events = {}
            for owner, sink in self._sinks.items():
                scope = self.declared(snapshot, owner)
                if scope is not None:
                    for declared_event in scope.events:
                        events.setdefault(declared_event, []).append((owner[1], sink))
            self._events, self._snapshot = events, snapshot
        return self._events.get(event, ())

    def receive(self, event, payload):
        routes = self._routes(event)
        if not routes:
            return
        guild_id = payload.get("guild_id")
        if not guild_id and event == "GUILD_UPDATE":
            guild_id = payload.get("id")
        try:
            guild = int(guild_id or 0)
        except (ValueError, TypeError):
            return
        if not 0 < guild < 2**64:
            return
        for module, sink in routes:
            if self.edge.policy.module_enabled(guild, module):
                try:
                    sink.observe(guild, event, payload)
                except Exception:
                    if sink.fault is not None:
                        with contextlib.suppress(Exception):
                            sink.fault(guild)

    def new_session(self):
        for sink in self._sinks.values():
            with contextlib.suppress(Exception):
                sink.new_session()

    async def refresh_revision(self, owner, guild, revision):
        sink = self._sinks.get(owner)
        if sink is not None and sink.refresh_rules is not None and self.enabled(owner, guild):
            await sink.refresh_rules(guild, revision)

    async def before_mutation(self, attempt):
        kind = mutation_kind(attempt)
        for owner, sink in self._sinks.items():
            scope = self.scope(owner, attempt.guild)
            if scope is None or kind not in scope.mutation_kinds or sink.mutation is None:
                continue
            try:
                await sink.mutation(attempt)
            except Exception:
                if sink.fault is not None:
                    with contextlib.suppress(Exception):
                        sink.fault(attempt.guild)


async def record_mutation(edge, attempt):
    captures = getattr(edge, "gateway_captures", None)
    if captures is not None:
        await captures.before_mutation(attempt)


class GatewayConnectionState(ConnectionState):
    def __init__(self, **options):
        super().__init__(**options)
        for event in HOOKS:
            original = self.parsers.get(event)
            if original is None:
                if event in CACHE_GATEWAY:
                    continue
                raise RuntimeError("Unsupported discord.py Gateway parser: " + event)

            def receive(data, *, event=event, original=original):
                client = self._get_client()
                versions = getattr(client, "source_versions", None)
                if versions and event in {"READY", "RESUMED", "MESSAGE_UPDATE", "MESSAGE_DELETE", "MESSAGE_DELETE_BULK"}:
                    with contextlib.suppress(ValueError, KeyError, TypeError):
                        versions.gateway(event, data)
                ingress = getattr(client, "private_ingress", None)
                if ingress is not None:
                    if event in {"GUILD_CREATE", "GUILD_MEMBERS_CHUNK"}:


                        data = dict(data)
                        for field in ("members", "presences"):
                            if field in data:
                                data[field] = (
                                    []
                                    if ingress.gate.paused or not ingress.gate.ready
                                    else [
                                        item
                                        for item in data[field]
                                        if not ingress.gate.fence.matches(item)
                                    ]
                                )
                    elif event in PROTECTED_GATEWAY:
                        value = data
                        if event.startswith("MESSAGE_"):
                            cached = self._get_message(int(data.get("id") or 0))
                            value = {
                                "raw": data,
                                "author": getattr(getattr(cached, "author", None), "id", 0),
                            }
                        if ingress.blocked(value):
                            if not ingress.gate.fence.matches(value) and event.startswith(
                                ("CHANNEL_", "THREAD_", "GUILD_ROLE_")
                            ):


                                return original(data)
                            return
                members = getattr(client, "member_cache", None)
                if members is not None:
                    try:
                        members.gateway(event, data)
                    except Exception:
                        members.invalidate()
                capture = getattr(client, "gateway_captures", None)
                if capture is not None:


                    with contextlib.suppress(Exception):
                        if event == "READY":
                            capture.new_session()
                        else:
                            capture.receive(event, data)
                original(data)

            self.parsers[event] = receive
