from __future__ import annotations

import asyncio
from collections import defaultdict
from collections.abc import Iterable
from dataclasses import dataclass
from typing import Protocol

from lumi.edge.registry import EdgeRouteRegistry
from lumi.transport import GROUPS


class ControlProjectionClient(Protocol):
    async def get_maintenance(self) -> bool: ...
    async def load_drains(self) -> Iterable[str]: ...
    async def set_maintenance(self, enabled: bool) -> None: ...
    async def set_drain(self, group: str, enabled: bool) -> None: ...


@dataclass(frozen=True, slots=True)
class RestartOutcome:
    group: str
    modules: tuple[str, ...]
    hard_restart: bool
    deployment: str
    revision: str | None


@dataclass(frozen=True, slots=True)
class CachedHealth:
    state: str
    round_trip_ms: float | None
    generation: int | None


class RuntimeControlPlane:
    def __init__(
        self,
        *,
        registry: EdgeRouteRegistry,
        store: ControlProjectionClient,
        lifecycle,
        activation_timeout: float = 30.0,
    ) -> None:
        self.registry = registry
        self.store = store
        self.lifecycle = lifecycle
        self.activation_timeout = activation_timeout
        self.maintenance = False
        self.manual_drains: set[str] = set()
        self.health: dict[str, CachedHealth] = {
            group: CachedHealth("offline", None, None) for group in GROUPS
        }
        self._locks: defaultdict[str, asyncio.Lock] = defaultdict(asyncio.Lock)

    async def load(self) -> None:
        self.maintenance = await self.store.get_maintenance()
        self.manual_drains = set(await self.store.load_drains())

    def apply_projection(self, snapshot) -> None:
        self.maintenance = snapshot.maintenance
        self.manual_drains = set(snapshot.drains)

    def resolve(self, target: str) -> tuple[str, tuple[str, ...]]:
        normalized = target.casefold()
        if normalized in GROUPS:
            group = normalized
        else:
            module = self.registry.snapshot.modules.get(normalized)
            if module is None:
                raise ValueError("unknown module or worker group")
            group = module.manifest.group
        return group, self.registry.modules_for_group(group)

    async def restart(self, target: str) -> RestartOutcome:
        group, _ = self.resolve(target)
        async with self._locks[group]:
            modules = self.registry.modules_for_group(group)
            snapshot = self.registry.snapshot
            previous_generations = {
                module: snapshot.modules[module].manifest.generation for module in modules
            }
            reply = await self.lifecycle.request("restart", group)
            if not await self._wait_activation(group, previous_generations):
                raise RuntimeError("replacement worker did not register a healthy generation")
            return RestartOutcome(
                group=group,
                modules=modules,
                hard_restart=reply.hard,
                deployment=reply.deployment,
                revision=reply.revision,
            )

    async def set_drain(self, target: str, enabled: bool) -> tuple[str, tuple[str, ...]]:
        group, modules = self.resolve(target)
        async with self._locks[group]:
            await self.store.set_drain(group, enabled)
            if enabled:
                self.manual_drains.add(group)
            else:
                self.manual_drains.discard(group)
            await self.lifecycle.request("drain", group)
        return group, modules

    async def set_maintenance(self, enabled: bool) -> None:
        await self.store.set_maintenance(enabled)
        self.maintenance = enabled

    async def refresh_health(self) -> None:
        rows = await self.lifecycle.health()
        for group in GROUPS:
            status = rows.get(group)
            fallback = "draining" if group in self.manual_drains else "offline"
            self.health[group] = CachedHealth(
                status.state if status else fallback,
                status.round_trip_ms if status else None,
                status.generation if status else None,
            )

    async def _wait_activation(self, group: str, previous: dict[str, int]) -> bool:


        deadline = asyncio.get_running_loop().time() + self.activation_timeout
        while True:
            snapshot = self.registry.snapshot
            routes_ready = all(
                (registered := snapshot.modules.get(module)) is not None
                and registered.manifest.group == group
                and registered.manifest.generation > generation
                and registered.runtime_state == "online"
                for module, generation in previous.items()
            )
            if routes_ready:
                return True
            remaining = deadline - asyncio.get_running_loop().time()
            if remaining <= 0:
                return False
            await asyncio.sleep(min(0.1, remaining))
