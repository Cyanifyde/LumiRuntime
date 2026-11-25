from __future__ import annotations

import asyncio
import time
from collections.abc import Mapping
from dataclasses import dataclass, field, replace
from types import MappingProxyType
from typing import Any

from lumi.contracts import (
    Announcement,
    CommandSpec,
    ComponentAliasSpec,
    ComponentSpec,
    Manifest,
    PassiveMatcherSpec,
    ReactionOfferSpec,
    ScheduledCallbackSpec,
    encode,
)
from lumi.edge.colours import LUMI_COLOUR, module_colours
from lumi.transport import GUILD_CONTROL_GROUP, TransportScopeError, reserved_control_roots

from .validation import ManifestError, compile_linear, validate_manifest


@dataclass(frozen=True, slots=True)
class RegisteredCommand:
    module: str
    group: str
    generation: int
    stream: str
    spec: CommandSpec
    pattern: Any
    presentation_mode: str
    presentation_modes: tuple[str, ...]
    fixed_arguments: Mapping[str, str | int | bool] = field(default_factory=dict, compare=False)


@dataclass(frozen=True, slots=True)
class RegisteredPassive:
    module: str
    group: str
    generation: int
    stream: str
    spec: PassiveMatcherSpec
    patterns: Mapping[str, Any]
    presentation_mode: str
    presentation_modes: tuple[str, ...]


@dataclass(frozen=True, slots=True)
class RegisteredReactionOffer:
    module: str
    group: str
    generation: int
    stream: str
    spec: ReactionOfferSpec
    pattern: Any
    presentation_mode: str
    presentation_modes: tuple[str, ...]


@dataclass(frozen=True, slots=True)
class RegisteredComponent:
    module: str
    group: str
    generation: int
    stream: str
    spec: ComponentSpec
    pattern: Any
    presentation_mode: str
    presentation_modes: tuple[str, ...]


@dataclass(frozen=True, slots=True)
class RegisteredScheduled:
    module: str
    group: str
    generation: int
    stream: str
    spec: ScheduledCallbackSpec
    presentation_mode: str
    presentation_modes: tuple[str, ...]


@dataclass(frozen=True, slots=True)
class RegisteredModule:
    manifest: Manifest
    runtime_state: str
    last_seen_ms: int


@dataclass(frozen=True, slots=True)
class RegistrySnapshot:
    revision: int
    commands: Mapping[str, RegisteredCommand]
    passive: Mapping[tuple[str, int | None], tuple[RegisteredPassive, ...]]
    reaction_offers: Mapping[tuple[str, int | None], tuple[RegisteredReactionOffer, ...]]
    reaction_offer_keys: Mapping[tuple[str, str], RegisteredReactionOffer]
    components: Mapping[tuple[str, str], RegisteredComponent]
    scheduled: Mapping[tuple[str, str], RegisteredScheduled]
    component_owners: Mapping[str, str]
    modules: Mapping[str, RegisteredModule]
    colour_modules: frozenset[str] = frozenset()
    branded_modules: frozenset[str] = frozenset()
    control_roots: frozenset[str] = frozenset()
    activity_boundary_modules: frozenset[str] = frozenset()
    activity_observers: frozenset[str] = frozenset()
    component_aliases: Mapping[str, tuple[RegisteredComponent, ComponentAliasSpec]] = field(
        default_factory=lambda: MappingProxyType({})
    )

    @classmethod
    def empty(cls) -> RegistrySnapshot:
        empty: Mapping[Any, Any] = MappingProxyType({})
        return cls(0, empty, empty, empty, empty, empty, empty, empty, empty)


def module_colour(snapshot: RegistrySnapshot, module: str) -> int:


    keys = getattr(snapshot, "colour_modules", None) or snapshot.modules
    return module_colours(keys, getattr(snapshot, "branded_modules", ())).get(module, LUMI_COLOUR)


class EdgeRouteRegistry:


    def __init__(self) -> None:
        self._snapshot = RegistrySnapshot.empty()
        try:
            self._snapshot = replace(self._snapshot, control_roots=reserved_control_roots())
        except TransportScopeError:


            pass
        self._write_lock = asyncio.Lock()

    @property
    def snapshot(self) -> RegistrySnapshot:
        return self._snapshot

    async def apply(self, announcement: Announcement) -> RegistrySnapshot:
        encode(announcement)
        compiled = await asyncio.to_thread(self._compile, announcement)
        async with self._write_lock:
            current = self._snapshot
            if announcement.kind == "retire":
                updated = self._retire(current, announcement.manifest)
            else:
                updated = self._install(current, compiled)
            if (current.colour_modules, current.branded_modules) != (
                updated.colour_modules, updated.branded_modules,
            ):


                await asyncio.to_thread(
                    module_colours, updated.colour_modules, updated.branded_modules,
                )
            self._snapshot = updated
            return updated

    def _compile(self, announcement: Announcement) -> tuple[Manifest, dict[str, Any]]:
        if announcement.kind not in {"announce", "heartbeat", "retire"}:
            raise ManifestError("unsupported announcement kind")
        validate_manifest(announcement.manifest)
        pattern_values = {command.grammar for command in announcement.manifest.commands}
        for passive in announcement.manifest.passive:
            pattern_values.update(
                node.value
                for node in self._walk(passive.expression)
                if node.op == "regex" and node.value is not None
            )
            pattern_values.update(repeated.pattern for repeated in passive.repeated)
            pattern_values.update(extraction.pattern for extraction in passive.extractions)
            for extraction in passive.component_extractions:
                pattern_values.update(
                    node.value
                    for node in self._walk(extraction.expression)
                    if node.op == "regex" and node.value is not None
                )
                pattern_values.update(
                    capture.pattern
                    for capture in extraction.captures
                    if capture.pattern is not None
                )
        pattern_values.update(offer.pattern for offer in announcement.manifest.reaction_offers)
        pattern_values.update(component.grammar for component in announcement.manifest.components)
        patterns = {
            pattern: compile_linear(
                pattern, anchored=pattern.startswith("^") and pattern.endswith("$")
            )
            for pattern in pattern_values
        }
        return announcement.manifest, patterns

    def _walk(self, expression):
        yield expression
        for child in expression.children:
            yield from self._walk(child)

    def _install(
        self,
        current: RegistrySnapshot,
        compiled: tuple[Manifest, dict[str, Any]],
    ) -> RegistrySnapshot:
        manifest, patterns = compiled
        previous = current.modules.get(manifest.module)
        now_ms = int(time.time() * 1000)
        if previous:
            old_generation = previous.manifest.generation
            if manifest.generation < old_generation:
                raise ManifestError("stale worker generation")
            if manifest.generation == old_generation:
                if manifest != previous.manifest:
                    raise ManifestError("a generation cannot change its manifest")
                modules = dict(current.modules)
                modules[manifest.module] = replace(
                    previous,
                    runtime_state="online",
                    last_seen_ms=now_ms,
                )
                return replace(
                    current,
                    revision=current.revision + 1,
                    modules=MappingProxyType(modules),
                )

        control_roots = current.control_roots | reserved_control_roots() | {
            command.root for command in manifest.commands if command.guild_control
        }

        commands = {
            root: route
            for root, route in current.commands.items()
            if route.module != manifest.module
        }
        components = {
            key: route
            for key, route in current.components.items()
            if route.module != manifest.module
        }
        component_aliases = {
            name: (route, alias)
            for name, (route, alias) in current.component_aliases.items()
            if route.module != manifest.module
        }
        scheduled = {
            key: route
            for key, route in current.scheduled.items()
            if route.module != manifest.module
        }
        component_owners = {
            namespace: owner
            for namespace, owner in current.component_owners.items()
            if owner != manifest.module
        }
        passive: dict[tuple[str, int | None], list[RegisteredPassive]] = {
            key: [route for route in routes if route.module != manifest.module]
            for key, routes in current.passive.items()
        }
        reaction_offers: dict[tuple[str, int | None], list[RegisteredReactionOffer]] = {
            key: [route for route in routes if route.module != manifest.module]
            for key, routes in current.reaction_offers.items()
        }
        reaction_offer_keys = {
            key: route
            for key, route in current.reaction_offer_keys.items()
            if route.module != manifest.module
        }

        for command in manifest.commands:
            presentation_mode = command.presentation_mode or manifest.presentation_mode
            route = RegisteredCommand(
                manifest.module,
                manifest.group,
                manifest.generation,
                manifest.job_stream,
                command,
                patterns[command.grammar],
                presentation_mode,
                command.presentation_modes or (presentation_mode,),
            )
            for root in (command.root, *command.aliases):
                key = root.casefold()
                if key in control_roots and (
                    manifest.group != GUILD_CONTROL_GROUP or manifest.module != key
                    or command.root != key
                ):
                    raise ManifestError(f"command root {key!r} belongs to a control authority")
                owner = commands.get(key)
                if owner and owner.module != manifest.module:


                    if not command.guild_control or owner.spec.guild_control:
                        raise ManifestError(f"command root {key!r} belongs to {owner.module}")
                commands[key] = route

        for component in manifest.components:
            owner = component_owners.get(component.namespace)
            if owner and owner != manifest.module:
                raise ManifestError(
                    f"component namespace {component.namespace!r} belongs to {owner}"
                )
            component_owners[component.namespace] = manifest.module
            presentation_mode = component.presentation_mode or manifest.presentation_mode
            route = components[(component.namespace, component.action)] = RegisteredComponent(
                manifest.module,
                manifest.group,
                manifest.generation,
                manifest.job_stream,
                component,
                patterns[component.grammar],
                presentation_mode,
                component.presentation_modes or (presentation_mode,),
            )
            for alias in component.legacy_aliases:
                if alias.name in component_aliases:
                    raise ManifestError("legacy component prefix already registered")
                component_aliases[alias.name] = route, alias

        for specification in manifest.scheduled:
            presentation_mode = specification.presentation_mode or manifest.presentation_mode
            scheduled[(manifest.module, specification.route)] = RegisteredScheduled(
                manifest.module,
                manifest.group,
                manifest.generation,
                manifest.job_stream,
                specification,
                presentation_mode,
                specification.presentation_modes or (presentation_mode,),
            )

        for specification in manifest.passive:
            presentation_mode = specification.presentation_mode or manifest.presentation_mode
            route = RegisteredPassive(
                manifest.module,
                manifest.group,
                manifest.generation,
                manifest.job_stream,
                specification,
                MappingProxyType(patterns),
                presentation_mode,
                specification.presentation_modes or (presentation_mode,),
            )
            keys = [(specification.event, None)]
            if specification.bot_ids:
                keys = [(specification.event, bot_id) for bot_id in specification.bot_ids]
            for key in keys:
                passive.setdefault(key, []).append(route)

        for specification in manifest.reaction_offers:
            presentation_mode = specification.presentation_mode or manifest.presentation_mode
            route = RegisteredReactionOffer(
                manifest.module,
                manifest.group,
                manifest.generation,
                manifest.job_stream,
                specification,
                patterns[specification.pattern],
                presentation_mode,
                specification.presentation_modes or (presentation_mode,),
            )
            reaction_offer_keys[(manifest.module, specification.key)] = route
            if specification.prepared_only:
                continue
            for event in specification.events:
                keys = [(event, None)]
                if specification.bot_ids:
                    keys = [(event, bot_id) for bot_id in specification.bot_ids]
                for key in keys:
                    reaction_offers.setdefault(key, []).append(route)

        modules = dict(current.modules)
        modules[manifest.module] = RegisteredModule(manifest, "online", now_ms)
        return RegistrySnapshot(
            revision=current.revision + 1,
            commands=MappingProxyType(commands),
            passive=MappingProxyType(
                {key: tuple(routes) for key, routes in passive.items() if routes}
            ),
            reaction_offers=MappingProxyType(
                {key: tuple(routes) for key, routes in reaction_offers.items() if routes}
            ),
            reaction_offer_keys=MappingProxyType(reaction_offer_keys),
            components=MappingProxyType(components),
            component_aliases=MappingProxyType(component_aliases),
            scheduled=MappingProxyType(scheduled),
            component_owners=MappingProxyType(component_owners),
            modules=MappingProxyType(modules),
            colour_modules=current.colour_modules | {manifest.module},
            branded_modules=(current.branded_modules - {manifest.module})
            | ({manifest.module} if manifest.accent_style == "brand" else frozenset()),
            control_roots=control_roots,
            activity_observers=(current.activity_observers - {manifest.module}) | (
                {manifest.module} if manifest.activity_observations else frozenset()
            ),
            activity_boundary_modules=(current.activity_boundary_modules - {manifest.module})
            | ({manifest.module} if any(
                "activity_boundary" in command.context_fields for command in manifest.commands
            ) or any(
                spec.guild_sources is not None and spec.guild_sources.include_activity
                for spec in (*manifest.commands, *manifest.passive, *manifest.components)
            ) else frozenset()),
        )

    def _retire(self, current: RegistrySnapshot, manifest: Manifest) -> RegistrySnapshot:
        previous = current.modules.get(manifest.module)
        if previous is None:
            return current
        if manifest.generation < previous.manifest.generation:
            raise ManifestError("stale retirement generation")
        commands = {
            key: route for key, route in current.commands.items() if route.module != manifest.module
        }
        components = {
            key: route
            for key, route in current.components.items()
            if route.module != manifest.module
        }
        scheduled = {
            key: route
            for key, route in current.scheduled.items()
            if route.module != manifest.module
        }
        component_owners = {
            namespace: owner
            for namespace, owner in current.component_owners.items()
            if owner != manifest.module
        }
        passive = {
            key: tuple(route for route in routes if route.module != manifest.module)
            for key, routes in current.passive.items()
        }
        reaction_offers = {
            key: tuple(route for route in routes if route.module != manifest.module)
            for key, routes in current.reaction_offers.items()
        }
        reaction_offer_keys = {
            key: route
            for key, route in current.reaction_offer_keys.items()
            if route.module != manifest.module
        }
        modules = dict(current.modules)
        modules.pop(manifest.module, None)
        return RegistrySnapshot(
            revision=current.revision + 1,
            commands=MappingProxyType(commands),
            passive=MappingProxyType({key: value for key, value in passive.items() if value}),
            reaction_offers=MappingProxyType(
                {key: value for key, value in reaction_offers.items() if value}
            ),
            reaction_offer_keys=MappingProxyType(reaction_offer_keys),
            components=MappingProxyType(components),
            component_aliases=MappingProxyType({
                name: (route, alias)
                for name, (route, alias) in current.component_aliases.items()
                if route.module != manifest.module
            }),
            scheduled=MappingProxyType(scheduled),
            component_owners=MappingProxyType(component_owners),
            modules=MappingProxyType(modules),
            colour_modules=current.colour_modules,
            branded_modules=current.branded_modules,
            control_roots=current.control_roots,
            activity_boundary_modules=current.activity_boundary_modules - {manifest.module},
            activity_observers=current.activity_observers - {manifest.module},
        )

    async def mark_stale(
        self, *, stale_after_ms: int, now_ms: int | None = None
    ) -> RegistrySnapshot:
        now_ms = now_ms or int(time.time() * 1000)
        async with self._write_lock:
            modules = dict(self._snapshot.modules)
            changed = False
            for name, module in tuple(modules.items()):
                if (
                    now_ms - module.last_seen_ms > stale_after_ms
                    and module.runtime_state != "offline"
                ):
                    modules[name] = replace(module, runtime_state="offline")
                    changed = True
            if changed:
                self._snapshot = replace(
                    self._snapshot,
                    revision=self._snapshot.revision + 1,
                    modules=MappingProxyType(modules),
                )
            return self._snapshot

    def modules_for_group(self, group: str) -> tuple[str, ...]:
        return tuple(
            sorted(
                name
                for name, registered in self._snapshot.modules.items()
                if registered.manifest.group == group
            )
        )
