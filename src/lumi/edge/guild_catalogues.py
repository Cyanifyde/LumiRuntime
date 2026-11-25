from __future__ import annotations

import logging
from dataclasses import replace
from types import MappingProxyType

from lumi.contracts import CommandSpec, GuildCommandSnapshot, decode
from lumi.shared.quiescence import tracked_read
from lumi.shared.guild_catalogues import catalogue_keys
from lumi.transport import GROUPS, require_module_owner

from .command_loading import NATIVE_COMMANDS
from .registry import RegisteredCommand
from .validation import IDENTIFIER, RESERVED_ROOTS, compile_linear, validate_help_text

LOGGER = logging.getLogger(__name__)


class GuildCatalogueCache:


    def __init__(self, queue):
        self.queue = queue
        self.snapshots = {}
        self.compiled = {}
        self.stopped = False

    def apply(self, group, payload):
        try:
            snapshot = decode(payload, GuildCommandSnapshot)
            if group != snapshot.group or not IDENTIFIER.fullmatch(snapshot.module):
                raise ValueError("catalogue group mismatch")
            require_module_owner(snapshot.module, group)
            roots = set()
            for route in snapshot.commands:
                if route.root in roots or len(route.arguments) > 16:
                    raise ValueError("invalid guild route")
                roots.add(route.root)
                validate_help_text(route.help)
                if any(
                    line.split(maxsplit=1)[0].removeprefix(".") != route.root
                    for line in route.help.usage.splitlines() if line.strip()
                ) or any(
                    not IDENTIFIER.fullmatch(key) or isinstance(value, str) and len(value) > 256
                    for key, value in route.arguments.items()
                ):
                    raise ValueError("invalid guild route arguments")
            guild = self.snapshots.setdefault(snapshot.guild_id, {})
            key = group, snapshot.module
            previous = guild.get(key)
            if previous and snapshot.revision <= previous.revision:
                if snapshot.revision == previous.revision and snapshot != previous:
                    raise ValueError("conflicting catalogue revision")
                return False
            guild[key] = snapshot
            self.compiled.pop(snapshot.guild_id, None)
            return True
        except (ValueError, TypeError):
            LOGGER.warning("guild_catalogue_rejected")
            return False

    def effective(self, registry, guild_id):
        catalogues = self.snapshots.get(guild_id)
        if not catalogues:
            return registry
        cached = self.compiled.get(guild_id)
        if cached and cached[0] == registry.revision:
            return cached[1]
        commands = dict(registry.commands)
        for (group, module), catalogue in sorted(catalogues.items()):
            registered = registry.modules.get(module)
            if not registered or registered.manifest.group != group:
                continue
            manifest = registered.manifest
            source = manifest.guild_commands
            if source is None:
                continue
            pattern = compile_linear(source.grammar, anchored=True)
            for route in catalogue.commands:
                if (
                    route.root in commands or route.root in RESERVED_ROOTS or route.root in NATIVE_COMMANDS
                    or route.root in registry.control_roots
                    or set(route.arguments) & pattern.groupindex.keys()
                ):
                    continue
                spec = CommandSpec(
                    route.root, source.handler, source.grammar, captures=source.captures,
                    help=route.help, acl_key=source.acl_key, public=True, context_fields=source.context_fields,
                    deadline_ms=source.deadline_ms, presentation_mode=manifest.presentation_mode,
                )
                commands[route.root] = RegisteredCommand(
                    module, group, manifest.generation, manifest.job_stream, spec, pattern,
                    manifest.presentation_mode, (manifest.presentation_mode,),
                    fixed_arguments=MappingProxyType(route.arguments),
                )
        effective = replace(registry, commands=MappingProxyType(commands))
        self.compiled[guild_id] = (registry.revision, effective)
        return effective

    @tracked_read
    async def reconstruct(self):
        cursors = {}
        for group in GROUPS:
            current, stream = catalogue_keys(group)
            latest = await self.queue.xrevrange(stream, count=1)
            cursors[stream] = latest[0][0] if latest else "0-0"
            for payload in await self.queue.hvals(current):
                self.apply(group, payload)
        return cursors

    async def run(self):
        cursor = await self.reconstruct()
        while not self.stopped:
            cursor = await self._read_once(cursor)

    @tracked_read
    async def _read_once(self, cursors):
        for stream, entries in await self.queue.xread(cursors, count=64, block=5000):
            stream = stream.decode() if isinstance(stream, bytes) else stream
            group = stream.rpartition(":")[2]
            for identifier, fields in entries:
                cursors[stream] = identifier
                payload = fields.get(b"catalogue") or fields.get("catalogue")
                if payload is not None:
                    self.apply(group, payload)
        return cursors

    def stop(self):
        self.stopped = True
