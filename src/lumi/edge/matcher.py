from __future__ import annotations

import re
import unicodedata
from collections.abc import Callable
from dataclasses import dataclass
from decimal import Decimal, InvalidOperation
from typing import Any
from urllib.parse import urlsplit

import emoji as emoji_lib

from lumi.contracts import (
    CaptureSpec,
    GuildSourceSpec,
    MatcherExpr,
    ModalSpec,
    PendingPresentationSpec,
    TypedRequest,
)

from .message_fetches import content_digest
from .registry import RegisteredCommand, RegisteredReactionOffer, RegistrySnapshot
from lumi.shared.request_scope import memo

DURATION_TOKEN = re.compile(r"(?i)(\d+)([smhdw])")
CUSTOM_EMOJI_INPUT = re.compile(
    r"^<(?P<animated>a?):(?P<name>[A-Za-z0-9_]{2,32}):(?P<id>\d{17,20})>$"
)
CUSTOM_EMOJI_NAME = re.compile(r"^:(?P<name>[A-Za-z0-9_]{2,32})(?:~(?P<ordinal>[1-9]\d{0,3}))?:$")


class MatchError(ValueError):
    pass


class EmojiLookupMiss(MatchError):
    pass


@dataclass(frozen=True, slots=True)
class MatchedRoute:
    module: str
    group: str
    generation: int
    stream: str
    deadline_ms: int
    queue_class: str
    request: TypedRequest
    presentation_mode: str = "v2"
    presentation_modes: tuple[str, ...] = ()
    acl_key: str = ""
    public: bool = False
    actor_id: int | None = None
    watch_ttl_ms: int = 0
    pending: PendingPresentationSpec | None = None
    loading: bool = False
    private_loading: bool = False
    guild_sources: GuildSourceSpec | None = None


@dataclass(frozen=True, slots=True)
class MatchedReactionOffer:
    registered: RegisteredReactionOffer
    arguments: dict[str, Any]
    context: dict[str, Any]


def _convert(value: str | None, capture: CaptureSpec) -> Any:
    if value is None:
        if capture.required:
            raise MatchError(f"missing capture: {capture.name}")
        return None
    if capture.kind == "text":
        converted: Any = value
    elif capture.kind == "normalized_text":
        converted = " ".join(unicodedata.normalize("NFKC", value).split())
    elif capture.kind == "snowflake":
        if not re.fullmatch(r"\d{17,20}", value):
            raise MatchError("invalid snowflake")
        converted = int(value)
    elif capture.kind == "integer":
        converted = int(value)
    elif capture.kind == "boolean":
        folded = value.casefold()
        if folded not in {"true", "false", "on", "off", "yes", "no", "1", "0"}:
            raise MatchError("invalid boolean")
        converted = folded in {"true", "on", "yes", "1"}
    elif capture.kind == "duration":
        converted = _duration_seconds(value)
    elif capture.kind == "compact_integer":
        converted = _compact_integer(value)
    else:
        raise MatchError("unsupported capture type")
    if isinstance(converted, (str, int)):
        length_or_value = len(converted) if isinstance(converted, str) else converted
        if capture.minimum is not None and length_or_value < capture.minimum:
            raise MatchError(f"capture {capture.name} is below its minimum")
        if capture.maximum is not None and length_or_value > capture.maximum:
            raise MatchError(f"capture {capture.name} exceeds its maximum")
    return converted


def _duration_seconds(value: str) -> int:
    value = "".join(value.split())
    position = 0
    total = 0
    units = {"s": 1, "m": 60, "h": 3600, "d": 86_400, "w": 604_800}
    for token in DURATION_TOKEN.finditer(value):
        if token.start() != position:
            raise MatchError("invalid duration")
        total += int(token.group(1)) * units[token.group(2).casefold()]
        position = token.end()
    if not value or position != len(value):
        raise MatchError("invalid duration")
    return total


def _compact_integer(value: str) -> int:
    raw = value.strip()
    match = re.fullmatch(
        r"(?i)(?:(\d+|\d{1,3}(?:,\d{3})+)|"
        r"(\d+(?:\.\d{1,3})?)([km]))",
        raw,
    )
    if match is None:
        raise MatchError("invalid compact integer")
    try:
        amount = Decimal((match.group(1) or match.group(2)).replace(",", ""))
    except InvalidOperation as exc:
        raise MatchError("invalid compact integer") from exc
    multiplier = {None: 1, "k": 1_000, "m": 1_000_000}[
        match.group(3).casefold() if match.group(3) else None
    ]
    result = amount * multiplier
    if result != result.to_integral_value() or result < 0 or result > 2_147_483_647:
        raise MatchError("invalid compact integer")
    return int(result)


class EdgeMatcher:
    def accepts_channel_watch(self, route, event: dict[str, Any]) -> bool:
        spec = route.spec
        return bool(
            spec.channel_scoped
            and (spec.requires_embeds is None or spec.requires_embeds == bool(event.get("embeds")))
            and (
                spec.requires_attachments is None
                or spec.requires_attachments == bool(event.get("attachments"))
            )
            and self._evaluate(spec.expression, event, route.patterns)
        )

    def match_command(
        self,
        snapshot: RegistrySnapshot,
        *,
        content: str,
        prefix: str,
        event: dict[str, Any],
    ) -> MatchedRoute | None:
        if not content.startswith(prefix):
            return None
        body = content[len(prefix) :]
        root, separator, tail = body.partition(" ")
        if not root:
            return None
        registered = snapshot.commands.get(root.casefold())
        if registered is None:
            return None
        argument_text = f" {tail}" if separator else ""
        def parse():
            match = registered.pattern.fullmatch(argument_text)
            if match is None:
                raise MatchError("command arguments do not match the announced grammar")
            return self._captures(match.groupdict(), registered.spec.captures)


        arguments = memo("command-arguments", (self, registered, argument_text), parse)
        return self._command_result(registered, arguments, event)

    def _command_result(
        self,
        route: RegisteredCommand,
        arguments: dict[str, Any],
        event: dict[str, Any],
    ) -> MatchedRoute:
        context = self._context(event, route.spec.context_fields)
        context["dispatch"] = "command"
        if route.spec.guild_control:
            context["_guild_control"] = True
        if route.fixed_arguments:
            arguments = {**route.fixed_arguments, **arguments}
        return MatchedRoute(
            route.module,
            route.group,
            route.generation,
            route.stream,
            route.spec.deadline_ms,
            route.spec.queue_class,
            TypedRequest(
                route.spec.handler,
                route.spec.handler,
                arguments,
                context,
            ),
            route.presentation_mode,
            route.presentation_modes,
            route.spec.acl_key,
            route.spec.public,
            pending=route.spec.pending,
            loading=route.spec.loading,
            guild_sources=route.spec.guild_sources,
        )

    def match_passive(
        self,
        snapshot: RegistrySnapshot,
        *,
        event_type: str,
        event: dict[str, Any],
        enabled: Callable[[str], bool] | None = None,
    ) -> tuple[MatchedRoute, ...]:
        author_id = event.get("author_id")
        candidates = (
            *snapshot.passive.get((event_type, author_id), ()),
            *snapshot.passive.get((event_type, None), ()),
        )
        matched: list[MatchedRoute] = []
        seen: set[tuple[str, str]] = set()
        fallbacks: dict[tuple[str, str], tuple[str, ...]] = {}
        source_event = event
        webhook = event.get("webhook_reply")
        without_webhook = {k: v for k, v in event.items() if k != "webhook_reply"} if webhook else event
        binding = event.get("bound_reply")
        without_binding = (
            {k: v for k, v in without_webhook.items() if k != "bound_reply"}
            if binding else without_webhook
        )
        for route in candidates:
            exact_binding = isinstance(binding, dict) and (
                binding.get("module"), binding.get("group"), binding.get("route")
            ) == (route.module, route.group, route.spec.route)
            if route.spec.bound_human_reply and not exact_binding:
                continue
            event = (
                without_webhook if route.spec.bound_human_reply else without_binding
            )
            if route.spec.recipient_webhook and isinstance(webhook, dict) and webhook.get("module") == route.module:
                event = {**event, "webhook_reply": source_event["webhook_reply"]}
            if route.spec.own_output_deleted and not (
                event_type == "message_delete" and event.get("_own_output_deleted") is True
                and event.get("_own_output_module") in (None, route.module)
                and all(type(event.get(key)) is int and event[key] > 0 for key in ("guild_id", "channel_id", "message_id"))
            ):
                continue
            if route.spec.personal_reaction != bool(source_event.get("_reaction_binding")):
                continue
            if route.spec.personal_reaction and source_event["_reaction_binding"] != (
                route.group, route.module, route.spec.route,
            ):
                continue
            if route.spec.channel_scoped:
                continue
            identity = (route.module, route.spec.route)
            if identity in seen:
                continue
            seen.add(identity)
            if enabled is not None and not route.spec.own_output_deleted and not enabled(route.module):
                continue
            if route.spec.reply_prompt and event.get("reply_prompt_module") != route.module:
                continue
            if route.spec.hydrate and not bool(event.get("passive_hydrated")):
                continue
            if route.spec.hydrate_on and not any(
                field in event.get("_native_edit_fields", ()) for field in route.spec.hydrate_on
            ):
                continue
            if route.spec.requires_embeds is not None:
                if bool(event.get("embeds")) != route.spec.requires_embeds:
                    continue
            if route.spec.requires_attachments is not None:
                if bool(event.get("attachments")) != route.spec.requires_attachments:
                    continue
            if not self._evaluate(route.spec.expression, event, route.patterns):
                continue
            arguments: dict[str, Any] = {}
            extraction_failed = False
            for extraction in route.spec.extractions:
                selected = next(
                    (
                        item
                        for value in self._field_values(event, extraction.field)
                        if (item := route.patterns[extraction.pattern].fullmatch(value)) is not None
                    ),
                    None,
                )
                if selected is None:
                    extraction_failed = True
                    break
                try:
                    arguments.update(self._captures(selected.groupdict(), extraction.captures))
                except MatchError:
                    extraction_failed = True
                    break
            if extraction_failed:
                continue
            for repeated in route.spec.repeated:
                values = self._field_values(event, repeated.field)
                rows: list[dict[str, Any]] = []
                pattern = route.patterns[repeated.pattern]
                for value in values:
                    for item in pattern.finditer(value):
                        rows.append(self._captures(item.groupdict(), repeated.captures))
                        if len(rows) >= repeated.maximum:
                            break
                    if len(rows) >= repeated.maximum:
                        break
                arguments[repeated.field] = rows
            component_extraction_failed = False
            for extraction in route.spec.component_extractions:
                rows: list[dict[str, Any]] = []
                for component in self._components(event.get("components")):
                    if not self._evaluate(extraction.expression, component, route.patterns):
                        continue
                    row: dict[str, Any] = {}
                    valid = True
                    for capture in extraction.captures:
                        raw_values = self._field_values(component, capture.field)
                        raw = raw_values[0] if raw_values else None
                        value = raw
                        if capture.pattern is not None and raw is not None:
                            match = route.patterns[capture.pattern].fullmatch(raw)
                            value = match.groupdict().get(capture.name) if match else None
                        specification = CaptureSpec(
                            capture.name,
                            capture.kind,
                            capture.required,
                            capture.minimum,
                            capture.maximum,
                        )
                        try:
                            row[capture.name] = _convert(value, specification)
                        except MatchError:
                            valid = False
                            break
                    if valid:
                        if len(rows) >= extraction.maximum:
                            component_extraction_failed = True
                            break
                        rows.append(row)
                if component_extraction_failed:
                    break
                arguments[extraction.name] = rows
            if component_extraction_failed:
                continue
            subject_id: int | None = None
            subject_value: Any = None
            if route.spec.subject_capture is not None:
                subject_value = arguments.get(route.spec.subject_capture)
            elif route.spec.subject_field is not None:
                values = self._field_values(event, route.spec.subject_field)
                subject_value = values[0] if values else None
            if subject_value is not None:
                try:
                    subject_id = int(subject_value)
                except (TypeError, ValueError):
                    subject_id = None
            if route.spec.subject_required and not subject_id:
                continue
            if route.spec.reference_author_max_age_ms:
                reply_created_at_ms = int(event.get("reply_created_at_ms") or 0)
                created_at_ms = int(event.get("created_at_ms") or 0)
                if (
                    not subject_id
                    or bool(event.get("reply_author_bot"))
                    or event.get("reply_webhook_id") is not None
                    or int(event.get("reply_channel_id") or 0) != int(event.get("channel_id") or 0)
                    or int(event.get("reply_guild_id") or 0) != int(event.get("guild_id") or 0)
                    or reply_created_at_ms <= 0
                    or created_at_ms < reply_created_at_ms
                    or created_at_ms - reply_created_at_ms > route.spec.reference_author_max_age_ms
                ):
                    continue
            context = self._context(event, route.spec.context_fields)
            if route.spec.source_cleanup:
                ids = tuple(event.get(field) for field in (
                    "message_id", "reference_message_id", "reply_reference_message_id",
                ))
                if (
                    event.get("_trusted_reply_chain") is not True
                    or event.get("webhook_id") is not None
                    or any(type(value) is not int or value <= 0 for value in ids)
                    or len(set(ids)) != 3
                    or not isinstance(event.get("reply_content"), str)
                ):
                    continue


                context["_source_cleanup"] = {
                    "schema": 2,
                    "source": ids[0],
                    "source_author": author_id,
                    "command": ids[1],
                    "prompt": ids[2],
                    "content_digest": content_digest(event["reply_content"]),
                }
            if subject_id is not None:
                context["actor_id"] = subject_id
            context["dispatch"] = "passive"
            if route.spec.own_output_deleted:


                context["_own_output_deleted"] = True
            if route.spec.prepare_reply is not None:
                context["_prepared_reply"] = route.spec.prepare_reply
                context["kind"] = event_type
            if route.spec.unless_routes_match:
                fallbacks[identity] = route.spec.unless_routes_match
            matched.append(
                MatchedRoute(
                    route.module,
                    route.group,
                    route.generation,
                    route.stream,
                    route.spec.deadline_ms,
                    route.spec.queue_class,
                    TypedRequest(
                        route.spec.route,
                        route.spec.handler,
                        arguments,
                        context,
                    ),
                    route.presentation_mode,
                    route.presentation_modes,
                    route.spec.acl_key
                    if (route.spec.personal_reaction or route.spec.bound_human_reply)
                    else "",
                    bool(route.spec.personal_reaction or route.spec.bound_human_reply),
                    subject_id,
                    route.spec.watch_ttl_ms,
                    guild_sources=route.spec.guild_sources,
                )
            )
        if not fallbacks:
            return tuple(matched)
        admitted = {(route.module, route.request.route) for route in matched}
        return tuple(
            route for route in matched
            if not any(
                (route.module, other) in admitted
                for other in fallbacks.get((route.module, route.request.route), ())
            )
        )

    def has_passive_candidates(
        self,
        snapshot: RegistrySnapshot,
        *,
        event_type: str,
        author_id: int | None,
        context_field: str | None = None,
        enabled: Callable[[str], bool] | None = None,
    ) -> bool:
        candidates = (
            *snapshot.passive.get((event_type, author_id), ()),
            *snapshot.passive.get((event_type, None), ()),
        )
        return any(
            (context_field is None or context_field in route.spec.context_fields)
            and (enabled is None or enabled(route.module))
            for route in candidates
        )

    def passive_watch_ttl(
        self,
        snapshot: RegistrySnapshot,
        *,
        event_type: str,
        author_id: int | None,
        context_field: str | None = None,
        enabled: Callable[[str], bool] | None = None,
    ) -> int:
        candidates = (
            *snapshot.passive.get((event_type, author_id), ()),
            *snapshot.passive.get((event_type, None), ()),
        )
        return max(
            (
                route.spec.watch_ttl_ms
                for route in candidates
                if route.spec.watch_ttl_ms
                and (context_field is None or context_field in route.spec.context_fields)
                and (enabled is None or enabled(route.module))
            ),
            default=0,
        )

    def needs_passive_hydration(
        self,
        snapshot: RegistrySnapshot,
        *,
        event_type: str,
        author_id: int | None,
        enabled: Callable[[str], bool] | None = None,
        watched: bool = True,
        partial_event: dict[str, Any] | None = None,
    ) -> bool:
        candidates = (
            *snapshot.passive.get((event_type, author_id), ()),
            *snapshot.passive.get((event_type, None), ()),
        )
        return any(
            route.spec.hydrate
            and (enabled is None or enabled(route.module))
            and (
                not route.spec.hydrate_on or (
                    partial_event is not None
                    and any(field in partial_event.get("_native_edit_fields", ()) for field in route.spec.hydrate_on)
                )
            )
            and (
                watched
                or (
                    route.spec.hydrate_matching_edit
                    and partial_event is not None
                    and self._evaluate(route.spec.expression, partial_event, route.patterns)
                )
            )
            for route in candidates
        )

    def has_reaction_offer_candidates(
        self,
        snapshot: RegistrySnapshot,
        *,
        event_type: str,
        author_id: int | None,
        enabled: Callable[[str], bool] | None = None,
    ) -> bool:
        candidates = (
            *snapshot.reaction_offers.get((event_type, author_id), ()),
            *snapshot.reaction_offers.get((event_type, None), ()),
        )
        return any(enabled is None or enabled(route.module) for route in candidates)

    def match_reaction_offers(
        self,
        snapshot: RegistrySnapshot,
        *,
        event_type: str,
        event: dict[str, Any],
        enabled: Callable[[str], bool] | None = None,
    ) -> tuple[MatchedReactionOffer, ...]:
        author_id = event.get("author_id")
        candidates = (
            *snapshot.reaction_offers.get((event_type, author_id), ()),
            *snapshot.reaction_offers.get((event_type, None), ()),
        )
        matched: list[MatchedReactionOffer] = []
        seen: set[tuple[str, str]] = set()
        for route in candidates:
            identity = (route.module, route.spec.key)
            if identity in seen:
                continue
            seen.add(identity)
            if enabled is not None and not enabled(route.module):
                continue
            if route.spec.requires_embeds is not None:
                if bool(event.get("embeds")) != route.spec.requires_embeds:
                    continue
            if route.spec.requires_image and not event.get("primary_image_url"):
                continue
            selected = next(
                (
                    match
                    for value in self._field_values(event, route.spec.field)
                    if (match := route.pattern.fullmatch(value)) is not None
                ),
                None,
            )
            if selected is None:
                continue
            try:
                arguments = self._captures(selected.groupdict(), route.spec.captures)
            except MatchError:
                continue
            context = self._context(event, route.spec.context_fields)
            matched.append(MatchedReactionOffer(route, arguments, context))
        return tuple(matched)

    def reaction_offer_callback(
        self,
        route: RegisteredReactionOffer,
        *,
        arguments: dict[str, Any],
        event: dict[str, Any],
    ) -> MatchedRoute:
        callback_event = self._context(event, route.spec.context_fields)
        callback_event["dispatch"] = "reaction_offer"
        if route.spec.prepared_only:
            callback_event["_prepared_reply"] = route.spec.key
        return MatchedRoute(
            route.module,
            route.group,
            route.generation,
            route.stream,
            route.spec.deadline_ms,
            route.spec.queue_class,
            TypedRequest(
                f"{route.module}.{route.spec.key}",
                route.spec.callback,
                arguments,
                callback_event,
            ),
            route.presentation_mode,
            route.presentation_modes,
            "",
            True,
        )

    def match_component(
        self,
        snapshot: RegistrySnapshot,
        *,
        custom_id: str,
        event: dict[str, Any],
    ) -> MatchedRoute | None:
        if len(custom_id) > 100 or not custom_id.startswith("lumi:"):
            return None
        parts = custom_id.split(":", 3)
        if len(parts) != 4:
            return None
        _, namespace, action, payload = parts
        registered = snapshot.components.get((namespace, action))
        if registered is None:
            return None
        match = registered.pattern.fullmatch(payload)
        if match is None:
            raise MatchError("component payload does not match its announced grammar")
        arguments = self._captures(match.groupdict(), registered.spec.captures)
        input_kind = registered.spec.input_kind
        raw_values = event.get("component_values", ())
        if input_kind == "string_values":
            if not isinstance(raw_values, (list, tuple)):
                raise MatchError("invalid string selection")
            arguments["values"] = tuple(str(value) for value in raw_values[:25])
        elif input_kind in {"user_ids", "role_ids", "channel_ids"}:
            try:
                arguments["values"] = tuple(int(value) for value in raw_values[:25])
            except (TypeError, ValueError) as exc:
                raise MatchError("invalid snowflake selection") from exc
            if input_kind == "user_ids":
                resolved_users = event.get("component_user_ids", ())
                if not isinstance(resolved_users, (list, tuple, set, frozenset)):
                    raise MatchError("invalid resolved user selection")
                try:
                    resolved_ids = {int(value) for value in resolved_users}
                except (TypeError, ValueError) as exc:
                    raise MatchError("invalid resolved user selection") from exc
                if any(value not in resolved_ids for value in arguments["values"]):
                    raise MatchError("unresolved user selection")
            if input_kind == "role_ids":
                resolved_roles = event.get("component_role_ids", ())
                if not isinstance(resolved_roles, (list, tuple, set, frozenset)):
                    raise MatchError("invalid resolved role selection")
                try:
                    resolved_ids = {int(value) for value in resolved_roles}
                except (TypeError, ValueError) as exc:
                    raise MatchError("invalid resolved role selection") from exc
                if any(value not in resolved_ids for value in arguments["values"]):
                    raise MatchError("unresolved role selection")
            if input_kind == "channel_ids":
                raw_types = event.get("component_channel_types", {})
                if isinstance(raw_types, dict):
                    if registered.spec.channel_types and any(
                        int(raw_types.get(str(value), -1)) not in registered.spec.channel_types
                        for value in arguments["values"]
                    ):
                        raise MatchError("selected channel has an invalid type")
                    arguments["channel_types"] = {
                        str(key): int(value) for key, value in raw_types.items()
                    }
        elif input_kind == "text":
            value = event.get("component_text")
            if not isinstance(value, str):
                raise MatchError("missing text input")
            arguments["value"] = value
        elif input_kind == "modal":
            specification = registered.spec.modal
            if specification is None:
                raise MatchError("missing modal specification")
            if event.get("component_modal_submit") is True:
                values = event.get("component_modal_values")
                if not isinstance(values, dict):
                    raise MatchError("missing modal input")
                files = event.get("component_modal_files", {})
                if not isinstance(files, dict):
                    raise MatchError("invalid modal files")
                modal_values, modal_files = self._modal_arguments(
                    specification,
                    values,
                    files,
                    event,
                )
                arguments["values"] = modal_values
                arguments["files"] = modal_files
        context = self._context(event, registered.spec.context_fields)
        context["dispatch"] = "component"
        if registered.spec.guild_control:
            context["_guild_control"] = True
        return MatchedRoute(
            registered.module,
            registered.group,
            registered.generation,
            registered.stream,
            registered.spec.deadline_ms,
            registered.spec.queue_class,
            TypedRequest(
                f"{namespace}.{action}",
                registered.spec.handler,
                arguments,
                context,
            ),
            registered.presentation_mode,
            registered.presentation_modes,
            registered.spec.acl_key,
            registered.spec.public,
            pending=registered.spec.pending,
            private_loading=registered.spec.private_loading,
            guild_sources=registered.spec.guild_sources,
        )

    @staticmethod
    def _modal_arguments(
        specification: ModalSpec,
        raw_values: dict[str, Any],
        raw_files: dict[str, Any],
        event: dict[str, Any],
    ) -> tuple[dict[str, Any], dict[str, tuple[dict[str, Any], ...]]]:
        fields = {field.key: field for field in specification.fields}
        if set(raw_values) - set(fields) or set(raw_files) - set(fields):
            raise MatchError("modal supplied an undeclared field")
        values: dict[str, Any] = {}
        files: dict[str, tuple[dict[str, Any], ...]] = {}
        channel_types = event.get("component_channel_types", {})
        if not isinstance(channel_types, dict):
            channel_types = {}
        resolved_users = event.get("component_user_ids", ())
        if not isinstance(resolved_users, (list, tuple, set, frozenset)):
            resolved_users = ()
        resolved_user_ids = {str(value) for value in resolved_users}
        resolved_roles = event.get("component_role_ids", ())
        if not isinstance(resolved_roles, (list, tuple, set, frozenset)):
            resolved_roles = ()
        resolved_role_ids = {str(value) for value in resolved_roles}
        guild_roles = event.get("component_guild_role_ids", ())
        if isinstance(guild_roles, (list, tuple, set, frozenset)):
            resolved_role_ids.update(str(value) for value in guild_roles)
        for field in specification.fields:
            if field.kind in {"text", "emoji"}:
                value = raw_values.get(field.key)
                if value is None:
                    if field.required:
                        raise MatchError("required modal field is missing")
                    continue
                if not isinstance(value, str):
                    raise MatchError("invalid modal text")
                if not field.minimum <= len(value) <= field.maximum:
                    raise MatchError("modal text is outside its limits")
                values[field.key] = (
                    EdgeMatcher._modal_emoji(value, event) if field.kind == "emoji" else value
                )
                continue
            if field.kind == "file_upload":
                selected = raw_files.get(field.key, ())
                if not isinstance(selected, (list, tuple)):
                    raise MatchError("invalid modal files")
                if not selected and not field.required:
                    continue
                if not field.min_values <= len(selected) <= field.max_values:
                    raise MatchError("modal file count is outside its limits")
                validated: list[dict[str, Any]] = []
                for item in selected:
                    if not isinstance(item, dict):
                        raise MatchError("invalid modal file")
                    file_name = item.get("filename")
                    url = item.get("url")
                    content_type = item.get("content_type")
                    size = item.get("size")
                    try:
                        parsed_url = urlsplit(url) if isinstance(url, str) else None
                    except ValueError:
                        parsed_url = None
                    if (
                        not isinstance(file_name, str)
                        or not file_name
                        or len(file_name) > 255
                        or parsed_url is None
                        or parsed_url.scheme != "https"
                        or not parsed_url.hostname
                        or parsed_url.username is not None
                        or parsed_url.password is not None
                        or not isinstance(size, int)
                        or size <= 0
                        or size > 25 * 1024 * 1024
                        or (content_type is not None and not isinstance(content_type, str))
                    ):
                        raise MatchError("invalid modal file metadata")
                    if field.file_types and not any(
                        (
                            allowed in {"image", "video", "audio"}
                            and isinstance(content_type, str)
                            and content_type.startswith(f"{allowed}/")
                        )
                        or (
                            allowed.startswith(".")
                            and file_name.casefold().endswith(allowed.casefold())
                        )
                        for allowed in field.file_types
                    ):
                        raise MatchError("modal file type is not allowed")
                    validated.append(
                        {
                            key: item[key]
                            for key in (
                                "id",
                                "filename",
                                "url",
                                "proxy_url",
                                "size",
                                "content_type",
                                "width",
                                "height",
                            )
                            if key in item
                        }
                    )
                files[field.key] = tuple(validated)
                continue
            selected = raw_values.get(field.key, ())
            if selected is None:
                selected = ()
            if not isinstance(selected, (list, tuple)):
                raise MatchError("invalid modal selection")
            if not selected and not field.required:
                continue
            if not field.min_values <= len(selected) <= field.max_values:
                raise MatchError("modal selection count is outside its limits")
            strings = tuple(str(value) for value in selected)
            if field.kind == "string_select":
                allowed_values = {choice.value for choice in field.choices}
                if any(value not in allowed_values for value in strings):
                    raise MatchError("modal selection is not allowed")
                values[field.key] = strings
                continue
            if any(not re.fullmatch(r"\d{17,20}", value) for value in strings):
                raise MatchError("invalid modal snowflake selection")
            if field.kind == "user_select" and any(
                value not in resolved_user_ids for value in strings
            ):
                raise MatchError("unresolved modal user selection")
            if field.kind == "role_select" and any(
                value not in resolved_role_ids for value in strings
            ):
                raise MatchError("unresolved modal role selection")
            if (
                field.kind == "channel_select"
                and field.channel_types
                and any(
                    int(channel_types.get(value, -1)) not in field.channel_types
                    for value in strings
                )
            ):
                raise MatchError("modal channel has an invalid type")
            values[field.key] = tuple(int(value) for value in strings)
        return values, files

    @staticmethod
    def _modal_emoji(value: str, event: dict[str, Any]) -> str:
        value = value.strip()
        if emoji_lib.is_emoji(value):
            return value

        raw_catalog = event.get("component_guild_emojis", ())
        if not isinstance(raw_catalog, (list, tuple)):
            raise MatchError("invalid guild emoji catalogue")
        by_id: dict[int, tuple[str, bool]] = {}
        by_name: dict[str, list[tuple[int, str, bool]]] = {}
        for item in raw_catalog:
            if not isinstance(item, dict) or item.get("available") is not True:
                continue
            identifier = item.get("id")
            name = item.get("name")
            animated = item.get("animated")
            if (
                not isinstance(identifier, int)
                or identifier <= 0
                or not isinstance(name, str)
                or re.fullmatch(r"[A-Za-z0-9_]{2,32}", name) is None
                or not isinstance(animated, bool)
            ):
                continue
            by_id[identifier] = (name, animated)
            by_name.setdefault(name.casefold(), []).append((identifier, name, animated))

        custom = CUSTOM_EMOJI_INPUT.fullmatch(value)
        if custom is not None:
            identifier = int(custom.group("id"))
            resolved = by_id.get(identifier)
            if resolved is None:
                raise EmojiLookupMiss("custom emoji is not available in this server")
            return EdgeMatcher._custom_emoji(identifier, *resolved)

        named = CUSTOM_EMOJI_NAME.fullmatch(value)
        if named is not None:
            requested = named.group("name")
            matches = by_name.get(requested.casefold(), ())
            exact = tuple(item for item in matches if item[1] == requested)
            if len(exact) == 1:
                identifier, name, animated = exact[0]
                return EdgeMatcher._custom_emoji(identifier, name, animated)
            if len(matches) == 1:
                identifier, name, animated = matches[0]
                return EdgeMatcher._custom_emoji(identifier, name, animated)
            if len(matches) > 1:
                raise MatchError("custom emoji name is ambiguous")
            expanded = emoji_lib.emojize(value, language="alias")
            if expanded != value and emoji_lib.is_emoji(expanded):
                return expanded
            raise EmojiLookupMiss("custom emoji was not found in this server")

        raise MatchError("invalid emoji")

    @staticmethod
    def _custom_emoji(identifier: int, name: str, animated: bool) -> str:
        prefix = "a" if animated else ""
        return f"<{prefix}:{name}:{identifier}>"

    def _captures(
        self,
        values: dict[str, str | None],
        specifications: tuple[CaptureSpec, ...],
    ) -> dict[str, Any]:
        return {
            capture.name: _convert(values.get(capture.name), capture) for capture in specifications
        }

    def _evaluate(
        self,
        expression: MatcherExpr,
        event: dict[str, Any],
        patterns: dict[str, Any] | Any,
    ) -> bool:
        if expression.op == "all":
            return all(self._evaluate(child, event, patterns) for child in expression.children)
        if expression.op == "any":
            return any(self._evaluate(child, event, patterns) for child in expression.children)
        if expression.op == "not":
            return not self._evaluate(expression.children[0], event, patterns)
        values = self._field_values(event, expression.field or "")
        if expression.op == "exists":
            return bool(values)
        target = expression.value or ""
        if expression.op == "exact":
            return any(value == target for value in values)
        if expression.op == "prefix":
            return any(value.startswith(target) for value in values)
        if expression.op == "contains":
            return any(target in value for value in values)
        if expression.op == "regex":
            pattern = patterns[target]
            return any(pattern.search(value) is not None for value in values)
        return False

    def _field_values(self, event: dict[str, Any], field: str) -> tuple[str, ...]:
        current: list[Any] = [event]
        for part in field.split("."):
            next_values: list[Any] = []
            for value in current:
                if isinstance(value, dict) and part in value:
                    candidate = value[part]
                    next_values.extend(candidate if isinstance(candidate, list) else [candidate])
                elif isinstance(value, list):
                    next_values.extend(value)
            current = next_values
        return tuple(str(value) for value in current if value is not None)

    @staticmethod
    def _components(value: object) -> tuple[dict[str, Any], ...]:
        if not isinstance(value, (list, tuple)):
            return ()
        found: list[dict[str, Any]] = []
        stack = list(reversed(value))
        while stack:
            item = stack.pop()
            if not isinstance(item, dict):
                continue
            if item.get("type") != 1:
                found.append(item)
            children = item.get("components")
            if isinstance(children, list):
                stack.extend(reversed(children))
            child = item.get("component")
            if isinstance(child, dict):
                stack.append(child)
        return tuple(found)

    def _context(
        self,
        event: dict[str, Any],
        context_fields: tuple[str, ...] = (),
    ) -> dict[str, Any]:
        allowed = {
            "kind",
            "actor_id",
            "author_id",
            "message_id",
            "guild_id",
            "channel_id",
            "offer_id",
            "offer_owner_id",
            "created_at_ms",
            "edited_at_ms",
            "reply",
            "member_role_ids",
            "emoji",
            "reaction_revision",
            "resource_id",
            "event_received_at_ms",
            "dispatch",
            "interaction_id",
            "interaction_created_at_ms",
            "administrator",
        }
        allowed.update(context_fields)
        context = {key: value for key, value in event.items() if key in allowed}
        if (
            "same_channel_reply_content" in context_fields
            and event.get("reply_author_bot") is False
            and event.get("reply_webhook_id") is None
            and event.get("guild_id")
            and event.get("reply_guild_id") == event["guild_id"]
            and event.get("channel_id")
            and event.get("reply_channel_id") == event["channel_id"]
        ):

            context["same_channel_reply_content"] = event.get("reply_content")
        return context
