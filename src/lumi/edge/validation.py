from __future__ import annotations

import re
from collections.abc import Iterable

import emoji as emoji_lib
import re2
from discord import Permissions

from lumi.contracts import (
    CaptureSpec,
    CommandSpec,
    HelpSpec,
    Manifest,
    MatcherExpr,
    PendingPresentationSpec,
)
from lumi.contracts.gateway import GUILD_BASELINE_EVENTS, GUILD_CAPTURE_EVENTS
from lumi.contracts.layout_message import WRITE_LAYOUT_MESSAGE
from lumi.contracts.channel_messages import CHANNEL_MESSAGE_ACTIONS, DELETE_CHANNEL_MESSAGES, READ_CHANNEL_MESSAGES
from lumi.contracts.channel_configuration import READ_CHANNEL_CONFIGURATION
from lumi.contracts.channel_resources import (
    CHANNEL_RESOURCE_ACTIONS, READ_CHANNEL_RESOURCES, READS as RESOURCE_READS,
    WRITE_CHANNEL_RESOURCE, WRITES as RESOURCE_WRITES,
)
from lumi.contracts.staff_recovery import RECOVERY_ACTIONS
from lumi.contracts.staff_moderation import STAFF_EFFECT
from lumi.contracts.member_effects import MEMBER_EFFECT_PERMISSIONS
from lumi.contracts.member_profiles import (
    LIST_GUILD_MEMBERS, MEMBER_PROFILE_ACTIONS, READ_MEMBER_PROFILE, SET_MEMBER_NICKNAME,
)
from lumi.contracts.native_mutations import NATIVE_MUTATION_KINDS
from lumi.contracts.role_access import ACTOR_ROLE_ACTIONS, READ_GUILD_ROLES, ROLE_FACTS, SET_ROLE_MEMBERSHIP
from lumi.shared.discord_effects import UPDATE_BOUND_PANEL
from lumi.transport import GROUPS, GUILD_CONTROL_GROUP

from .bound_panels import opener as bound_panel_openers
from .channel_messages import openers as channel_message_openers


IDENTIFIER = re.compile(r"^[a-z][a-z0-9_-]{0,63}$")
ROUTE_IDENTIFIER = re.compile(r"^[a-z][a-z0-9_-]*(?:\.[a-z][a-z0-9_-]*)+$")
ACL_IDENTIFIER = re.compile(r"^[a-z][a-z0-9_-]*(?:\.[a-z][a-z0-9_-]*)*$")
UNSAFE_REGEX = (
    re.compile(r"\\[1-9]"),
    re.compile(r"\(\?P="),
    re.compile(r"\(\?<=[^)]"),
    re.compile(r"\(\?<![^)]"),
    re.compile(r"\(\?>"),
    re.compile(r"\(\?\("),
)
CUSTOM_EMOJI = re.compile(r"^<a?:[A-Za-z0-9_]{2,32}:\d{17,20}>$")

MAX_PATTERN_LENGTH = 4096
MAX_CAPTURES = 32
MAX_COMMANDS = 256
MAX_PASSIVE = 512
MAX_REACTION_OFFERS = 128
MAX_COMPONENTS = 256
MAX_SCHEDULED = 128
ALLOWED_PERMISSIONS = frozenset(
    {
        "administrator",
        "ban_members",
        "manage_roles",
        "kick_members",
        "moderate_members",
        "manage_messages",
        "manage_channels",
        "manage_nicknames",
        "view_audit_log",
    }
)
PRESENTATION_MODES = frozenset({"v2", "legacy", "plain"})
ALLOWED_CAPABILITIES = frozenset(
    {
        "fetch_message",
        "create_message",
        "edit_message",
        "delete_message",
        "add_reaction",
        "remove_reaction",
        "add_member_role",
        "remove_member_role",
        *ACTOR_ROLE_ACTIONS,
        WRITE_LAYOUT_MESSAGE,
        "send_dm",
        "send_recipient_webhook",
        "send_message",
        "send_mention_batch",
        "read_mention_batch_members",
        "bind_message_reply",
        "channel_watch",
        "read_interaction_context",
        "read_guild_sources",
        "read_actor_post_channels",
        "read_owned_history",
        "read_channel_members",
        "read_embed_sources",
        "reply_text",
        "publish_channel_message",
        "mutate_owned_message",
        UPDATE_BOUND_PANEL,
        "send_role_message",
        "resolve_members",
        "search_members",
        "delete_source_messages",
        STAFF_EFFECT,
        *RECOVERY_ACTIONS,
        *MEMBER_EFFECT_PERMISSIONS,
        *MEMBER_PROFILE_ACTIONS,
        *CHANNEL_MESSAGE_ACTIONS,
        READ_CHANNEL_CONFIGURATION,
        *CHANNEL_RESOURCE_ACTIONS,
        *ROLE_FACTS,
    }
)
ALLOWED_CONTEXT_FIELDS = frozenset(
    {
        "author_avatar_url",
        "author_bot",
        "author_name",
        "author_role_ids",
        "channel_name",
        "content",
        "entity_labels",
        "guild_name",
        "embed_title",
        "embed_titles",
        "embed_descriptions",
        "embed_author_name",
        "embed_author_icon_url",
        "message_document",
        "source_text",
        "components",
        "channel_parent_id",
        "channel_nsfw",
        "interaction_user_id",
        "interaction_name",
        "requester_id",
        "source_revision_ms",
        "webhook_id",
        "image_source",
        "event_order",
        "message_snapshot",
        "captured_message",
        "hydrated_message",
        "reaction_observation",
        "bound_reply",
        "activity_exempt",
        "mentioned_role_ids",
        "mentioned_user_ids",
        "mentions_everyone",
        "mentions_here",
        "origin_channel_id",
        "prefix",
        "primary_image_url",
        "primary_embed_image_url",
        "reference_message_id",
        "reference_embed",
        "reply_content",
        "same_channel_reply_content",
        "recipient_mentions",
        "webhook_reply",
        "reply_author_id",
        "reply_author_bot",
        "reply_channel_id",
        "reply_created_at_ms",
        "reply_edited_at_ms",
        "reply_guild_id",
        "reply_reference_message_id",
        "reply_prompt_module",
        "reply_webhook_id",
        "used_prefix",
        "visible_channel_ids",
        "activity_boundary",
    }
)
REACTION_OFFER_FIELDS = frozenset(
    {
        "content",
        "message_document",
        "embed_titles",
        "embed_descriptions",
    }
)
REACTION_AUDIENCE_FIELDS = frozenset({"interaction_user_id", "reply_author_id", "requester_id"})
RESERVED_ROOTS = frozenset(
    {
        "help",
        "lumi",
        "ping",
        "block",
        "unblock",
        "restart",
        "drain",
        "maintenance",
        "modules",
    }
)


class ManifestError(ValueError):
    pass


def validate_pending(
    pending: PendingPresentationSpec | None, mode: str, modes: tuple[str, ...]
) -> None:
    if pending is None:
        return
    if mode != "v2" or (modes and modes != ("v2",)):
        raise ManifestError("pending presentation requires a single V2 family")
    if pending.animation != "spinner" or len(pending.description) > 120:
        raise ManifestError("invalid pending presentation")
    if any(ord(character) < 32 for character in pending.description):
        raise ManifestError("invalid pending description")


def validate_presentation_modes(
    default: str | None,
    declared: tuple[str, ...],
    *,
    inherited: str,
    label: str,
) -> None:
    effective = default or inherited
    if effective not in PRESENTATION_MODES:
        raise ManifestError(f"invalid {label} presentation mode")
    if len(declared) != len(set(declared)) or not set(declared).issubset(PRESENTATION_MODES):
        raise ManifestError(f"invalid {label} presentation modes")
    if declared and effective not in declared:
        raise ManifestError(f"{label} default presentation mode is not declared")


def validate_context_fields(
    fields: tuple[str, ...], *, webhook_replies: bool = False, bound_replies: bool = False,
    personal_reactions: bool = False, reference_embed: bool = False,
) -> None:
    if (
        len(fields) != len(set(fields)) or not set(fields).issubset(ALLOWED_CONTEXT_FIELDS)
        or ("webhook_reply" in fields and not webhook_replies)
        or ("bound_reply" in fields and not bound_replies)
        or ("reaction_observation" in fields and not personal_reactions)
        or ("reference_embed" in fields and not reference_embed)
    ):
        raise ManifestError("invalid context field declaration")


def compile_linear(pattern: str, *, anchored: bool) -> re2._Regexp:
    if len(pattern) > MAX_PATTERN_LENGTH:
        raise ManifestError("regex is too large")
    anchored_start = pattern.startswith("^") or re.match(r"^\(\?[ims]+\)\^", pattern)
    if anchored and not (anchored_start and pattern.endswith("$")):
        raise ManifestError("command grammar must be anchored")
    if any(rule.search(pattern) for rule in UNSAFE_REGEX):
        raise ManifestError("regex uses a construct outside the RE2 subset")
    try:
        compiled = re2.compile(pattern)
    except Exception as exc:
        raise ManifestError("invalid RE2 expression") from exc
    if compiled.groups > MAX_CAPTURES:
        raise ManifestError("regex has too many capture groups")
    return compiled


def validate_capture(capture: CaptureSpec) -> None:
    if not IDENTIFIER.fullmatch(capture.name):
        raise ManifestError(f"invalid capture name: {capture.name}")
    if capture.kind not in {
        "text",
        "normalized_text",
        "snowflake",
        "integer",
        "duration",
        "boolean",
        "compact_integer",
    }:
        raise ManifestError(f"unsupported capture type: {capture.kind}")
    if (
        capture.minimum is not None
        and capture.maximum is not None
        and capture.minimum > capture.maximum
    ):
        raise ManifestError("capture minimum exceeds maximum")


def walk_expression(expression: MatcherExpr) -> Iterable[MatcherExpr]:
    yield expression
    for child in expression.children:
        yield from walk_expression(child)


def validate_expression(expression: MatcherExpr) -> None:
    nodes = tuple(walk_expression(expression))
    if len(nodes) > 128:
        raise ManifestError("matcher expression is too large")
    for node in nodes:
        if node.op in {"all", "any"}:
            if not node.children or node.field is not None or node.value is not None:
                raise ManifestError(f"invalid {node.op} matcher")
        elif node.op == "not":
            if len(node.children) != 1 or node.field is not None or node.value is not None:
                raise ManifestError("invalid not matcher")
        elif node.op == "exists":
            if not node.field or node.value is not None or node.children:
                raise ManifestError("invalid exists matcher")
        elif node.op in {"exact", "prefix", "contains", "regex"}:
            if not node.field or node.value is None or node.children:
                raise ManifestError(f"invalid {node.op} matcher")
            if node.op == "regex":
                compile_linear(node.value, anchored=False)
        else:
            raise ManifestError(f"unsupported matcher operation: {node.op}")


def validate_command_help(manifest: Manifest, command: CommandSpec) -> None:
    help_spec = command.help

    if help_spec is None and command.owner_scope == "global":
        return
    if help_spec is None:
        raise ManifestError("commands must declare help syntax and description")
    validate_help_text(help_spec)

    for line in help_spec.usage.splitlines():
        if not line.strip() or line.split(maxsplit=1)[0].removeprefix(".") not in (
            command.root, *command.aliases
        ):
            raise ManifestError("help syntax must name its command")


def validate_help_text(help_spec: HelpSpec) -> None:
    if not isinstance(help_spec, HelpSpec):
        raise ManifestError("invalid help metadata")
    if (
        not isinstance(help_spec.usage, str) or not help_spec.usage.strip()
        or len(help_spec.usage) > 1000
        or not isinstance(help_spec.summary, str) or not help_spec.summary.strip()
        or len(help_spec.summary) > 2000 or type(help_spec.hidden) is not bool
    ):
        raise ManifestError("invalid command help metadata")
    if any(ord(char) < 32 for char in help_spec.usage + help_spec.summary if char != "\n"):
        raise ManifestError("invalid command help text")


def validate_command_loading(command: CommandSpec) -> None:
    if type(command.loading) is not bool:
        raise ManifestError("command loading must be a boolean")
    reason = command.loading_skip_reason
    if reason is None:
        if not command.loading:
            raise ManifestError("disabled command loading requires a loading skip reason")
        return
    if (
        not isinstance(reason, str)
        or not 1 <= len(reason) <= 160
        or reason != reason.strip()
        or not reason.isprintable()
    ):
        raise ManifestError("loading skip reason must be 1-160 printable characters")
    if command.loading:
        raise ManifestError("loading skip reason requires loading=False")


def validate_manifest(manifest: Manifest) -> None:
    if manifest.guild_commands is not None:
        source = manifest.guild_commands
        if (
            not ROUTE_IDENTIFIER.fullmatch(source.handler)
            or not source.handler.startswith(manifest.module + ".")
            or not 100 <= source.deadline_ms <= 900_000
            or not any(
                c.acl_key == source.acl_key and c.public and c.guild_only
                and c.configure_in_guild and not c.owner_scope and not c.required_permissions
                for c in manifest.commands
            )
        ):
            raise ManifestError("invalid guild command source")
        validate_context_fields(source.context_fields)
        pattern = compile_linear(source.grammar, anchored=True)
        if set(pattern.groupindex) != {capture.name for capture in source.captures}:
            raise ManifestError("guild command captures do not match named groups")
        for capture in source.captures:
            validate_capture(capture)
    if manifest.help is not None:
        validate_help_text(manifest.help)
    if len(manifest.action_presentation_modes) != len(
        set(manifest.action_presentation_modes)
    ) or not set(manifest.action_presentation_modes).issubset(PRESENTATION_MODES):
        raise ManifestError("invalid action presentation families")
    if manifest.schema != 2:
        raise ManifestError("unsupported manifest schema")
    if not IDENTIFIER.fullmatch(manifest.module):
        raise ManifestError("invalid module name")
    if not IDENTIFIER.fullmatch(manifest.group):
        raise ManifestError("invalid group name")
    if not manifest.display_name or len(manifest.display_name) > 80:
        raise ManifestError("invalid module display name")
    if not manifest.summary or len(manifest.summary) > 200:
        raise ManifestError("invalid module summary")
    if type(manifest.accent_color) is not int or not 0 <= manifest.accent_color <= 0xFFFFFF:
        raise ManifestError("invalid module accent colour")
    if manifest.presentation_mode not in PRESENTATION_MODES:
        raise ManifestError("invalid presentation mode")
    if manifest.generation < 1:
        raise ManifestError("generation must be positive")
    if not manifest.job_stream.startswith("lumi:jobs:"):
        raise ManifestError("job stream is outside the Lumi namespace")
    if len(manifest.commands) > MAX_COMMANDS:
        raise ManifestError("too many command routes")
    if len(manifest.passive) > MAX_PASSIVE:
        raise ManifestError("too many passive routes")
    if len(manifest.reaction_offers) > MAX_REACTION_OFFERS:
        raise ManifestError("too many reaction offers")
    if len(manifest.components) > MAX_COMPONENTS:
        raise ManifestError("too many component routes")
    if len(manifest.scheduled) > MAX_SCHEDULED:
        raise ManifestError("too many scheduled callback routes")
    capture = manifest.gateway_capture
    if capture is not None and (
        manifest.group not in GROUPS or not capture.events
        or len(set(capture.events)) != len(capture.events)
        or not set(capture.events).issubset(GUILD_CAPTURE_EVENTS)
        or len(set(capture.baseline_events)) != len(capture.baseline_events)
        or not set(capture.baseline_events).issubset(GUILD_BASELINE_EVENTS)
        or len(set(capture.mutation_kinds)) != len(capture.mutation_kinds)
        or not set(capture.mutation_kinds).issubset(NATIVE_MUTATION_KINDS)
    ):
        raise ManifestError("invalid Gateway capture declaration")
    guard = manifest.guarded_replies
    if capture is not None and (manifest.commands or manifest.components) and guard is None:
        raise ManifestError("interactive Gateway capture routes require guarded replies")
    if guard is not None and (
        "view_channel" not in guard.private_bot_permissions
        or len(set(guard.private_bot_permissions)) != len(guard.private_bot_permissions)
        or not set(guard.private_bot_permissions).issubset(Permissions.VALID_FLAGS)
        or any(component.private_loading for component in manifest.components)
        or any(offer.prepared_only for offer in manifest.reaction_offers)
    ):
        raise ManifestError("invalid guarded reply permissions")
    transition = manifest.channel_transition
    if transition is not None:
        overrides = (
            transition.control_handlers + transition.drain_handlers + transition.suppress_handlers
        )
        if len(overrides) != len(set(overrides)):
            raise ManifestError("conflicting channel transition declarations")
        interactive = manifest.commands + manifest.components
        background = (
            tuple(spec.handler for spec in manifest.passive + manifest.scheduled)
            + tuple(spec.callback for spec in manifest.reaction_offers)
        )
        for handler in transition.control_handlers:
            routes = [spec for spec in interactive if spec.handler == handler]
            if (
                not routes or handler in background
                or (manifest.guild_commands is not None and manifest.guild_commands.handler == handler)
                or not all(
                    {"manage_messages", "manage_channels"}.intersection(spec.required_permissions)
                    and getattr(spec, "guild_only", True)
                    for spec in routes
                )
            ):
                raise ManifestError("channel transition controls require guild management permissions")
        registered = set(background).union(spec.handler for spec in interactive)
        if not set(overrides).issubset(registered):
            raise ManifestError("channel transition refers to an unregistered handler")
        if transition.rebuild_posts and not transition.quiesce_guild:
            raise ManifestError("channel transition rebuilds require guild quiescence")
    if len(manifest.capabilities) != len(set(manifest.capabilities)) or not set(
        manifest.capabilities
    ).issubset(ALLOWED_CAPABILITIES):
        raise ManifestError("invalid Discord action capability")
    if (READ_CHANNEL_CONFIGURATION in manifest.capabilities) != any(
        route.channel_configuration is not None for route in (*manifest.commands, *manifest.components)
    ):
        raise ManifestError("native channel configuration requires a declared route")
    if ("send_recipient_webhook" in manifest.capabilities) != any(
        spec.recipient_webhook is not None for spec in manifest.passive
    ):
        raise ManifestError("recipient webhook capability requires its own passive route")
    if set(manifest.capabilities).intersection(MEMBER_EFFECT_PERMISSIONS) != {
        command.member_effect.kind for command in manifest.commands if command.member_effect
    }:
        raise ManifestError("member effects require a matching guild command declaration")
    role_routes = [route.role_access for route in manifest.commands + manifest.components if route.role_access]
    role_capabilities = set()
    if role_routes:
        role_capabilities.add(READ_GUILD_ROLES)
        if any(scope.allow_write for scope in role_routes):
            role_capabilities.add(SET_ROLE_MEMBERSHIP)
    if role_capabilities != set(manifest.capabilities).intersection(ROLE_FACTS):
        raise ManifestError("role access requires a matching guild route declaration")
    role_capabilities = ACTOR_ROLE_ACTIONS if any(c.actor_role_access for c in manifest.components) else set()
    if role_capabilities != set(manifest.capabilities).intersection(ACTOR_ROLE_ACTIONS):
        raise ManifestError("actor role access requires a matching component declaration")

    roots: set[str] = set()
    acl_keys: set[str] = set()
    inherited_acls: set[str] = set()
    command_scopes: dict[str, str] = {}
    handlers: set[str] = set()
    public_guild_acls = {
        command.acl_key for command in manifest.commands
        if command.public and command.configure_in_guild and command.guild_only
        and not command.owner_scope and not command.required_permissions
    }

    def check_guild_sources(spec, route_allowed):
        if spec is not None and (
            not route_allowed or "read_guild_sources" not in manifest.capabilities
            or spec.acl_key not in public_guild_acls or type(spec.include_activity) is not bool
        ):
            raise ManifestError("guild sources require an owning public guild command ACL")

    def check_channel_members(spec, enabled):
        if spec is not None and (
            not enabled or "read_channel_members" not in manifest.capabilities
            or not spec.channel_types or len(set(spec.channel_types)) != len(spec.channel_types)
            or not set(spec.channel_types) <= {0, 5, 10, 11, 12}
            or "view_channel" not in spec.bot_permissions
            or len(set(spec.bot_permissions)) != len(spec.bot_permissions)
            or not set(spec.bot_permissions) <= {
                "view_channel", "read_message_history", "send_messages",
                "send_messages_in_threads", "attach_files", "embed_links",
            }
        ):
            raise ManifestError("invalid channel member read scope")

    def check_private_channels(spec, enabled):
        if spec is not None and (
            not enabled or "read_interaction_context" not in manifest.capabilities
            or "view_channel" not in spec.bot_permissions
            or any(
                len(permissions) != len(set(permissions))
                or not set(permissions) <= Permissions.VALID_FLAGS.keys()
                for permissions in (spec.bot_permissions, spec.bot_guild_permissions)
            )
        ):
            raise ManifestError("invalid private channel fact scope")

    def check_embed_sources(spec, enabled):
        if spec is not None and (
            not enabled or "read_embed_sources" not in manifest.capabilities
            or spec.acl_key not in public_guild_acls or not 1 <= len(spec.author_ids) <= 10
            or len(set(spec.author_ids)) != len(spec.author_ids)
            or any(type(value) is not int or not 0 < value < 2**64 for value in spec.author_ids)
        ):
            raise ManifestError("invalid embed source read scope")

    def check_channel_effect(spec, enabled, acl, capability, *, sources=None, scheduled=False):
        if spec is not None and (
            not enabled or capability not in manifest.capabilities
            or not spec.channel_types or len(set(spec.channel_types)) != len(spec.channel_types)
            or not set(spec.channel_types) <= {0, 5, 10, 11, 12}
            or (spec.cross_channel_acl and (
                spec.cross_channel_acl != acl or acl not in public_guild_acls
            ))
        ):
            raise ManifestError(
                "invalid text reply scope" if capability == "reply_text"
                else "invalid channel publication scope" if capability == "publish_channel_message"
                else "invalid message mutation scope"
            )
        if spec is not None and capability == "publish_channel_message" and (
            type(spec.require_source_proof) is not bool or type(spec.require_history) is not bool
            or spec.require_source_proof and sources is None
        ):
            raise ManifestError("invalid channel publication source scope")
        if spec is not None and capability == "mutate_owned_message" and (
            any(type(flag) is not bool for flag in (
                spec.allow_delete, spec.allow_replacement, spec.require_replacement_proof,
            ))
            or spec.require_replacement_proof and (sources is None or not spec.allow_replacement)
        ):
            raise ManifestError("invalid message mutation source scope")
        delivery = getattr(spec, "delivery", None)
        if delivery is not None and (
            not scheduled or spec.cross_channel_acl
            or (delivery.private and spec.channel_types != (0,))
            or len(delivery.bot_permissions) != len(set(delivery.bot_permissions))
            or not set(delivery.bot_permissions).issubset(Permissions.VALID_FLAGS)
            or (delivery.recorded_target and (
                capability != "mutate_owned_message" or not delivery.private or spec.allow_delete
            ))
        ):
            raise ManifestError("invalid scheduled delivery requirements")

    layout_controls = tuple(component for component in manifest.components if component.layout_write)
    if (WRITE_LAYOUT_MESSAGE in manifest.capabilities) != bool(layout_controls) or any(
        component.public
        or not component.acl_key
        or not component.session_bound
        or component.owner_scope
        or component.guild_control
        or not {"administrator", "manage_channels", "manage_messages", "manage_roles"}.intersection(
            component.required_permissions
        )
        for component in layout_controls
    ):
        raise ManifestError("persistent layout writes require a bound management control")

    for command in manifest.commands:
        check_channel_members(command.channel_members, command.guild_only and not command.owner_scope)
        check_private_channels(
            command.private_channel_facts,
            command.guild_only and command.public and bool(command.acl_key) and not command.owner_scope,
        )
        check_channel_effect(
            command.text_reply, command.guild_only and not command.owner_scope,
            command.acl_key if command.public else "", "reply_text",
        )
        check_channel_effect(
            command.channel_publication, command.guild_only and not command.owner_scope,
            command.acl_key if command.public else "", "publish_channel_message",
            sources=command.embed_sources,
        )
        check_channel_effect(
            command.message_mutation, command.guild_only and not command.owner_scope,
            command.acl_key if command.public else "", "mutate_owned_message",
            sources=command.embed_sources,
        )
        check_embed_sources(
            command.embed_sources,
            command.embed_sources is not None and command.embed_sources.acl_key == command.acl_key
            and command.guild_only and command.public and not command.owner_scope,
        )
        check_guild_sources(
            command.guild_sources,
            command.guild_sources is not None and command.guild_sources.acl_key == command.acl_key
            and command.public and command.guild_only and command.configure_in_guild
            and not command.owner_scope and not command.required_permissions,
        )
        if (
            command.owner_scope not in {"", "server", "global"}
            or type(command.use_default_prefix) is not bool
            or type(command.configure_in_guild) is not bool
            or (command.owner_scope == "global" and (command.public or command.configure_in_guild))
            or (command.owner_scope == "server" and not command.guild_only)
            or (command.use_default_prefix and command.owner_scope != "global")
            or (not command.configure_in_guild and not (command.owner_scope or command.guild_control))
            or command_scopes.setdefault(command.handler, command.owner_scope) != command.owner_scope
        ):
            raise ManifestError("invalid owner command declaration")
        if type(command.guild_control) is not bool or command.guild_control and (
            manifest.group != GUILD_CONTROL_GROUP or not command.guild_only
            or command.public or command.configure_in_guild or command.owner_scope or command.aliases
            or command.root != manifest.module or command.acl_key != command.root
        ):
            raise ManifestError("invalid guild control declaration")
        validate_command_help(manifest, command)
        member_effect = command.member_effect
        if member_effect is not None:
            captures = {capture.name: capture for capture in command.captures}
            permission = MEMBER_EFFECT_PERMISSIONS.get(member_effect.kind)
            targets = member_effect.target_fields
            reason = captures.get(member_effect.reason_field)
            if (
                not command.guild_only or not command.public or not command.acl_key
                or permission is None or permission not in command.required_permissions
                or not targets or len(targets) != len(set(targets))
                or any(name not in captures or captures[name].kind != "snowflake" for name in targets)
                or (member_effect.reason_field is not None and (
                    reason is None or reason.kind != "text" or reason.maximum is None
                    or not 0 < reason.maximum <= 400
                ))
            ):
                raise ManifestError("member effect requires guild permissions and typed captures")
        if command.member_target:
            snowflakes = {capture.name for capture in command.captures if capture.kind == "snowflake"}
            if (
                not command.guild_only or not command.required_permissions
                or len(command.member_target) != len(set(command.member_target))
                or not set(command.member_target).issubset(snowflakes)
            ):
                raise ManifestError("member target requires guild permissions and snowflake captures")
        if command.role_access:
            targets = command.role_access.target_fields
            snowflakes = {capture.name for capture in command.captures if capture.kind == "snowflake"}
            if (
                not command.guild_only or not command.public or not command.acl_key
                or "manage_roles" not in command.required_permissions or not targets
                or len(targets) != len(set(targets)) or not set(targets).issubset(snowflakes)
            ):
                raise ManifestError("command role access requires guild permissions and a target")
        validate_command_loading(command)
        validate_pending(
            command.pending,
            command.presentation_mode or manifest.presentation_mode,
            command.presentation_modes,
        )
        if not IDENTIFIER.fullmatch(command.root):
            raise ManifestError("invalid command root")
        if not ROUTE_IDENTIFIER.fullmatch(command.handler):
            raise ManifestError("invalid command handler")
        if command.deadline_ms < 100 or command.deadline_ms > 900_000:
            raise ManifestError("invalid command deadline")
        validate_presentation_modes(
            command.presentation_mode,
            command.presentation_modes,
            inherited=manifest.presentation_mode,
            label="command",
        )
        if not set(command.required_permissions).issubset(ALLOWED_PERMISSIONS):
            raise ManifestError("invalid command permission requirement")
        if command.channel_configuration is not None and (
            not command.guild_only or not command.public or "manage_messages" not in command.required_permissions
        ):
            raise ManifestError("native channel configuration requires a guild moderation command")
        if command.bot_channel_facts and (
            not command.guild_only or not command.public
            or len(command.bot_channel_facts) != len(set(command.bot_channel_facts))
            or not set(command.bot_channel_facts).issubset(Permissions.VALID_FLAGS)
        ):
            raise ManifestError("invalid command bot channel facts")
        if command.bot_guild_permissions and (
            not command.guild_only
            or len(command.bot_guild_permissions) != len(set(command.bot_guild_permissions))
            or not set(command.bot_guild_permissions).issubset(Permissions.VALID_FLAGS)
        ):
            raise ManifestError("invalid command bot guild permission requirement")
        validate_context_fields(
            command.context_fields,
            reference_embed=command.guild_only and "reference_message_id" in command.context_fields,
        )
        compile_linear(command.grammar, anchored=True)
        for capture in command.captures:
            validate_capture(capture)
        for root in (command.root, *command.aliases):
            normalized = root.casefold()
            if not IDENTIFIER.fullmatch(normalized) or normalized in roots:
                raise ManifestError("invalid or duplicate command root")
            if normalized in RESERVED_ROOTS:
                raise ManifestError("command root is reserved by the edge")
            roots.add(normalized)
        if len(command.acl_key) > 128 or not ACL_IDENTIFIER.fullmatch(command.acl_key):
            raise ManifestError("invalid command ACL key")
        if command.configure_in_guild:
            if command.acl_key in acl_keys:
                raise ManifestError("duplicate command ACL key")
            acl_keys.add(command.acl_key)
        elif command.owner_scope == "server":
            inherited_acls.add(command.acl_key)
        handlers.add(command.handler)
    if not inherited_acls.issubset(acl_keys):
        raise ManifestError("owner command refers to an undeclared guild ACL")

    routes: set[str] = set()
    passive_by_route = {spec.route: spec for spec in manifest.passive}
    prepared_sources: dict[str, set[str]] = {}
    prepared_keys = {offer.key for offer in manifest.reaction_offers if offer.prepared_only}
    for passive in manifest.passive:
        if passive.recipient_webhook is not None and (
            not re.fullmatch(r"Lumi [A-Za-z][A-Za-z0-9 _-]{0,39}", passive.recipient_webhook.name)
            or passive.event != "message_create"
            or passive.bot_ids or passive.channel_scoped or passive.subject_field or passive.subject_capture
            or not passive.route.startswith(manifest.module + ".")
            or not passive.handler.startswith(manifest.module + ".")
            or not {"recipient_mentions", "mentioned_user_ids", "webhook_reply"}.issubset(passive.context_fields)
        ):
            raise ManifestError("recipient webhooks require a human message route and recipient context")
        if "webhook_reply" in passive.context_fields and passive.recipient_webhook is None:
            raise ManifestError("webhook reply context requires a managed webhook route")
        if type(passive.own_output_deleted) is not bool or (passive.own_output_deleted and (
            passive.event != "message_delete" or passive.bot_ids or passive.context_fields
            or passive.subject_capture or passive.subject_field or passive.extractions
            or passive.repeated or passive.component_extractions or passive.hydrate
            or passive.channel_scoped or passive.source_cleanup or passive.watch_ttl_ms
            or not passive.route.startswith(manifest.module + ".")
            or not passive.handler.startswith(manifest.module + ".")
        )):
            raise ManifestError("invalid owned-output deletion observer")
        check_guild_sources(
            passive.guild_sources,
            passive.event == "message_create" and not passive.bot_ids and not passive.channel_scoped
            and not passive.subject_capture and not passive.subject_field and not passive.prepare_reply
            and passive.route.startswith(manifest.module + ".")
            and passive.handler.startswith(manifest.module + "."),
        )
        if passive.prepare_reply is not None:
            events = prepared_sources.setdefault(passive.prepare_reply, set())
            if (
                passive.prepare_reply not in prepared_keys
                or passive.event not in {"message_create", "message_edit"}
                or passive.event in events
                or not passive.route.startswith(manifest.module + ".")
                or not passive.handler.startswith(manifest.module + ".")
                or not {"content", "source_revision_ms"}.issubset(passive.context_fields)
                or passive.bot_ids or passive.subject_field or passive.subject_capture
                or passive.channel_scoped or passive.personal_reaction or passive.bound_human_reply
                or passive.reply_prompt or passive.watch_ttl_ms or passive.hydrate
            ):
                raise ManifestError("invalid prepared reply source")
            events.add(passive.event)
        fallback_targets = passive.unless_routes_match
        if len(fallback_targets) > MAX_PASSIVE or len(set(fallback_targets)) != len(fallback_targets):
            raise ManifestError("invalid passive fallback targets")
        for target_name in fallback_targets:
            target = passive_by_route.get(target_name)
            if (
                target is None or target is passive or target.event != passive.event
                or target.unless_routes_match or target.channel_scoped
            ):
                raise ManifestError("invalid passive fallback target")
        channel_command = next(
            (command for command in manifest.commands if command.acl_key == passive.acl_key),
            None,
        )
        if passive.acl_key and not (
            passive.channel_scoped or passive.personal_reaction or passive.bound_human_reply
        ):
            raise ManifestError("passive ACL binding requires a channel-scoped route")
        if ("reaction_observation" in passive.context_fields) != passive.personal_reaction:
            raise ManifestError("reaction observations require a personal reaction route")
        if passive.personal_reaction and not (
            manifest.group in GROUPS
            and passive.event == "reaction_add"
            and passive.route.startswith(manifest.module + ".")
            and passive.handler.startswith(manifest.module + ".")
            and passive.subject_field == "actor_id"
            and passive.subject_required and passive.acl_key
            and channel_command is not None and channel_command.public
            and channel_command.guild_only and channel_command.configure_in_guild
            and not channel_command.owner_scope and not channel_command.required_permissions
            and not passive.bot_ids and not passive.subject_capture
            and not passive.channel_scoped and not passive.bound_human_reply
            and not passive.reply_prompt and not passive.hydrate and not passive.recipient_webhook
        ):
            raise ManifestError("invalid personal reaction route")
        if ("bound_reply" in passive.context_fields) != passive.bound_human_reply or (
            passive.bound_reply_activity_exempt and not passive.bound_human_reply
        ):
            raise ManifestError("bound reply context requires a bound human reply route")
        if passive.bound_human_reply and not (
            passive.event == "message_create"
            and passive.route.startswith(manifest.module + ".")
            and passive.handler.startswith(manifest.module + ".")
            and "bind_message_reply" in manifest.capabilities
            and passive.subject_field == "actor_id"
            and passive.subject_required and passive.acl_key
            and channel_command is not None and channel_command.public
            and not channel_command.required_permissions
            and not passive.bot_ids and not passive.channel_scoped and not passive.reply_prompt
            and not passive.hydrate and not passive.recipient_webhook
        ):
            raise ManifestError("invalid bound human reply")
        if passive.channel_scoped and (
            passive.event != "message_create"
            or passive.bot_ids
            or passive.hydrate
            or passive.reply_prompt
            or passive.watch_ttl_ms
            or not passive.route.startswith(manifest.module + ".")
            or not passive.handler.startswith(manifest.module + ".")
            or "channel_watch" not in manifest.capabilities
            or channel_command is None
            or not channel_command.public
            or channel_command.required_permissions
        ):
            raise ManifestError("invalid channel-scoped route")
        if not ROUTE_IDENTIFIER.fullmatch(passive.route):
            raise ManifestError("invalid passive route")
        if passive.route in routes:
            raise ManifestError("duplicate passive route")
        routes.add(passive.route)
        if passive.event not in {
            "message_create",
            "message_edit",
            "message_delete",
            "reaction_add",
            "reaction_remove",
            "channel_delete",
            "role_delete",
            "emoji_delete",
        }:
            raise ManifestError("unsupported passive event")
        validate_presentation_modes(
            passive.presentation_mode,
            passive.presentation_modes,
            inherited=manifest.presentation_mode,
            label="passive",
        )
        validate_context_fields(
            passive.context_fields, webhook_replies=passive.recipient_webhook is not None,
            bound_replies=passive.bound_human_reply,
            personal_reactions=passive.personal_reaction,
        )
        validate_expression(passive.expression)
        capture_names: set[str] = set()
        for extraction in passive.extractions:
            compiled = compile_linear(extraction.pattern, anchored=True)
            declared = {capture.name for capture in extraction.captures}
            if (
                not extraction.field
                or not declared
                or len(declared) != len(extraction.captures)
                or declared != set(compiled.groupindex)
                or capture_names & declared
            ):
                raise ManifestError("invalid passive field extraction")
            capture_names.update(declared)
            for capture in extraction.captures:
                validate_capture(capture)
        for repeated in passive.repeated:
            if repeated.maximum < 1 or repeated.maximum > 100:
                raise ManifestError("invalid repeated extraction bound")
            compile_linear(repeated.pattern, anchored=False)
            for capture in repeated.captures:
                validate_capture(capture)
        component_names: set[str] = set()
        for extraction in passive.component_extractions:
            if (
                not IDENTIFIER.fullmatch(extraction.name)
                or extraction.name in component_names
                or not 1 <= extraction.maximum <= 25
                or not extraction.captures
            ):
                raise ManifestError("invalid component extraction")
            component_names.add(extraction.name)
            validate_expression(extraction.expression)
            capture_keys: set[str] = set()
            for capture in extraction.captures:
                capture_spec = CaptureSpec(
                    name=capture.name,
                    kind=capture.kind,
                    required=capture.required,
                    minimum=capture.minimum,
                    maximum=capture.maximum,
                )
                validate_capture(capture_spec)
                if capture.name in capture_keys or not capture.field:
                    raise ManifestError("invalid component capture")
                capture_keys.add(capture.name)
                if capture.pattern is not None:
                    compiled = compile_linear(capture.pattern, anchored=True)
                    if set(compiled.groupindex) != {capture.name}:
                        raise ManifestError("component capture pattern must name its capture")
        if passive.subject_capture is not None and passive.subject_capture not in capture_names:
            raise ManifestError("passive subject capture is not declared")
        if passive.subject_field is not None and not passive.subject_field:
            raise ManifestError("invalid passive subject field")
        if passive.subject_capture is not None and passive.subject_field is not None:
            raise ManifestError("passive subject source is ambiguous")
        if passive.subject_required and not (
            passive.subject_capture is not None or passive.subject_field is not None
        ):
            raise ManifestError("required passive subject has no source")
        if passive.hydrate and passive.event != "message_edit":
            raise ManifestError("only message edits may request hydration")
        if type(passive.hydrate_matching_edit) is not bool or (
            passive.hydrate_matching_edit and (not passive.hydrate or passive.channel_scoped)
        ):
            raise ManifestError("matching edit hydration requires an ordinary hydrated edit route")
        if passive.hydrate_on and (
            not passive.hydrate or len(set(passive.hydrate_on)) != len(passive.hydrate_on)
            or not set(passive.hydrate_on).issubset({"content", "embeds", "attachments", "components"})
        ):
            raise ManifestError("invalid native edit hydration fields")
        if passive.watch_ttl_ms and not 1_000 <= passive.watch_ttl_ms <= 15_300_000:
            raise ManifestError("invalid passive edit watch expiry")
        if passive.reference_author_max_age_ms and not (
            passive.subject_field == "reply_author_id"
            and 1_000 <= passive.reference_author_max_age_ms <= 86_400_000
        ):
            raise ManifestError("invalid referenced-message author resolution")
        if passive.reply_prompt and not (
            passive.bot_ids
            and passive.subject_field == "reply_author_id"
            and passive.subject_required
            and passive.reference_author_max_age_ms
        ):
            raise ManifestError("reply prompts require an exact bot and bounded human reply")
        if passive.source_cleanup and not (
            passive.reply_prompt and "delete_source_messages" in manifest.capabilities
        ):
            raise ManifestError("source cleanup requires a module-owned reply prompt")
        handlers.add(passive.handler)

    moderation_routes = [p for p in manifest.passive if p.staff_moderation is not None]
    if (STAFF_EFFECT in manifest.capabilities) != bool(moderation_routes):
        raise ManifestError("staff effects require a declared passive source")
    for route in moderation_routes:
        scope = route.staff_moderation
        if (
            route.event not in {"message_create", "message_edit"}
            or route.bot_ids or route.subject_field or route.subject_capture
            or "source_revision_ms" not in route.context_fields
            or not any(command.guild_only and "administrator" in command.required_permissions
                       for command in manifest.commands)
            or not scope.viewer_permissions or "administrator" not in scope.protected_role_permissions
            or not {"view_channel", "read_message_history", "send_messages", "embed_links", "attach_files"}.issubset(
                scope.log_permissions
            )
            or any(len(set(flags)) != len(flags) or not set(flags).issubset(Permissions.VALID_FLAGS)
                   for flags in (scope.log_permissions, scope.viewer_permissions, scope.protected_role_permissions))
            or len(scope.operations) != len(set(scope.operations))
            or not set(scope.operations).issubset({"inspect", "observe_source", "send", "edit", "delete",
                                                  "add_role", "remove_role", "kick", "ban"})
        ):
            raise ManifestError("invalid staff moderation scope")

    scheduled_routes: set[str] = set()
    if (UPDATE_BOUND_PANEL in manifest.capabilities) != any(callback.bound_panel for callback in manifest.scheduled):
        raise ManifestError("public panel updates require a declared actor callback")
    recovery = any(callback.staff_recovery is not None for callback in manifest.scheduled)
    if bool(set(manifest.capabilities) & RECOVERY_ACTIONS) != recovery or (
        recovery and not RECOVERY_ACTIONS.issubset(manifest.capabilities)
    ):
        raise ManifestError("staff recovery actions require a declared callback")
    message_declarations = tuple(callback.channel_messages for callback in manifest.scheduled if callback.channel_messages is not None)
    if (
        (READ_CHANNEL_MESSAGES in manifest.capabilities) != bool(message_declarations)
        or (DELETE_CHANNEL_MESSAGES in manifest.capabilities) != any(access.allow_delete for access in message_declarations)
    ):
        raise ManifestError("channel message actions require explicit actor callbacks")
    resource_operations = {
        operation for callback in manifest.scheduled if callback.channel_resources is not None
        for operation in callback.channel_resources.operations
    }
    if (
        (READ_CHANNEL_RESOURCES in manifest.capabilities) != bool(resource_operations & RESOURCE_READS)
        or (WRITE_CHANNEL_RESOURCE in manifest.capabilities) != bool(resource_operations & RESOURCE_WRITES)
    ):
        raise ManifestError("channel resources require a declared actor callback")
    for scheduled in manifest.scheduled:
        resources = scheduled.channel_resources
        if resources is not None and (
            scheduled.scope != "actor" or not resources.operations
            or len(set(resources.operations)) != len(resources.operations)
            or not set(resources.operations) <= (RESOURCE_READS | RESOURCE_WRITES)
            or not any(
                command.acl_key == scheduled.acl_key and command.public and command.guild_only
                and command.configure_in_guild and command.channel_configuration is not None
                for command in manifest.commands
            )
        ):
            raise ManifestError("channel resources require a matching guild management command")
        recovery_spec = scheduled.staff_recovery
        if recovery_spec is not None and (
            scheduled.scope != "guild"
            or not recovery_spec.viewer_permissions
            or not {"view_channel", "read_message_history", "send_messages", "embed_links"}.issubset(
                recovery_spec.bot_permissions
            )
            or any(
                len(flags) != len(set(flags)) or not set(flags).issubset(Permissions.VALID_FLAGS)
                for flags in (recovery_spec.viewer_permissions, recovery_spec.bot_permissions)
            )
            or not any(command.guild_only and "administrator" in command.required_permissions
                       for command in manifest.commands)
        ):
            raise ManifestError("invalid staff recovery scope")
        if scheduled.channel_messages is not None and (
            scheduled.scope != "actor" or not channel_message_openers(manifest, scheduled.acl_key)
        ):
            raise ManifestError("channel message actions require a matching guild moderation command")
        if scheduled.bound_panel and (scheduled.scope != "actor" or not bound_panel_openers(manifest, scheduled.acl_key)):
            raise ManifestError("invalid actor-scoped panel updates")
        profiles = scheduled.member_profiles
        if profiles is not None and (
            scheduled.scope != "actor"
            or manifest.group not in GROUPS
            or not profiles.actor_permissions
            or not profiles.bot_permissions
            or any(
                len(set(flags)) != len(flags) or not set(flags).issubset(Permissions.VALID_FLAGS)
                for flags in (profiles.actor_permissions, profiles.bot_permissions)
            )
            or not any(
                command.acl_key == scheduled.acl_key and command.public and command.guild_only
                and command.configure_in_guild
                and command.owner_scope == ("server" if profiles.owner_only else "")
                and set(command.required_permissions).issubset(profiles.actor_permissions)
                and set(command.bot_guild_permissions).issubset(profiles.bot_permissions)
                for command in manifest.commands
            )
            or READ_MEMBER_PROFILE not in manifest.capabilities
            or (profiles.allow_directory and LIST_GUILD_MEMBERS not in manifest.capabilities)
            or (profiles.allow_nickname_write and (
                SET_MEMBER_NICKNAME not in manifest.capabilities
                or "manage_nicknames" not in profiles.actor_permissions
                or "manage_nicknames" not in profiles.bot_permissions
            ))
        ):
            raise ManifestError("invalid actor-scoped member profile operations")
        check_channel_members(scheduled.channel_members, scheduled.scope == "guild")
        check_embed_sources(scheduled.embed_sources, scheduled.scope == "guild")
        check_channel_effect(scheduled.text_reply, scheduled.scope == "guild", "", "reply_text")
        check_channel_effect(
            scheduled.channel_publication, scheduled.scope == "guild", "", "publish_channel_message",
            sources=scheduled.embed_sources, scheduled=True,
        )
        check_channel_effect(
            scheduled.message_mutation, scheduled.scope == "guild", "", "mutate_owned_message",
            sources=scheduled.embed_sources, scheduled=True,
        )
        if scheduled.own_history and (
            scheduled.scope != "guild" or "read_owned_history" not in manifest.capabilities
        ):
            raise ManifestError("owned history requires a declared guild callback")
        history_permissions = scheduled.own_history_bot_permissions
        if (
            ((scheduled.own_history_private or history_permissions) and not scheduled.own_history)
            or len(history_permissions) != len(set(history_permissions))
            or not set(history_permissions).issubset(Permissions.VALID_FLAGS)
        ):
            raise ManifestError("invalid scheduled history requirements")
        if (
            not ROUTE_IDENTIFIER.fullmatch(scheduled.route)
            or scheduled.route in scheduled_routes
            or not ROUTE_IDENTIFIER.fullmatch(scheduled.handler)
            or not scheduled.route.startswith(f"{manifest.module}.")
            or not scheduled.handler.startswith(f"{manifest.module}.")
            or not 100 <= scheduled.deadline_ms <= 900_000
            or scheduled.scope not in {"actor", "guild"}
            or (scheduled.acl_key and not ACL_IDENTIFIER.fullmatch(scheduled.acl_key))
        ):
            raise ManifestError("invalid scheduled callback")
        if scheduled.mention_templates:
            keys = {template.key for template in scheduled.mention_templates}
            if (
                not {"send_mention_batch", "read_mention_batch_members"}.issubset(manifest.capabilities)
                or scheduled.scope != "guild" or scheduled.acl_key not in public_guild_acls
                or (scheduled.presentation_mode or manifest.presentation_mode) != "plain"
                or len(keys) != len(scheduled.mention_templates) or len(keys) > MAX_SCHEDULED
                or any(
                    not IDENTIFIER.fullmatch(item.key) or not 1 <= len(item.caption) <= 2000
                    or item.caption.strip() != item.caption or not item.caption.isprintable()
                    or any(char in item.caption for char in "<>@")
                    for item in scheduled.mention_templates
                )
            ):
                raise ManifestError("invalid scheduled mention templates")
        validate_presentation_modes(
            scheduled.presentation_mode,
            scheduled.presentation_modes,
            inherited=manifest.presentation_mode,
            label="scheduled callback",
        )
        scheduled_routes.add(scheduled.route)
        handlers.add(scheduled.handler)

    if type(manifest.activity_observations) is not bool or (
        manifest.activity_observations and manifest.group not in GROUPS
    ):
        raise ManifestError("invalid activity observation declaration")

    offer_keys: set[str] = set()
    for offer in manifest.reaction_offers:
        if not IDENTIFIER.fullmatch(offer.key) or offer.key in offer_keys:
            raise ManifestError("invalid or duplicate reaction offer key")
        offer_keys.add(offer.key)
        if not ROUTE_IDENTIFIER.fullmatch(offer.callback):
            raise ManifestError("invalid reaction offer callback")
        if not offer.callback.startswith(f"{manifest.module}."):
            raise ManifestError("reaction offer callback crosses module boundaries")
        if (
            not offer.events
            or len(offer.events) != len(set(offer.events))
            or not set(offer.events).issubset({"message_create", "message_edit"})
        ):
            raise ManifestError("invalid reaction offer events")
        if offer.field not in REACTION_OFFER_FIELDS:
            raise ManifestError("invalid reaction offer field")
        if type(offer.prepared_only) is not bool:
            raise ManifestError("invalid prepared reaction offer")
        if offer.prepared_only and not (
            prepared_sources.get(offer.key)
            and not offer.bot_ids and offer.pattern == r"^$" and not offer.captures
            and not offer.audience_fields and offer.claim_if_missing and not offer.require_owner
            and offer.reaction_events == ("reaction_add",)
        ):
            raise ManifestError("invalid prepared reply offer")
        if (
            type(offer.preparation_cooldown_ms) is not int
            or not 0 <= offer.preparation_cooldown_ms <= 60_000
            or (offer.preparation_cooldown_ms and not offer.prepared_only)
        ):
            raise ManifestError("invalid preparation cooldown")
        if not offer.prepared_only and (
            not offer.bot_ids
            or len(offer.bot_ids) != len(set(offer.bot_ids))
            or len(offer.bot_ids) > 64
            or any(value <= 0 for value in offer.bot_ids)
        ):
            raise ManifestError("invalid reaction offer bot IDs")
        if not (emoji_lib.is_emoji(offer.emoji) or CUSTOM_EMOJI.fullmatch(offer.emoji)):
            raise ManifestError("invalid reaction offer emoji")
        if not 1_000 <= offer.ttl_ms <= 86_400_000:
            raise ManifestError("invalid reaction offer expiry")
        if not 100 <= offer.deadline_ms <= 900_000:
            raise ManifestError("invalid reaction offer deadline")
        if (
            not offer.reaction_events
            or len(offer.reaction_events) != len(set(offer.reaction_events))
            or not set(offer.reaction_events).issubset({"reaction_add", "reaction_remove"})
        ):
            raise ManifestError("invalid reaction callback events")
        if len(offer.audience_fields) != len(set(offer.audience_fields)) or not set(
            offer.audience_fields
        ).issubset(REACTION_AUDIENCE_FIELDS):
            raise ManifestError("invalid reaction offer audience fields")
        if type(offer.require_owner) is not bool or (
            offer.require_owner and (not offer.audience_fields or offer.claim_if_missing)
        ):
            raise ManifestError("owner-required offers cannot claim an unresolved audience")
        validate_context_fields(offer.context_fields)
        validate_presentation_modes(
            offer.presentation_mode,
            offer.presentation_modes,
            inherited=manifest.presentation_mode,
            label="reaction offer",
        )
        compiled_offer = compile_linear(offer.pattern, anchored=True)
        declared_captures = {capture.name for capture in offer.captures}
        if len(declared_captures) != len(offer.captures):
            raise ManifestError("duplicate reaction offer capture")
        if declared_captures != set(compiled_offer.groupindex):
            raise ManifestError("reaction offer captures do not match named groups")
        for capture in offer.captures:
            validate_capture(capture)
        handlers.add(offer.callback)

    actions: set[tuple[str, str]] = set()
    for component in manifest.components:
        if component.actor_role_access and (
            not component.human_guild_member or not component.source_message_facts
            or not component.public or not component.ephemeral or component.modal is not None
            or component.owner_scope or component.guild_control
        ):
            raise ManifestError("actor role access requires a public guild member control")
        if component.own_history_footers and (
            "read_owned_history" not in manifest.capabilities
            or not component.session_bound or not component.public or not component.acl_key
            or component.owner_scope or component.guild_control
            or "administrator" not in component.required_permissions
        ):
            raise ManifestError("owned footer history requires a bound administrator control")
        if component.role_access and (
            not component.session_bound or not component.public or not component.acl_key
            or "manage_roles" not in component.required_permissions
            or component.role_access.target_fields
            or not any(capture.name == "session" for capture in component.captures)
        ):
            raise ManifestError("component role access requires a staff-bound session")
        check_channel_members(component.channel_members, not component.owner_scope)
        check_private_channels(
            component.private_channel_facts,
            component.public and bool(component.acl_key) and not component.owner_scope
            and (component.session_bound or component.source_message_facts),
        )
        check_channel_effect(
            component.text_reply, component.session_bound and not component.owner_scope,
            component.acl_key if component.public else "", "reply_text",
        )
        check_channel_effect(
            component.channel_publication, component.session_bound and not component.owner_scope,
            component.acl_key if component.public else "", "publish_channel_message",
            sources=component.embed_sources,
        )
        check_channel_effect(
            component.message_mutation, component.session_bound and not component.owner_scope,
            component.acl_key if component.public else "", "mutate_owned_message",
            sources=component.embed_sources,
        )
        check_embed_sources(
            component.embed_sources,
            component.embed_sources is not None and component.embed_sources.acl_key == component.acl_key
            and component.session_bound and component.public and not component.owner_scope,
        )
        check_guild_sources(
            component.guild_sources,
            component.guild_sources is not None
            and component.guild_sources.acl_key == component.acl_key
            and component.session_bound and component.public and not component.owner_scope
            and not component.required_permissions,
        )
        if component.owner_scope not in {"", "server", "global"} or (
            component.owner_scope == "global" and (component.public or not component.session_bound)
        ):
            raise ManifestError("invalid owner component declaration")
        validate_pending(
            component.pending,
            component.presentation_mode or manifest.presentation_mode,
            component.presentation_modes,
        )
        if not IDENTIFIER.fullmatch(component.namespace):
            raise ManifestError("invalid component namespace")
        if not IDENTIFIER.fullmatch(component.action):
            raise ManifestError("invalid component action")
        aliases = component.legacy_aliases
        selection = component.channel_selection
        if selection is not None and (
            component.input_kind != "channel_ids" or not component.session_bound
            or not component.required_permissions or not 1 <= selection.max_values <= 25
            or len(set(selection.bot_permissions)) != len(selection.bot_permissions)
            or not set(selection.bot_permissions).issubset({
                "view_channel", "read_message_history", "send_messages", "embed_links", "attach_files",
            })
        ):
            raise ManifestError("channel selection facts require a bound, permissioned channel picker")
        if component.private_access_checkpoint and (
            not component.ephemeral or not component.session_bound
            or not component.required_permissions or component.owner_scope
            or component.private_loading
        ):
            raise ManifestError("access checkpoints require a private permissioned session")
        if aliases and (
            component.input_kind != "none"
            or len(aliases) > MAX_CAPTURES
            or len({alias.name for alias in aliases}) != len(aliases)
            or any(
                not IDENTIFIER.fullmatch(alias.name) or alias.name == "lumi"
                or not re.fullmatch(r"[:|;,/_-]", alias.separator)
                for alias in aliases
            )
        ):
            raise ManifestError("invalid legacy component aliases")
        identity = (component.namespace, component.action)
        if identity in actions:
            raise ManifestError("duplicate component action")
        if not ROUTE_IDENTIFIER.fullmatch(component.handler):
            raise ManifestError("invalid component handler")
        if component.input_kind not in {
            "none",
            "string_values",
            "user_ids",
            "role_ids",
            "channel_ids",
            "text",
            "modal",
        }:
            raise ManifestError("invalid component input kind")
        if not set(component.required_permissions).issubset(ALLOWED_PERMISSIONS):
            raise ManifestError("invalid component permission requirement")
        if component.channel_configuration is not None and (
            not component.public or not component.session_bound or "manage_messages" not in component.required_permissions
        ):
            raise ManifestError("native channel configuration requires a bound moderation component")
        if component.bot_channel_facts and (
            not component.public or len(component.bot_channel_facts) != len(set(component.bot_channel_facts))
            or not set(component.bot_channel_facts).issubset(Permissions.VALID_FLAGS)
        ):
            raise ManifestError("invalid component bot channel facts")
        if component.bot_guild_permissions and (
            component.owner_scope == "global"
            or len(component.bot_guild_permissions) != len(set(component.bot_guild_permissions))
            or not set(component.bot_guild_permissions).issubset(Permissions.VALID_FLAGS)
        ):
            raise ManifestError("invalid component bot guild permission requirement")
        validate_context_fields(component.context_fields)
        if any(type(flag) is not bool for flag in (
            component.source_message_facts, component.human_guild_member,
        )):
            raise ManifestError("invalid component Discord admission flags")
        if component.human_guild_member and component.owner_scope == "global":
            raise ManifestError("guild membership cannot be required by a global owner route")
        resources = component.guild_resources
        if resources is not None:
            if (
                not component.session_bound
                or "administrator" not in component.required_permissions
                or component.owner_scope == "global"
                or not (resources.roles or resources.text_channels)
                or (resources.role_permissions and not resources.roles)
                or (
                    (resources.channel_bot_permissions or resources.channel_viewer_permissions)
                    and not resources.text_channels
                )
                or (
                    resources.selected_only
                    and (
                        resources.roles == resources.text_channels
                        or component.input_kind != ("role_ids" if resources.roles else "channel_ids")
                    )
                )
                or any(
                    len(flags) != len(set(flags)) or not set(flags).issubset(Permissions.VALID_FLAGS)
                    for flags in (
                        resources.role_permissions, resources.channel_bot_permissions,
                        resources.channel_viewer_permissions,
                    )
                )
            ):
                raise ManifestError("guild resource facts require a bound administrator route")
        if component.channel_types and component.input_kind != "channel_ids":
            raise ManifestError("channel types require a channel selection")
        if len(component.channel_types) != len(set(component.channel_types)) or not set(
            component.channel_types
        ).issubset({0, 2, 4, 5, 10, 11, 12, 13, 15, 16}):
            raise ManifestError("invalid channel type restriction")
        if component.modal is not None:
            if component.input_kind != "modal":
                raise ManifestError("modal component must use modal input")
            if not component.modal.title or len(component.modal.title) > 45:
                raise ManifestError("invalid modal title")
            if not 1 <= len(component.modal.fields) <= 5:
                raise ManifestError("modal must contain 1 to 5 fields")
            keys: set[str] = set()
            for field in component.modal.fields:
                if not IDENTIFIER.fullmatch(field.key) or field.key in keys:
                    raise ManifestError("invalid or duplicate modal field key")
                keys.add(field.key)
                if not field.label or len(field.label) > 45:
                    raise ManifestError("invalid modal field label")
                if field.kind not in {
                    "text",
                    "emoji",
                    "role_select",
                    "user_select",
                    "channel_select",
                    "file_upload",
                    "string_select",
                }:
                    raise ManifestError("invalid modal field kind")
                if field.kind in {"text", "emoji"}:
                    if field.style not in {"short", "paragraph"}:
                        raise ManifestError("invalid modal field style")
                    if field.kind == "emoji" and field.style != "short":
                        raise ManifestError("emoji modal fields must be short")
                    if not 0 <= field.minimum <= field.maximum <= 4000:
                        raise ManifestError("invalid modal field limits")
                    if len(field.placeholder or "") > 100:
                        raise ManifestError("modal placeholder is too long")
                    if field.value is not None and len(field.value) > field.maximum:
                        raise ManifestError("modal default exceeds its limit")
                    if (
                        field.default_ids
                        or field.channel_types
                        or field.file_types
                        or field.choices
                        or field.min_values != 1
                        or field.max_values != 1
                    ):
                        raise ManifestError("text modal field has selection settings")
                else:
                    if field.placeholder is not None or field.value is not None:
                        raise ManifestError("selection modal field has text settings")
                    maximum = 10 if field.kind == "file_upload" else 25
                    if not 0 <= field.min_values <= field.max_values <= maximum:
                        raise ManifestError("invalid modal selection limits")
                    if field.required and field.min_values < 1:
                        raise ManifestError("required modal selection cannot be empty")
                    if len(field.default_ids) > field.max_values:
                        raise ManifestError("too many modal default values")
                    if field.channel_types and field.kind != "channel_select":
                        raise ManifestError("modal channel types require a channel select")
                    if len(field.channel_types) != len(set(field.channel_types)) or not set(
                        field.channel_types
                    ).issubset({0, 2, 4, 5, 10, 11, 12, 13, 15, 16}):
                        raise ManifestError("invalid modal channel types")
                    if field.file_types and field.kind != "file_upload":
                        raise ManifestError("modal file types require a file upload")
                    if len(field.file_types) > 10 or any(
                        value not in {"image", "video", "audio"}
                        and not re.fullmatch(r"\.[A-Za-z0-9]{1,16}", value)
                        for value in field.file_types
                    ):
                        raise ManifestError("invalid modal file types")
                    if field.choices and field.kind != "string_select":
                        raise ManifestError("modal choices require a string select")
                    if field.kind == "string_select":
                        if not 1 <= len(field.choices) <= 25:
                            raise ManifestError("invalid modal choices")
                        choice_values: set[str] = set()
                        for choice in field.choices:
                            if (
                                not choice.label
                                or len(choice.label) > 100
                                or not choice.value
                                or len(choice.value) > 100
                                or len(choice.description or "") > 100
                                or choice.value in choice_values
                            ):
                                raise ManifestError("invalid modal choice")
                            choice_values.add(choice.value)
                    elif field.choices:
                        raise ManifestError("modal field has unexpected choices")
        elif component.input_kind == "modal":
            raise ManifestError("modal input requires a modal schema")
        validate_presentation_modes(
            component.presentation_mode,
            component.presentation_modes,
            inherited=manifest.presentation_mode,
            label="component",
        )
        if len(component.presentation_modes) > 1:
            raise ManifestError("component routes must use one presentation mode")
        if component.acl_key and (
            len(component.acl_key) > 128 or not ACL_IDENTIFIER.fullmatch(component.acl_key)
        ):
            raise ManifestError("invalid component ACL key")
        compile_linear(component.grammar, anchored=True)
        actions.add(identity)
        for capture in component.captures:
            validate_capture(capture)
        if component.session_bound and not any(
            capture.name == "session" for capture in component.captures
        ):
            raise ManifestError("session-bound component must capture a session")
        if component.modal_defaults and (component.modal is None or not component.session_bound):
            raise ManifestError("modal defaults require a session-bound modal")
        if type(component.native_selection) is not bool or component.native_selection and (
            not component.session_bound or component.input_kind != "string_values"
        ):
            raise ManifestError("native selection requires a session-bound string select")
        if component.modal_prefix_field is not None and (
            not component.guild_control or component.modal is None or component.modal_defaults
            or not any(
                field.key == component.modal_prefix_field and field.kind == "text"
                for field in component.modal.fields
            )
        ):
            raise ManifestError("modal prefix default requires a declared guild-control text field")
        if component.durable_receipts and (
            component.input_kind != "none" or component.session_bound or not component.public
        ):
            raise ManifestError("durable receipts require a public button route")
        if type(component.private_loading) is not bool:
            raise ManifestError("component private_loading must be a boolean")
        if component.private_loading and not component.ephemeral:
            raise ManifestError("private loading requires an ephemeral route")
        if component.private_feedback is not None and (
            not component.private_loading
            or not component.durable_receipts
            or not all(
                isinstance(text, str) and len(text) <= 512 and text.strip()
                for text in (
                    component.private_feedback.timeout_text, component.private_feedback.rejected_text,
                )
            )
        ):
            raise ManifestError("private feedback requires bounded text and a durable private route")
        handlers.add(component.handler)

    has_guild_controls = any(route.guild_control for route in (*manifest.commands, *manifest.components))
    if manifest.accent_style not in {"assigned", "brand"} or (
        manifest.accent_style == "brand" and not has_guild_controls
    ):
        raise ManifestError("brand accent requires a declared guild control surface")
    if has_guild_controls:
        if (
            not manifest.commands or any(not route.guild_control for route in manifest.commands)
            or any(not route.guild_control for route in manifest.components)
            or manifest.passive or manifest.reaction_offers or manifest.guild_commands
            or any(component.namespace != manifest.module for component in manifest.components)
        ):
            raise ManifestError("guild controls require an isolated declared surface")
    for component in manifest.components:
        if type(component.guild_control) is not bool or component.guild_control and (
            manifest.group != GUILD_CONTROL_GROUP or component.public
            or not component.session_bound or component.owner_scope
        ):
            raise ManifestError("invalid guild control component")
