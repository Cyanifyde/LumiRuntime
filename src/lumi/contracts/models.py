from __future__ import annotations

from enum import StrEnum
from typing import Annotated, Any, Literal

import msgspec

from lumi.contracts.member_effects import MemberEffectSpec
from lumi.contracts.channel_messages import ChannelMessageAccess, ChannelMessageDeletion, ChannelMessageQuery
from lumi.contracts.channel_configuration import ChannelConfigurationAccess, ChannelConfigurationQuery
from lumi.contracts.channel_resources import ChannelResourceAccess, ChannelResourceRequest
from lumi.contracts.member_profiles import MemberProfileAccess, NicknameMutation
from lumi.contracts.role_access import ActorRoleChanges, RoleAccessSpec, RoleFactsQuery, RoleMembership
from lumi.contracts.staff_recovery import StaffRecoveryAccess
from lumi.contracts.staff_moderation import StaffEffect, StaffModerationAccess


MAX_CHANNEL_FACT_TARGETS = 25


class RuntimeState(StrEnum):
    ONLINE = "online"
    DRAINING = "draining"
    RESTARTING = "restarting"
    OFFLINE = "offline"


class ModuleState(StrEnum):
    READY = "ready"
    RETIRING = "retiring"


class CaptureSpec(msgspec.Struct, frozen=True, forbid_unknown_fields=True):
    name: str
    kind: str = "text"
    required: bool = True
    minimum: int | None = None
    maximum: int | None = None


class HelpSpec(msgspec.Struct, frozen=True, forbid_unknown_fields=True):


    usage: str
    summary: str
    hidden: bool = False


class ModalChoice(msgspec.Struct, frozen=True, forbid_unknown_fields=True):
    label: str
    value: str
    description: str | None = None


class ModalFieldSpec(msgspec.Struct, frozen=True, forbid_unknown_fields=True):
    key: str
    label: str
    kind: str = "text"
    style: str = "short"
    required: bool = True
    minimum: int = 0
    maximum: int = 4000
    placeholder: str | None = None
    value: str | None = None
    min_values: int = 1
    max_values: int = 1
    default_ids: tuple[int, ...] = ()
    channel_types: tuple[int, ...] = ()
    file_types: tuple[str, ...] = ()
    choices: tuple[ModalChoice, ...] = ()


class ModalSpec(msgspec.Struct, frozen=True, forbid_unknown_fields=True):
    title: str
    fields: tuple[ModalFieldSpec, ...]


class PendingPresentationSpec(msgspec.Struct, frozen=True, forbid_unknown_fields=True):


    description: str = ""
    animation: str = "spinner"


class GuildSourceSpec(msgspec.Struct, frozen=True, forbid_unknown_fields=True):


    acl_key: str
    include_activity: bool = False


class ChannelMemberFactsSpec(msgspec.Struct, frozen=True, forbid_unknown_fields=True):


    channel_types: tuple[int, ...] = (0,)
    bot_permissions: tuple[str, ...] = ("view_channel",)


class PrivateChannelFactsSpec(
    msgspec.Struct, frozen=True, forbid_unknown_fields=True, omit_defaults=True,
):


    bot_permissions: tuple[str, ...] = ("view_channel",)
    bot_guild_permissions: tuple[str, ...] = ()


class EmbedSourceSpec(msgspec.Struct, frozen=True, forbid_unknown_fields=True):


    acl_key: str
    author_ids: tuple[int, ...]


class TextReplySpec(msgspec.Struct, frozen=True, forbid_unknown_fields=True):


    cross_channel_acl: str = ""
    channel_types: tuple[int, ...] = (0, 5)


class ScheduledDeliverySpec(msgspec.Struct, frozen=True, forbid_unknown_fields=True, omit_defaults=True):
    private: bool = False
    bot_permissions: tuple[str, ...] = ()
    recorded_target: bool = False


class ChannelPublicationSpec(msgspec.Struct, frozen=True, forbid_unknown_fields=True, omit_defaults=True):
    cross_channel_acl: str = ""
    channel_types: tuple[int, ...] = (0, 5)
    require_source_proof: bool = False
    require_history: bool = False
    delivery: ScheduledDeliverySpec | None = None


class MessageMutationSpec(msgspec.Struct, frozen=True, forbid_unknown_fields=True, omit_defaults=True):
    cross_channel_acl: str = ""
    channel_types: tuple[int, ...] = (0, 5)
    allow_delete: bool = False
    allow_replacement: bool = False
    require_replacement_proof: bool = False
    delivery: ScheduledDeliverySpec | None = None


class CommandSpec(msgspec.Struct, frozen=True, forbid_unknown_fields=True, omit_defaults=True):
    root: str
    handler: str
    grammar: str
    aliases: tuple[str, ...] = ()
    captures: tuple[CaptureSpec, ...] = ()
    help: HelpSpec | None = None
    acl_key: str = ""
    deadline_ms: int = 120_000
    queue_class: str = "ordinary"
    guild_only: bool = True
    public: bool = False
    required_permissions: tuple[str, ...] = ()
    presentation_mode: str | None = None
    presentation_modes: tuple[str, ...] = ()
    context_fields: tuple[str, ...] = ()
    pending: PendingPresentationSpec | None = None
    loading: bool = True
    loading_skip_reason: str | None = None
    owner_scope: Literal["", "server", "global"] = ""
    use_default_prefix: bool = False
    configure_in_guild: bool = True
    guild_sources: GuildSourceSpec | None = None
    channel_members: ChannelMemberFactsSpec | None = None
    private_channel_facts: PrivateChannelFactsSpec | None = None
    embed_sources: EmbedSourceSpec | None = None
    text_reply: TextReplySpec | None = None
    channel_publication: ChannelPublicationSpec | None = None
    message_mutation: MessageMutationSpec | None = None
    member_target: tuple[str, ...] = ()
    member_effect: MemberEffectSpec | None = None
    role_access: RoleAccessSpec | None = None
    bot_guild_permissions: tuple[str, ...] = ()
    bot_channel_facts: tuple[str, ...] = ()
    channel_configuration: ChannelConfigurationAccess | None = None
    guild_control: bool = False
    actor_admission: bool = False


class MatcherExpr(msgspec.Struct, frozen=True, forbid_unknown_fields=True):
    op: str
    field: str | None = None
    value: str | None = None
    children: tuple[MatcherExpr, ...] = ()


class RepeatedExtraction(msgspec.Struct, frozen=True, forbid_unknown_fields=True):
    field: str
    pattern: str
    maximum: int
    captures: tuple[CaptureSpec, ...]


class FieldExtraction(msgspec.Struct, frozen=True, forbid_unknown_fields=True):


    field: str
    pattern: str
    captures: tuple[CaptureSpec, ...]


class ComponentCaptureSpec(msgspec.Struct, frozen=True, forbid_unknown_fields=True):


    name: str
    field: str
    kind: str = "text"
    pattern: str | None = None
    required: bool = True
    minimum: int | None = None
    maximum: int | None = None


class ComponentExtractionSpec(msgspec.Struct, frozen=True, forbid_unknown_fields=True):


    name: str
    maximum: int
    expression: MatcherExpr
    captures: tuple[ComponentCaptureSpec, ...]


class RecipientWebhookSpec(msgspec.Struct, frozen=True, forbid_unknown_fields=True):
    name: str


class PassiveMatcherSpec(msgspec.Struct, frozen=True, forbid_unknown_fields=True, omit_defaults=True):
    route: str
    handler: str
    event: str
    expression: MatcherExpr
    bot_ids: tuple[int, ...] = ()
    component_prefix: str | None = None
    requires_embeds: bool | None = None
    requires_attachments: bool | None = None
    extractions: tuple[FieldExtraction, ...] = ()
    repeated: tuple[RepeatedExtraction, ...] = ()
    component_extractions: tuple[ComponentExtractionSpec, ...] = ()
    subject_capture: str | None = None
    subject_field: str | None = None
    subject_required: bool = False
    hydrate: bool = False
    watch_ttl_ms: int = 0
    reference_author_max_age_ms: int = 0
    deadline_ms: int = 120_000
    queue_class: str = "ordinary"
    presentation_mode: str | None = None
    presentation_modes: tuple[str, ...] = ()
    context_fields: tuple[str, ...] = ()
    reply_prompt: bool = False
    source_cleanup: bool = False
    staff_moderation: StaffModerationAccess | None = None
    channel_scoped: bool = False
    acl_key: str = ""
    personal_reaction: bool = False
    bound_human_reply: bool = False
    bound_reply_activity_exempt: bool = False
    unless_routes_match: tuple[str, ...] = ()
    prepare_reply: str | None = None
    guild_sources: GuildSourceSpec | None = None
    own_output_deleted: bool = False
    recipient_webhook: RecipientWebhookSpec | None = None
    hydrate_matching_edit: bool = False
    hydrate_on: tuple[str, ...] = ()


class ReactionOfferSpec(msgspec.Struct, frozen=True, forbid_unknown_fields=True):
    key: str
    callback: str
    pattern: str
    emoji: str
    events: tuple[str, ...] = ("message_create", "message_edit")
    field: str = "message_document"
    captures: tuple[CaptureSpec, ...] = ()
    bot_ids: tuple[int, ...] = ()
    requires_embeds: bool | None = None
    requires_image: bool = False
    context_fields: tuple[str, ...] = ()
    audience_fields: tuple[str, ...] = ("interaction_user_id", "reply_author_id")
    claim_if_missing: bool = False
    require_owner: bool = False
    reaction_events: tuple[str, ...] = ("reaction_add",)
    ttl_ms: int = 900_000
    remove_unauthorized: bool = True
    remove_after_dispatch: bool = True
    deadline_ms: int = 120_000
    queue_class: str = "ordinary"
    presentation_mode: str | None = None
    presentation_modes: tuple[str, ...] = ()
    prepared_only: bool = False
    preparation_cooldown_ms: int = 0


class PrivateFeedbackSpec(msgspec.Struct, frozen=True, forbid_unknown_fields=True):


    timeout_text: str
    rejected_text: str


class ComponentAliasSpec(
    msgspec.Struct, frozen=True, forbid_unknown_fields=True, omit_defaults=True,
):


    name: str
    separator: str = ":"


class ChannelSelectionSpec(msgspec.Struct, frozen=True, forbid_unknown_fields=True, omit_defaults=True):
    max_values: int
    bot_permissions: tuple[str, ...] = ()
    require_private: bool = False


class GuildResourcesSpec(msgspec.Struct, frozen=True, forbid_unknown_fields=True, omit_defaults=True):


    roles: bool = False
    text_channels: bool = False
    selected_only: bool = False
    role_permissions: tuple[str, ...] = ()
    channel_bot_permissions: tuple[str, ...] = ()


    channel_viewer_permissions: tuple[str, ...] = ()


class ComponentSpec(msgspec.Struct, frozen=True, forbid_unknown_fields=True, omit_defaults=True):
    namespace: str
    action: str
    handler: str
    grammar: str = "^$"
    captures: tuple[CaptureSpec, ...] = ()
    deadline_ms: int = 15_000
    queue_class: str = "ordinary"
    acl_key: str = ""
    public: bool = False
    input_kind: str = "none"
    required_permissions: tuple[str, ...] = ()
    ephemeral: bool = False
    modal: ModalSpec | None = None
    channel_types: tuple[int, ...] = ()
    presentation_mode: str | None = None
    presentation_modes: tuple[str, ...] = ()
    session_bound: bool = False
    role_access: RoleAccessSpec | None = None
    bot_guild_permissions: tuple[str, ...] = ()
    bot_channel_facts: tuple[str, ...] = ()
    channel_configuration: ChannelConfigurationAccess | None = None
    context_fields: tuple[str, ...] = ()
    pending: PendingPresentationSpec | None = None
    modal_defaults: bool = False
    durable_receipts: bool = False
    private_loading: bool = False
    private_feedback: PrivateFeedbackSpec | None = None
    owner_scope: Literal["", "server", "global"] = ""
    guild_sources: GuildSourceSpec | None = None
    channel_members: ChannelMemberFactsSpec | None = None
    private_channel_facts: PrivateChannelFactsSpec | None = None
    embed_sources: EmbedSourceSpec | None = None
    text_reply: TextReplySpec | None = None
    channel_publication: ChannelPublicationSpec | None = None
    message_mutation: MessageMutationSpec | None = None
    legacy_aliases: tuple[ComponentAliasSpec, ...] = ()
    private_access_checkpoint: bool = False
    channel_selection: ChannelSelectionSpec | None = None
    guild_resources: GuildResourcesSpec | None = None
    source_message_facts: bool = False
    human_guild_member: bool = False
    guild_control: bool = False
    modal_prefix_field: str | None = None
    native_selection: bool = False
    layout_write: bool = False
    own_history_footers: bool = False
    actor_role_access: bool = False


class ScheduledCallbackSpec(msgspec.Struct, frozen=True, forbid_unknown_fields=True, omit_defaults=True):
    route: str
    handler: str
    deadline_ms: int = 300_000
    queue_class: str = "ordinary"
    presentation_mode: str | None = None
    presentation_modes: tuple[str, ...] = ()
    scope: str = "actor"
    acl_key: str = ""
    mention_templates: tuple[MentionTemplate, ...] = ()
    own_history: bool = False
    own_history_private: bool = False
    own_history_bot_permissions: tuple[str, ...] = ()
    channel_members: ChannelMemberFactsSpec | None = None
    embed_sources: EmbedSourceSpec | None = None
    text_reply: TextReplySpec | None = None
    channel_publication: ChannelPublicationSpec | None = None
    message_mutation: MessageMutationSpec | None = None
    member_profiles: MemberProfileAccess | None = None
    bound_panel: bool = False
    channel_messages: ChannelMessageAccess | None = None
    staff_recovery: StaffRecoveryAccess | None = None
    channel_resources: ChannelResourceAccess | None = None


class MentionTemplate(msgspec.Struct, frozen=True, forbid_unknown_fields=True):
    key: str
    caption: str


class GuildCommandSourceSpec(msgspec.Struct, frozen=True, forbid_unknown_fields=True):


    handler: str
    acl_key: str
    context_fields: tuple[str, ...] = ()
    grammar: str = "^$"
    captures: tuple[CaptureSpec, ...] = ()
    deadline_ms: int = 30_000


class GuildCommandDefinition(msgspec.Struct, frozen=True, forbid_unknown_fields=True):
    root: Annotated[str, msgspec.Meta(pattern=r"^[a-z0-9][a-z0-9_-]{0,63}$")]
    help: HelpSpec
    arguments: dict[str, str | int | bool] = msgspec.field(default_factory=dict)


class GuildCommandSnapshot(msgspec.Struct, frozen=True, forbid_unknown_fields=True):
    schema: Literal[1]
    module: str
    group: str
    guild_id: Annotated[int, msgspec.Meta(gt=0)]
    revision: Annotated[int, msgspec.Meta(ge=0)]
    commands: Annotated[tuple[GuildCommandDefinition, ...], msgspec.Meta(max_length=100)]


class FunCategoryDefinition(msgspec.Struct, frozen=True, forbid_unknown_fields=True):
    id: Annotated[str, msgspec.Meta(pattern=r"^[a-f0-9]{16}$")]
    name: Annotated[str, msgspec.Meta(pattern=r"^[a-z0-9][a-z0-9_-]{0,31}$")]
    mode: Literal["solo", "targeted"]


class FunCatalogueSnapshot(msgspec.Struct, frozen=True, forbid_unknown_fields=True):
    schema: Literal[1]
    guild_id: Annotated[int, msgspec.Meta(gt=0)]
    revision: Annotated[int, msgspec.Meta(ge=0)]
    categories: Annotated[tuple[FunCategoryDefinition, ...], msgspec.Meta(max_length=100)]


class ChannelTransitionSpec(
    msgspec.Struct, frozen=True, forbid_unknown_fields=True, omit_defaults=True,
):


    quiesce_guild: bool = False
    control_handlers: tuple[str, ...] = ()
    drain_handlers: tuple[str, ...] = ()
    suppress_handlers: tuple[str, ...] = ()
    rebuild_posts: bool = False


class GatewayCaptureSpec(msgspec.Struct, frozen=True, forbid_unknown_fields=True):


    events: tuple[str, ...]
    baseline_events: tuple[str, ...] = ()
    mutation_kinds: tuple[str, ...] = ()


class GuardedRepliesSpec(msgspec.Struct, frozen=True, forbid_unknown_fields=True, omit_defaults=True):
    private_bot_permissions: tuple[str, ...] = ("view_channel",)


class Manifest(msgspec.Struct, frozen=True, forbid_unknown_fields=True, omit_defaults=True):
    schema: int
    module: str
    group: str
    generation: int
    job_stream: str
    display_name: str = ""
    summary: str = ""
    accent_color: int = 0
    presentation_mode: str = "v2"
    commands: tuple[CommandSpec, ...] = ()
    passive: tuple[PassiveMatcherSpec, ...] = ()
    reaction_offers: tuple[ReactionOfferSpec, ...] = ()
    components: tuple[ComponentSpec, ...] = ()
    scheduled: tuple[ScheduledCallbackSpec, ...] = ()
    capabilities: tuple[str, ...] = ()
    version: str = "0.0.0"
    activity_observations: bool = False
    action_presentation_modes: tuple[str, ...] = ()
    help: HelpSpec | None = None
    guild_commands: GuildCommandSourceSpec | None = None
    channel_transition: ChannelTransitionSpec | None = None
    gateway_capture: GatewayCaptureSpec | None = None
    guarded_replies: GuardedRepliesSpec | None = None
    accent_style: Literal["assigned", "brand"] = "assigned"


class Announcement(msgspec.Struct, frozen=True, forbid_unknown_fields=True):
    kind: str
    manifest: Manifest
    announced_at_ms: int
    routing_revision: int = 0


class AITextVariant(msgspec.Struct, frozen=True, forbid_unknown_fields=True):
    outcome: str
    pattern: str
    captures: tuple[CaptureSpec, ...] = ()
    terminal: bool = False


class AIProfileSpec(msgspec.Struct, frozen=True, forbid_unknown_fields=True):
    key: str
    version: str
    model: str
    system_prompt: str
    input_schema: dict[str, Any]
    response_mode: str
    text_variants: tuple[AITextVariant, ...] = ()
    response_schema: dict[str, Any] | None = None
    repair_prompt: str | None = None
    deadline_ms: int = 30_000
    max_attempts: int = 2
    temperature: float = 0.0
    thinking: bool = False
    max_output_tokens: int = 1_200


class AIProfileAnnouncement(msgspec.Struct, frozen=True, forbid_unknown_fields=True):
    schema: int
    kind: str
    module: str
    group: str
    generation: int
    profile: AIProfileSpec
    profile_hash: str
    announced_at_ms: int


class AICallRequest(msgspec.Struct, frozen=True, forbid_unknown_fields=True):
    schema: int
    call_id: str
    profile_key: str
    profile_hash: str
    caller_module: str
    caller_group: str
    caller_generation: int
    actor_id: int | None
    expires_at_ms: int
    input: dict[str, Any]


class AICallResult(msgspec.Struct, frozen=True, forbid_unknown_fields=True):
    schema: int
    call_id: str
    status: str
    outcome: str | None = None
    captures: dict[str, Any] = msgspec.field(default_factory=dict)
    value: Any = None
    elapsed_ms: int | None = None
    prompt_tokens: int | None = None
    output_tokens: int | None = None


class TypedRequest(msgspec.Struct, frozen=True, forbid_unknown_fields=True):
    route: str
    handler: str
    arguments: dict[str, Any]
    event: dict[str, Any]


class ImageSourceContext(msgspec.Struct, frozen=True, forbid_unknown_fields=True):


    url: str
    content_type: str = ""
    file_name: str = ""
    size: int = 0
    width: int | None = None
    height: int | None = None
    origin: str = "attachment"


class JobEnvelope(msgspec.Struct, frozen=True, forbid_unknown_fields=True):
    schema: int
    job_id: str
    module: str
    group: str
    generation: int
    admitted_at_ms: int
    expires_at_ms: int
    guild_id: int | None
    channel_id: int | None
    actor_id: int
    queue_class: str
    request: TypedRequest
    guild_sources: GuildSourceSpec | None = None


class PresentationField(msgspec.Struct, frozen=True, forbid_unknown_fields=True):
    name: str
    value: str
    inline: bool = False


class PresentationOption(msgspec.Struct, frozen=True, forbid_unknown_fields=True):
    label: str
    value: str
    description: str | None = None
    emoji: str | None = None
    default: bool = False


class PresentationControl(msgspec.Struct, frozen=True, forbid_unknown_fields=True):
    kind: str
    namespace: str
    action: str
    payload: str
    label: str | None = None
    emoji: str | None = None
    style: str = "secondary"
    placeholder: str | None = None
    options: tuple[PresentationOption, ...] = ()
    min_values: int = 1
    max_values: int = 1
    default_ids: tuple[int, ...] = ()
    channel_types: tuple[int, ...] = ()
    disabled: bool = False


class PresentationMedia(msgspec.Struct, frozen=True, forbid_unknown_fields=True):
    media_url: str | None = None
    file_name: str | None = None
    description: str | None = None
    spoiler: bool = False


class PresentationBlock(msgspec.Struct, frozen=True, forbid_unknown_fields=True):


    kind: str
    text: str = ""
    level: int = 0
    media_url: str | None = None
    file_name: str | None = None
    description: str | None = None
    spoiler: bool = False
    divider: bool = True
    spacing: str = "small"
    controls: tuple[PresentationControl, ...] = ()
    items: tuple[PresentationMedia, ...] = ()


class PresentationPage(msgspec.Struct, frozen=True, forbid_unknown_fields=True):
    description: str = ""
    title: str | None = None
    url: str | None = None
    fields: tuple[PresentationField, ...] = ()
    footer: str | None = None
    image_url: str | None = None
    thumbnail_url: str | None = None
    author_name: str | None = None
    author_icon_url: str | None = None
    timestamp_ms: int | None = None
    controls: tuple[PresentationControl, ...] = ()
    file_name: str | None = None
    blocks: tuple[PresentationBlock, ...] = ()


class TemporaryAttachment(msgspec.Struct, frozen=True, forbid_unknown_fields=True):
    reference: str
    file_name: str
    sha256: str
    size: int
    expires_at_ms: int


class DiscordIntent(msgspec.Struct, frozen=True, forbid_unknown_fields=True):
    kind: str
    content: str | None = None
    pages: tuple[PresentationPage, ...] = ()
    attachment_ref: str | None = None
    attachments: tuple[TemporaryAttachment, ...] = ()
    session_id: str | None = None
    controls: tuple[PresentationControl, ...] = ()
    presentation_mode: str | None = None
    mention_actor: bool = False
    public_controls: bool = False
    source_guild_ids: tuple[int, ...] = ()


class ScheduledDispatch(msgspec.Struct, frozen=True, forbid_unknown_fields=True):
    schema: int
    operation: str
    schedule_key: str
    module: str
    group: str
    generation: int
    route: str
    revision: int
    guild_id: int
    channel_id: int | None
    actor_id: int
    due_at_ms: int
    expires_at_ms: int
    arguments: dict[str, Any] = msgspec.field(default_factory=dict)


class PurgeFlag(msgspec.Struct, frozen=True, forbid_unknown_fields=True):
    name: str
    enabled: bool


class PurgeText(msgspec.Struct, frozen=True, forbid_unknown_fields=True):
    name: str
    value: str


class PurgeBoundary(msgspec.Struct, frozen=True, forbid_unknown_fields=True):
    message_id: int
    guild_id: int | None = None
    channel_id: int | None = None


class PurgeSpec(msgspec.Struct, frozen=True, forbid_unknown_fields=True):
    limit: int | None = None
    keep_pins: bool = True
    users: tuple[int, ...] = ()
    roles: tuple[int, ...] = ()
    exclude_users: tuple[int, ...] = ()
    exclude_roles: tuple[int, ...] = ()
    flags: tuple[PurgeFlag, ...] = ()
    text: tuple[PurgeText, ...] = ()
    before: PurgeBoundary | None = None
    after: PurgeBoundary | None = None
    older_seconds: int | None = None
    newer_seconds: int | None = None
    since_ms: int | None = None


class MemberSearchSpec(msgspec.Struct, frozen=True, forbid_unknown_fields=True):
    query: Annotated[str, msgspec.Meta(max_length=100)]
    selected_id: Annotated[int, msgspec.Meta(gt=0)] | None = None


class ChannelWatchSpec(msgspec.Struct, frozen=True, forbid_unknown_fields=True):
    operation: Literal["publish", "edit", "read", "close", "forget", "verify", "authorize_stop"]
    route: Annotated[str, msgspec.Meta(max_length=128)]
    token: Annotated[str, msgspec.Meta(max_length=32)]
    duration_ms: Annotated[int, msgspec.Meta(ge=1000, le=300000)] = 60000
    cursor: Annotated[str, msgspec.Meta(max_length=48)] = "0-0"
    revision: Annotated[int, msgspec.Meta(ge=0)] = 0
    resolved: bool = False


class MemberSearchCandidate(msgspec.Struct, frozen=True, forbid_unknown_fields=True):
    id: int
    username: Annotated[str, msgspec.Meta(max_length=128)]
    display_name: Annotated[str, msgspec.Meta(max_length=128)]
    match: Literal["exact", "prefix", "substring", "typo"]


class MemberSearchResult(msgspec.Struct, frozen=True, forbid_unknown_fields=True):
    status: Literal["matched", "choose", "not_found", "too_many", "changed"]
    candidates: Annotated[tuple[MemberSearchCandidate, ...], msgspec.Meta(max_length=25)] = ()


class KarutaCSVReference(msgspec.Struct, frozen=True, forbid_unknown_fields=True):
    reference: Annotated[str, msgspec.Meta(pattern="^[a-f0-9]{64}$")]
    sha256: Annotated[str, msgspec.Meta(pattern="^[a-f0-9]{64}$")]
    size: Annotated[int, msgspec.Meta(gt=0, le=25 * 1024 * 1024)]
    expires_at_ms: Annotated[int, msgspec.Meta(gt=0)]
    owner_id: Annotated[int, msgspec.Meta(gt=0)]
    guild_id: Annotated[int, msgspec.Meta(gt=0)]
    channel_id: Annotated[int, msgspec.Meta(gt=0)]
    message_id: Annotated[int, msgspec.Meta(gt=0)]
    created_at_ms: Annotated[int, msgspec.Meta(gt=0)]


MAX_MENTION_RECIPIENTS = 50


class MentionBatchRef(msgspec.Struct, frozen=True, forbid_unknown_fields=True):
    batch_id: Annotated[str, msgspec.Meta(pattern="^[a-f0-9]{32}$")]
    revision: Annotated[int, msgspec.Meta(gt=0)]


class MentionRecipient(msgspec.Struct, frozen=True, forbid_unknown_fields=True):
    user_id: Annotated[int, msgspec.Meta(gt=0)]
    expires_at_ms: Annotated[int, msgspec.Meta(gt=0)]


class MentionBatchPlan(msgspec.Struct, frozen=True, forbid_unknown_fields=True):
    reference: MentionBatchRef
    module: str
    group: str
    job_id: str
    generation: int
    guild_id: int
    channel_id: int
    template: str
    due_at_ms: int
    expires_at_ms: int
    recipients: Annotated[
        tuple[MentionRecipient, ...], msgspec.Meta(min_length=1, max_length=MAX_MENTION_RECIPIENTS)
    ]


OWNED_HISTORY_PAGE = 100
OWNED_FOOTER_WINDOW_MS = 86_400_000
DISCORD_MEMBER_QUERY_LIMIT = 100
EMBED_SOURCE_LIMIT = 10
SOURCE_ROLE_LIMIT = 60


class OwnedHistoryQuery(msgspec.Struct, frozen=True, forbid_unknown_fields=True, omit_defaults=True):
    after_ms: int
    limit: int
    cursor: int | None = None

    reply_to: int | None = None
    attachment_name: str | None = None
    component_id: str | None = None
    nonce: str | None = None
    footer_sha256: str | None = None


class MessageRef(msgspec.Struct, frozen=True, forbid_unknown_fields=True):
    channel_id: int
    message_id: int


class EmbedSourceQuery(msgspec.Struct, frozen=True, forbid_unknown_fields=True):
    actor_id: int
    viewer_id: int
    sources: Annotated[tuple[MessageRef, ...], msgspec.Meta(max_length=EMBED_SOURCE_LIMIT)]
    role_ids: Annotated[tuple[int, ...], msgspec.Meta(max_length=SOURCE_ROLE_LIMIT)]
    binding: str


class MessageMutation(msgspec.Struct, frozen=True, forbid_unknown_fields=True):
    operation: Literal["edit", "delete"]
    preserve_attachments: bool = False
    owner_component: str | None = None


class EffectDelivery(msgspec.Struct, frozen=True, forbid_unknown_fields=True, omit_defaults=True):
    participants: tuple[int, ...] = ()
    nsfw: bool = False
    nonce: str | None = None


class DiscordEffectGrant(msgspec.Struct, frozen=True, forbid_unknown_fields=True, omit_defaults=True):


    module: str
    generation: int
    kind: str
    guild_id: int
    channel_id: int
    message_id: int | None
    digest: str
    expires_at_ms: int
    source_digest: str | None = None
    mutation_digest: str | None = None
    delivery: EffectDelivery | None = None


class BoundReplySpec(msgspec.Struct, frozen=True, forbid_unknown_fields=True, omit_defaults=True):


    operation: Literal["open", "edit", "close"]
    token: Annotated[str, msgspec.Meta(pattern=r"^[a-f0-9]{32}$")]
    route: Annotated[str, msgspec.Meta(max_length=128)] = ""
    ttl_ms: Annotated[int, msgspec.Meta(ge=0, le=900_000)] = 0
    ping_actor: bool = False
    source_author_id: Annotated[int, msgspec.Meta(ge=1)] | None = None


class DiscordAction(msgspec.Struct, frozen=True, forbid_unknown_fields=True, omit_defaults=True):
    schema: int
    action_id: str
    job_id: str
    module: str
    group: str
    generation: int
    expires_at_ms: int
    kind: str
    guild_id: int | None
    idempotency_key: str
    channel_id: int | None = None
    message_id: int | None = None
    member_id: int | None = None
    role_id: int | None = None
    remove_role_id: int | None = None
    role_ids: tuple[int, ...] = ()
    emoji: str | None = None
    emoji_keys: tuple[str, ...] = ()
    reason: str | None = None
    page: PresentationPage | None = None
    intent: DiscordIntent | None = None
    content: str | None = None
    username: str | None = None
    avatar_url: str | None = None
    persistent: bool = False
    reminder_prompt: None = None
    bound_reply: BoundReplySpec | None = None
    reaction_revision: int | None = None
    duration_seconds: int | None = None
    forbid_source_channel: bool = False
    respect_nsfw: bool = False
    member_ids: tuple[int, ...] = ()
    imageguard_incident_id: None = None
    warning_id: str | None = None
    message_limit: int | None = None
    page_number: int | None = None

    moderation_session: str | None = None
    purge_spec: PurgeSpec | None = None
    purge_id: str | None = None
    rename_id: str | None = None
    nickname: str | None = None

    giveaway_id: str | None = None
    giveaway_revision: int | None = None
    member_search: MemberSearchSpec | None = None
    role_edit: None = None

    modlog_spec: None = None
    channel_watch: ChannelWatchSpec | None = None
    mention_batch: MentionBatchRef | None = None
    read_proof: str | None = None
    prove_message_author: bool = False


    target_channel_ids: Annotated[
        tuple[int, ...], msgspec.Meta(max_length=MAX_CHANNEL_FACT_TARGETS)
    ] | None = None
    message_link_field: str | None = None
    own_history: OwnedHistoryQuery | None = None
    embed_sources: EmbedSourceQuery | None = None
    message_mutation: MessageMutation | None = None
    timeout_until_ms: int | None = None
    role_facts: RoleFactsQuery | None = None
    role_membership: RoleMembership | None = None
    actor_role_changes: ActorRoleChanges | None = None
    member_directory_after: int | None = None
    refresh_member_profile: bool = False
    nickname_mutation: NicknameMutation | None = None
    bound_panel_session: str | None = None
    channel_message_query: ChannelMessageQuery | None = None
    channel_message_deletion: ChannelMessageDeletion | None = None
    channel_configuration_query: ChannelConfigurationQuery | None = None
    staff_effect: StaffEffect | None = None
    channel_resource: ChannelResourceRequest | None = None


class DiscordActionResult(msgspec.Struct, frozen=True, forbid_unknown_fields=True):
    schema: int
    action_id: str
    ok: bool
    status: str
    data: dict[str, Any] = msgspec.field(default_factory=dict)
    error_code: str | None = None
    completed_at_ms: int = 0


class ReactionRouteSnapshot(msgspec.Struct, frozen=True, forbid_unknown_fields=True):
    schema: int
    revision: int
    message_ids: tuple[int, ...]
    published_at_ms: int


class AFKActiveSnapshot(msgspec.Struct, frozen=True, forbid_unknown_fields=True):


    schema: int
    guild_id: int
    revision: int
    user_ids: tuple[int, ...]
    published_at_ms: int


class ActorRoutingSnapshot(msgspec.Struct, frozen=True, forbid_unknown_fields=True):
    schema: Literal[1]
    module: Annotated[str, msgspec.Meta(pattern=r"^[a-z][a-z0-9_-]{0,63}$")]
    group: str
    guild_id: Annotated[int, msgspec.Meta(ge=0)]
    revision: Annotated[int, msgspec.Meta(ge=0)]
    user_ids: tuple[int, ...]


class ResponseRevision(msgspec.Struct, frozen=True, forbid_unknown_fields=True):
    session: Annotated[str, msgspec.Meta(pattern=r"^[a-f0-9]{16}$")]
    revision: Annotated[int, msgspec.Meta(ge=0)]


class ResponseRevisionBinding(msgspec.Struct, frozen=True, forbid_unknown_fields=True):
    module: str
    actor_id: int
    guild_id: int | None
    channel_id: int | None
    revision: int
    expires_at_ms: int


class PreparedReplyOffer(msgspec.Struct, frozen=True, forbid_unknown_fields=True):


    source_hash: Annotated[str, msgspec.Meta(pattern=r"^[a-f0-9]{64}$")]
    source_revision_ms: Annotated[int, msgspec.Meta(ge=0)]
    reply: DiscordIntent


class ResultEnvelope(msgspec.Struct, frozen=True, forbid_unknown_fields=True):
    schema: int
    job_id: str
    module: str
    group: str
    generation: int
    status: str
    intents: tuple[DiscordIntent, ...] = ()
    error_code: str | None = None
    completed_at_ms: int = 0
    response_revision: ResponseRevision | None = None
    prepared_reply_offer: PreparedReplyOffer | None = None


class RuntimeMetric(msgspec.Struct, frozen=True, forbid_unknown_fields=True):
    module: str
    group: str
    generation: int
    state: RuntimeState
    queue_depth: int = 0
    worker_rtt_ms: float | None = None


class RoleGrant(msgspec.Struct, frozen=True, forbid_unknown_fields=True):
    key: str
    role_ids: tuple[int, ...]


class ChannelGrant(msgspec.Struct, frozen=True, forbid_unknown_fields=True):
    key: str
    channel_ids: tuple[int, ...] = ()
    category_ids: tuple[int, ...] = ()


class GuildPolicySnapshot(msgspec.Struct, frozen=True, forbid_unknown_fields=True):
    schema: int
    guild_id: int
    revision: int
    prefix: str
    enabled_modules: tuple[str, ...]
    enabled_commands: tuple[str, ...] = ()
    module_roles: tuple[RoleGrant, ...] = ()
    command_roles: tuple[RoleGrant, ...] = ()
    module_channels: tuple[ChannelGrant, ...] = ()
    command_channels: tuple[ChannelGrant, ...] = ()
    published_at_ms: int = 0


class CatalogCommand(msgspec.Struct, frozen=True, forbid_unknown_fields=True):
    root: str
    acl_key: str
    usage: str
    summary: str
    public: bool
    guild_only: bool


class CatalogModule(msgspec.Struct, frozen=True, forbid_unknown_fields=True):
    key: str
    group: str
    display_name: str
    summary: str
    accent_color: int
    presentation_mode: str
    runtime_state: str
    commands: tuple[CatalogCommand, ...] = ()


class AcceptedCatalog(msgspec.Struct, frozen=True, forbid_unknown_fields=True):
    schema: int
    revision: int
    modules: tuple[CatalogModule, ...]
    published_at_ms: int
