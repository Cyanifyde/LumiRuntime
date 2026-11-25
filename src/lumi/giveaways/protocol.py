import hashlib
import json

from lumi.contracts import ChannelMemberFactsSpec, ChannelPublicationSpec, EmbedSourceQuery, EmbedSourceSpec, MessageMutationSpec, MessageRef, TextReplySpec, encode
from lumi.contracts.models import DISCORD_MEMBER_QUERY_LIMIT

from . import expanded as v2
from . import models as engine


CHANNEL_MEMBERS = "read_channel_members"
SOURCE_FACTS = "read_embed_sources"
ACTOR_CHANNELS = "read_actor_post_channels"
REPLY_CHANNEL = TextReplySpec()
REPLY_GUILD = TextReplySpec(cross_channel_acl="giveaway")
PUBLISH_CHANNEL = ChannelPublicationSpec(require_source_proof=True, require_history=True)
PUBLISH_GUILD = ChannelPublicationSpec(
    cross_channel_acl="giveaway", require_source_proof=True, require_history=True,
)
MUTATE_CHANNEL = MessageMutationSpec(
    allow_delete=True, allow_replacement=True, require_replacement_proof=True,
)
MUTATE_GUILD = MessageMutationSpec(
    cross_channel_acl="giveaway", allow_delete=True, allow_replacement=True,
    require_replacement_proof=True,
)
MUTATE_RENDER_CHANNEL = MessageMutationSpec()
MUTATE_RENDER_GUILD = MessageMutationSpec(cross_channel_acl="giveaway", allow_delete=True)


def post_location(record):
    return dict(record=record["id"], guild=record["guild"], channel=record["channel"], message=record["message"])


def card_prizes(spec, *, previous=None):
    prior = {p["id"]: p for p in (previous or {}).get("prizes", ())}
    return tuple(p for p in spec["prizes"] if p["kind"] == "card" and prior.get(p["id"]) != p)


def source_query(record, operation):
    edit = record.get("pending_edit") if operation == "edit" else None
    actor = edit["actor"] if edit else record["host"]
    spec = edit["spec"] if edit else record.get("spec")


    selected = revision_digest(json.dumps(
        [record.get("last_edit_job"), edit["draft"], edit["base"], spec],
        ensure_ascii=False, sort_keys=True,
    )) if edit else record.get("source_hash")
    binding = hashlib.sha256(encode((
        operation, record["id"], record["revision"], record["host"], engine.signature(record),
        selected,
    ))).hexdigest()
    if edit or v2.is_v2(record):
        roles = {
            role for rule in [*spec.get("baseline", []), spec["rules"]]
            for role in (*rule["roles"], *rule["excluded"])
        }
        cards = card_prizes(spec, previous=record["spec"] if edit else None)
        sources = tuple(MessageRef(prize["source_channel"], prize["card"]["message"]) for prize in cards)
    else:
        roles = ()
        sources = (MessageRef(record["source_channel"], record["card"]["message"]),)
    return EmbedSourceQuery(actor, record["host"], sources, tuple(sorted(roles)), binding)


HOST_PERMISSIONS = frozenset({
    "administrator", "manage_guild", "manage_messages", "moderate_members", "kick_members", "ban_members",
})
SOURCE_SCOPE = EmbedSourceSpec("giveaway", tuple(sorted(engine.BOTS)))


def publication_binding(record):
    delivery = record["deliveries"]["publish"]

    return dict(
        record=record["id"], guild=record["guild"], channel=record["channel"],
        revision=delivery.get("revision", record["revision"]), attempt=delivery.get("attempt"),
    )


def revision_digest(signature):
    return hashlib.sha256(signature.encode()).hexdigest()


def pending_edit_digest(record):
    pending = record.get("pending_edit")
    if not pending:
        return None
    return revision_digest(json.dumps(
        [record.get("last_edit_job"), pending], ensure_ascii=False, sort_keys=True,
    ))


DRAW_BATCH_SIZE = DISCORD_MEMBER_QUERY_LIMIT
DRAW_DESTINATION = ChannelMemberFactsSpec(
    channel_types=(0, 5),
    bot_permissions=("view_channel", "send_messages", "read_message_history", "attach_files"),
)
