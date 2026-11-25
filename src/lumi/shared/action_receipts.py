from lumi.contracts.channel_messages import DELETE_CHANNEL_MESSAGES
from lumi.contracts.channel_resources import WRITE_CHANNEL_RESOURCE
from lumi.contracts.layout_message import ATTEMPT, WRITE_LAYOUT_MESSAGE, layout_receipt
from lumi.contracts.member_effects import MEMBER_EFFECT_PERMISSIONS, member_receipt_key
from lumi.contracts.member_profiles import SET_MEMBER_NICKNAME
from lumi.contracts.role_access import SET_ROLE_MEMBERSHIP, WRITE_ACTOR_ROLES, role_write_receipt
from lumi.contracts.staff_moderation import STAFF_EFFECT
from lumi.shared.discord_effects import MUTATE_MESSAGE, PUBLISH_MESSAGE, UPDATE_BOUND_PANEL, effect_receipt_key
from lumi.shared.mention_batches import projected_receipt_key

SCOPED_EFFECTS = frozenset({
    PUBLISH_MESSAGE, MUTATE_MESSAGE, SET_MEMBER_NICKNAME, UPDATE_BOUND_PANEL, DELETE_CHANNEL_MESSAGES, STAFF_EFFECT,
    WRITE_CHANNEL_RESOURCE,
})


def action_receipt_key(action):
    group, module, attempt = action.group, action.module, action.idempotency_key
    if action.kind == "send_mention_batch":
        return projected_receipt_key(group, module, attempt)
    if action.kind in SCOPED_EFFECTS and attempt.startswith("effect:"):
        return effect_receipt_key(group, module, attempt)
    if action.kind in MEMBER_EFFECT_PERMISSIONS:
        return member_receipt_key(group, module, action.job_id)
    if action.kind in {SET_ROLE_MEMBERSHIP, WRITE_ACTOR_ROLES}:
        return role_write_receipt(group, module, attempt)
    if action.kind == WRITE_LAYOUT_MESSAGE and ATTEMPT.fullmatch(attempt):
        return layout_receipt(group, module, attempt)
    return f"lumi:edge-action-receipt:{module}:{attempt}"
