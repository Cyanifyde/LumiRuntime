from lumi.contracts import MentionBatchPlan, MentionBatchRef, encode


def plan_key(group: str, module: str, reference: MentionBatchRef) -> str:
    return f"lumi:mention-batch:{group}:{module}:{reference.batch_id}:{reference.revision}"


def receipt_identity(reference: MentionBatchRef) -> str:
    return f"mention-batch:{reference.batch_id}:{reference.revision}"


def projected_receipt_key(group: str, module: str, identity: str) -> str:
    return f"lumi:edge-action-receipt:{group}:{module}:{identity}"


async def publish_plan(redis, plan: MentionBatchPlan, now_ms: int) -> bool:
    remaining = plan.expires_at_ms - now_ms
    if remaining <= 0:
        return False
    return bool(await redis.set(
        plan_key(plan.group, plan.module, plan.reference), encode(plan), nx=True, px=remaining,
    ))
