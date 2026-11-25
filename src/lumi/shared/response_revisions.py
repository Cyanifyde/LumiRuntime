import re

from lumi.contracts import ResponseRevisionBinding, encode
from lumi.transport import GROUPS


def revision_key(group, session):
    if group not in GROUPS or not re.fullmatch(r"[a-f0-9]{16}", session):
        raise ValueError("invalid response revision scope")
    return f"lumi:ui-response-revision:{group}:{session}"


def publish_revision(
    pipeline, group, session, *, module, actor_id, guild_id, channel_id,
    revision, expires_at_ms, ttl,
):

    binding = ResponseRevisionBinding(module, actor_id, guild_id, channel_id, revision, expires_at_ms)
    pipeline.set(revision_key(group, session), encode(binding), ex=ttl)
