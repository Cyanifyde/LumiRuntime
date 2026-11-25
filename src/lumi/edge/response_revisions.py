import asyncio
import time
from contextlib import asynccontextmanager
from weakref import WeakValueDictionary

from lumi.contracts import ResponseRevisionBinding, decode
from lumi.shared.response_revisions import revision_key


@asynccontextmanager
async def revision_guard(edge, job, result):
    fence = result.response_revision
    if fence is None:
        yield True
        return
    if len(result.intents) != 1 or result.intents[0].session_id != fence.session:
        yield False
        return
    try:
        key = revision_key(job.group, fence.session)
    except ValueError:
        yield False
        return
    locks = getattr(edge, "_response_revision_locks", None)
    if locks is None:
        locks = edge._response_revision_locks = WeakValueDictionary()
    async with locks.setdefault(key, asyncio.Lock()):
        raw = await edge.cache_redis.get(key)
        try:
            binding = decode(raw, ResponseRevisionBinding) if raw is not None else None
        except ValueError:
            binding = None
        yield bool(
            binding
            and binding.module == job.module
            and binding.actor_id == job.actor_id
            and binding.guild_id == job.guild_id
            and binding.channel_id == job.channel_id
            and binding.revision == fence.revision
            and binding.expires_at_ms > int(time.time() * 1000)
        )
