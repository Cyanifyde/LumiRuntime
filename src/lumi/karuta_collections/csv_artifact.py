import hashlib

from lumi.artifact_paths import karuta_csv_root as artifact_root

MAX_BYTES = 25 * 1024 * 1024
ARTIFACTS = "lumi:karuta-csv:artifacts"


def read(reference, root, *, owner, guild, channel, source, now):
    if (
        reference.owner_id != owner
        or reference.guild_id != guild
        or reference.channel_id != channel
        or reference.message_id != source
        or reference.expires_at_ms <= now
        or not 0 < reference.size <= MAX_BYTES
    ):
        raise ValueError("invalid CSV reference")
    root = artifact_root(root)
    if root.is_symlink():
        raise ValueError("invalid CSV directory")
    root = root.resolve()
    path = root / reference.reference
    if (
        len(reference.reference) != 64
        or any(c not in "0123456789abcdef" for c in reference.reference)
        or root not in path.resolve().parents
        or path.is_symlink()
        or path.stat().st_size != reference.size
    ):
        raise ValueError("invalid CSV artifact")
    data = path.read_bytes()
    if hashlib.sha256(data).hexdigest() != reference.sha256:
        raise ValueError("CSV artifact changed")
    return data


async def prune(cache, temp_root, now):
    import asyncio

    root = artifact_root(temp_root)
    if root.is_symlink():
        raise ValueError("invalid CSV directory")
    for raw in await cache.zrangebyscore(ARTIFACTS, "-inf", now, start=0, num=128):
        reference = raw.decode() if isinstance(raw, bytes) else raw
        if len(reference) != 64 or any(c not in "0123456789abcdef" for c in reference):
            raise ValueError("invalid CSV reference")
        path = root / reference
        if path.is_symlink():
            raise ValueError("invalid CSV artifact")
        await asyncio.to_thread(path.unlink, missing_ok=True)
        await cache.zrem(ARTIFACTS, raw)
