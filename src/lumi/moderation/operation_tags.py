from lumi.contracts import GuildPolicySnapshot, decode
from lumi.shared.discord_observations import identifier


async def tag_operation(queue, record, *, source_key=None):
    guild = record["guild"]
    raw = await queue.hget("lumi:policy:current", str(guild))
    if raw:
        tag_from_policy(decode(raw, GuildPolicySnapshot), record, source_key=source_key)


def tag_from_policy(policy, record, *, source_key=None):
    if policy.guild_id == record["guild"] and "modlog" in policy.enabled_modules:
        record.update(
            modlog_id=identifier("lumi-journal", source_key or record["id"]),
            modlog_revision=record["created"],
        )
