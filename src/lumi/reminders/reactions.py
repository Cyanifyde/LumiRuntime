from __future__ import annotations

import hashlib
import secrets

from lumi.notes.models import emoji_key

PROMPT_WINDOW = 900_000

SCHEMA = (
    """CREATE TABLE IF NOT EXISTS reminder_reaction_state (
    state_key CHAR(64) CHARACTER SET ascii COLLATE ascii_bin PRIMARY KEY,
    guild_id BIGINT UNSIGNED NOT NULL,user_id BIGINT UNSIGNED NOT NULL,
    author_id BIGINT UNSIGNED NOT NULL,kind VARCHAR(16) NOT NULL,
    document JSON NOT NULL,INDEX owner(guild_id,user_id)) ENGINE=InnoDB""",
)


def key(*parts):
    return hashlib.sha256(":".join(map(str, parts)).encode()).hexdigest()


async def new_timer(tx, source, *, guild, user, channel, due, now, reaction_key, activation):
    if due <= now:
        return {"error": "That time has passed."}
    if await tx.count(user) >= 20 or await tx.count() >= 1000:
        return {"error": "Reminder limit reached."}
    for _ in range(8):
        identifier = secrets.token_hex(4)
        if not await tx.record(identifier):
            break
    else:
        raise RuntimeError("Reminder identifier unavailable.")
    text = " ".join(source["content"].split())[:200]
    jump = source["jump"]
    text = f"{text}\n{jump}" if text else jump
    record = dict(
        id=identifier,
        guild_id=guild,
        user_id=user,
        channel_id=channel,
        revision=1,
        current=None,
        created_ms=now,
        updated_ms=now,
        text=text,
        interval_ms=None,
        anchor_ms=due,
        next_ms=due,
        status="active",
        wake_ms=due,
        closed_ms=None,
        error=None,
        reaction=True,
        source_author_id=source["author_id"],
        reaction_key=reaction_key,
        activation=activation,
        delivery_deadline_ms=due + 300_000,
    )
    await tx.put_record(record)
    return {"kind": "create", "id": identifier, "due_ms": due, "interval_ms": None}


class ReactionReminders:
    async def reaction_delivery(self, guild, user):
        async with self.transaction(guild) as tx:
            row = await tx.state(key("delivery", guild, user))
            return row["mode"] if row else "dm"

    async def set_reaction_delivery(self, guild, user, mode):
        if mode not in {"dm", "channel"}:
            raise ValueError("Choose DM or Channel.")
        async with self.transaction(guild) as tx:
            await tx.put_state(
                key("delivery", guild, user), user, user, "preferences", {"mode": mode}
            )

    async def react(self, capture, *, user, now):
        message = capture.message
        guild, channel = message.guild_id, message.channel_id
        identifier = key("reaction", guild, user, message.message_id, emoji_key(capture.emoji))
        async with self.transaction(guild) as tx:
            link = await tx.state(identifier) or {}
            if link.get("reminder"):
                active = await tx.record(link["reminder"])
                if active and active["status"] in {"active", "paused"}:
                    return {"kind": "existing", "id": active["id"], "due_ms": active["next_ms"]}
            if link.get("activation") == capture.activation:
                return link.get("receipt", {"kind": "seen"})
            if link.get("prompt"):
                prompt = await tx.state(key("prompt", link["prompt"]))
                if prompt and prompt["status"] == "pending" and prompt["expires_at_ms"] > now:
                    return {
                        "kind": "prompt",
                        "token": prompt["token"],
                        "message_id": prompt.get("message_id"),
                    }
            source = {
                "content": message.content,
                "jump": message.jump_url,
                "author_id": message.author_id,
            }
            if capture.duration_ms:
                receipt = await new_timer(
                    tx,
                    source,
                    guild=guild,
                    user=user,
                    channel=channel,
                    due=capture.received_at_ms + capture.duration_ms,
                    now=now,
                    reaction_key=identifier,
                    activation=capture.activation,
                )
                link = {
                    "activation": capture.activation,
                    "receipt": receipt,
                    "reminder": receipt.get("id"),
                }
            else:
                if (
                    await tx.count(user) + await tx.pending_prompt_count(user, now) >= 20
                    or await tx.count() + await tx.pending_prompt_count(None, now) >= 1000
                ):
                    return {"error": "Reminder limit reached."}
                token = secrets.token_hex(16)
                prompt = dict(
                    token=token,
                    status="pending",
                    owner_id=user,
                    guild_id=guild,
                    channel_id=channel,
                    source=source,
                    reaction_key=identifier,
                    activation=capture.activation,
                    created_ms=now,
                    expires_at_ms=now + PROMPT_WINDOW,
                )
                await tx.put_state(key("prompt", token), user, message.author_id, "prompt", prompt)
                receipt = {"kind": "prompt", "token": token, "message_id": None}
                link = {"activation": capture.activation, "receipt": receipt, "prompt": token}
            await tx.put_state(identifier, user, message.author_id, "link", link)
            return receipt

    async def bind_reaction_prompt(self, guild, user, token, message_id, *, expires_at_ms=None):
        async with self.transaction(guild) as tx:
            prompt = await tx.state(key("prompt", token))
            if prompt and prompt["owner_id"] == user and prompt["status"] == "pending":
                prompt["message_id"] = message_id
                if (
                    type(expires_at_ms) is int
                    and prompt["expires_at_ms"]
                    <= expires_at_ms
                    <= prompt["expires_at_ms"] + 120_000
                ):
                    prompt["expires_at_ms"] = expires_at_ms
                await tx.put_state(
                    key("prompt", token), user, prompt["source"]["author_id"], "prompt", prompt
                )

    async def reaction_reply(
        self, *, guild, user, channel, token, prompt_id, delay, received_ms, now
    ):
        async with self.transaction(guild) as tx:
            prompt = await tx.state(key("prompt", token))
            if (
                not prompt
                or prompt["owner_id"] != user
                or prompt["guild_id"] != guild
                or prompt["channel_id"] != channel
                or prompt.get("message_id") != prompt_id
            ):
                return None
            if prompt["status"] == "complete":
                return prompt["receipt"]
            if (
                prompt["expires_at_ms"] <= now
                or received_ms > prompt["expires_at_ms"]
                or received_ms < prompt["created_ms"]
            ):
                return None
            receipt = await new_timer(
                tx,
                prompt["source"],
                guild=guild,
                user=user,
                channel=channel,
                due=received_ms + delay,
                now=now,
                reaction_key=prompt["reaction_key"],
                activation=prompt["activation"],
            )
            if receipt.get("error"):
                return receipt
            prompt.update(status="complete", receipt=receipt)
            author = prompt["source"]["author_id"]
            await tx.put_state(key("prompt", token), user, author, "prompt", prompt)
            await tx.put_state(
                prompt["reaction_key"],
                user,
                author,
                "link",
                {"activation": prompt["activation"], "receipt": receipt, "reminder": receipt["id"]},
            )
            return receipt
