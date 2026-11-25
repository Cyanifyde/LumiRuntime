import secrets
from contextlib import suppress

from redis.exceptions import RedisError

from lumi.ui_keys import read_ui

from .operation_tags import tag_operation
from .rename import RUNNABLE, TERMINAL, TTL, declined, parse
from .rules import identifier


class RenameControls:
    def __init__(self, store, queue, cache, group, clock, *, wake, revoke):
        self.store, self.queue, self.cache, self.group, self.clock = store, queue, cache, group, clock
        self.wake, self.revoke = wake, revoke

    async def handle(self, request):
        event, args = request.event, request.arguments
        guild, actor, channel, source = (
            event.get(key) for key in ("guild_id", "actor_id", "channel_id", "message_id")
        )
        if (
            any(type(i) is not int or i <= 0 for i in (guild, actor, channel, source))
            or not self.live(event)
        ):
            return "unavailable", None, False
        if request.handler == "moderation.rename_confirm":
            return await self.confirm(request, guild, actor, channel, source)
        if not (
            event.get("dispatch") == "command"
            and request.route == request.handler == "moderation.rename"
        ):
            return "unavailable", None, False

        operation, name = parse(args.get("raw"))
        key = identifier(guild, source, "rename-" + operation)
        if operation == "start":
            record = dict(guild=guild, actor=actor, channel=channel, source=source, key=key,
                          name=name, created=self.clock())
            await tag_operation(self.queue, record, source_key=key)
            if not self.live(event):
                return "unavailable", None, False
            row, status = await self.store.create(record)
            if row is None:
                return status, None, False
            if (row["actor"], row["name"], row["channel"]) != (actor, name, channel):
                return "unavailable", None, False
            if status == "created":
                await self.nudge()
            return "status", row, not row["message"]

        row = await self.store.get(guild)
        if row is None:
            return "missing", None, False
        if row["actor"] != actor:
            return "unavailable", None, False
        now, bind = self.clock(), False
        changes = None
        if operation == "undo" and row["state"] not in TERMINAL:
            if row["state"] != "undo_confirm" and row["mode"] != "undo":
                changes = dict(state="undo_confirm", previous=row["state"], expires=now + TTL,
                               token=secrets.token_hex(8), message=0, native_panel=False, channel=channel,
                               next=now + TTL, error="")
                bind = True
        elif operation == "cancel" and row["state"] not in TERMINAL:
            changes = dict(state="expired" if row["state"] in {"listing", "confirm"} else "cancelled",
                           error="", next=now)
        elif operation == "resume" and (
            row["state"] in {"paused", "cancelled"}
            or (row["state"] in RUNNABLE and row["message"] and not row.get("native_panel"))
        ):
            changes = dict(state="queued" if row["mode"] == "undo" or row["listed"] else "listing",
                           next=now, retry=True, error="", channel=channel)
            if row["state"] == "undo_confirm" or (row["mode"] == "apply" and row["listed"] and not row["approved"]):
                confirmation = "undo_confirm" if row["state"] == "undo_confirm" else "confirm"
                changes.update(state=confirmation, expires=now + TTL, next=now + TTL,
                               message=0, native_panel=False, token=secrets.token_hex(8))
                bind = True
            elif not row["message"] or not row.get("native_panel") or row["channel"] != channel:
                changes.update(message=0, native_panel=False, token=secrets.token_hex(8))
                bind = True
        if changes is not None:
            if not self.live(event):
                return "unavailable", None, False
            previous = row
            row = await self.store.update(previous, changes, self.clock(), receipt=key)
            if row is None:
                return "unavailable", None, False


            await self.revoke(previous)
            await self.nudge()
        return "status", row, bind

    async def confirm(self, request, guild, actor, channel, source):
        event, args = request.event, request.arguments
        if (
            event.get("dispatch") != "component"
            or request.route not in {"moderation.rename_confirm", "moderation.rename_decline"}
            or not args.get("session") or not args.get("rename_id")
        ):
            return "expired", None, False
        row = await self.store.get(guild, args["rename_id"])
        if not row or (row["actor"], row["channel"], row["token"]) != (
            actor, channel, args["session"],
        ) or row["state"] not in {"confirm", "undo_confirm"}:
            return "expired", None, False
        bound = await read_ui(self.cache, "message", row["token"], self.group)
        if not bound or int(bound) != source or not self.live(event) or row["expires"] <= self.clock():
            return "expired", None, False
        if row.get("message") and not row.get("native_panel"):
            return "expired", None, False
        changes = dict(next=self.clock(), message=source, native_panel=True, expires=0)
        if request.route == "moderation.rename_confirm":
            changes.update(state="queued", mode="undo" if row["state"] == "undo_confirm" else "apply",
                           error="")
            if row["state"] == "confirm":
                changes["approved"] = True
        else:
            changes["state"] = declined(row)
        previous = row
        row = await self.store.update(previous, changes, self.clock())
        if row is None:
            return "expired", None, False
        await self.revoke(previous)
        await self.nudge()
        return "status", row, False

    def live(self, event):
        return self.clock() < event.get("_expires_at_ms", 0)

    async def nudge(self):

        with suppress(RedisError, OSError, TimeoutError):
            await self.wake.notify()
