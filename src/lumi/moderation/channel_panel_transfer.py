import msgspec

from lumi.contracts import DiscordIntent, PresentationPage
from lumi.contracts.channel_resources import ChannelResourcePost, ChannelResourceRequest

from .purge import PurgeError


class ChannelPanelTransfer:
    def __init__(self, journal, transport):
        self.journal, self.transport = journal, transport

    async def update_panel(self, record, group, index, item, **changes):
        def update(latest):
            entries = latest.get("plans", {}).get(group, ())
            if latest["phase"] != record["phase"] or index >= len(entries) or entries[index] != item:
                raise PurgeError("A saved panel changed. Reset stopped.")
            entries[index].update(changes)

        return await self.journal.mutate(record["id"], update)

    async def step(self, row, record, job, authorize):
        for group, items in sorted(record["plans"].items()):
            for position, item in enumerate(items):
                if item.get("message") and item.get("decorated"):
                    continue
                await self.panel_step(row, record, job, authorize, group, position, item)
                return False
        return True

    async def panel_step(self, row, record, job, authorize, group, position, item):
        async def checkpoint(**changes):
            return await self.update_panel(record, group, position, item, **changes)

        if "source_pinned" not in item:
            pinned = False
            for source in dict.fromkeys(item["sources"]):
                result = await self.transport.request(row, job, ChannelResourceRequest(
                    "inspect_post", record["id"], record["old"], post=ChannelResourcePost(message=source),
                ))
                if not result.ok:
                    raise PurgeError("A saved panel could not be inspected.")
                if result.data["present"] and not result.data["owned"]:
                    raise PurgeError("A saved panel is not owned by Lumi.")
                pinned |= result.data.get("pinned", False)
            item = (await checkpoint(source_pinned=pinned))["plans"][group][position]

        if not item.get("message"):
            nonce = item.get("nonce") or f"reset:{record['id']}:{group[:1]}:{position}"
            intent = msgspec.convert(item["intent"], type=DiscordIntent)
            intent = msgspec.structs.replace(intent, presentation_mode=intent.presentation_mode or "v2")
            post = ChannelResourcePost(nonce=nonce, group=group, module=item["module"], owner=item.get("owner"))

            async def stage(attempt):
                await checkpoint(sending=True, nonce=nonce, publish_attempt=attempt)

            message = await self.publish(row, record, job, authorize, post, intent, stage,
                                         sending=item.get("sending"), attempt=item.get("publish_attempt"))
            if message is None:
                return


            latest = await self.journal.get(record["id"])
            staged = latest["plans"][group][position]
            if staged.get("nonce") != nonce or staged.get("key") != item.get("key") or staged.get("message"):
                raise PurgeError("A saved panel changed. Reset stopped.")
            await self.update_panel(record, group, position, staged, message=message, sending=False)
            return

        decorations = tuple(("react", emoji) for emoji in dict.fromkeys(item.get("reactions", ())))
        if item["source_pinned"]:
            decorations += (("pin", None),)
        offset = item.get("decorated_count", 0)
        if offset >= len(decorations):
            await checkpoint(decorated=True)
            return
        operation, emoji = decorations[offset]
        attempt = item.get("decorate_attempt")
        if attempt:
            result = await self.transport.receipt(attempt)
        else:
            attempt = self.transport.attempt()
            item = (await checkpoint(decorate_attempt=attempt))["plans"][group][position]
            result = await self.transport.request(row, job, ChannelResourceRequest(
                operation, record["id"], record["new"],
                reason=f"Lumi reset {row['id']}" if operation == "pin" else None,
                post=ChannelResourcePost(message=item["message"], module=item["module"], group=group, emoji=emoji),
            ), attempt=attempt, authorize=authorize)
        if not result or not result.ok:
            raise PurgeError("A replacement panel decoration could not be confirmed.")
        await checkpoint(decorated_count=offset + 1, decorate_attempt=None, decorated=offset + 1 == len(decorations))

    async def publish(self, row, record, job, authorize, post, intent, stage, *, sending=False, attempt=None):
        if not sending:
            attempt = self.transport.attempt()
            await stage(attempt)
            result = await self.transport.request(row, job, ChannelResourceRequest(
                "publish_post", record["id"], record["new"], post=post,
            ), attempt=attempt, authorize=authorize, intent=intent)
        else:
            result = await self.transport.receipt(attempt) if attempt else None
        if result is not None and result.ok:
            return result.data["message_id"]
        if result is not None and result.status in {"failed", "rejected"} and result.error_code not in {
            "ambiguous", "action_in_progress", "discord_action_failed", "unavailable",
        }:
            raise PurgeError("A replacement panel could not be published.")
        search = msgspec.structs.replace(post, session=intent.session_id)
        found = await self.transport.request(row, job, ChannelResourceRequest(
            "locate_post", record["id"], record["new"], post=search,
        ))
        matches = found.data.get("messages", ()) if found.ok else ()
        if len(matches) > 1:
            raise PurgeError("A replacement panel send is unconfirmed.")
        return matches[0] if matches else None

    async def progress(self, row, record, job, authorize):
        if record.get("progress_message"):
            return record["progress_message"]

        async def stage(attempt):
            def mark(latest):
                if latest["phase"] != record["phase"] or latest.get("progress_sending") or latest.get("progress_message"):
                    raise PurgeError("Reset progress changed.")
                latest.update(progress_sending=True, progress_attempt=attempt)

            await self.journal.mutate(record["id"], mark)

        post = ChannelResourcePost(nonce="reset-progress:" + row["id"], module=self.transport.module,
                                   group=self.transport.group)
        intent = DiscordIntent("send", pages=(PresentationPage(description="Resetting channel."),), presentation_mode="v2")
        message = await self.publish(row, record, job, authorize, post, intent, stage,
                                     sending=record.get("progress_sending"), attempt=record.get("progress_attempt"))
        if message is None:
            return None

        def finish(latest):
            if latest["phase"] != record["phase"] or not latest.get("progress_sending"):
                raise PurgeError("Reset progress changed.")
            latest.update(progress_message=message, progress_sending=False)

        await self.journal.mutate(record["id"], finish)
        return message
