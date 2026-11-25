from __future__ import annotations

from .store import AFKInteraction

MAX_CONVERSATION_MESSAGES = 5


def conversations(
    interactions: tuple[AFKInteraction, ...],
) -> tuple[tuple[AFKInteraction, ...], ...]:
    def key(item: AFKInteraction, message_id: int | None = None) -> tuple[int, int, int, int]:
        return (
            item.guild_id,
            item.user_id,
            item.channel_id,
            item.message_id if message_id is None else message_id,
        )

    by_id = {key(item): item for item in interactions}
    grouped: dict[tuple[int, int, int, int], list[AFKInteraction]] = {}
    for item in interactions:
        root = by_id.get(key(item, item.reply_to_message_id)) if item.reply_to_message_id else None
        if (
            root is None
            or root.reply_to_message_id is not None
            or root.message_id >= item.message_id
            or root.occurred_at > item.occurred_at
        ):
            root = item
        members = grouped.setdefault(key(root), [root])
        if item.message_id != root.message_id and all(
            member.message_id != item.message_id for member in members
        ):
            members.append(item)

    result: list[tuple[AFKInteraction, ...]] = []
    for members in grouped.values():
        root, *replies = members
        replies.sort(key=lambda item: (item.occurred_at, item.message_id))
        if not replies:
            result.append((root,))
            continue


        for start in range(0, len(replies), MAX_CONVERSATION_MESSAGES - 1):
            result.append((root, *replies[start : start + MAX_CONVERSATION_MESSAGES - 1]))
    return tuple(result)
