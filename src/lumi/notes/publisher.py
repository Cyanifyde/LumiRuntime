from __future__ import annotations

import hashlib

from lumi.contracts.reaction_subscriptions import ReactionSubscription, ReactionSubscriptions
from lumi.shared.reaction_subscriptions import publish_subscriptions

from .models import emoji_key


def shortcut_tag(emoji, duration_ms=0):
    return hashlib.sha256(f"{emoji_key(emoji)}:{duration_ms}".encode()).hexdigest()[:32]


def projections(snapshot):
    yield ReactionSubscriptions(
        1,
        "community",
        "notes",
        snapshot.user_id,
        snapshot.revision,
        (
            ReactionSubscription(
                snapshot.notes_emoji, "notes.reaction", shortcut_tag(snapshot.notes_emoji)
            ),
        )
        if snapshot.notes_emoji
        else (),
    )
    yield ReactionSubscriptions(
        1,
        "community",
        "reminders",
        snapshot.user_id,
        snapshot.revision,
        tuple(
            ReactionSubscription(
                mapping.emoji,
                "reminders.reaction",
                shortcut_tag(mapping.emoji, mapping.duration_ms),
            )
            for mapping in snapshot.reminders
        ),
    )


class ShortcutPublisher:
    def __init__(self, store, redis):
        self.store, self.redis = store, redis

    async def flush(self, *, restore=False):
        for snapshot in await self.store.publications(all_users=restore):
            for projection in projections(snapshot):
                await publish_subscriptions(self.redis, projection)
            await self.store.published(snapshot)
