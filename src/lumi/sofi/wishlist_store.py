from __future__ import annotations

from dataclasses import dataclass

import msgspec

from .store import MySQLSofiStore

SCHEMA = (
    """
    CREATE TABLE IF NOT EXISTS sofi_wishlists (
        guild_id BIGINT UNSIGNED NOT NULL,
        owner_id BIGINT UNSIGNED NOT NULL,
        state_json JSON NOT NULL,
        updated_at TIMESTAMP(6) NOT NULL DEFAULT CURRENT_TIMESTAMP(6)
            ON UPDATE CURRENT_TIMESTAMP(6),
        PRIMARY KEY (guild_id, owner_id)
    ) ENGINE=InnoDB DEFAULT CHARSET=utf8mb4 COLLATE=utf8mb4_bin
    """,
)


@dataclass(frozen=True)
class WishlistEntry:
    position: int
    name: str
    series: str
    wishes: int


@dataclass(frozen=True)
class WishlistPage:
    number: int
    order: int
    entries: tuple[WishlistEntry, ...]


@dataclass(frozen=True)
class WishlistObservation:
    guild_id: int
    owner_id: int
    owner_label: str
    observer_id: int
    channel_id: int
    message_id: int
    created_at_ms: int
    total: int
    capacity: int
    page_count: int
    page: WishlistPage


@dataclass(frozen=True)
class SavedWishlist:
    observation: WishlistObservation
    pages: tuple[WishlistPage, ...]

    entries: tuple[WishlistEntry, ...] = ()
    complete: bool = False
    floor_order: int = 0


def validate_observation(value: WishlistObservation) -> None:
    if any(
        not 10**16 <= identifier < 2**64
        for identifier in (
            value.guild_id,
            value.owner_id,
            value.observer_id,
            value.channel_id,
            value.message_id,
        )
    ):
        raise ValueError("invalid wishlist identity")
    if not (
        value.created_at_ms > 0
        and value.page.order >= value.created_at_ms * 1000
        and 0 <= value.total <= value.capacity <= 100
        and 1 <= value.page.number <= value.page_count <= max(value.total, 1)
        and 0 < len(value.owner_label) <= 100
    ):
        raise ValueError("invalid wishlist bounds")
    entries = value.page.entries
    if value.total == 0:
        if entries or value.page_count != 1:
            raise ValueError("invalid empty wishlist")
    elif not entries or len(entries) > value.total:
        raise ValueError("missing wishlist entries")
    positions = [item.position for item in entries]
    if positions and (
        positions != list(range(positions[0], positions[0] + len(positions)))
        or not 1 <= positions[0] <= positions[-1] <= value.total
    ):
        raise ValueError("invalid wishlist positions")
    for item in entries:
        if (
            not 0 <= item.wishes <= 2_147_483_647
            or not 0 < len(item.name) <= 240
            or not 0 < len(item.series) <= 240
            or "\n" in item.name + item.series
            or "\r" in item.name + item.series
        ):
            raise ValueError("invalid wishlist entry")


def merge_observation(
    previous: SavedWishlist | None, incoming: WishlistObservation
) -> SavedWishlist:

    validate_observation(incoming)
    pages: dict[int, WishlistPage] = {}
    entries = previous.entries if previous else ()
    floor_order = incoming.created_at_ms * 1000
    if previous:
        old = previous.observation
        if (old.guild_id, old.owner_id) != (incoming.guild_id, incoming.owner_id):
            raise ValueError("wishlist owner mismatch")
        if incoming.message_id < old.message_id:
            return previous
        if incoming.message_id == old.message_id:
            if (old.channel_id, old.created_at_ms, old.observer_id) != (
                incoming.channel_id,
                incoming.created_at_ms,
                incoming.observer_id,
            ):
                raise ValueError("wishlist source mismatch")
            floor_order = previous.floor_order
            if incoming.page.order < floor_order:
                return previous
            same_size = (old.total, old.capacity, old.page_count) == (
                incoming.total,
                incoming.capacity,
                incoming.page_count,
            )
            if not same_size and incoming.page.order <= old.page.order:
                return previous
            if not same_size:
                floor_order = incoming.page.order
            if same_size:
                pages = {page.number: page for page in previous.pages}
                saved = pages.get(incoming.page.number)
                if saved and incoming.page.order <= saved.order:
                    return previous
                if saved and tuple((e.position, e.name, e.series) for e in saved.entries) != tuple(
                    (e.position, e.name, e.series) for e in incoming.page.entries
                ):
                    pages = {}
                    floor_order = incoming.page.order
    pages[incoming.page.number] = incoming.page
    ordered = tuple(pages[key] for key in sorted(pages))
    candidate = tuple(item for page in ordered for item in page.entries)
    complete = tuple(sorted(pages)) == tuple(range(1, incoming.page_count + 1)) and (
        tuple(item.position for item in candidate) == tuple(range(1, incoming.total + 1))
        and len({(item.name, item.series) for item in candidate}) == incoming.total
    )

    if complete:
        entries = candidate
    observation = incoming
    if previous and incoming.message_id == previous.observation.message_id:
        if incoming.page.order < previous.observation.page.order:
            observation = previous.observation
    return SavedWishlist(observation, ordered, entries, complete, floor_order)


class MySQLWishlistStore:
    def __init__(self, store: MySQLSofiStore) -> None:
        self._store = store

    async def migrate(self) -> None:
        async with self._store._require_pool().acquire() as connection:
            async with connection.cursor() as cursor:
                for statement in SCHEMA:
                    await cursor.execute(statement)
            await connection.commit()

    async def wishlist(self, guild_id: int, owner_id: int) -> SavedWishlist | None:
        async with self._store._require_pool().acquire() as connection:
            async with connection.cursor() as cursor:
                await cursor.execute(
                    "SELECT state_json FROM sofi_wishlists WHERE guild_id=%s AND owner_id=%s",
                    (guild_id, owner_id),
                )
                row = await cursor.fetchone()
            await connection.rollback()
        return msgspec.json.decode(row[0], type=SavedWishlist) if row else None

    async def observe(self, incoming: WishlistObservation) -> SavedWishlist:
        initial = merge_observation(None, incoming)
        async with self._store._require_pool().acquire() as connection:
            try:
                async with connection.cursor() as cursor:
                    await cursor.execute(
                        "INSERT INTO sofi_wishlists (guild_id,owner_id,state_json) "
                        "VALUES (%s,%s,%s) ON DUPLICATE KEY UPDATE owner_id=VALUES(owner_id)",
                        (incoming.guild_id, incoming.owner_id, msgspec.json.encode(initial)),
                    )
                    await cursor.execute(
                        "SELECT state_json FROM sofi_wishlists "
                        "WHERE guild_id=%s AND owner_id=%s FOR UPDATE",
                        (incoming.guild_id, incoming.owner_id),
                    )
                    row = await cursor.fetchone()
                    assert row is not None
                    previous = msgspec.json.decode(row[0], type=SavedWishlist)
                    merged = merge_observation(previous, incoming)
                    if merged != previous:
                        await cursor.execute(
                            "UPDATE sofi_wishlists SET state_json=%s "
                            "WHERE guild_id=%s AND owner_id=%s",
                            (msgspec.json.encode(merged), incoming.guild_id, incoming.owner_id),
                        )
                await connection.commit()
                return merged
            except BaseException:
                await connection.rollback()
                raise
