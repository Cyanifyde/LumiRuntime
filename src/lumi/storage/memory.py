from __future__ import annotations

import asyncio


class MemoryControlStore:
    def __init__(self) -> None:
        self.blocks: set[int] = set()
        self.temporary_blocks: dict[int, int] = {}
        self.maintenance = False
        self.drains: set[str] = set()
        self.available = True
        self._lock = asyncio.Lock()

    def _check(self) -> None:
        if not self.available:
            raise ConnectionError("control store unavailable")

    async def migrate(self) -> None:
        self._check()

    async def load_blocks(self) -> frozenset[int]:
        self._check()
        return frozenset(self.blocks)

    async def add_block(self, user_id: int, actor_id: int) -> bool:
        self._check()
        async with self._lock:
            before = len(self.blocks)
            self.blocks.add(user_id)
            return len(self.blocks) != before

    async def load_temporary_blocks(self) -> dict[int, int]:
        self._check()
        return dict(self.temporary_blocks)

    async def add_temporary_block(
        self, user_id: int, expires_at_ms: int, source_module: str
    ) -> bool:
        del source_module
        self._check()
        async with self._lock:
            self.temporary_blocks[user_id] = max(
                expires_at_ms, self.temporary_blocks.get(user_id, 0)
            )
            return True

    async def remove_block(self, user_id: int) -> bool:
        self._check()
        async with self._lock:
            existed = user_id in self.blocks
            existed = existed or user_id in self.temporary_blocks
            self.blocks.discard(user_id)
            self.temporary_blocks.pop(user_id, None)
            return existed

    async def delete_expired_temporary_blocks(self, now_ms: int) -> int:
        self._check()
        async with self._lock:
            expired = [
                user_id
                for user_id, expires_at_ms in self.temporary_blocks.items()
                if expires_at_ms <= now_ms
            ]
            for user_id in expired:
                self.temporary_blocks.pop(user_id, None)
            return len(expired)

    async def get_maintenance(self) -> bool:
        self._check()
        return self.maintenance

    async def set_maintenance(self, enabled: bool) -> None:
        self._check()
        self.maintenance = enabled

    async def load_drains(self) -> frozenset[str]:
        self._check()
        return frozenset(self.drains)

    async def set_drain(self, group: str, enabled: bool) -> None:
        self._check()
        if enabled:
            self.drains.add(group)
        else:
            self.drains.discard(group)
