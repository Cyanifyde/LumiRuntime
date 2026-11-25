from __future__ import annotations

import asyncio
from collections import OrderedDict, deque
from collections.abc import Callable
from dataclasses import dataclass
from typing import TypeVar

T = TypeVar("T")


class RenderBusyError(RuntimeError):
    pass


class RenderSupersededError(RuntimeError):
    pass


@dataclass(slots=True)
class _Work[T]:
    actor_id: int
    key: str
    function: Callable[[], T]
    future: asyncio.Future[T]


class FairRenderScheduler:


    def __init__(self, *, maximum: int = 16) -> None:
        self._maximum = maximum
        self._queues: OrderedDict[int, deque[_Work[object]]] = OrderedDict()
        self._active_actor: int | None = None
        self._accepted = 0
        self._accepting = True
        self._condition = asyncio.Condition()
        self._runner: asyncio.Task[None] | None = None
        self._wake_task: asyncio.Task[None] | None = None

    async def start(self) -> None:
        if self._runner is None:
            self._runner = asyncio.create_task(self._run(), name="karuta-sketch-renderer")

    async def submit(self, actor_id: int, key: str, function: Callable[[], T]) -> T:
        await self.start()
        loop = asyncio.get_running_loop()
        future: asyncio.Future[T] = loop.create_future()
        async with self._condition:
            if not self._accepting:
                raise RenderBusyError
            queue = self._queues.get(actor_id, deque())
            actor_count = len(queue) + int(self._active_actor == actor_id)
            for index, existing in enumerate(queue):
                if existing.key == key:
                    if not existing.future.done():
                        existing.future.set_exception(RenderSupersededError())
                    queue[index] = _Work(actor_id, key, function, future)
                    self._condition.notify()
                    break
            else:
                if self._accepted >= self._maximum or actor_count >= 2:
                    raise RenderBusyError
                queue.append(_Work(actor_id, key, function, future))
                self._queues[actor_id] = queue
                self._accepted += 1
                self._condition.notify()
        return await future

    async def _run(self) -> None:
        while True:
            async with self._condition:
                await self._condition.wait_for(
                    lambda: any(self._queues.values()) or not self._accepting
                )
                if not any(self._queues.values()) and not self._accepting:
                    return
                actor_id, queue = self._queues.popitem(last=False)
                work = queue.popleft()
                if queue:
                    self._queues[actor_id] = queue
                self._active_actor = actor_id
            try:
                if work.future.done():


                    continue
                result = await asyncio.to_thread(work.function)
            except BaseException as exc:
                if not work.future.done():
                    work.future.set_exception(exc)
            else:
                if not work.future.done():
                    work.future.set_result(result)
            finally:
                async with self._condition:
                    self._active_actor = None
                    self._accepted -= 1
                    self._condition.notify_all()

    def begin_drain(self) -> None:
        self._accepting = False
        if self._runner is not None:
            self._wake_task = asyncio.create_task(self._notify())

    def resume(self) -> None:
        self._accepting = True
        if self._runner is None or self._runner.done():
            self._runner = asyncio.create_task(self._run(), name="karuta-sketch-renderer")

    def drain_ready(self) -> bool:
        return self._accepted == 0 and self._active_actor is None

    async def close(self) -> None:
        self._accepting = False
        await self._notify()
        if self._wake_task is not None:
            await self._wake_task
        if self._runner is not None:
            await self._runner

    async def _notify(self) -> None:
        async with self._condition:
            self._condition.notify_all()
