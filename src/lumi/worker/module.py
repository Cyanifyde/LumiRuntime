from __future__ import annotations

from typing import Protocol

from lumi.contracts import Manifest, ResultEnvelope, TypedRequest


class WorkerModule(Protocol):
    @property
    def manifest(self) -> Manifest: ...

    async def handle(
        self, request: TypedRequest, job_id: str, generation: int
    ) -> ResultEnvelope: ...

    async def close(self) -> None: ...
