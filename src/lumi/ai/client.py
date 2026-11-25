from __future__ import annotations

import time

import aiohttp

from lumi.contracts import AICallRequest, AICallResult, decode, encode


class AIUnavailable(RuntimeError):
    pass


class AIClient:
    def __init__(
        self,
        *,
        endpoint: str = "http://worker-ai:8080/v1/calls",
        session: aiohttp.ClientSession | None = None,
    ) -> None:
        self._endpoint = endpoint
        self._session = session
        self._owns_session = session is None

    async def start(self) -> None:
        if self._session is None or self._session.closed:
            self._session = aiohttp.ClientSession(
                connector=aiohttp.TCPConnector(limit=8, ttl_dns_cache=60)
            )

    async def call(self, request: AICallRequest) -> AICallResult:
        await self.start()
        assert self._session is not None
        remaining = (request.expires_at_ms - int(time.time() * 1_000)) / 1_000
        if remaining <= 0:
            raise AIUnavailable("AI request has expired")
        try:
            async with self._session.post(
                self._endpoint,
                data=encode(request),
                headers={"Content-Type": "application/json", "Accept": "application/json"},
                allow_redirects=False,
                timeout=aiohttp.ClientTimeout(total=min(remaining + 1, 125)),
            ) as response:
                payload = await response.content.read(64 * 1024 + 1)
                if response.status != 200 or len(payload) > 64 * 1024:
                    raise AIUnavailable("AI worker rejected the request")
                result = decode(payload, AICallResult)
                if result.call_id != request.call_id:
                    raise AIUnavailable("AI worker returned a mismatched response")
                return result
        except (TimeoutError, aiohttp.ClientError, ValueError) as exc:
            raise AIUnavailable("AI worker is unavailable") from exc

    async def close(self) -> None:
        if self._owns_session and self._session is not None:
            await self._session.close()
        self._session = None
