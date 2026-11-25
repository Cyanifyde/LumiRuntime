from __future__ import annotations

import aiohttp


async def read_limited_response(response: aiohttp.ClientResponse, *, limit: int) -> bytes:
    if (response.content_length or 0) > limit:
        raise ValueError("download exceeds limit")
    body = bytearray()
    async for chunk in response.content.iter_chunked(64 * 1024):
        if len(body) + len(chunk) > limit:
            raise ValueError("download exceeds limit")
        body.extend(chunk)
    return bytes(body)
