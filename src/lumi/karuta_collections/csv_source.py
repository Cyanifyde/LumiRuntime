from __future__ import annotations

import re
from urllib.parse import urlsplit

import aiohttp

from lumi.shared.downloads import read_limited_response

from .query import SPREADSHEET_URL

MAX_BYTES = 25 * 1024 * 1024
DISCORD_FILE_HOSTS = frozenset({"cdn.discordapp.com", "media.discordapp.net"})


class CSVSourceError(ValueError):
    pass


def modal_file(request) -> tuple[str, int]:
    files = request.arguments.get("files", {}).get("csv", ())
    if len(files) != 1 or not isinstance(files[0], dict):
        raise CSVSourceError("Attach one CSV.")
    file = files[0]
    url, size, identifier, filename = (
        file.get(key) for key in ("url", "size", "id", "filename")
    )
    if (
        not isinstance(url, str) or not isinstance(filename, str)
        or not filename.casefold().endswith(".csv")
        or type(size) is not int or not 0 < size <= MAX_BYTES
        or type(identifier) not in {str, int}
        or not re.fullmatch(r"[1-9][0-9]{0,19}", str(identifier))
    ):
        raise CSVSourceError("Invalid CSV.")
    try:
        parsed = urlsplit(url)
        pieces = parsed.path.split("/")
        if (
            parsed.scheme != "https" or parsed.netloc not in DISCORD_FILE_HOSTS
            or parsed.fragment or len(pieces) != 5
            or pieces[1] not in {"attachments", "ephemeral-attachments"}
            or not pieces[2].isascii() or not pieces[2].isdigit() or int(pieces[2]) <= 0
            or pieces[3] != str(identifier) or not pieces[4]
        ):
            raise ValueError("unapproved attachment URL")
    except ValueError:
        raise CSVSourceError("Invalid CSV.") from None
    return url, size


async def download_csv(request, export_url: str | None = None) -> bytes:
    if export_url is None:
        url, expected_size = modal_file(request)
    else:
        if not re.fullmatch(SPREADSHEET_URL, export_url):
            raise CSVSourceError("Invalid CSV.")
        url, expected_size = export_url, None
    try:
        async with aiohttp.ClientSession(timeout=aiohttp.ClientTimeout(total=20)) as session:
            async with session.get(url, allow_redirects=False) as response:
                if response.status != 200:
                    raise ValueError("download failed")
                data = await read_limited_response(response, limit=expected_size or MAX_BYTES)
        if not data or (expected_size is not None and len(data) != expected_size):
            raise ValueError("invalid attachment size")
        return data
    except (aiohttp.ClientError, ValueError, OSError, TimeoutError):
        raise CSVSourceError("Invalid CSV.") from None
