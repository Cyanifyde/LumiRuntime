import re
from urllib.parse import urlsplit


def attachment_url(value: str, channel: int, attachment: int) -> bool:
    try:
        p = urlsplit(value)
        port = p.port
    except ValueError:
        return False
    return (
        len(value) <= 2048
        and p.scheme == "https"
        and not p.username
        and not p.password
        and port in {None, 443}
        and p.hostname in {"cdn.discordapp.com", "media.discordapp.net"}
        and bool(re.fullmatch(rf"/attachments/{channel}/{attachment}/[^/]+", p.path))
    )


def safe_image(value: object) -> str | None:
    value = str(value or "")
    try:
        p = urlsplit(value)
    except ValueError:
        return None
    if (
        p.scheme == "https"
        and p.hostname in {"cdn.discordapp.com", "media.discordapp.net"}
        and not p.username
        and not p.password
        and len(value) <= 2048
    ):
        return value
    return None


def profile_image(value: object) -> str | None:

    url = safe_image(value)
    if not url:
        return None
    path = urlsplit(url).path
    if re.fullmatch(r"/embed/avatars/[0-9]+\.png", path):
        return None
    match = re.fullmatch(
        r"(/avatars/[0-9]+|/guilds/[0-9]+/users/[0-9]+/avatars)/"
        r"([A-Za-z0-9_]+)\.(?:png|gif|webp|jpe?g)",
        path,
        re.IGNORECASE,
    )
    if not match:
        return url
    prefix, key = match.groups()
    extension = "gif" if key.startswith("a_") else "png"
    return f"https://cdn.discordapp.com{prefix}/{key}.{extension}?size=1024"
