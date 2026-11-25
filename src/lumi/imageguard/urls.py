from __future__ import annotations

import hashlib
import json
import re
from urllib.parse import unquote, urlsplit

HOSTS = frozenset({"cdn.discordapp.com", "media.discordapp.net"})
EXTENSIONS = (".png", ".jpg", ".jpeg", ".webp", ".gif")
URL_PREFIX = (
    r'https://(?:cdn\.discordapp\.com|media\.discordapp\.net)'
    r'/(?:attachments|ephemeral-attachments)/'
)
URL_PATTERN = URL_PREFIX + r'[^\s<>"\x00-\x1f]+'
IMAGE_PATH = URL_PREFIX + r'[^\s<>"\x00-\x1f?#]+\.(?i:png|jpe?g|webp|gif)'
IMAGE_ATTACHMENT = "^" + IMAGE_PATH + r'(?:\?[^\s<>"\x00-\x1f#]*)?$'
IMAGE_LINK = IMAGE_PATH + r'(?:\?|[).,;!\]]*(?:$|[\s<>"\x00-\x1f]))'


def canonical(url):
    if not isinstance(url, str) or len(url) > 2048:
        return None
    try:
        p = urlsplit(url)
        if (
            p.scheme != "https"
            or p.netloc not in HOSTS
            or p.fragment
            or not p.path.startswith(("/attachments/", "/ephemeral-attachments/"))
            or "\\" in unquote(p.path)
            or any(part in {".", ".."} for part in unquote(p.path).split("/"))
            or not p.path.lower().endswith(EXTENSIONS)
        ):
            return None
        return p.netloc + p.path
    except ValueError:
        return None


def signature(images):
    return hashlib.sha256(json.dumps([canonical(i["url"]) for i in images]).encode()).hexdigest()


def _candidates(attachments, links):
    result, seen = [], set()
    for items in (attachments, links):
        for url, attachment_id in items:
            key = canonical(url)
            if key and key not in seen:
                seen.add(key)
                result.append(dict(url=url, attachment_id=attachment_id))
            if len(result) == 10:
                return result
    return result


def observed_sources(arguments):

    return _candidates(
        ((row["url"], 0) for row in arguments.get("attachments.url", ())),
        ((row["url"].rstrip(").,;!]"), 0) for row in arguments.get("content", ())),
    )


def sources(event):

    return _candidates(
        (
            (item.get("url"), int(item.get("id") or 0))
            for item in event.get("attachments", ())
            if isinstance(item, dict)
        ),
        (
            (match[0].rstrip(").,;!]"), 0)
            for match in re.finditer(URL_PATTERN, str(event.get("content") or "")[:4000])
        ),
    )
