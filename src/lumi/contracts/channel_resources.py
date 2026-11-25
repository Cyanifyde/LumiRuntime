import hashlib
from typing import Literal

import msgspec

READ_CHANNEL_RESOURCES = "read_channel_resources"
WRITE_CHANNEL_RESOURCE = "write_channel_resource"
CHANNEL_RESOURCE_ACTIONS = frozenset({READ_CHANNEL_RESOURCES, WRITE_CHANNEL_RESOURCE})
POST_READS = frozenset({"inspect_post", "locate_post", "list_posts"})
POST_WRITES = frozenset({"publish_post", "edit_post", "react", "pin"})
READS = frozenset({"observe", "find_created", "transport_state"}) | POST_READS
WRITES = frozenset({"clone", "edit", "flags", "delete"}) | POST_WRITES

ROLLBACK_POST_LIMIT = 500


class ChannelResourceAccess(msgspec.Struct, frozen=True, forbid_unknown_fields=True):
    operations: tuple[str, ...]


class TextChannelEdit(msgspec.Struct, frozen=True, forbid_unknown_fields=True):
    name: str
    topic: str | None
    position: int
    nsfw: bool
    slowmode: int
    archive: int
    thread_slowmode: int


class ChannelResourcePost(msgspec.Struct, frozen=True, forbid_unknown_fields=True, omit_defaults=True):
    message: int | None = None
    nonce: str | None = None
    module: str | None = None
    group: str | None = None
    owner: int | None = None
    emoji: str | None = None
    session: str | None = None


class ChannelResourceRequest(msgspec.Struct, frozen=True, forbid_unknown_fields=True, omit_defaults=True):
    operation: Literal[
        "observe", "find_created", "transport_state", "clone", "edit", "flags", "delete", "inspect_post", "locate_post", "list_posts",
        "publish_post", "edit_post", "react", "pin",
    ]
    transition: str
    target: int
    relative_to_peer: bool = False
    compare: str | None = None
    reason: str | None = None
    name: str | None = None
    edit: TextChannelEdit | None = None
    flags: int | None = None
    post: ChannelResourcePost | None = None
    only_owned_posts: tuple[int, ...] | None = None
    delivery_state: bool = False
    companion: bool = False
    cleanup: bool = False
    peer_compare: str | None = None


def configuration_digest(settings):

    layout = sorted((name, value) for name, value in settings.items() if name != "position")
    return hashlib.sha256(msgspec.json.encode(layout)).hexdigest()


def resource_payload(query, intent):
    return hashlib.sha256(msgspec.json.encode((query, intent))).hexdigest()
