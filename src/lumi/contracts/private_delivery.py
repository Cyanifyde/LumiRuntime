from typing import Annotated

import msgspec

PRIVATE_TEXT_LIMIT = 3900
UPLOAD_LIMIT = 25 * 1024**2
CHUNK_BYTES = 256 * 1024
CHUNK_ENCODING_LIMIT = 4 * ((CHUNK_BYTES + 2) // 3)
Filename = Annotated[str, msgspec.Meta(pattern=r"\A[a-zA-Z0-9][a-zA-Z0-9_.-]{0,95}\Z")]
PrivateText = Annotated[str, msgspec.Meta(max_length=PRIVATE_TEXT_LIMIT)]
EncodedChunk = Annotated[str, msgspec.Meta(max_length=CHUNK_ENCODING_LIMIT)]


class InlineAttachment(msgspec.Struct, frozen=True, forbid_unknown_fields=True):
    filename: Filename
    data: EncodedChunk


class PrivateReply(msgspec.Struct, frozen=True, forbid_unknown_fields=True, omit_defaults=True):
    text: PrivateText
    attachment: InlineAttachment | None = None


class PrivateCompletion(msgspec.Struct, frozen=True, forbid_unknown_fields=True):
    subject: Annotated[int, msgspec.Meta(ge=1)]
    text: Annotated[str, msgspec.Meta(min_length=1, max_length=PRIVATE_TEXT_LIMIT)]


class AttachmentChunk(msgspec.Struct, frozen=True, forbid_unknown_fields=True):
    filename: Filename
    size: Annotated[int, msgspec.Meta(ge=1, le=UPLOAD_LIMIT)]
    offset: Annotated[int, msgspec.Meta(ge=0, le=UPLOAD_LIMIT)]
    data: EncodedChunk
