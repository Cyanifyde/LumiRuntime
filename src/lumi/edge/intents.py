from __future__ import annotations

import re
import time
from dataclasses import dataclass
from urllib.parse import urlsplit

import emoji as emoji_lib

from lumi.contracts import (
    DiscordIntent,
    PresentationBlock,
    PresentationControl,
    PresentationPage,
    TemporaryAttachment,
)
from lumi.contracts.layout_message import MAX_LAYOUT_COMPONENTS, ordered_component_count
from lumi.shared.attachment_payloads import BYTE_LIMIT, FILE_LIMIT


class InvalidIntent(ValueError):
    pass


@dataclass(frozen=True, slots=True)
class ValidatedIntent:
    kind: str
    content: str | None
    pages: tuple[PresentationPage, ...]
    attachment_ref: str | None
    session_id: str | None
    presentation_mode: str
    controls: tuple[PresentationControl, ...] = ()
    attachments: tuple[TemporaryAttachment, ...] = ()
    mention_actor: bool = False
    public_controls: bool = False


class IntentValidator:
    ALLOWED_KINDS = frozenset({"send", "send_parts", "edit", "paginator", "ephemeral"})
    PRESENTATION_MODES = frozenset({"v2", "legacy", "plain"})

    def validate(
        self,
        intent: DiscordIntent,
        *,
        presentation_mode: str = "v2",
        presentation_modes: tuple[str, ...] = (),
        retained_uploads: frozenset[str] = frozenset(),
        scheduled_files: bool = False,
        layout_message: bool = False,
    ) -> ValidatedIntent:
        if intent.kind not in self.ALLOWED_KINDS:
            raise InvalidIntent("unsupported Discord intent")
        allowed_modes = presentation_modes or (presentation_mode,)
        if (
            presentation_mode not in self.PRESENTATION_MODES
            or not allowed_modes
            or presentation_mode not in allowed_modes
            or len(allowed_modes) != len(set(allowed_modes))
            or not set(allowed_modes).issubset(self.PRESENTATION_MODES)
        ):
            raise InvalidIntent("unsupported presentation mode")
        selected_mode = intent.presentation_mode or presentation_mode
        if selected_mode not in allowed_modes:
            raise InvalidIntent("response presentation mode was not declared by the route")
        if intent.content is not None and len(intent.content) > 2000:
            raise InvalidIntent("message content exceeds Discord's limit")
        if selected_mode == "v2" and intent.content is not None:
            raise InvalidIntent("Components V2 responses cannot use message content")
        if intent.mention_actor and (selected_mode != "plain" or not intent.content):
            raise InvalidIntent("actor mentions require plain message content")
        if selected_mode == "v2" and intent.controls:
            raise InvalidIntent("Components V2 controls must belong to a presentation page")
        if getattr(intent, "public_controls", False) and (
            selected_mode != "legacy" or intent.session_id or intent.kind == "paginator"
        ):
            raise InvalidIntent("public controls require a non-session legacy response")
        if selected_mode == "plain" and intent.pages:
            raise InvalidIntent("plain responses cannot use presentation pages")
        if selected_mode in {"legacy", "plain"} and any(page.controls for page in intent.pages):
            raise InvalidIntent("legacy controls must belong to the response")
        if intent.kind == "paginator" and len(intent.pages) > 100:
            raise InvalidIntent("too many paginator pages")
        max_pages = MAX_LAYOUT_COMPONENTS if layout_message else 10
        if intent.kind not in {"paginator", "send_parts"} and len(intent.pages) > max_pages:
            raise InvalidIntent("too many presentation pages")
        if intent.kind == "send_parts" and not 2 <= len(intent.pages) <= 100:
            raise InvalidIntent("continuation response requires two to 100 pages")
        if intent.kind == "send_parts" and (
            intent.controls
            or intent.session_id
            or intent.attachment_ref
            or intent.attachments
            or selected_mode == "plain"
        ):
            raise InvalidIntent("continuation response has incompatible fields")
        if intent.kind == "paginator" and not intent.pages:
            raise InvalidIntent("paginator has no pages")
        if intent.kind == "paginator" and selected_mode == "plain":
            raise InvalidIntent("plain responses cannot paginate")
        if intent.kind == "paginator" and intent.controls:
            raise InvalidIntent("paginators cannot include custom response controls")
        if (
            intent.content is None
            and not intent.pages
            and not intent.controls
            and intent.attachment_ref is None
            and not intent.attachments
        ):
            raise InvalidIntent("response is empty")
        for page in intent.pages:
            self._validate_page(page, selected_mode)
        if (
            selected_mode == "legacy"
            and intent.kind != "paginator"
            and sum(self._legacy_text_count(page) for page in intent.pages) > 6000
        ):
            raise InvalidIntent("legacy embeds exceed Discord's text limit")
        if selected_mode == "v2" and len(intent.pages) > 1 and intent.kind in {"send", "edit"}:
            if sum(self._component_count(page) for page in intent.pages) > 40:
                raise InvalidIntent("Components V2 payload exceeds Discord's component limit")
        if intent.attachment_ref is not None and intent.attachments:
            raise InvalidIntent("legacy and structured attachments cannot be combined")
        if intent.attachment_ref is not None:
            if not re.fullmatch(r"tmp:[a-f0-9]{32,64}", intent.attachment_ref):
                raise InvalidIntent("invalid temporary attachment reference")
        file_names = tuple(
            file_name for page in intent.pages for file_name in self._page_file_names(page)
        )
        if intent.attachments:
            if len(intent.attachments) > FILE_LIMIT:
                raise InvalidIntent("too many temporary attachments")
            loose_files = scheduled_files and selected_mode == "legacy" and not file_names
            if selected_mode != "plain" and not loose_files and len(file_names) != len(intent.attachments):
                raise InvalidIntent("file presentations must match temporary attachments")
            if len(set(file_names)) != len(file_names):
                raise InvalidIntent("attachment file names must be unique")
            if selected_mode != "plain" and not loose_files and {item.file_name for item in intent.attachments} != set(
                file_names
            ):
                raise InvalidIntent("attachment file names do not match presentations")
            total_size = 0
            now_ms = int(time.time() * 1000)
            for item in intent.attachments:
                if not re.fullmatch(r"(?:tmp:[a-f0-9]{32,64}|job-file:[a-f0-9]{32})", item.reference):
                    raise InvalidIntent("invalid temporary attachment reference")
                if item.reference.startswith("job-file:") and not scheduled_files and intent.kind not in {"send", "ephemeral"}:
                    raise InvalidIntent("job-bound attachments require a direct response")
                if not re.fullmatch(r"[A-Za-z0-9][A-Za-z0-9._-]{0,99}", item.file_name):
                    raise InvalidIntent("invalid temporary attachment file name")
                if not re.fullmatch(r"[a-f0-9]{64}", item.sha256):
                    raise InvalidIntent("invalid temporary attachment hash")
                if item.size <= 0 or item.size > BYTE_LIMIT:
                    raise InvalidIntent("invalid temporary attachment size")
                if item.expires_at_ms <= now_ms:
                    raise InvalidIntent("temporary attachment has expired")
                total_size += item.size
            if total_size > BYTE_LIMIT:
                raise InvalidIntent("temporary attachments exceed the edge limit")
        elif retained_uploads:
            if intent.kind != "edit" or intent.attachment_ref or set(file_names) != retained_uploads:
                raise InvalidIntent("file presentation must match the retained uploads")
        elif bool(file_names) != bool(intent.attachment_ref) or len(file_names) > 1:
            raise InvalidIntent("file presentation must match one temporary attachment")
        if intent.session_id is not None:
            if not re.fullmatch(r"[a-f0-9]{16,32}", intent.session_id):
                raise InvalidIntent("invalid interaction session identifier")
        self._validate_controls(intent.controls)
        return ValidatedIntent(
            intent.kind,
            intent.content,
            intent.pages,
            intent.attachment_ref,
            intent.session_id,
            selected_mode,
            intent.controls,
            intent.attachments,
            intent.mention_actor,
            getattr(intent, "public_controls", False),
        )

    def _validate_page(self, page: PresentationPage, presentation_mode: str) -> None:
        if page.blocks:
            if presentation_mode != "v2":
                raise InvalidIntent("ordered presentation blocks require Components V2")
            if any(
                (
                    page.description,
                    page.title,
                    page.url,
                    page.fields,
                    page.footer,
                    page.image_url,
                    page.thumbnail_url,
                    page.author_name,
                    page.author_icon_url,
                    page.timestamp_ms,
                    page.controls,
                    page.file_name,
                )
            ):
                raise InvalidIntent("ordered presentation blocks cannot mix legacy page fields")
            if len(page.blocks) > 40:
                raise InvalidIntent("too many ordered presentation blocks")
            text_total = 0
            for block in page.blocks:
                text_total += self._validate_block(block)
            if text_total > 4000:
                raise InvalidIntent("Components V2 text exceeds Discord's limit")
            if self._component_count(page) > 40:
                raise InvalidIntent("Components V2 payload exceeds Discord's component limit")
            return
        if len(page.title or "") > 256:
            raise InvalidIntent("presentation title exceeds Discord's limit")
        description_limit = 4096 if presentation_mode == "legacy" else 4000
        if len(page.description) > description_limit:
            raise InvalidIntent("presentation text exceeds Discord's limit")
        if len(page.fields) > 25:
            raise InvalidIntent("too many presentation fields")
        if len(page.footer or "") > 2048:
            raise InvalidIntent("presentation footer exceeds Discord's limit")
        if len(page.author_name or "") > 256:
            raise InvalidIntent("presentation author exceeds Discord's limit")
        self._validate_url(page.url)
        for url in (
            page.image_url,
            page.thumbnail_url,
            page.author_icon_url,
        ):
            self._validate_url(url, allow_attachment=True)
        if page.file_name is not None:
            if not re.fullmatch(r"[A-Za-z0-9][A-Za-z0-9._-]{0,99}", page.file_name):
                raise InvalidIntent("invalid attachment file name")
        if page.file_name and page.image_url:
            raise InvalidIntent("presentation image is ambiguous")
        text_total = len(page.description)
        body_parts = (
            int(bool(page.author_name)) + int(bool(page.title)) + int(bool(page.description))
        )
        text_total += max(0, body_parts - 1)
        if page.author_name:
            text_total += len(page.author_name) + 4
        if page.title:
            text_total += len(page.title) + 3
            if page.url:
                text_total += len(page.url) + 4
        if page.footer:
            text_total += len(page.footer) + 3
        if page.timestamp_ms is not None:
            text_total += 32
        for field in page.fields:
            if not field.name or len(field.name) > 256:
                raise InvalidIntent("invalid presentation field name")
            if not field.value or len(field.value) > 1024:
                raise InvalidIntent("invalid presentation field value")
            text_total += len(field.name) + len(field.value) + 5
        if presentation_mode == "v2" and text_total > 4000:
            raise InvalidIntent("Components V2 text exceeds Discord's limit")
        if presentation_mode == "legacy" and self._legacy_text_count(page) > 6000:
            raise InvalidIntent("legacy embed exceeds Discord's text limit")
        self._validate_controls(page.controls)
        if presentation_mode == "v2" and self._component_count(page) > 40:
            raise InvalidIntent("Components V2 payload exceeds Discord's component limit")

    @staticmethod
    def _legacy_text_count(page: PresentationPage) -> int:
        return sum(
            (
                len(page.title or ""),
                len(page.description),
                len(page.footer or ""),
                len(page.author_name or ""),
                *(len(field.name) + len(field.value) for field in page.fields),
            )
        )

    @staticmethod
    def _component_count(page: PresentationPage) -> int:


        count = 1
        if page.blocks:
            return ordered_component_count(page.blocks)
        if page.thumbnail_url or page.author_icon_url:
            count += 3
        elif page.author_name or page.title or page.description:
            count += 1
        count += len(page.fields) * 2
        count += int(bool(page.image_url))
        count += int(bool(page.file_name))
        count += 2 if page.footer else 0
        count += int(page.timestamp_ms is not None)

        buttons = 0
        for control in page.controls:
            if control.kind == "button":
                buttons += 1
                if buttons == 5:
                    count += 6
                    buttons = 0
                continue
            if buttons:
                count += 1 + buttons
                buttons = 0
            count += 2
        if buttons:
            count += 1 + buttons
        return count

    def _validate_controls(self, controls: tuple[PresentationControl, ...]) -> None:
        if len(controls) > 25:
            raise InvalidIntent("too many presentation controls")
        for control in controls:
            if control.kind not in {
                "button",
                "string_select",
                "user_select",
                "role_select",
                "channel_select",
            }:
                raise InvalidIntent("unsupported presentation control")
            if len(control.namespace) > 64 or len(control.action) > 64:
                raise InvalidIntent("presentation control identity is too long")
            if len(control.payload) > 80:
                raise InvalidIntent("presentation control payload is too long")
            if len(control.label or "") > 80 or len(control.placeholder or "") > 150:
                raise InvalidIntent("presentation control label is too long")
            self._validate_emoji(control.emoji)
            if not 0 <= control.min_values <= control.max_values <= 25:
                raise InvalidIntent("invalid presentation selection limits")
            if len(control.default_ids) > control.max_values:
                raise InvalidIntent("too many presentation defaults")
            if len(control.options) > 25:
                raise InvalidIntent("too many select options")
            for option in control.options:
                if not option.label or not option.value:
                    raise InvalidIntent("invalid select option")
                if len(option.label) > 100 or len(option.value) > 100:
                    raise InvalidIntent("select option is too long")
                if len(option.description or "") > 100:
                    raise InvalidIntent("select description is too long")
                self._validate_emoji(option.emoji)
            if control.kind == "button":
                if not (control.label or control.emoji):
                    raise InvalidIntent("button requires a label or emoji")
                if (
                    control.style not in {"primary", "secondary", "success", "danger"}
                    or control.placeholder
                    or control.options
                    or control.default_ids
                    or control.channel_types
                    or control.min_values != 1
                    or control.max_values != 1
                ):
                    raise InvalidIntent("button has incompatible fields")
            elif control.kind == "string_select":
                if (
                    not control.options
                    or control.max_values > len(control.options)
                    or len({option.value for option in control.options}) != len(control.options)
                    or control.label
                    or control.emoji
                    or control.default_ids
                    or control.channel_types
                ):
                    raise InvalidIntent("string select has incompatible fields")
                defaults = sum(option.default for option in control.options)
                if defaults > control.max_values:
                    raise InvalidIntent("too many default select options")
            elif (
                control.label
                or control.emoji
                or control.options
                or (control.kind != "channel_select" and control.channel_types)
            ):
                raise InvalidIntent("entity select has incompatible fields")

    def _validate_block(self, block: PresentationBlock) -> int:
        if block.kind not in {
            "heading",
            "text",
            "media",
            "file",
            "gallery",
            "separator",
            "section",
            "action_row",
        }:
            raise InvalidIntent("unsupported ordered presentation block")
        if block.kind == "gallery":
            if not 1 <= len(block.items) <= 10 or any(
                (block.text, block.media_url, block.file_name, block.controls, block.description)
            ):
                raise InvalidIntent("invalid media gallery")
            for item in block.items:
                if bool(item.media_url) == bool(item.file_name):
                    raise InvalidIntent("gallery item requires exactly one source")
                self._validate_url(item.media_url)
                if item.file_name and not re.fullmatch(
                    r"[A-Za-z0-9][A-Za-z0-9._-]{0,99}", item.file_name
                ):
                    raise InvalidIntent("invalid gallery attachment file name")
                if len(item.description or "") > 1024:
                    raise InvalidIntent("gallery description is too long")
            return 0
        if block.items:
            raise InvalidIntent("gallery items require a gallery block")
        if block.kind == "heading":
            if not block.text or len(block.text) > 256 or block.level not in {1, 2, 3}:
                raise InvalidIntent("invalid heading block")
            if block.controls or block.media_url or block.file_name:
                raise InvalidIntent("heading block has incompatible fields")
            return len(block.text) + block.level + 1
        if block.kind == "text":
            if not block.text or len(block.text) > 4000:
                raise InvalidIntent("invalid text block")
            if block.controls or block.media_url or block.file_name:
                raise InvalidIntent("text block has incompatible fields")
            return len(block.text)
        if block.kind in {"media", "file"}:
            if block.kind == "file" and (block.media_url or not block.file_name):
                raise InvalidIntent("file blocks require an uploaded attachment")
            if bool(block.media_url) == bool(block.file_name):
                raise InvalidIntent("media block requires exactly one source")
            self._validate_url(block.media_url)
            if block.file_name and not re.fullmatch(
                r"[A-Za-z0-9][A-Za-z0-9._-]{0,99}", block.file_name
            ):
                raise InvalidIntent("invalid attachment file name")
            if len(block.description or "") > 1024 or block.controls or block.text:
                raise InvalidIntent("invalid media block")
            return len(block.description or "")
        if block.kind == "separator":
            if block.spacing not in {"small", "large"}:
                raise InvalidIntent("invalid separator spacing")
            if block.text or block.controls or block.media_url or block.file_name:
                raise InvalidIntent("separator block has incompatible fields")
            return 0
        if block.kind == "section":
            if not block.text or len(block.text) > 4000:
                raise InvalidIntent("invalid section text")
            if len(block.controls) != 1 or block.controls[0].kind != "button":
                raise InvalidIntent("section requires one button accessory")
            if block.media_url or block.file_name:
                raise InvalidIntent("section block has incompatible fields")
            self._validate_controls(block.controls)
            return len(block.text)
        if block.text or block.media_url or block.file_name:
            raise InvalidIntent("action row has incompatible fields")
        if not 1 <= len(block.controls) <= 5:
            raise InvalidIntent("action row requires one to five controls")
        self._validate_controls(block.controls)
        if len(block.controls) > 1 and any(control.kind != "button" for control in block.controls):
            raise InvalidIntent("select controls must occupy their own action row")
        return 0

    @staticmethod
    def _validate_emoji(value: str | None) -> None:
        if value is None:
            return
        if len(value) > 160 or not (
            emoji_lib.is_emoji(value) or re.fullmatch(r"<a?:[A-Za-z0-9_]{2,32}:\d{17,20}>", value)
        ):
            raise InvalidIntent("invalid presentation emoji")

    @staticmethod
    def _page_file_names(page: PresentationPage) -> tuple[str, ...]:
        names = [page.file_name] if page.file_name else []
        names.extend(
            block.file_name
            for block in page.blocks
            if block.kind in {"media", "file"} and block.file_name
        )
        names.extend(
            item.file_name
            for block in page.blocks
            if block.kind == "gallery"
            for item in block.items
            if item.file_name
        )
        names.extend(
            url.removeprefix("attachment://")
            for url in (page.image_url, page.thumbnail_url, page.author_icon_url)
            if url and url.startswith("attachment://")
        )
        return tuple(dict.fromkeys(names))

    def component_count(self, page: PresentationPage) -> int:

        self._validate_page(page, "v2")
        return self._component_count(page)

    def _validate_url(self, value: str | None, *, allow_attachment: bool = False) -> None:
        if value is None:
            return
        if len(value) > 2048:
            raise InvalidIntent("presentation URL is too long")
        if allow_attachment and value.startswith("attachment://"):
            if not re.fullmatch(
                r"[A-Za-z0-9][A-Za-z0-9._-]{0,99}", value.removeprefix("attachment://")
            ):
                raise InvalidIntent("invalid media attachment file name")
            return
        parsed = urlsplit(value)
        if parsed.scheme != "https" or not parsed.netloc or parsed.username or parsed.password:
            raise InvalidIntent("presentation URL must be public HTTPS")
