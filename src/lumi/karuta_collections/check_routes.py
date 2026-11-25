from lumi.contracts import (
    CaptureSpec,
    CommandSpec,
    ComponentSpec,
    FieldExtraction,
    HelpSpec,
    MatcherExpr,
    ModalFieldSpec,
    ModalSpec,
    PassiveMatcherSpec,
    RepeatedExtraction,
)

from .query import REPLY_FACTS, SPREADSHEET_URL

MODULE = "karuta_collections"
TARGET = r"(?:<@!?(?P<target>\d{17,20})>|(?P<target_id>\d{17,20}))"


def commands():
    captures = (
        CaptureSpec("target", "snowflake", required=False),
        CaptureSpec("target_id", "snowflake", required=False),
        CaptureSpec("help", required=False),
    )
    return (
        CommandSpec(
            root="kdupes",
            aliases=("checkdupes",),
            handler=MODULE + ".kdupes",
            grammar=r"(?i)^\s*(?:(?P<help>help)|" + TARGET + r")?\s*$",
            captures=captures,
            acl_key="kdupes",
            public=True,
            presentation_mode="legacy",
            help=HelpSpec("kdupes [@user | ID]", "Show same-edition duplicates."),
        ),
        CommandSpec(
            root="wlcheck",
            handler=MODULE + ".wlcheck",
            grammar=r"(?i)^\s*(?:(?P<help>help)|(?:(?P<mode>kc|kwl)\s+)?" + TARGET + r")?\s*$",
            captures=(*captures, CaptureSpec("mode", required=False)),
            acl_key="wlcheck",
            public=True,
            presentation_mode="v2",
            help=HelpSpec("wlcheck [kc | kwl] <@user | ID>", "Compare saved cards and wishlists."),
        ),
        CommandSpec(
            root="kcimport",
            handler=MODULE + ".kcimport",
            grammar=r"(?i)^\s*(?P<help>help)?\s*$",
            captures=(CaptureSpec("help", required=False),),
            acl_key="kcimport",
            public=True,
            deadline_ms=60_000,
            context_fields=("source_revision_ms",),
            help=HelpSpec("kcimport", "Upload your collection CSV."),
        ),
    )


def controls():
    return (
        ComponentSpec(
            namespace=MODULE,
            action="csv_upload",
            handler=MODULE + ".import_upload",
            grammar=r"^(?P<session>[a-f0-9]{16})~(?P<revision>[0-9]{1,9})$",
            captures=(CaptureSpec("session"), CaptureSpec("revision", "integer", minimum=1)),
            acl_key="kcimport",
            public=True,
            session_bound=True,
            input_kind="modal",
            modal=ModalSpec(
                "Import collection",
                (
                    ModalFieldSpec(
                        "csv", "CSV", kind="file_upload", max_values=1, file_types=(".csv",)
                    ),
                ),
            ),
            deadline_ms=60_000,
            presentation_mode="v2",
        ),
        *(
            ComponentSpec(
                namespace=MODULE,
                action=prefix + "_" + action,
                handler=MODULE + ".check_control",
                grammar=r"^(?P<session>[a-f0-9]{16})~(?P<revision>[0-9]{1,9})$",
                captures=(CaptureSpec("session"), CaptureSpec("revision", "integer", minimum=1)),
                acl_key="kdupes" if prefix == "dupes" else "wlcheck",
                public=True,
                session_bound=True,
                presentation_mode="legacy" if prefix == "dupes" else "v2",
            )
            for prefix in ("dupes", "wishlist")
            for action in (
                ("previous", "indicator", "next")
                if prefix == "dupes"
                else ("previous", "indicator", "next", "swap")
            )
        ),
    )


def wishlist_routes(heading):
    owner = (
        r"(?is)^[\s\S]*?Showing[ \t]+wishlist[ \t]+of[ \t]+"
        r"<@!?(?P<owner>\d{17,20})>[^\r\n]*[\s\S]*$"
    )

    slots = (
        r"(?is)^[\s\S]*?Available slots:[ \t*`]*(?P<available>\d{1,4})"
        r"[*`]*[ \t]*/[ \t]*(?P<capacity>\d{1,4})\b[\s\S]*$"
    )
    page = (
        r"(?is)^(?:[\s\S]*?Showing characters[ \t]+(?P<start>[\d,]{1,7})"
        r"[ \t]*[\x{2013}\x{2212}-][ \t]*"
        r"(?P<end>[\d,]{1,7})[ \t]+of[ \t]+(?P<total>[\d,]{1,7})\b[\s\S]*|[\s\S]*)$"
    )
    row = (
        r"(?m)^[ \t]*-[ \t]+(?P<series>[^\r\n·]{1,240})[ \t]*·[ \t]*"
        r"(?P<character>[^\r\n]{1,250})[ \t]*$"
    )
    return tuple(
        PassiveMatcherSpec(
            route=MODULE + ".wishlist_" + event,
            handler=MODULE + ".wishlist",
            event=event,
            bot_ids=(-2,),
            requires_embeds=True,
            hydrate=event == "message_edit",
            watch_ttl_ms=900_000,
            subject_capture="owner",
            subject_required=True,
            expression=MatcherExpr(
                "all",
                children=(
                    heading("(?:Character )?Wishlist"),
                    MatcherExpr("not", children=(MatcherExpr("exists", "webhook_id"),)),
                    MatcherExpr("regex", "message_document", owner),
                ),
            ),
            extractions=(
                FieldExtraction("message_document", owner, (CaptureSpec("owner", "snowflake"),)),
                FieldExtraction(
                    "message_document",
                    slots,
                    (
                        CaptureSpec("available", "integer", maximum=1000),
                        CaptureSpec("capacity", "integer", maximum=1000),
                    ),
                ),
                FieldExtraction(
                    "message_document",
                    page,
                    tuple(
                        CaptureSpec(key, "compact_integer", required=False, minimum=0, maximum=1000)
                        for key in ("start", "end", "total")
                    ),
                ),
            ),
            repeated=(
                RepeatedExtraction(
                    "embed_descriptions",
                    row,
                    11,
                    (
                        CaptureSpec("series", "normalized_text", maximum=240),
                        CaptureSpec("character", "normalized_text", maximum=250),
                    ),
                ),
            ),
            context_fields=(
                "source_revision_ms",
                "event_order",
                "webhook_id",
                *REPLY_FACTS,
            ),
        )
        for event in ("message_create", "message_edit")
    )


def spreadsheet_routes(heading):
    url = SPREADSHEET_URL
    owner = r"(?is)^[\s\S]*?<@!?(?P<owner>\d{17,20})>, your new spreadsheet[\s\S]*$"
    common = (
        heading("Collection Spreadsheet"),
        MatcherExpr("not", children=(MatcherExpr("exists", "webhook_id"),)),
    )
    return (
        PassiveMatcherSpec(
            route=MODULE + ".spreadsheet_pending",
            handler=MODULE + ".spreadsheet_pending",
            event="message_create",
            bot_ids=(-2,),
            requires_embeds=True,
            expression=MatcherExpr(
                "all",
                children=(
                    *common,
                    MatcherExpr("not", children=(MatcherExpr("regex", "message_document", url),)),
                ),
            ),
            watch_ttl_ms=120_000,
        ),
        *(
            PassiveMatcherSpec(
                route=MODULE + ".spreadsheet_" + event,
                handler=MODULE + ".spreadsheet",
                event=event,
                bot_ids=(-2,),
                requires_embeds=True,
                hydrate=event == "message_edit",
                watch_ttl_ms=120_000,
                deadline_ms=60_000,
                subject_capture="owner",
                subject_required=True,
                expression=MatcherExpr(
                    "all",
                    children=(
                        *common,
                        MatcherExpr("regex", "message_document", url),
                    ),
                ),
                extractions=(
                    FieldExtraction(
                        "message_document", owner, (CaptureSpec("owner", "snowflake"),)
                    ),
                    FieldExtraction(
                        "embed_descriptions", rf"^[\s\S]*?(?P<export_url>{url})[\s\S]*$",
                        (CaptureSpec("export_url", maximum=128),),
                    ),
                ),
                context_fields=(
                    "embed_titles",
                    "embed_descriptions",
                    "reference_message_id",
                    *REPLY_FACTS,
                    "webhook_id",
                    "source_revision_ms",
                ),
            )
            for event in ("message_create", "message_edit")
        ),
    )
