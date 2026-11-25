from lumi.contracts import (
    CaptureSpec,
    ComponentCaptureSpec,
    ComponentExtractionSpec,
    FieldExtraction,
    MatcherExpr,
    PassiveMatcherSpec,
    RepeatedExtraction,
)

from .models import BOT_IDS


def _optional(pattern, captures):
    return FieldExtraction(
        "message_document", "(?is)^(?:" + pattern + r"[\s\S]*|[\s\S]*)$", captures
    )


def detail_extractions(game):
    owner = r"\d{17,20}"
    extractions = [
        _optional(
            rf"[ \t*]*<@!?(?P<dropper>{owner})>[ \t*]+"
            r"(?:is[ \t*]+dropping|(?:just[ \t*]+)?dropped)\b",
            (CaptureSpec("dropper", "snowflake", required=False),),
        ),
        _optional(
            r"(?P<activity>I'm dropping [1-9] cards since this server is currently active!|"
            r"The vote has concluded!)",
            (CaptureSpec("activity", required=False, maximum=100),),
        ),
    ]
    if game == "sofi":
        extractions.extend(
            (
                _optional(
                    rf"Mass\s+drop\s+[1-9]\d*/[1-9]\d*\s+by\s+<@!?(?P<mass_dropper>{owner})>",
                    (CaptureSpec("mass_dropper", "snowflake", required=False),),
                ),
                _optional(
                    rf"<@!?(?P<series_dropper>{owner})>\s+(?:[^\r\n]{{1,240}}?\s+was\s+selected|selected\s+[^\r\n]{{1,240}}?)!\s+Dropping\s+(?P<slots>[1-3])\s+characters?\.{{3}}",
                    (
                        CaptureSpec("series_dropper", "snowflake", required=False),
                        CaptureSpec("slots", "integer", required=False, minimum=1, maximum=3),
                    ),
                ),
            )
        )
    return tuple(extractions)


def detail_rows(game):
    if game != "sofi":
        return ()
    return (
        RepeatedExtraction(
            "message_document",
            r"(?m)^[ \t]*`?(?P<position>[1-3])\.`?[ \t]+[^|\r\n]{1,200}\|"
            r"[ \t]*G[ \t]*[•·:][ \t]*`?[ \t]*(?P<number>\d{1,10})?"
            r"[ \t]*`?[ \t]*\|[ \t]*(?P<character>[^\r\n|•·]{1,240}?)[ \t]+"
            r"[•·][ \t]+(?P<series>[^\r\n]{1,240}?)[ \t]*$",
            3,
            (
                CaptureSpec("position", "integer", minimum=1, maximum=3),
                CaptureSpec("number", "integer", required=False, minimum=0, maximum=9_999_999_999),
                CaptureSpec("character", "normalized_text", minimum=1, maximum=240),
                CaptureSpec("series", "normalized_text", minimum=1, maximum=240),
            ),
        ),
    )


def detail_components(game):
    if game == "glitch":
        return (
            ComponentExtractionSpec(
                "cards",
                10,
                MatcherExpr(
                    "all",
                    children=(
                        MatcherExpr("exact", "type", "2"),
                        MatcherExpr("regex", "custom_id", r"^[a-f0-9]{24,64}$"),
                        MatcherExpr(
                            "any",
                            children=(
                                MatcherExpr("exact", "emoji.id", "-4"),
                                MatcherExpr("exact", "emoji.id", "-5"),
                            ),
                        ),
                    ),
                ),
                (
                    ComponentCaptureSpec("token", "custom_id", maximum=100),
                    ComponentCaptureSpec(
                        "character",
                        "label",
                        "normalized_text",
                        pattern=r"^(?P<character>[^\r\n#]{1,200}?)(?:[ \t]+#[\d,]+)?$",
                        maximum=200,
                    ),
                    ComponentCaptureSpec(
                        "number",
                        "label",
                        "compact_integer",
                        pattern=r"^[^\r\n]{1,200}?[ \t]+#(?P<number>[\d,]+)$",
                        required=False,
                        minimum=0,
                        maximum=2_147_483_647,
                    ),
                ),
            ),
        )
    if game == "sofi":
        return (
            ComponentExtractionSpec(
                "cards",
                3,
                MatcherExpr(
                    "all",
                    children=(
                        MatcherExpr("exact", "type", "2"),
                        MatcherExpr("regex", "custom_id", r"^(?:sdrop|drop)_\d{17,20}_[0-2]$"),
                        MatcherExpr(
                            "any",
                            children=(
                                MatcherExpr("exact", "emoji.name", "❤"),
                                MatcherExpr("exact", "emoji.name", "❤️"),
                                MatcherExpr("exact", "emoji.name", "dropheart"),
                            ),
                        ),
                    ),
                ),
                (
                    ComponentCaptureSpec(
                        "position",
                        "custom_id",
                        "integer",
                        pattern=r"^(?:sdrop|drop)_\d{17,20}_(?P<position>[0-2])$",
                        minimum=0,
                        maximum=2,
                    ),
                    ComponentCaptureSpec("token", "custom_id", maximum=100),
                ),
            ),
        )
    return ()


def passive_routes() -> tuple[PassiveMatcherSpec, ...]:
    shapes = {
        "karuta": (
            MatcherExpr("regex", "image_source.file_name", r"^card\.(?:webp|png)$"),
            MatcherExpr("regex", "image_source.width", r"^(?:836|1110)$"),
            MatcherExpr("exact", "image_source.height", "419"),
        ),
        "glitch": (
            MatcherExpr("exact", "image_source.file_name", "drop.png"),
            MatcherExpr("exact", "image_source.width", "696"),
            MatcherExpr("exact", "image_source.height", "493"),
        ),
        "sofi": (
            MatcherExpr(
                "any",
                children=(
                    MatcherExpr(
                        "all",
                        children=(
                            MatcherExpr(
                                "regex",
                                "image_source.file_name",
                                r"^(?:[a-f0-9]{8}(?:-[a-f0-9]{4}){3}-[a-f0-9]{12}\.webp|seriesdrop\.png)$",
                            ),
                            MatcherExpr("exact", "image_source.width", "1008"),
                            MatcherExpr("exact", "image_source.height", "524"),
                        ),
                    ),


                    MatcherExpr(
                        "all",
                        children=(
                            MatcherExpr("exists", "image_source"),
                            MatcherExpr(
                                "regex",
                                "components.components.custom_id",
                                r"^(?:drop|sdrop)_\d{17,20}_[0-2]$",
                            ),
                        ),
                    ),
                ),
            ),
            MatcherExpr(
                "regex",
                "message_document",
                r"(?is)(?:\bdropping\b[\s\S]*?\b(?:cards|characters?)\b|"
                r"^Mass\s+drop\s+[1-9]\d*/[1-9]\d*\s+by\s+<@!?\d{17,20}>|"
                r"^The\s+vote\s+has\s+concluded!(?:\s|$))",
            ),
        ),
    }
    return tuple(
        PassiveMatcherSpec(
            route=f"drop_counts.{game}_{event}",
            handler=f"drop_counts.observe_{game}",
            event=event,
            bot_ids=(bot_id,),
            requires_attachments=True if game != "sofi" else None,
            expression=MatcherExpr(
                "all",
                children=(
                    MatcherExpr("exact", "author_bot", "True"),
                    MatcherExpr("not", children=(MatcherExpr("exists", "webhook_id"),)),
                    MatcherExpr("exists", "guild_id"),
                    *shapes[game],
                ),
            ),
            hydrate=event == "message_edit",
            watch_ttl_ms=60 * 60 * 1000,
            deadline_ms=120_000,
            extractions=detail_extractions(game),
            repeated=detail_rows(game),
            component_extractions=detail_components(game),
            context_fields=(
                "author_bot",
                "webhook_id",
                "source_revision_ms",
                "event_order",
                "entity_labels",
            )
            + (("image_source",) if game == "karuta" else ()),
        )
        for game, bot_id in BOT_IDS.items()
        for event in ("message_create", "message_edit")
    )
