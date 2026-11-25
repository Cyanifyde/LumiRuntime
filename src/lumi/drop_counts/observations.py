from .statistics import CardDetail, DropDetail, positive_number


def detail_from_request(record, request):
    args, event = request.arguments, request.event
    owners = {
        args.get(key) for key in ("dropper", "mass_dropper", "series_dropper") if args.get(key)
    }
    owner = next(iter(owners)) if len(owners) == 1 else None
    kind = "user" if owner else "server" if args.get("activity") else "unknown"
    cards = {}
    if record.game != "karuta":
        for index, item in enumerate(args.get("cards", ())):
            position = index + 1 if record.game == "glitch" else item["position"] + 1
            cards[position] = CardDetail(
                position,
                positive_number(item.get("number")),
                character=item.get("character"),
                token=item["token"],
            )
        if record.game == "sofi":
            for item in args.get("message_document", ()):
                position = item["position"]
                cards[position] = CardDetail(
                    position,
                    positive_number(item.get("number")),
                    character=item["character"],
                    series=item["series"],
                    token=cards[position].token if position in cards else "",
                )
    detail = DropDetail(
        record,
        int(event.get("source_revision_ms") or record.occurred_at_ms),
        int(event.get("event_order") or 0),
        owner,
        kind,
        tuple(cards[p] for p in sorted(cards)),
        None,
        "",
        str(dict(event.get("entity_labels", ())).get(f"<@{owner}>", "")).removeprefix("@")[:80],
    )
    if not detail.valid():
        raise ValueError("Invalid detailed drop.")
    return detail
