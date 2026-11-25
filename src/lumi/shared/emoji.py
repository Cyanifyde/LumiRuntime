import unicodedata


def canonical_emoji(value: str) -> str:
    raw = str(value or "").strip()
    if raw.endswith(">") and ":" in raw:
        identifier = raw[:-1].rpartition(":")[2]
        if identifier.isdigit():
            return f"custom:{identifier}"
    return "unicode:" + unicodedata.normalize("NFC", raw).replace("\ufe0e", "").replace(
        "\ufe0f", ""
    )
