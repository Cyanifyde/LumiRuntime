from __future__ import annotations

import unicodedata


def validate_prefix(value: str) -> str:
    prefix = unicodedata.normalize("NFKC", value)
    if not 1 <= len(prefix) <= 5:
        raise ValueError("Prefix must contain 1 to 5 characters.")
    categories = tuple(unicodedata.category(character) for character in prefix)
    if any(
        character.isspace() or not character.isprintable() or category[0] in {"C", "Z"}
        for character, category in zip(prefix, categories, strict=True)
    ):
        raise ValueError("Prefix must use visible characters only.")
    if all(category[0] == "M" for category in categories):
        raise ValueError("Prefix must contain a visible base character.")
    if any(character in prefix for character in ("`", "/", "@")):
        raise ValueError("Prefix contains a reserved character.")
    if "<@" in prefix or "<@&" in prefix:
        raise ValueError("Prefix cannot contain a mention.")
    return prefix
