def escape_embed_text(value: str) -> str:
    value = value.replace("@", "@\u200b")
    for character in ("\\", "`", "*", "_", "~", "|", ">", "#", "[", "]", "(", ")"):
        value = value.replace(character, f"\\{character}")
    return value
