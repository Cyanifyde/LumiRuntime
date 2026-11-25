from __future__ import annotations

import re
from functools import lru_cache


@lru_cache(maxsize=64)
def pattern(subject: int):
    return re.compile(rf"(?<![0-9]){subject}(?![0-9])")


def references(value, subject: int) -> bool:
    if type(value) is int:
        return value == subject
    if isinstance(value, str):
        return bool(pattern(subject).search(value))
    if isinstance(value, bytes):
        try:
            return references(value.decode("utf-8"), subject)
        except UnicodeError:
            return False
    if isinstance(value, dict):
        return any(references(k, subject) or references(v, subject) for k, v in value.items())
    if isinstance(value, (tuple, list, set, frozenset)):
        return any(references(item, subject) for item in value)
    return False
