from __future__ import annotations

import re
import string
import unicodedata
from dataclasses import dataclass
from difflib import SequenceMatcher

from .models import DISCORD_TOKEN, escape, units

UNKNOWN = "Earlier text unavailable."
_LINE = re.compile(r"(?: {0,3}(?:#{1,6}|-#|>{1,3}|[-+*]|\d+[.)])[^\S\n]+)+")
_URL = re.compile(r"https?://[^\s<>]+")
_WORDS = re.compile(r"\s+|\S+")
_MAX_DIFF_TOKENS = 512


@dataclass(frozen=True)
class _Part:
    text: str
    native: bool = False


def _link(text: str, start: int) -> tuple[str, str, int] | None:

    end = text.find("](", start + 1, min(len(text), start + 2048))
    if end < 0:
        return None
    depth, i = 1, end + 2
    while i < min(len(text), start + 4096):
        if text[i] == "\\":
            i += 2
            continue
        if text[i] == "(":
            depth += 1
        elif text[i] == ")":
            depth -= 1
            if not depth:
                return text[start + 1 : end], text[end + 2 : i], i + 1
        i += 1
    return None


def _plain(text: str, depth: int = 0) -> list[_Part]:
    parts: list[_Part | None] = []
    pending: dict[str, list[int]] = {c: [] for c in ("*", "_", "~~", "||")}
    i = 0
    while i < len(text):
        if i == 0 or text[i - 1] == "\n":
            prefix = _LINE.match(text, i)
            if prefix:
                i = prefix.end()
                if i == len(text):
                    break
        c = text[i]
        if c == "\\" and i + 1 < len(text) and text[i + 1] in string.punctuation:
            token = DISCORD_TOKEN.match(text, i + 1)
            end = token.end() if token else i + 2
            parts.append(_Part(text[i + 1 : end]))
            i = end
            continue
        if c == "`":
            n = 1
            while i + n < len(text) and text[i + n] == "`":
                n += 1
            end = text.find("`" * n, i + n)
            if end >= 0:
                body = text[i + n : end]
                if n >= 3 and "\n" in body:
                    language, rest = body.split("\n", 1)
                    if re.fullmatch(r"[\w+.#-]{0,32}", language):
                        body = rest.removesuffix("\n")
                parts.append(_Part(body))
                i = end + n
                continue
            parts.append(_Part("`" * n))
            i += n
            continue
        token = DISCORD_TOKEN.match(text, i)
        if token:
            parts.append(_Part(token[0], True))
            i = token.end()
            continue
        if c == "[" and depth < 8 and (link := _link(text, i)):
            label, target, i = link
            parts.extend(_plain(label, depth + 1))


            parts.append(_Part(" (" + target.strip("<>") + ")"))
            continue
        url = _URL.match(text, i + 1 if c == "<" else i)
        if url:
            body = url[0]
            for marker, openings in pending.items():
                count = min(len(openings), (len(body) - len(body.rstrip(marker[0]))) // len(marker))
                if count:
                    body = body[: -count * len(marker)]
            end = url.start() + len(body)
            if c == "<" and end < len(text) and text[end] == ">":
                end += 1
            parts.append(_Part(body))
            i = end
            continue
        if c in "*_~|":
            end = i + 1
            while end < len(text) and text[end] == c:
                end += 1
            count = end - i
            marker = c if c in "*_" else c * 2
            width = len(marker)
            left, right = text[i - 1 : i], text[end : end + 1]
            intraword = c == "_" and count == 1 and left.isalnum() and right.isalnum()
            opening = bool(right and not right.isspace()) and not intraword
            closing = bool(left and not left.isspace()) and not intraword
            closing_count = min(count // width, len(pending[marker])) if closing else 0
            for index in range(count // width):
                if index < closing_count:
                    parts[pending[marker].pop()] = None
                else:
                    if opening:
                        pending[marker].append(len(parts))
                    parts.append(_Part(marker))
            if count % width:
                parts.append(_Part(c))
            i = end
            continue
        parts.append(_Part(c))
        i += 1


    merged: list[_Part] = []
    literal: list[str] = []
    for part in parts:
        if part is None or not part.text:
            continue
        if not part.native:
            literal.append(part.text)
        else:
            if literal:
                merged.append(_Part("".join(literal)))
                literal = []
            merged.append(part)
    if literal:
        merged.append(_Part("".join(literal)))
    return merged


def plain_text(text: str) -> str:

    return "".join(p.text for p in _plain(text))


def _words(text: str) -> list[_Part]:
    return [
        atom
        for part in _plain(text)
        for atom in ([part] if part.native else [_Part(m[0]) for m in _WORDS.finditer(part.text)])
    ]


def _changes(before: list[_Part], after: list[_Part]) -> tuple[set[int], set[int]]:


    a, b = [p.text for p in before], [p.text for p in after]
    first = 0
    while first < min(len(a), len(b)) and a[first] == b[first]:
        first += 1
    tail = 0
    while tail < min(len(a), len(b)) - first and a[-1 - tail] == b[-1 - tail]:
        tail += 1
    aend, bend = len(a) - tail, len(b) - tail
    left, right = a[first:aend], b[first:bend]
    if max(len(left), len(right)) > _MAX_DIFF_TOKENS:
        return set(range(first, aend)), set(range(first, bend))
    changed_a, changed_b = set(), set()
    for op, i, j, k, m in SequenceMatcher(None, left, right, autojunk=False).get_opcodes():
        if op != "equal":
            changed_a.update(range(first + i, first + j))
            changed_b.update(range(first + k, first + m))
    return changed_a, changed_b


def _glyphs(text: str):

    cluster = ""
    for c in text:
        attach = (
            unicodedata.category(c).startswith("M")
            or c == "\u200d"
            or cluster.endswith("\u200d")
            or "\U0001f3fb" <= c <= "\U0001f3ff"
            or "\U000e0020" <= c <= "\U000e007f"
            or (len(cluster) == 1 and "🇦" <= c <= "🇿" and "🇦" <= cluster <= "🇿")
        )
        if cluster and not attach:
            yield cluster
            cluster = ""
        cluster += c
    if cluster:
        yield cluster


def _display(words: list[_Part], changes: set[int]) -> list[tuple[str, bool]]:
    return [
        (part.text if part.native else escape(glyph, 1000000), changed)
        for i, part in enumerate(words)
        for changed in (i in changes and not part.text.isspace(),)
        for glyph in ([part.text] if part.native else _glyphs(part.text))
    ]


def _cost(atoms: list[tuple[str, bool]]) -> int:
    size, active = 0, False
    for text, changed in atoms:
        if changed != active:
            size += 2
            active = changed
        size += units(text)
    return size + 2 * active


def _render(atoms: list[tuple[str, bool]], limit: int, *, excerpt: bool) -> list[str]:
    if not atoms:
        return []
    clipped = excerpt and _cost(atoms) > limit
    budget = limit - int(clipped)
    pages, current = [], []
    size, active = 0, False
    for text, changed in atoms:


        if units(text) + 4 > budget and not DISCORD_TOKEN.fullmatch(text):

            split = [(m[0], changed) for m in re.finditer(r"\\.|[^\\]", text, re.S)]
        else:
            split = [(text, changed)]
        for atom, mark in split:
            transition = 2 if mark != active else 0
            if size + transition + units(atom) + 2 * mark > budget:
                if active:
                    current.append("__")
                pages.append("".join(current) + ("…" if clipped else ""))
                if excerpt:
                    return pages
                current, size, active = [], 0, False
            if mark != active:
                current.append("__")
                size += 2
                active = mark
            current.append(atom)
            size += units(atom)
    if active:
        current.append("__")
    if current:
        pages.append("".join(current))
    return pages


def edit_text(record: dict, *, limit: int = 800, full: bool = False) -> list[tuple[str, str]]:

    before = record.get("before", UNKNOWN) or ""
    after = record.get("after") or ""
    a, b = _words(before), _words(after)
    unknown = record.get("previous_unknown", before == UNKNOWN)
    ca, cb = (set(), set()) if unknown else _changes(a, b)
    result = []
    for label, words, changed in (("Before", a, ca), ("After", b, cb)):
        if not any(p.text.strip() for p in words):
            continue
        pages = _render(_display(words, changed), limit, excerpt=not full)
        result.extend(
            (label + (f" · {i + 1}" if len(pages) > 1 else ""), text)
            for i, text in enumerate(pages)
        )
    return result
