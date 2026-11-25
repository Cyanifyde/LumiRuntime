from __future__ import annotations

import hashlib
import re
from dataclasses import dataclass

from .models import DATE_PATTERN, MAX_CHARS, MAX_DEPTH, MAX_TOKENS

_NUMBER = re.compile(
    r"(?:[0-9]{1,3}(?:,[0-9]{3})+|[0-9]+|(?=\.))(?:\.[0-9]*)?"
    r"(?:[eE][+-]?[0-9]{1,3})?"
)
_DATE = re.compile(DATE_PATTERN)
_SYMBOLS = str.maketrans({"\u00d7": "*", "÷": "/", "\u2212": "-", "\u00a0": " "})


class InvalidCalculation(ValueError):
    pass


def source_hash(raw: str) -> str:
    return hashlib.sha256(raw.encode("utf-8")).hexdigest()


def clean(raw: str) -> str:
    if not isinstance(raw, str) or not raw or len(raw) > MAX_CHARS:
        raise InvalidCalculation()
    text = raw.strip()
    if text.startswith("`") and text.endswith("`") and not text.startswith("``"):
        text = text[1:-1].strip()

    text = "".join(chr(ord(c) - 0xFEE0) if 0xFF01 <= ord(c) <= 0xFF5E else c for c in text)
    text = text.translate(_SYMBOLS)
    if not text or any(c in text for c in ("`", "\n", "\r", "\x00")) or _DATE.fullmatch(text):
        raise InvalidCalculation()
    return text


def candidate(raw: str) -> bool:
    try:
        text = clean(raw)
    except InvalidCalculation:
        return False
    if re.search(r"[^0-9.,+\-*/^()%kKmMbBtTeE \t]", text):
        return False

    if re.fullmatch(r"[+-]?(?:[0-9.,]+)(?:[eE][+-]?[0-9]+)?[kKmMbBtT]?", text):
        return False
    try:
        tokens = tokenize(text)
    except InvalidCalculation:
        return False
    if sum(t.kind == "number" for t in tokens) == 1 and all(
        t.kind in {"number", "(", ")", "+", "-"} for t in tokens
    ):
        return False
    return any(c in text for c in "+-*/^%") or bool(re.search(r"\)\s*[0-9(]|[0-9)]\s*\(", text))


@dataclass(frozen=True, slots=True)
class Token:
    kind: str
    text: str


def tokenize(raw: str) -> list[Token]:
    text = clean(raw)
    tokens: list[Token] = []
    position = 0
    depth = 0
    while position < len(text):
        c = text[position]
        if c in " \t":
            position += 1
            continue
        number = _NUMBER.match(text, position)
        if number is not None and number[0] and number[0] != ".":
            literal = number[0]
            position = number.end()
            if position < len(text) and text[position] in ".,eE":
                raise InvalidCalculation()
            suffix = ""
            if position < len(text) and text[position] in "kKmMbBtT":
                suffix = text[position].lower()
                position += 1
            numeric = literal.replace(",", "").replace("e", "E")
            if "E" in numeric:
                numeric = f"({numeric})"
            if len(numeric) > 110:
                raise InvalidCalculation()
            if suffix:
                numeric = f"({numeric}*1E{dict(k=3, m=6, b=9, t=12)[suffix]})"
            tokens.append(Token("number", numeric))
        elif text.startswith("**", position):
            tokens.append(Token("^", "^"))
            position += 2
        elif c in "+-*/^()%":
            if c == "(":
                depth += 1
                if depth > MAX_DEPTH:
                    raise InvalidCalculation()
            elif c == ")":
                depth -= 1
                if depth < 0:
                    raise InvalidCalculation()
            tokens.append(Token(c, c))
            position += 1
        else:
            raise InvalidCalculation()
        if len(tokens) > MAX_TOKENS:
            raise InvalidCalculation()
    if depth:
        raise InvalidCalculation()
    return tokens


@dataclass(frozen=True, slots=True)
class _Term:
    text: str
    percentage: bool = False
    number: bool = False


class _Translation:


    def __init__(self, tokens: list[Token]) -> None:
        self.tokens = tokens
        self.position = 0

    def expression(self, minimum: int = 0, depth: int = 0) -> _Term:
        if depth > MAX_TOKENS or self.position >= len(self.tokens):
            raise InvalidCalculation()
        first = self.tokens[self.position]
        self.position += 1
        if first.kind == "number":
            left = _Term(first.text, number=True)
        elif first.kind in {"+", "-"}:
            right = self.expression(60, depth + 1)


            left = _Term(f"({first.text}({right.text}))", right.percentage)
        elif first.kind == "(":
            left = self.expression(0, depth + 1)
            if self.position >= len(self.tokens) or self.tokens[self.position].kind != ")":
                raise InvalidCalculation()
            self.position += 1
            left = _Term(f"({left.text})", left.percentage)
        else:
            raise InvalidCalculation()
        while self.position < len(self.tokens):
            token = self.tokens[self.position]
            if token.kind == ")":
                break
            if token.kind == "%":
                if minimum > 65:
                    break
                if left.percentage:
                    raise InvalidCalculation()
                self.position += 1
                left = _Term(f"({left.text}/100)", percentage=True)
                continue
            implicit = token.kind in {"(", "number"}
            if implicit:

                if token.kind == "number" and (left.number or left.percentage):
                    raise InvalidCalculation()
                operator, binding = "*", 50
            elif token.kind in {"+", "-", "*", "/", "^"}:
                operator = token.kind
                binding = {"+": 40, "-": 40, "*": 50, "/": 50, "^": 70}[operator]
            else:
                raise InvalidCalculation()
            if binding < minimum:
                break
            if not implicit:
                self.position += 1
            right = self.expression(binding if operator == "^" else binding + 1, depth + 1)
            if operator in {"+", "-"} and right.percentage:
                left = _Term(f"({left.text}*(1{operator}{right.text}))")
            else:
                left = _Term(f"({left.text}{operator}{right.text})")
        return left


def translate(raw: str) -> str:
    if not candidate(raw):
        raise InvalidCalculation()
    parser = _Translation(tokenize(raw))
    result = parser.expression()
    if parser.position != len(parser.tokens) or len(result.text) > 8_192:
        raise InvalidCalculation()
    return result.text


def display(raw: str) -> str:
    text = clean(raw).replace("**", "^")
    parts = []
    position = 0
    while position < len(text):
        number = _NUMBER.match(text, position)
        if number is not None:
            parts.append(number[0])
            position = number.end()
        else:
            char = text[position]
            parts.append(
                {"+": " + ", "-": " \u2212 ", "*": " \u00d7 ", "/": " ÷ ", "^": " ^ "}.get(
                    char, char
                )
            )
            position += 1
    return " ".join("".join(parts).split())
