from __future__ import annotations

import math
import re
from dataclasses import dataclass
from typing import Any

import msgspec
import re2

from lumi.contracts import AIProfileAnnouncement, AITextVariant, CaptureSpec

IDENTIFIER = re.compile(r"^[a-z][a-z0-9_-]{0,63}$")
PROFILE_KEY = re.compile(r"^[a-z][a-z0-9_-]{0,63}\.[a-z][a-z0-9_.-]{0,127}$")
HASH = re.compile(r"^[a-f0-9]{64}$")
UNSAFE_REGEX = (
    re.compile(r"\\[1-9]"),
    re.compile(r"\(\?P="),
    re.compile(r"\(\?<=[^)]"),
    re.compile(r"\(\?<![^)]"),
    re.compile(r"\(\?>"),
    re.compile(r"\(\?\("),
)
ALLOWED_MODELS = frozenset({"gemma4:31b"})
ALLOWED_TYPES = frozenset({"object", "array", "string", "integer", "number", "boolean", "null"})
MAX_PROFILE_BYTES = 32 * 1024
MAX_SCHEMA_DEPTH = 8
MAX_SCHEMA_NODES = 128
MAX_PATTERN_LENGTH = 4_096
MAX_CAPTURES = 32


class AIValidationError(ValueError):
    pass


@dataclass(frozen=True, slots=True)
class CompiledVariant:
    spec: AITextVariant
    pattern: re2._Regexp


@dataclass(frozen=True, slots=True)
class ValidatedProfile:
    announcement: AIProfileAnnouncement
    variants: tuple[CompiledVariant, ...]


def validate_profile_announcement(
    announcement: AIProfileAnnouncement,
) -> ValidatedProfile:
    if announcement.schema != 1 or announcement.kind not in {"ready", "heartbeat", "retire"}:
        raise AIValidationError("unsupported profile announcement")
    if not IDENTIFIER.fullmatch(announcement.module) or not IDENTIFIER.fullmatch(
        announcement.group
    ):
        raise AIValidationError("invalid profile owner")
    if announcement.generation < 1:
        raise AIValidationError("invalid profile generation")
    profile = announcement.profile
    if not PROFILE_KEY.fullmatch(profile.key) or not profile.key.startswith(
        f"{announcement.module}."
    ):
        raise AIValidationError("profile namespace does not match its owner")
    if not HASH.fullmatch(announcement.profile_hash):
        raise AIValidationError("invalid profile hash")
    if len(msgspec.json.encode(profile)) > MAX_PROFILE_BYTES:
        raise AIValidationError("profile is too large")
    if not profile.version or len(profile.version) > 64:
        raise AIValidationError("invalid profile version")
    if profile.model not in ALLOWED_MODELS:
        raise AIValidationError("model is not allowed")
    if not profile.system_prompt or len(profile.system_prompt.encode("utf-8")) > 16_384:
        raise AIValidationError("invalid system prompt")
    if profile.response_mode not in {"text", "json"}:
        raise AIValidationError("invalid response mode")
    if not 1 <= profile.max_attempts <= 2:
        raise AIValidationError("invalid retry limit")
    if not 100 <= profile.deadline_ms <= 120_000:
        raise AIValidationError("invalid profile deadline")
    if not 0 <= profile.temperature <= 2:
        raise AIValidationError("invalid temperature")
    if not 1 <= profile.max_output_tokens <= 4_096:
        raise AIValidationError("invalid output token ceiling")
    if profile.repair_prompt is not None and (
        not profile.repair_prompt or len(profile.repair_prompt.encode("utf-8")) > 4_096
    ):
        raise AIValidationError("invalid repair prompt")
    _validate_schema(profile.input_schema)

    variants: list[CompiledVariant] = []
    if profile.response_mode == "text":
        if not profile.text_variants or profile.response_schema is not None:
            raise AIValidationError("invalid text response contract")
        outcomes: set[str] = set()
        for variant in profile.text_variants:
            if (
                not IDENTIFIER.fullmatch(variant.outcome)
                or variant.outcome in outcomes
                or not variant.pattern.startswith("^")
                or not variant.pattern.endswith("$")
                or len(variant.pattern) > MAX_PATTERN_LENGTH
                or any(rule.search(variant.pattern) for rule in UNSAFE_REGEX)
            ):
                raise AIValidationError("invalid text response variant")
            outcomes.add(variant.outcome)
            captures = {capture.name for capture in variant.captures}
            if len(captures) != len(variant.captures) or len(captures) > MAX_CAPTURES:
                raise AIValidationError("invalid response captures")
            for capture in variant.captures:
                _validate_capture(capture)
            try:
                pattern = re2.compile(variant.pattern)
            except Exception as exc:
                raise AIValidationError("invalid response regex") from exc
            if pattern.groups > MAX_CAPTURES:
                raise AIValidationError("too many response captures")
            if set(pattern.groupindex) != captures:
                raise AIValidationError("response capture declaration mismatch")
            variants.append(CompiledVariant(variant, pattern))
    else:
        if profile.text_variants or profile.response_schema is None:
            raise AIValidationError("invalid JSON response contract")
        _validate_schema(profile.response_schema)
    return ValidatedProfile(announcement, tuple(variants))


def validate_value(value: Any, schema: dict[str, Any], *, path: str = "$") -> Any:
    expected = schema["type"]
    if expected == "object":
        if not isinstance(value, dict) or any(not isinstance(key, str) for key in value):
            raise AIValidationError(f"{path} must be an object")
        properties = schema.get("properties", {})
        required = set(schema.get("required", ()))
        missing = required - set(value)
        if missing or (
            not schema.get("additionalProperties", False) and set(value) - set(properties)
        ):
            raise AIValidationError(f"{path} has invalid properties")
        return {
            key: validate_value(item, properties[key], path=f"{path}.{key}")
            for key, item in value.items()
            if key in properties
        }
    if expected == "array":
        if not isinstance(value, list):
            raise AIValidationError(f"{path} must be an array")
        minimum = int(schema.get("minItems", 0))
        maximum = int(schema.get("maxItems", 128))
        if not minimum <= len(value) <= maximum:
            raise AIValidationError(f"{path} has invalid length")
        item_schema = schema["items"]
        return [
            validate_value(item, item_schema, path=f"{path}[{index}]")
            for index, item in enumerate(value)
        ]
    if expected == "string":
        if not isinstance(value, str):
            raise AIValidationError(f"{path} must be a string")
        if (
            not int(schema.get("minLength", 0))
            <= len(value)
            <= int(schema.get("maxLength", 16_384))
        ):
            raise AIValidationError(f"{path} has invalid length")
    elif expected == "integer":
        if isinstance(value, bool) or not isinstance(value, int):
            raise AIValidationError(f"{path} must be an integer")
        if value < schema.get("minimum", value) or value > schema.get("maximum", value):
            raise AIValidationError(f"{path} is outside its bounds")
    elif expected == "number":
        if isinstance(value, bool) or not isinstance(value, (int, float)):
            raise AIValidationError(f"{path} must be a number")
        if not math.isfinite(float(value)):
            raise AIValidationError(f"{path} must be finite")
        if value < schema.get("minimum", value) or value > schema.get("maximum", value):
            raise AIValidationError(f"{path} is outside its bounds")
    elif expected == "boolean":
        if not isinstance(value, bool):
            raise AIValidationError(f"{path} must be a boolean")
    elif expected == "null":
        if value is not None:
            raise AIValidationError(f"{path} must be null")
    if "enum" in schema and value not in schema["enum"]:
        raise AIValidationError(f"{path} is not an allowed value")
    return value


def convert_capture(value: str | None, capture: CaptureSpec) -> Any:
    if value is None:
        if capture.required:
            raise AIValidationError("missing response capture")
        return None
    if capture.kind in {"text", "normalized_text"}:
        converted: Any = value if capture.kind == "text" else " ".join(value.split())
        measured = len(converted)
    elif capture.kind == "integer":
        converted = int(value)
        measured = converted
    elif capture.kind == "boolean":
        folded = value.casefold()
        if folded not in {"true", "false"}:
            raise AIValidationError("invalid boolean response capture")
        converted = folded == "true"
        measured = int(converted)
    else:
        raise AIValidationError("unsupported response capture")
    if capture.minimum is not None and measured < capture.minimum:
        raise AIValidationError("response capture is below its minimum")
    if capture.maximum is not None and measured > capture.maximum:
        raise AIValidationError("response capture exceeds its maximum")
    return converted


def _validate_capture(capture: CaptureSpec) -> None:
    if not IDENTIFIER.fullmatch(capture.name) or capture.kind not in {
        "text",
        "normalized_text",
        "integer",
        "boolean",
    }:
        raise AIValidationError("invalid response capture")
    if (
        capture.minimum is not None
        and capture.maximum is not None
        and capture.minimum > capture.maximum
    ):
        raise AIValidationError("invalid response capture bounds")


def _validate_schema(schema: dict[str, Any]) -> None:
    nodes = 0

    def walk(node: Any, depth: int) -> None:
        nonlocal nodes
        nodes += 1
        if nodes > MAX_SCHEMA_NODES or depth > MAX_SCHEMA_DEPTH or not isinstance(node, dict):
            raise AIValidationError("schema is too complex")
        expected = node.get("type")
        if expected not in ALLOWED_TYPES:
            raise AIValidationError("unsupported schema type")
        allowed = {"type", "enum"}
        if expected == "object":
            allowed |= {"properties", "required", "additionalProperties"}
            properties = node.get("properties")
            required = node.get("required", [])
            if (
                not isinstance(properties, dict)
                or len(properties) > 32
                or any(not isinstance(key, str) or len(key) > 64 for key in properties)
                or not isinstance(required, list)
                or any(key not in properties for key in required)
                or len(set(required)) != len(required)
                or node.get("additionalProperties", False) is not False
            ):
                raise AIValidationError("invalid object schema")
            for child in properties.values():
                walk(child, depth + 1)
        elif expected == "array":
            allowed |= {"items", "minItems", "maxItems"}
            minimum = node.get("minItems", 0)
            maximum = node.get("maxItems", 128)
            if (
                "items" not in node
                or not isinstance(minimum, int)
                or not isinstance(maximum, int)
                or not 0 <= minimum <= maximum <= 128
            ):
                raise AIValidationError("invalid array schema")
            walk(node["items"], depth + 1)
        elif expected == "string":
            allowed |= {"minLength", "maxLength"}
            minimum = node.get("minLength", 0)
            maximum = node.get("maxLength", 16_384)
            if (
                not isinstance(minimum, int)
                or not isinstance(maximum, int)
                or not 0 <= minimum <= maximum <= 16_384
            ):
                raise AIValidationError("invalid string schema")
        elif expected in {"integer", "number"}:
            allowed |= {"minimum", "maximum"}
            minimum = node.get("minimum")
            maximum = node.get("maximum")
            if (
                (
                    minimum is not None
                    and (isinstance(minimum, bool) or not isinstance(minimum, (int, float)))
                )
                or (
                    maximum is not None
                    and (isinstance(maximum, bool) or not isinstance(maximum, (int, float)))
                )
                or (minimum is not None and maximum is not None and minimum > maximum)
            ):
                raise AIValidationError("invalid numeric schema")
        if set(node) - allowed:
            raise AIValidationError("unsupported schema keyword")
        enum = node.get("enum")
        if enum is not None and (not isinstance(enum, list) or not 1 <= len(enum) <= 32):
            raise AIValidationError("invalid schema enum")

    walk(schema, 0)
