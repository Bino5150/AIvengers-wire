"""Strict JSON-line protocol validation for AIvengers Wire."""

from __future__ import annotations

import json
import re
from typing import Any, Iterable

from .errors import ProtocolError

MAX_FRAME_BYTES = 65_536
MAX_BODY_CHARS = 32_768
MAX_CLIENT_ID_CHARS = 128
MAX_READ_LIMIT = 200

_NAME_RE = re.compile(r"^[a-z0-9](?:[a-z0-9._-]{0,62}[a-z0-9])?$")


def _strict_object(pairs: Iterable[tuple[str, Any]]) -> dict[str, Any]:
    result: dict[str, Any] = {}
    for key, value in pairs:
        if key in result:
            raise ProtocolError(f"duplicate JSON key: {key}")
        result[key] = value
    return result


def decode_frame(raw: bytes) -> dict[str, Any]:
    if not raw:
        raise ProtocolError("empty request")
    if len(raw) > MAX_FRAME_BYTES:
        raise ProtocolError("request exceeds maximum frame size")
    try:
        text = raw.decode("utf-8")
    except UnicodeDecodeError as exc:
        raise ProtocolError("request is not valid UTF-8") from exc
    try:
        value = json.loads(text, object_pairs_hook=_strict_object)
    except ProtocolError:
        raise
    except (json.JSONDecodeError, ValueError) as exc:
        raise ProtocolError("request is not valid JSON") from exc
    if not isinstance(value, dict):
        raise ProtocolError("request must be a JSON object")
    return value


def encode_frame(value: dict[str, Any]) -> bytes:
    return (json.dumps(value, ensure_ascii=False, separators=(",", ":")) + "\n").encode(
        "utf-8"
    )


def require_exact_keys(
    request: dict[str, Any], *, required: set[str], optional: set[str] | None = None
) -> None:
    optional = optional or set()
    missing = required - request.keys()
    unknown = request.keys() - required - optional
    if missing:
        raise ProtocolError(f"missing fields: {', '.join(sorted(missing))}")
    if unknown:
        raise ProtocolError(f"unknown fields: {', '.join(sorted(unknown))}")


def validate_name(value: Any, *, field: str) -> str:
    if not isinstance(value, str) or not _NAME_RE.fullmatch(value):
        raise ProtocolError(
            f"{field} must be 1-64 lowercase letters, digits, dots, dashes, or underscores"
        )
    return value


def validate_body(value: Any) -> str:
    if not isinstance(value, str):
        raise ProtocolError("body must be a string")
    body = value.strip()
    if not body:
        raise ProtocolError("body must not be empty")
    if len(body) > MAX_BODY_CHARS:
        raise ProtocolError(f"body exceeds {MAX_BODY_CHARS} characters")
    return body


def validate_nonnegative_int(value: Any, *, field: str) -> int:
    if isinstance(value, bool) or not isinstance(value, int) or value < 0:
        raise ProtocolError(f"{field} must be a non-negative integer")
    return value


def validate_positive_int(value: Any, *, field: str) -> int:
    result = validate_nonnegative_int(value, field=field)
    if result == 0:
        raise ProtocolError(f"{field} must be greater than zero")
    return result


def validate_read_limit(value: Any) -> int:
    result = validate_positive_int(value, field="limit")
    if result > MAX_READ_LIMIT:
        raise ProtocolError(f"limit must not exceed {MAX_READ_LIMIT}")
    return result


def validate_client_id(value: Any) -> str:
    if not isinstance(value, str) or not value or len(value) > MAX_CLIENT_ID_CHARS:
        raise ProtocolError(
            f"client_id must be a non-empty string up to {MAX_CLIENT_ID_CHARS} characters"
        )
    return value
