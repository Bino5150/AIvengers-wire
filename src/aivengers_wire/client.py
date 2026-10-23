"""Synchronous client for the local AIvengers Wire server."""

from __future__ import annotations

import json
import socket
from pathlib import Path
from typing import Any, Iterator

from .errors import WireError
from .protocol import MAX_FRAME_BYTES, encode_frame, validate_name
from .storage import WireStore, load_token


class WireClient:
    def __init__(self, store: WireStore, *, seat: str, token_path: Path | None = None):
        self.store = store
        self.seat = seat
        self.token_path = token_path or (store.seat_dir / f"{seat}.token")

    def request(self, payload: dict[str, Any]) -> dict[str, Any]:
        token = load_token(self.token_path)
        sock = socket.socket(socket.AF_UNIX, socket.SOCK_STREAM)
        sock.settimeout(5.0)
        try:
            sock.connect(str(self.store.socket_path))
            with sock.makefile("rwb", buffering=0) as stream:
                self._write(stream, {"op": "auth", "seat": self.seat, "token": token})
                auth = self._read(stream)
                if not auth.get("ok"):
                    raise WireError(auth.get("message", "authentication failed"))
                self._write(stream, payload)
                response = self._read(stream)
                if not response.get("ok"):
                    raise WireError(response.get("message", "wire operation failed"))
                return response
        except FileNotFoundError as exc:
            raise WireError(f"wire server is not running at {self.store.socket_path}") from exc
        except (ConnectionRefusedError, socket.timeout) as exc:
            raise WireError(f"cannot reach wire server at {self.store.socket_path}: {exc}") from exc
        finally:
            sock.close()

    @staticmethod
    def _write(stream: Any, payload: dict[str, Any]) -> None:
        stream.write(encode_frame(payload))

    @staticmethod
    def _read(stream: Any) -> dict[str, Any]:
        raw = stream.readline(MAX_FRAME_BYTES + 1)
        if not raw:
            raise WireError("wire server closed the connection without a response")
        if len(raw) > MAX_FRAME_BYTES:
            raise WireError("wire server response exceeds maximum frame size")
        try:
            response = json.loads(raw.decode("utf-8"))
        except (UnicodeDecodeError, json.JSONDecodeError) as exc:
            raise WireError("wire server returned an invalid response") from exc
        if not isinstance(response, dict):
            raise WireError("wire server returned a non-object response")
        return response

    # ── watch: streaming notification listener ────────────────────────────

    def watch(
        self,
        *,
        channels: list[str] | None = None,
        timeout_ms: int | None = None,
        idle_timeout_seconds: float | None = None,
    ) -> Iterator[dict[str, Any]]:
        """Subscribe to persisted-message notifications and yield event frames.

        Opens one authenticated connection, sends the ``watch`` op, yields the
        ``subscribed`` ack (carrying the watermark) and then every ``message``
        / ``timeout`` / ``overflow`` event frame as it arrives, until the
        server closes the connection (overflow) or the subscription times out.
        The generator ends cleanly on EOF; ``socket.timeout`` becomes
        :class:`WireError` so a hung relay surfaces instead of blocking
        forever.

        Events never advance cursors. The canonical no-lost-wake-up pattern is
        subscribe-then-read: note the ack's ``watermark``, read from your
        durable cursor up to it, then treat events with ``id > watermark`` as
        new. Duplicates (an event for a message you also read) are harmless —
        dedupe by canonical ``id``.

        ``channels`` filters which channels raise events (None = all).
        ``timeout_ms`` asks the server to end the subscription with a
        ``timeout`` event after that long without... anything to deliver.
        ``idle_timeout_seconds`` is the client-side socket timeout used while
        waiting for frames; it defaults to generous headroom over the server
        timeout so server-side expiry is what you actually observe.
        """
        if channels is not None:
            if not channels:
                raise WireError("channels must be a non-empty list when filtering")
            channels = [validate_name(value, field="channel") for value in channels]
        payload: dict[str, Any] = {"op": "watch"}
        if channels is not None:
            payload["channels"] = channels
        if timeout_ms is not None:
            if isinstance(timeout_ms, bool) or not isinstance(timeout_ms, int) or timeout_ms <= 0:
                raise WireError("timeout_ms must be a positive integer")
            payload["timeout_ms"] = timeout_ms
        token = load_token(self.token_path)
        if idle_timeout_seconds is None:
            idle_timeout_seconds = 30.0 + (timeout_ms / 1000.0 if timeout_ms else 0.0)
        sock = socket.socket(socket.AF_UNIX, socket.SOCK_STREAM)
        sock.settimeout(idle_timeout_seconds)
        try:
            sock.connect(str(self.store.socket_path))
            with sock.makefile("rwb", buffering=0) as stream:
                self._write(stream, {"op": "auth", "seat": self.seat, "token": token})
                auth = self._read(stream)
                if not auth.get("ok"):
                    raise WireError(auth.get("message", "authentication failed"))
                self._write(stream, payload)
                while True:
                    frame = self._read(stream)
                    if frame.get("ok") is not True:
                        raise WireError(frame.get("message", "wire operation failed"))
                    if "event" not in frame:
                        raise WireError("wire server sent a non-event frame during watch")
                    yield frame
        except FileNotFoundError as exc:
            raise WireError(
                f"wire server is not running at {self.store.socket_path}"
            ) from exc
        except (ConnectionRefusedError, socket.timeout) as exc:
            raise WireError(
                f"cannot reach wire server at {self.store.socket_path}: {exc}"
            ) from exc
        finally:
            sock.close()
