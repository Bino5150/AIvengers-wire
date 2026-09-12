"""Synchronous client for the local AIvengers Wire server."""

from __future__ import annotations

import json
import socket
from pathlib import Path
from typing import Any

from .errors import WireError
from .protocol import MAX_FRAME_BYTES, encode_frame
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
