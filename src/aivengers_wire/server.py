"""Local Unix-socket server for AIvengers Wire."""

from __future__ import annotations

import asyncio
import errno
import logging
import os
import socket
import stat
import struct
from pathlib import Path
from typing import Any

from .errors import AuthenticationError, ProtocolError, WireError
from .protocol import (
    MAX_FRAME_BYTES,
    decode_frame,
    encode_frame,
    require_exact_keys,
    validate_body,
    validate_client_id,
    validate_name,
    validate_nonnegative_int,
    validate_positive_int,
    validate_read_limit,
)
from .storage import PeerCredentials, Seat, WireStore

LOG = logging.getLogger("aivengers_wire")


class WireServer:
    def __init__(self, store: WireStore):
        self.store = store
        self._server: asyncio.AbstractServer | None = None

    async def start(self) -> None:
        self.store.initialize()
        self._prepare_socket_path()
        self._server = await asyncio.start_unix_server(
            self._handle_client,
            path=self.store.socket_path,
            limit=MAX_FRAME_BYTES + 1,
        )
        os.chmod(self.store.socket_path, 0o600)

    async def serve_forever(self) -> None:
        if self._server is None:
            await self.start()
        assert self._server is not None
        async with self._server:
            await self._server.serve_forever()

    async def close(self) -> None:
        if self._server is not None:
            self._server.close()
            await self._server.wait_closed()
            self._server = None
        try:
            if self.store.socket_path.exists() and stat.S_ISSOCK(
                self.store.socket_path.lstat().st_mode
            ):
                self.store.socket_path.unlink()
        except FileNotFoundError:
            pass

    def _prepare_socket_path(self) -> None:
        path = self.store.socket_path
        try:
            mode = path.lstat().st_mode
        except FileNotFoundError:
            return
        if not stat.S_ISSOCK(mode):
            raise WireError(f"refusing to replace non-socket path: {path}")
        probe = socket.socket(socket.AF_UNIX, socket.SOCK_STREAM)
        try:
            probe.settimeout(0.25)
            probe.connect(str(path))
        except OSError as exc:
            if exc.errno not in {errno.ECONNREFUSED, errno.ENOENT}:
                raise WireError(f"cannot inspect existing socket {path}: {exc}") from exc
        else:
            raise WireError(f"wire server is already listening at {path}")
        finally:
            probe.close()
        path.unlink(missing_ok=True)

    @staticmethod
    def _peer_credentials(writer: asyncio.StreamWriter) -> PeerCredentials:
        peer_socket = writer.get_extra_info("socket")
        if peer_socket is None or not hasattr(socket, "SO_PEERCRED"):
            raise WireError("Unix peer credentials are unavailable")
        raw = peer_socket.getsockopt(
            socket.SOL_SOCKET, socket.SO_PEERCRED, struct.calcsize("3i")
        )
        pid, uid, gid = struct.unpack("3i", raw)
        return PeerCredentials(pid=pid, uid=uid, gid=gid)

    async def _read_request(self, reader: asyncio.StreamReader) -> dict[str, Any] | None:
        try:
            raw = await reader.readline()
        except ValueError as exc:
            raise ProtocolError("request exceeds maximum frame size") from exc
        if not raw:
            return None
        if not raw.endswith(b"\n"):
            raise ProtocolError("request must end with a newline")
        return decode_frame(raw[:-1])

    @staticmethod
    async def _send(writer: asyncio.StreamWriter, response: dict[str, Any]) -> None:
        writer.write(encode_frame(response))
        await writer.drain()

    async def _handle_client(
        self, reader: asyncio.StreamReader, writer: asyncio.StreamWriter
    ) -> None:
        peer: PeerCredentials | None = None
        seat: Seat | None = None
        try:
            peer = self._peer_credentials(writer)
            auth = await self._read_request(reader)
            if auth is None:
                return
            seat = self._authenticate_request(auth)
            await self._send(writer, {"ok": True, "seat": seat.name})

            while True:
                request = await self._read_request(reader)
                if request is None:
                    return
                response = self._dispatch(request, seat=seat, peer=peer)
                await self._send(writer, {"ok": True, **response})
        except AuthenticationError as exc:
            await self._safe_error(writer, "authentication_failed", str(exc))
        except ProtocolError as exc:
            await self._safe_error(writer, "invalid_request", str(exc))
        except WireError as exc:
            await self._safe_error(writer, "operation_failed", str(exc))
        except (BrokenPipeError, ConnectionResetError):
            return
        except Exception:
            LOG.exception(
                "unexpected wire server error for seat=%r peer=%r",
                None if seat is None else seat.name,
                peer,
            )
            await self._safe_error(writer, "internal_error", "wire operation failed")
        finally:
            writer.close()
            try:
                await writer.wait_closed()
            except (BrokenPipeError, ConnectionResetError):
                pass

    async def _safe_error(self, writer: asyncio.StreamWriter, code: str, message: str) -> None:
        try:
            await self._send(writer, {"ok": False, "error": code, "message": message})
        except (BrokenPipeError, ConnectionResetError):
            pass

    def _authenticate_request(self, request: dict[str, Any]) -> Seat:
        require_exact_keys(request, required={"op", "seat", "token"})
        if request["op"] != "auth":
            raise AuthenticationError("first request must authenticate a seat")
        seat_name = validate_name(request["seat"], field="seat")
        token = request["token"]
        if not isinstance(token, str) or not token or len(token) > 512:
            raise AuthenticationError("invalid seat credentials")
        return self.store.authenticate(seat_name, token)

    def _dispatch(
        self, request: dict[str, Any], *, seat: Seat, peer: PeerCredentials
    ) -> dict[str, Any]:
        op = request.get("op")
        if op == "ping":
            require_exact_keys(request, required={"op"})
            return {"status": "ready", "seat": seat.name}
        if op == "post":
            require_exact_keys(
                request,
                required={"op", "channel", "body"},
                optional={"reply_to", "forwarded_for", "client_id"},
            )
            channel = validate_name(request["channel"], field="channel")
            body = validate_body(request["body"])
            reply_to_value = request.get("reply_to")
            reply_to = (
                None
                if reply_to_value is None
                else validate_positive_int(reply_to_value, field="reply_to")
            )
            forwarded_value = request.get("forwarded_for")
            forwarded_for = (
                None
                if forwarded_value is None
                else validate_name(forwarded_value, field="forwarded_for")
            )
            client_value = request.get("client_id")
            client_id = None if client_value is None else validate_client_id(client_value)
            message = self.store.post_message(
                seat=seat,
                peer=peer,
                channel=channel,
                body=body,
                reply_to=reply_to,
                forwarded_for=forwarded_for,
                client_id=client_id,
            )
            return {"message": message}
        if op == "read":
            require_exact_keys(
                request,
                required={"op", "after", "limit"},
                optional={"channel"},
            )
            channel_value = request.get("channel")
            channel = (
                None
                if channel_value is None
                else validate_name(channel_value, field="channel")
            )
            after = validate_nonnegative_int(request["after"], field="after")
            limit = validate_read_limit(request["limit"])
            return {
                "messages": self.store.read_messages(
                    channel=channel, after=after, limit=limit
                )
            }
        if op == "cursor":
            require_exact_keys(request, required={"op", "channel"})
            channel = validate_name(request["channel"], field="channel")
            return {"channel": channel, "last_message_id": self.store.get_cursor(seat=seat, channel=channel)}
        if op == "advance":
            require_exact_keys(
                request, required={"op", "channel", "last_message_id"}
            )
            channel = validate_name(request["channel"], field="channel")
            last_message_id = validate_nonnegative_int(
                request["last_message_id"], field="last_message_id"
            )
            advanced = self.store.advance_cursor(
                seat=seat, channel=channel, last_message_id=last_message_id
            )
            return {"channel": channel, "last_message_id": advanced}
        if op == "channels":
            require_exact_keys(request, required={"op"})
            return {"channels": self.store.list_channels()}
        raise ProtocolError(f"unsupported operation: {op!r}")


async def run_server(store: WireStore) -> None:
    server = WireServer(store)
    await server.start()
    LOG.info("AIvengers Wire listening at %s", store.socket_path)
    try:
        await server.serve_forever()
    finally:
        await server.close()
