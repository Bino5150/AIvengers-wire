"""Local Unix-socket server for AIvengers Wire."""

from __future__ import annotations

import asyncio
import errno
import logging
import os
import socket
import stat
import struct
from dataclasses import dataclass
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

#: Watch subscriptions are bounded: a listener whose socket stops draining
#: gets dropped once its pending outbound bytes pass this limit. The drop is
#: announced with an ``overflow`` event when possible, then the connection is
#: closed; the listener reconnects and catches up from its durable cursor.
SUBSCRIBER_BUFFER_LIMIT = 262_144

#: Upper bound on channels per watch subscription and on the requested
#: subscription timeout, so one seat cannot pin unbounded server resources.
MAX_WATCH_CHANNELS = 16
MAX_WATCH_TIMEOUT_MS = 3_600_000


@dataclass
class _Subscription:
    """One authenticated listener's watch state."""

    channels: frozenset[str] | None  # None = all channels
    timer: asyncio.TimerHandle | None = None


class WireServer:
    def __init__(self, store: WireStore):
        self.store = store
        self._server: asyncio.AbstractServer | None = None
        # writer -> subscription. Mutated only from the event loop thread and
        # never across an await, so registration plus watermark capture is
        # atomic with respect to message persistence.
        self._subscribers: dict[Any, _Subscription] = {}

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
        for writer in list(self._subscribers):
            self._unsubscribe(writer)
        self._subscribers.clear()
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
                response = self._dispatch(request, seat=seat, peer=peer, writer=writer)
                committed = (
                    response["message"]
                    if isinstance(response.get("message"), dict)
                    else None
                )
                # Queue the poster's response before any event frame, so a
                # subscriber that posts on its own watched connection still
                # reads its response first. Publication of a committed message
                # is driven by durable persistence, never by the posting
                # connection's I/O: the response is queued without waiting on
                # its drain, fanout runs immediately, and only then does the
                # handler wait on the poster's drain. A disconnecting or
                # backpressured poster can therefore neither suppress nor
                # reorder committed-message notifications.
                try:
                    writer.write(encode_frame({"ok": True, **response}))
                except Exception:
                    pass  # Undeliverable response; the persisted message still publishes.
                if committed is not None and not committed.get("duplicate"):
                    # A client_id retry persists nothing new, so it does not
                    # ring the bell twice. The event carries exactly the
                    # envelope `read` returns for the same message.
                    await self._fanout(
                        {k: v for k, v in committed.items() if k != "duplicate"}
                    )
                if not await self._flush_response(writer, committed):
                    return
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
            self._unsubscribe(writer)
            writer.close()
            try:
                await writer.wait_closed()
            except (BrokenPipeError, ConnectionResetError):
                pass

    async def _flush_response(self, writer: Any, committed: dict[str, Any] | None) -> bool:
        """Await the queued response's drain; False when the connection is gone.

        ``committed`` is the persisted message this flush follows (None for
        non-post operations). Deliberate test seam: the only scheduling point
        after persistence and fanout, so controlled-scheduling tests can hold
        a backpressured poster's drain — keyed on the committed message —
        without touching framing, persistence, subscriptions or fanout.
        """
        try:
            await writer.drain()
        except (ConnectionError, RuntimeError):
            return False
        return True

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
        self,
        request: dict[str, Any],
        *,
        seat: Seat,
        peer: PeerCredentials,
        writer: Any,
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
        if op == "watch":
            return self._handle_watch(request, seat=seat, writer=writer)
        raise ProtocolError(f"unsupported operation: {op!r}")

    # ── watch: authenticated real-time notification subscription ──────────

    def _handle_watch(
        self, request: dict[str, Any], *, seat: Seat, writer: Any
    ) -> dict[str, Any]:
        """Register this connection as a notification listener.

        Returns the ``subscribed`` ack carrying the watermark synchronously:
        watermark capture and registry insertion happen in the same event-loop
        step with no await between them, so every message is either included
        in the subscriber's pre-subscribe read window (id <= watermark) or
        guaranteed to raise a later ``message`` event (id > watermark). No
        wake-up can be lost in between.

        Events never touch seat cursors; ``read``/``advance`` remain the only
        cursor mutators. A watch is per-connection: closing the socket (or a
        later ``overflow``/``timeout`` event) ends it.
        """
        require_exact_keys(request, required={"op"}, optional={"channels", "timeout_ms"})
        channels_value = request.get("channels")
        if channels_value is None:
            channels: frozenset[str] | None = None
        else:
            if not isinstance(channels_value, list) or not channels_value:
                raise ProtocolError("channels must be a non-empty list when present")
            if len(channels_value) > MAX_WATCH_CHANNELS:
                raise ProtocolError(f"channels must not exceed {MAX_WATCH_CHANNELS} entries")
            channels = frozenset(
                validate_name(value, field="channel") for value in channels_value
            )
        timeout_value = request.get("timeout_ms")
        timer: asyncio.TimerHandle | None = None
        if timeout_value is not None:
            timeout_ms = validate_positive_int(timeout_value, field="timeout_ms")
            if timeout_ms > MAX_WATCH_TIMEOUT_MS:
                raise ProtocolError(f"timeout_ms must not exceed {MAX_WATCH_TIMEOUT_MS}")
            loop = asyncio.get_running_loop()
            timer = loop.call_later(
                timeout_ms / 1000.0, self._on_watch_timeout, writer
            )
        watermark = self.store.max_message_id()
        previous = self._subscribers.get(writer)
        if previous is not None and previous.timer is not None:
            previous.timer.cancel()
        self._subscribers[writer] = _Subscription(channels=channels, timer=timer)
        LOG.info(
            "watch subscribed seat=%r channels=%r watermark=%d",
            seat.name,
            sorted(channels) if channels is not None else "*",
            watermark,
        )
        return {"event": "subscribed", "watermark": watermark}

    def _unsubscribe(self, writer: Any) -> None:
        subscription = self._subscribers.pop(writer, None)
        if subscription is not None and subscription.timer is not None:
            subscription.timer.cancel()

    def _on_watch_timeout(self, writer: Any) -> None:
        subscription = self._subscribers.get(writer)
        if subscription is None:
            return
        self._unsubscribe(writer)
        try:
            writer.write(encode_frame({"ok": True, "event": "timeout"}))
        except Exception:  # pragma: no cover - transport already gone
            pass
        LOG.info("watch timeout delivered")

    async def _fanout(self, message: dict[str, Any]) -> None:
        """Deliver one persisted message to every matching live subscriber.

        ``message`` must already be the exact envelope ``read`` returns (the
        caller strips the post-result-only ``duplicate`` field). Write-only on
        purpose: awaiting each subscriber's drain would let one slow listener
        stall the posting connection. Boundedness comes from the pre-write
        buffer check instead — a listener that stops draining is dropped with
        an ``overflow`` event once its pending bytes pass
        SUBSCRIBER_BUFFER_LIMIT, and catches up from its durable cursor after
        reconnecting.
        """
        if not self._subscribers:
            return
        channel = message["channel"]
        frame = encode_frame({"ok": True, "event": "message", "message": message})
        for writer, subscription in list(self._subscribers.items()):
            if (
                subscription.channels is not None
                and channel not in subscription.channels
            ):
                continue
            transport = writer.transport
            buffered = 0 if transport is None else transport.get_write_buffer_size()
            if buffered + len(frame) > SUBSCRIBER_BUFFER_LIMIT:
                await self._drop_subscriber(writer, message["id"], reason="overflow")
                continue
            try:
                writer.write(frame)
            except Exception:
                await self._drop_subscriber(writer, message["id"], reason="unwritable")

    async def _drop_subscriber(self, writer: Any, last_id: int, *, reason: str) -> None:
        self._unsubscribe(writer)
        try:
            writer.write(
                encode_frame(
                    {
                        "ok": True,
                        "event": "overflow",
                        "reason": reason,
                        "last_persisted_id": last_id,
                    }
                )
            )
        except Exception:
            pass
        try:
            writer.close()
        except Exception:  # pragma: no cover - transport already gone
            pass
        LOG.info("watch subscriber dropped reason=%s last_persisted_id=%d", reason, last_id)


async def run_server(store: WireStore) -> None:
    server = WireServer(store)
    await server.start()
    LOG.info("AIvengers Wire listening at %s", store.socket_path)
    try:
        await server.serve_forever()
    finally:
        await server.close()
