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
from .wake import MAX_WAKE_LIST_LIMIT

LOG = logging.getLogger("aivengers_wire")


def _fit_listing_response(
    response: dict[str, Any],
    rows_key: str,
    *,
    cursor_field_a: str,
    cursor_field_b: str,
    budget: int = MAX_FRAME_BYTES,
) -> dict[str, Any]:
    """Bound a listing response by its ACTUAL FINAL transport size.

    R3 bounded listings by serialized bytes; the reviewer's R3-B probe
    showed the measurement still ran BEFORE two fields that ship on the
    wire: the continuation cursor (``next_after``) and the server's
    ``ok`` envelope. A page measuring 65,530B bare actually measured
    65,558B complete and was refused by ordinary clients. R4 fix: the
    cursor is set BEFORE any measurement, every measurement is taken on
    the COMPLETE transport frame ``{"ok": true, **response}`` — the exact
    bytes a client reads — and the cursor is recomputed after every trim.

    R3-A fix: a legitimately EMPTY listing (fresh empty queue, filtered
    query, or a cursor exactly past the last record) is a successful,
    correctly shaped page — empty items, ``has_more=false``, no
    continuation cursor. Only a NON-EMPTY page trimmed down to nothing
    (a single legal row exceeding the frame budget — unreachable given
    admission caps: max wake row ≈ 13.5 KB with a maximal note, max session row ≈ 13 KB,
    both far below the 65,536-byte budget) is a contract violation and
    fails loudly rather than lying.

    Lossless pagination holds: dropped rows stay reachable through the
    truthful cursor, ``has_more`` is forced true whenever anything was
    dropped, and the cursor always names the last INCLUDED row, so a
    consumer walking pages can never loop forever (the cursor advances
    or ``has_more`` goes false).
    """
    rows = response[rows_key]
    started_empty = not rows

    def _refresh_cursor() -> None:
        if response["has_more"] and rows:
            last = rows[-1]
            response["next_after"] = {
                cursor_field_a: last[cursor_field_a],
                cursor_field_b: last[cursor_field_b],
            }
        else:
            response["next_after"] = None

    def _wire_bytes() -> int:
        # The COMPLETE final transport frame, envelope included — what an
        # ordinary client actually reads and bounds-checks.
        return len(encode_frame({"ok": True, **response}))

    # Cursor metadata exists BEFORE the first size measurement (R3-B).
    _refresh_cursor()
    while rows and _wire_bytes() > budget:
        rows.pop()
        response["has_more"] = True
        _refresh_cursor()
    if not rows and not started_empty:
        raise WireError(
            "listing response cannot fit the transport frame budget; "
            "this indicates an admission-cap contract violation"
        )
    return response

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
    """One authenticated listener's watch state.

    W0 wake note: ``seat`` records which seat the subscription belongs to
    (wake fanout is seat-scoped) and ``session_id`` optionally binds the
    watch to one registered session, so a session-targeted wake is delivered
    to that session's own watcher rather than sprayed across the seat.
    """

    channels: frozenset[str] | None  # None = all channels
    timer: asyncio.TimerHandle | None = None
    seat: str = ""
    session_id: str | None = None


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
                if request.get("op") == "watch":
                    # W0 wake: after the subscribed ack is on the wire, hand
                    # this watcher any wake intents that were queued for its
                    # seat (or its bound session) while nothing was listening.
                    await self._deliver_pending_wakes(seat, writer)
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
        if op == "register":
            require_exact_keys(
                request,
                required={"op", "adapter"},
                optional={
                    "capabilities",
                    "model_claim",
                    "lease_seconds",
                    "platform_pid",
                    "meta",
                },
            )
            session = self.store.register_session(
                seat=seat,
                adapter=request["adapter"],
                capabilities=request.get("capabilities"),
                model_claim=request.get("model_claim"),
                lease_seconds=request.get("lease_seconds"),
                platform_pid=request.get("platform_pid"),
                meta=request.get("meta"),
            )
            return {"session": session}
        if op == "heartbeat":
            require_exact_keys(
                request, required={"op", "session_id"}, optional={"state"}
            )
            session = self.store.heartbeat_session(
                seat=seat, session_id=request["session_id"], state=request.get("state")
            )
            return {"session": session}
        if op == "unregister":
            require_exact_keys(request, required={"op", "session_id"})
            session = self.store.unregister_session(
                seat=seat, session_id=request["session_id"]
            )
            return {"session": session}
        if op == "sessions":
            require_exact_keys(
                request,
                required={"op"},
                optional={"limit", "after_registered_at", "after_session_id"},
            )
            limit = request.get("limit", MAX_WAKE_LIST_LIMIT)
            if isinstance(limit, bool) or not isinstance(limit, int) or limit <= 0:
                raise ProtocolError("limit must be a positive integer")
            if limit > MAX_WAKE_LIST_LIMIT:
                raise ProtocolError(f"limit must not exceed {MAX_WAKE_LIST_LIMIT}")
            if (request.get("after_registered_at") is None) != (
                request.get("after_session_id") is None
            ):
                raise ProtocolError(
                    "after_registered_at and after_session_id must be given together"
                )
            # T2: fetch one sentinel row beyond the public cap so has-more
            # is truthful at the cap (the old handler hardcoded False and
            # offered no continuation at all).
            rows = self.store.list_sessions(
                seat=seat.name,
                limit=limit,
                after_registered_at=request.get("after_registered_at"),
                after_session_id=request.get("after_session_id"),
                include_sentinel=True,
            )
            has_more = len(rows) > limit
            response = {
                "sessions": rows[:limit],
                "has_more": has_more,
            }
            # T3: bound the response by its actual serialized frame size;
            # dropped rows stay reachable through the truthful cursor.
            return _fit_listing_response(
                response,
                "sessions",
                cursor_field_a="registered_at",
                cursor_field_b="session_id",
            )
        if op == "wake":
            require_exact_keys(
                request,
                required={"op", "target_seat", "task_ref"},
                optional={
                    "target_session_id",
                    "idempotency_key",
                    "priority",
                    "ttl_seconds",
                    "note",
                },
            )
            wake, resolution, candidates = self.store.request_wake(
                requester=seat,
                target_seat=request["target_seat"],
                task_ref=request["task_ref"],
                target_session_id=request.get("target_session_id"),
                idempotency_key=request.get("idempotency_key"),
                priority=request.get("priority", 5),
                ttl_seconds=request.get("ttl_seconds"),
                note=request.get("note"),
            )
            if resolution == "deliverable":
                # R2/Sol revision: a successful frame write is a TRANSMISSION
                # attempt, recorded as such (attempts+1) — never a durable
                # delivery mark. The wake stays pending until an authenticated
                # adapter receipt (wake_ack) transitions it to delivered, so
                # unacknowledged wakes replay on reconnect. No disconnect race
                # can strand a phantom "delivered" state because none is
                # written here.
                if self._fanout_wake(wake):
                    wake = self.store.record_wake_attempt(wake["wake_id"])
                    resolution = "transmitted"
                else:
                    resolution = "queued"
            return {"wake": wake, "resolution": resolution, "candidates": candidates}
        if op == "wake_ack":
            require_exact_keys(request, required={"op", "wake_id", "session_id"})
            wake = self.store.acknowledge_wake(
                seat=seat, wake_id=request["wake_id"], session_id=request["session_id"]
            )
            return {"wake": wake}
        if op == "claim":
            require_exact_keys(request, required={"op", "wake_id", "session_id"})
            return self.store.claim_wake(
                seat=seat, wake_id=request["wake_id"], session_id=request["session_id"]
            )
        if op == "complete":
            require_exact_keys(
                request,
                required={"op", "task_ref", "session_id", "outcome"},
                optional={"note"},
            )
            return self.store.complete_task(
                seat=seat,
                task_ref=request["task_ref"],
                session_id=request["session_id"],
                outcome=request["outcome"],
                note=request.get("note"),
            )
        if op == "wake_status":
            require_exact_keys(request, required={"op", "wake_id"})
            wake = self.store.get_wake(request["wake_id"])
            if wake is None:
                raise WireError("unknown wake")
            if seat.name not in (wake["target_seat"], wake["requested_by"]):
                raise WireError("wake is not addressed to this seat")
            return {"wake": wake}
        if op == "wake_list":
            require_exact_keys(
                request,
                required={"op"},
                optional={"state", "limit", "after_created_at", "after_wake_id"},
            )
            state = request.get("state")
            limit = request.get("limit", MAX_WAKE_LIST_LIMIT)
            if isinstance(limit, bool) or not isinstance(limit, int) or limit <= 0:
                raise ProtocolError("limit must be a positive integer")
            if limit > MAX_WAKE_LIST_LIMIT:
                raise ProtocolError(
                    f"limit must not exceed {MAX_WAKE_LIST_LIMIT}"
                )
            # T2: the sentinel row is fetched BEYOND the public cap. The old
            # ``fetch = min(limit + 1, MAX_WAKE_LIST_LIMIT)`` capped the
            # fetch AT the cap, so a default/max page could never see the
            # sentinel and reported ``has_more=false`` with the rest of the
            # queue unread — false completeness, not conservative reporting.
            if (request.get("after_created_at") is None) != (
                request.get("after_wake_id") is None
            ):
                raise ProtocolError(
                    "after_created_at and after_wake_id must be given together"
                )
            rows = self.store.list_wakes(
                seat=seat.name,
                state=state,
                limit=limit,
                after_created_at=request.get("after_created_at"),
                after_wake_id=request.get("after_wake_id"),
                include_sentinel=True,
            )
            has_more = len(rows) > limit
            response = {
                "wakes": rows[:limit],
                "has_more": has_more,
            }
            # T3: bound the response by its actual serialized frame size;
            # dropped rows stay reachable through the truthful cursor.
            return _fit_listing_response(
                response,
                "wakes",
                cursor_field_a="created_at",
                cursor_field_b="wake_id",
            )
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
        require_exact_keys(
            request, required={"op"}, optional={"channels", "timeout_ms", "session_id"}
        )
        session_id: str | None = None
        session_id_value = request.get("session_id")
        if session_id_value is not None:
            if not isinstance(session_id_value, str) or not session_id_value:
                raise ProtocolError("session_id must be a non-empty string when present")
            session = self.store.get_session(session_id_value)
            if session is None:
                raise WireError("unknown session")
            if session["seat"] != seat.name:
                raise WireError("session belongs to a different seat")
            if not session["lease_live"]:
                # R4: a retired/offline/expired session cannot bind a watcher
                # and consume its targeted queued wakes; the adapter must
                # register a current session instead.
                raise WireError(
                    f"session is {session['state']} and not lease-live; "
                    "register a new session to watch as a wake target"
                )
            session_id = session_id_value
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
        self._subscribers[writer] = _Subscription(
            channels=channels, timer=timer, seat=seat.name, session_id=session_id
        )
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

    # ── wake: durable targeted wake intents (W0) ─────────────────────────

    def _drop_subscriber_now(self, writer: Any, last_ref: str, *, reason: str) -> None:
        """Synchronous containment drop used by the wake fanout paths.

        Mirrors :meth:`_drop_subscriber` (which is async only by signature —
        it contains no awaits) so the wake dispatch, which runs inside the
        synchronous ``_dispatch`` call, can contain a stalled listener without
        restructuring the existing message fanout.
        """

        self._unsubscribe(writer)
        try:
            writer.write(
                encode_frame(
                    {
                        "ok": True,
                        "event": "overflow",
                        "reason": reason,
                        "last_persisted_id": last_ref,
                    }
                )
            )
        except Exception:
            pass
        try:
            writer.close()
        except Exception:  # pragma: no cover - transport already gone
            pass
        LOG.info("wake subscriber dropped reason=%s last_ref=%s", reason, last_ref)

    def _fanout_wake(self, wake: dict[str, Any]) -> int:
        """Deliver one wake event to the target's bound watcher, if live.

        A session-targeted wake goes only to the watch connection bound to
        that ``session_id``. A seat-level wake goes only to unbound watchers
        of the seat. Returns the number of sockets the frame was written to;
        the caller turns that into ``delivered`` vs ``queued`` honestly.
        Write-only, bounded by the same buffer rule as message fanout.
        """

        if not self._subscribers:
            return 0
        frame = encode_frame({"ok": True, "event": "wake", "wake": wake})
        target_session = wake.get("target_session_id")
        delivered = 0
        for writer, subscription in list(self._subscribers.items()):
            if subscription.seat != wake["target_seat"]:
                continue
            if target_session is not None and subscription.session_id != target_session:
                continue
            if target_session is None and subscription.session_id is not None:
                continue
            transport = writer.transport
            buffered = 0 if transport is None else transport.get_write_buffer_size()
            if buffered + len(frame) > SUBSCRIBER_BUFFER_LIMIT:
                self._drop_subscriber_now(writer, wake["wake_id"], reason="overflow")
                continue
            try:
                writer.write(frame)
            except Exception:
                self._drop_subscriber_now(writer, wake["wake_id"], reason="unwritable")
                continue
            delivered += 1
        return delivered

    async def _deliver_pending_wakes(self, seat: Seat, writer: Any) -> None:
        """Hand a freshly subscribed watcher its seat's queued wake intents.

        Runs immediately after the ``subscribed`` ack is flushed, so the
        client observes ack first, then catch-up wake events, then live
        traffic — the same subscribe-then-catch-up shape as message history.

        R2 revision: the batch is read-only and bounded
        (:data:`PENDING_REPLAY_BATCH` via the store); each successful frame
        write records a transmission attempt and NOTHING else — the wake
        stays pending until its adapter acks, so an overflow or disconnect
        here loses nothing: unwritten wakes remain pending and replay on the
        next subscription. Marking must never outrun writing.

        Flow control: wakes can carry a ``note`` of up to
        :data:`~aivengers_wire.wake.MAX_NOTE_CHARS` characters (≈ 13.5 KB per
        frame), so a full batch is ≈ 430 KB — more than
        SUBSCRIBER_BUFFER_LIMIT. Written as one synchronous burst, a socket
        with a small kernel send buffer (macOS defaults to 8 KiB for Unix
        stream sockets) overflowed and dropped a healthy watcher mid-replay,
        every reconnect. Each accepted write is therefore followed by this
        connection's own ``drain()``: replay advances at the watcher's read
        pace, only this watcher's handler waits, and the buffer-limit drop
        stays as the backstop for live fanout. Because the drain yields to
        the loop, each later wake is re-checked first — the watch may have
        timed out or been dropped (replay stops; the rest stay pending), and
        the wake may have been acknowledged, claimed, cancelled or expired
        meanwhile (it is skipped instead of replayed as stale ``pending``).
        """

        subscription = self._subscribers.get(writer)
        if subscription is None:
            return
        wakes = self.store.pending_wakes_for_watch(
            seat=seat.name, session_id=subscription.session_id
        )
        written = 0
        for index, wake in enumerate(wakes):
            if index:
                if self._subscribers.get(writer) is not subscription:
                    break
                current = self.store.get_wake(wake["wake_id"])
                if current is None or current["state"] != "pending":
                    continue
                wake = current
            frame = encode_frame({"ok": True, "event": "wake", "wake": wake})
            transport = writer.transport
            buffered = 0 if transport is None else transport.get_write_buffer_size()
            if buffered + len(frame) > SUBSCRIBER_BUFFER_LIMIT:
                self._drop_subscriber_now(writer, wake["wake_id"], reason="overflow")
                return
            try:
                writer.write(frame)
            except Exception:
                self._drop_subscriber_now(writer, wake["wake_id"], reason="unwritable")
                return
            # Attempt recorded only after the write was accepted; the wake's
            # durable state is untouched (pending until wake_ack).
            self.store.record_wake_attempt(wake["wake_id"])
            written += 1
            try:
                await writer.drain()
            except (ConnectionError, RuntimeError):
                break  # Connection gone; the rest stay pending for replay.
        if written:
            LOG.info(
                "pending wakes transmitted seat=%r count=%d", seat.name, written
            )


async def run_server(store: WireStore) -> None:
    server = WireServer(store)
    await server.start()
    LOG.info("AIvengers Wire listening at %s", store.socket_path)
    try:
        await server.serve_forever()
    finally:
        await server.close()
