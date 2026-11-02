"""Synchronous client for the local AIvengers Wire server."""

from __future__ import annotations

import json
import socket
from pathlib import Path
from typing import Any, Iterator

from .errors import WireError, WireResponseTooLargeError
from .protocol import MAX_RESPONSE_FRAME_BYTES, encode_frame, validate_name
from .storage import WireStore, load_token

#: Socket failures a request or watch can hit on the way to, or while talking
#: to, the relay. Each is reported as a ``WireError`` (with the original
#: exception chained as ``__cause__``) instead of escaping as a traceback.
TRANSPORT_ERRORS: tuple[type[OSError], ...] = (
    FileNotFoundError,
    PermissionError,
    ConnectionRefusedError,
    ConnectionResetError,
    BrokenPipeError,
    socket.timeout,
)

#: Rows asked of the relay per ``channels`` page, and the most pages a listing
#: may take (a runaway guard: 1,000 pages is 200,000 channels).
CHANNELS_PAGE_SIZE = 200
MAX_CHANNEL_PAGES = 1_000


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
                    error = WireError(response.get("message", "wire operation failed"))
                    code = response.get("error")
                    error.code = code if isinstance(code, str) else None
                    raise error
                return response
        except TRANSPORT_ERRORS as exc:
            raise self._transport_error(exc) from exc
        finally:
            sock.close()

    def _transport_error(self, exc: OSError) -> WireError:
        path = self.store.socket_path
        if isinstance(exc, FileNotFoundError):
            return WireError(f"wire server is not running at {path}")
        if isinstance(exc, PermissionError):
            return WireError(f"permission denied reaching wire server at {path}: {exc}")
        if isinstance(exc, (ConnectionResetError, BrokenPipeError)):
            return WireError(f"lost connection to wire server at {path}: {exc}")
        return WireError(f"cannot reach wire server at {path}: {exc}")

    @staticmethod
    def _write(stream: Any, payload: dict[str, Any]) -> None:
        # An unbuffered socket file's write() is a single send(), which may
        # take only part of the frame when the kernel send buffer is small
        # (macOS gives Unix sockets 8 KiB). Keep writing until the whole
        # frame is out; otherwise the relay waits forever for the rest of
        # the line and the request times out.
        frame = memoryview(encode_frame(payload))
        while frame:
            sent = stream.write(frame)
            if sent is None:
                raise socket.timeout("timed out writing to the wire server")
            frame = frame[sent:]

    @staticmethod
    def _read(stream: Any) -> dict[str, Any]:
        # Responses are bounded by MAX_RESPONSE_FRAME_BYTES, not the request
        # bound: the response to any legal post, and any single stored
        # message, fits it (see protocol.py).
        raw = stream.readline(MAX_RESPONSE_FRAME_BYTES + 1)
        if not raw:
            raise WireError("wire server closed the connection without a response")
        if len(raw) > MAX_RESPONSE_FRAME_BYTES:
            raise WireResponseTooLargeError("wire server response exceeds maximum frame size")
        try:
            response = json.loads(raw.decode("utf-8"))
        except (UnicodeDecodeError, json.JSONDecodeError) as exc:
            raise WireError("wire server returned an invalid response") from exc
        if not isinstance(response, dict):
            raise WireError("wire server returned a non-object response")
        return response

    def read_messages(
        self, *, channel: str | None, after: int, limit: int
    ) -> dict[str, Any]:
        """Read up to ``limit`` messages after ``after``, following continuations.

        The relay bounds each ``read`` page by bytes and reports ``has_more``
        and ``next_after``; this keeps reading from the last returned id until
        ``limit`` messages have arrived or none remain. The first request
        carries ``limit`` unchanged, so the relay validates it as before.

        Returns ``{"messages": [...], "has_more": ..., "next_after": ...}``.
        ``next_after`` is the id to pass as ``after`` to continue, or None
        when nothing remains. ``has_more`` is None when the relay predates
        bounded reads (it reports no ``has_more``); then only one page is read.
        """
        messages: list[dict[str, Any]] = []
        while True:
            payload: dict[str, Any] = {
                "op": "read",
                "after": after,
                "limit": limit - len(messages),
            }
            if channel is not None:
                payload["channel"] = channel
            response = self.request(payload)
            page = response["messages"]
            messages.extend(page)
            has_more = response.get("has_more")
            if not isinstance(has_more, bool):
                return {"messages": messages, "has_more": None, "next_after": None}
            if has_more and not page:
                raise WireError("wire server reported more messages but returned none")
            if not has_more or len(messages) >= limit:
                break
            after = page[-1]["id"]
        return {
            "messages": messages,
            "has_more": has_more,
            "next_after": messages[-1]["id"] if has_more else None,
        }

    def list_channels(self, *, page_size: int = CHANNELS_PAGE_SIZE) -> list[dict[str, Any]]:
        """Every channel row, in name order, however many channels exist.

        Walks the relay's ``channels`` pages by ``has_more``/``next_after`` so
        no single response has to hold the whole inventory. A relay that
        predates paging refuses the ``limit`` field as an unknown field; the
        unpaged request that relay understands is then sent instead, which
        is exactly what this method's callers sent before. A page that does
        not advance the cursor raises rather than looping.
        """
        rows: list[dict[str, Any]] = []
        after: str | None = None
        for _ in range(MAX_CHANNEL_PAGES):
            payload: dict[str, Any] = {"op": "channels", "limit": page_size}
            if after is not None:
                payload["after"] = after
            try:
                response = self.request(payload)
            except WireError as exc:
                if after is None and exc.code == "invalid_request" and "unknown fields" in str(exc):
                    return self.request({"op": "channels"})["channels"]
                raise
            page = response["channels"]
            rows.extend(page)
            if response.get("has_more") is not True:
                return rows
            next_after = response.get("next_after")
            if not page or not isinstance(next_after, str) or (after is not None and next_after <= after):
                raise WireError("wire server reported more channels but did not advance")
            after = next_after
        raise WireError(f"channel listing exceeded {MAX_CHANNEL_PAGES} pages")

    # ── watch: streaming notification listener ────────────────────────────

    def watch(
        self,
        *,
        channels: list[str] | None = None,
        timeout_ms: int | None = None,
        idle_timeout_seconds: float | None = None,
        session_id: str | None = None,
    ) -> Iterator[dict[str, Any]]:
        """Subscribe to persisted-message notifications and yield event frames.

        Opens one authenticated connection, sends the ``watch`` op, yields the
        ``subscribed`` ack (carrying the watermark) and then every ``message``
        / ``timeout`` / ``overflow`` event frame as it arrives. The generator
        ends cleanly — without raising — when the subscription completes
        normally: the server's ``timeout`` event ends the iteration, and an
        ``overflow`` event followed by the server closing the connection ends
        it too. Any other server-side close raises :class:`WireError` so a
        hung or broken relay surfaces instead of blocking forever.

        Events never advance cursors. The canonical no-lost-wake-up pattern is
        subscribe-then-read: note the ack's ``watermark``, read from your
        durable cursor up to it, then treat events with ``id > watermark`` as
        new. Duplicates (an event for a message you also read) are harmless —
        dedupe by canonical ``id``.

        ``channels`` filters which channels raise events (None = all).
        ``timeout_ms`` asks the server to end the subscription with a
        ``timeout`` event after that long without anything to deliver; that
        event terminates this iterator promptly. The server keeps the
        authenticated connection itself reusable for protocol clients; this
        iterator simply stops consuming it. ``idle_timeout_seconds`` is the
        client-side socket timeout used while waiting for frames; it defaults
        to generous headroom over the server timeout so server-side expiry is
        what you actually observe.

        W0 wake: ``session_id`` optionally binds this subscription to one
        registered session owned by this seat. A session-targeted wake is
        delivered only to the watcher bound to that session; a seat-level
        wake is delivered only to unbound watchers. Bound watchers also
        receive their seat's queued wake intents immediately after the
        ``subscribed`` ack (catch-up), as ``wake`` event frames. Wake events
        never advance cursors.
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
        if session_id is not None:
            if not isinstance(session_id, str) or not session_id:
                raise WireError("session_id must be a non-empty string when present")
            payload["session_id"] = session_id
        token = load_token(self.token_path)
        if idle_timeout_seconds is None:
            idle_timeout_seconds = 30.0 + (timeout_ms / 1000.0 if timeout_ms else 0.0)
        sock = socket.socket(socket.AF_UNIX, socket.SOCK_STREAM)
        sock.settimeout(idle_timeout_seconds)
        terminated = False
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
                    if frame["event"] == "timeout":
                        # Normal subscription completion: the server ends the
                        # subscription (and keeps the connection reusable for
                        # other clients); this iterator stops consuming it
                        # and returns instead of waiting on a silent socket.
                        return
                    if frame["event"] == "overflow":
                        # The server drops this listener after announcing the
                        # overflow, so the next read is EOF by design. Treat
                        # that documented close as normal termination; any
                        # other close still raises below.
                        terminated = True
        except TRANSPORT_ERRORS as exc:
            raise self._transport_error(exc) from exc
        except WireError:
            if terminated:
                return
            raise
        finally:
            sock.close()

    # ── wake: session registry + wake pipeline (W0) ──────────────────────
    # All wake operations use the same one-shot authenticated ``request``
    # pattern as every other relay operation: connect, auth, one request
    # frame, one response, close. No persistent connection is held.

    def register_session(
        self,
        *,
        adapter: str,
        capabilities: dict[str, bool] | None = None,
        model_claim: str | None = None,
        lease_seconds: int | None = None,
        platform_pid: int | None = None,
        meta: dict[str, Any] | None = None,
    ) -> dict[str, Any]:
        """Register a runtime session owned by this seat.

        The relay generates the ``session_id``; the caller never supplies its
        own identity. ``model_claim`` is recorded verbatim with evidence
        ``unverified`` — a Wire token authenticates a seat, never a model.
        """
        payload: dict[str, Any] = {"op": "register", "adapter": adapter}
        if capabilities is not None:
            payload["capabilities"] = capabilities
        if model_claim is not None:
            payload["model_claim"] = model_claim
        if lease_seconds is not None:
            payload["lease_seconds"] = lease_seconds
        if platform_pid is not None:
            payload["platform_pid"] = platform_pid
        if meta is not None:
            payload["meta"] = meta
        return self.request(payload)["session"]

    def heartbeat_session(
        self, *, session_id: str, state: str | None = None
    ) -> dict[str, Any]:
        """Renew the session's lease and optionally update its live state."""
        payload: dict[str, Any] = {"op": "heartbeat", "session_id": session_id}
        if state is not None:
            payload["state"] = state
        return self.request(payload)["session"]

    def unregister_session(self, *, session_id: str) -> dict[str, Any]:
        """Mark a session offline; its lease and live state end immediately."""
        return self.request({"op": "unregister", "session_id": session_id})["session"]

    def sessions(self) -> list[dict[str, Any]]:
        """List this seat's registered sessions with computed lease liveness."""
        return self.request({"op": "sessions"})["sessions"]

    def sessions_page(
        self,
        *,
        limit: int | None = None,
        after_registered_at: str | None = None,
        after_session_id: str | None = None,
    ) -> dict[str, Any]:
        """One full session-listing page with truthful continuation (T2).

        Returns ``{"sessions": [...], "has_more": bool, "next_after": ...}``;
        pass ``next_after``'s fields back as ``after_registered_at`` /
        ``after_session_id`` to walk a seat larger than one page.
        """
        payload: dict[str, Any] = {"op": "sessions"}
        if limit is not None:
            payload["limit"] = limit
        if after_registered_at is not None:
            payload["after_registered_at"] = after_registered_at
        if after_session_id is not None:
            payload["after_session_id"] = after_session_id
        return self.request(payload)

    def wake(
        self,
        *,
        target_seat: str,
        task_ref: str,
        target_session_id: str | None = None,
        idempotency_key: str | None = None,
        priority: int | None = None,
        ttl_seconds: int | None = None,
        note: str | None = None,
    ) -> dict[str, Any]:
        """Request a durable wake intent for an existing task grant.

        Returns the relay's honest receipt: ``resolution`` is one of
        ``delivered``, ``queued``, ``queued_offline``, ``ambiguous``,
        ``unsupported`` or ``duplicate`` (idempotent retry), plus the wake
        record and, for ``ambiguous``, the candidate session ids. A wake
        references an existing open task; it can never create one.
        """
        payload: dict[str, Any] = {
            "op": "wake",
            "target_seat": target_seat,
            "task_ref": task_ref,
        }
        if target_session_id is not None:
            payload["target_session_id"] = target_session_id
        if idempotency_key is not None:
            payload["idempotency_key"] = idempotency_key
        if priority is not None:
            payload["priority"] = priority
        if ttl_seconds is not None:
            payload["ttl_seconds"] = ttl_seconds
        if note is not None:
            payload["note"] = note
        return self.request(payload)

    def wake_ack(self, *, wake_id: str, session_id: str) -> dict[str, Any]:
        """Acknowledge a wake intent as an adapter. NOT a resume proof."""
        response = self.request(
            {"op": "wake_ack", "wake_id": wake_id, "session_id": session_id}
        )
        return response["wake"]

    def claim(self, *, wake_id: str, session_id: str) -> dict[str, Any]:
        """Atomically claim the wake's task for one session (exactly once)."""
        return self.request(
            {"op": "claim", "wake_id": wake_id, "session_id": session_id}
        )

    def complete(
        self,
        *,
        task_ref: str,
        session_id: str,
        outcome: str,
        note: str | None = None,
    ) -> dict[str, Any]:
        """Record a truthful completion or failure receipt for a claimed task."""
        payload: dict[str, Any] = {
            "op": "complete",
            "task_ref": task_ref,
            "session_id": session_id,
            "outcome": outcome,
        }
        if note is not None:
            payload["note"] = note
        return self.request(payload)

    def wake_status(self, *, wake_id: str) -> dict[str, Any]:
        """Fetch one wake's exact pipeline position (target/requester only)."""
        return self.request({"op": "wake_status", "wake_id": wake_id})["wake"]

    def wake_list(
        self,
        *,
        state: str | None = None,
        limit: int | None = None,
        after_created_at: str | None = None,
        after_wake_id: str | None = None,
    ) -> list[dict[str, Any]]:
        """List wakes addressed to or requested by this seat (catch-up path).

        R8: keyset-paginated and bounded; pass ``after_*`` values from the
        server's ``next_after`` to walk a backlog beyond one page.
        """
        payload: dict[str, Any] = {"op": "wake_list"}
        if state is not None:
            payload["state"] = state
        if limit is not None:
            payload["limit"] = limit
        if after_created_at is not None:
            payload["after_created_at"] = after_created_at
        if after_wake_id is not None:
            payload["after_wake_id"] = after_wake_id
        return self.request(payload)["wakes"]

    def wake_list_page(
        self,
        *,
        state: str | None = None,
        limit: int | None = None,
        after_created_at: str | None = None,
        after_wake_id: str | None = None,
    ) -> dict[str, Any]:
        """One full wake-listing page with truthful continuation (T2/T3).

        Returns ``{"wakes": [...], "has_more": bool, "next_after": ...}``.
        The plain :meth:`wake_list` helper is preserved for existing callers
        and still returns just the rows of a single page.
        """
        payload: dict[str, Any] = {"op": "wake_list"}
        if state is not None:
            payload["state"] = state
        if limit is not None:
            payload["limit"] = limit
        if after_created_at is not None:
            payload["after_created_at"] = after_created_at
        if after_wake_id is not None:
            payload["after_wake_id"] = after_wake_id
        return self.request(payload)

    def wake_list_iter(
        self,
        *,
        state: str | None = None,
        limit: int | None = None,
        max_pages: int = 1_000,
    ) -> Iterator[dict[str, Any]]:
        """Walk the ENTIRE wake backlog page by page via truthful cursors.

        T2/T3 companion to :meth:`wake_list_page`: yields each full page
        (including its continuation metadata) until ``has_more`` is false.
        ``max_pages`` is a runaway guard, not a data bound — a healthy
        server terminates on ``has_more=false`` long before it.
        """
        cursor: dict[str, str] | None = None
        for _ in range(max_pages):
            page = self.wake_list_page(
                state=state,
                limit=limit,
                after_created_at=cursor["created_at"] if cursor else None,
                after_wake_id=cursor["wake_id"] if cursor else None,
            )
            yield page
            if not page["has_more"] or not page["next_after"]:
                return
            cursor = page["next_after"]
        raise WireError(
            f"wake_list_iter exceeded {max_pages} pages; cursor did not terminate"
        )
