"""Adversarial verification for AIvengers Wire real-time notifications.

Every test runs against a real relay subprocess with a disposable state
directory (inherited from RunningWireTestCase), so receipts are end-to-end
socket truth, not in-process mocks.
"""

from __future__ import annotations

import asyncio
import contextlib
import json
import os
import socket
import struct
import tempfile
import time
import unittest
from pathlib import Path
from typing import Any

from aivengers_wire.client import WireClient
from aivengers_wire.errors import WireError
from aivengers_wire.protocol import encode_frame
from aivengers_wire.server import WireServer
from aivengers_wire.storage import WireStore

from test_wire import RunningWireTestCase


def watch_socket(store: WireStore, seat: str, payload: dict[str, Any]):
    """Open an authenticated watch connection; return (sock, stream, ack)."""
    token = (store.seat_dir / f"{seat}.token").read_text(encoding="utf-8").strip()
    sock = socket.socket(socket.AF_UNIX, socket.SOCK_STREAM)
    sock.settimeout(10.0)
    sock.connect(str(store.socket_path))
    stream = sock.makefile("rwb", buffering=0)
    stream.write(encode_frame({"op": "auth", "seat": seat, "token": token}))
    auth = json.loads(stream.readline())
    assert auth["ok"], auth
    stream.write(encode_frame(payload))
    ack = json.loads(stream.readline())
    return sock, stream, ack

def recv_line(sock: socket.socket, timeout: float) -> bytes:
    """Read one newline-terminated frame with raw recv (makefile streams are
    poisoned by a mid-read timeout; raw recv stays usable)."""
    sock.settimeout(timeout)
    buf = bytearray()
    while not buf.endswith(b"\n"):
        chunk = sock.recv(65_536)
        if not chunk:
            return b""
        buf.extend(chunk)
    return bytes(buf)


class WatchNotificationTests(RunningWireTestCase):
    def test_real_time_delivery_preserves_canonical_envelope(self) -> None:
        sock, stream, ack = watch_socket(self.store, "rookie", {"op": "watch"})
        try:
            self.assertTrue(ack["ok"])
            self.assertEqual(ack["event"], "subscribed")
            self.assertIsInstance(ack["watermark"], int)
            posted = self.client("tech").request(
                {"op": "post", "channel": "general", "body": "Doorbell test."}
            )["message"]
            event = json.loads(stream.readline())
            self.assertEqual(event["event"], "message")
            message = event["message"]
            # The event carries the exact canonical envelope read returns.
            self.assertEqual(message["id"], posted["id"])
            self.assertEqual(message["sender"], "tech")
            self.assertEqual(message["transport_actor"], "tech")
            self.assertEqual(message["provenance"], "direct")
            self.assertEqual(message["authority"], "peer")
            self.assertIs(message["owner"], False)
            self.assertEqual(message["body"], "Doorbell test.")
            self.assertEqual(message["peer"]["uid"], os.getuid())
        finally:
            sock.close()

    def test_subscription_race_loses_nothing_and_duplicates_nothing(self) -> None:
        # Post BEFORE the watcher subscribes.
        early = self.client("tech").request(
            {"op": "post", "channel": "general", "body": "posted pre-subscribe"}
        )["message"]
        sock, stream, ack = watch_socket(self.store, "rookie", {"op": "watch"})
        try:
            watermark = ack["watermark"]
            self.assertGreaterEqual(watermark, early["id"])
            # Canonical pattern: read from durable cursor up to the watermark.
            history = self.client("rookie").request(
                {"op": "read", "channel": "general", "after": 0, "limit": 50}
            )["messages"]
            self.assertIn(early["id"], [m["id"] for m in history])
            # Post AFTER subscribe: must arrive as an event, not be missed.
            late = self.client("tech").request(
                {"op": "post", "channel": "general", "body": "posted post-subscribe"}
            )["message"]
            event = json.loads(stream.readline())
            self.assertEqual(event["event"], "message")
            self.assertGreater(event["message"]["id"], watermark)
            self.assertEqual(event["message"]["id"], late["id"])
            # Union of read window and events covers everything exactly once.
            event_ids = [event["message"]["id"]]
            read_ids = [m["id"] for m in history if m["id"] <= watermark]
            self.assertNotIn(event["message"]["id"], read_ids)
            self.assertEqual(
                sorted(read_ids + event_ids),
                sorted({early["id"], late["id"]}),
            )
        finally:
            sock.close()

    def test_multiple_subscribers_receive_consecutive_posts_in_canonical_order(self) -> None:
        socks = []
        try:
            for seat in ("rookie", "bino-gateway"):
                sock, stream, ack = watch_socket(self.store, seat, {"op": "watch"})
                self.assertEqual(ack["event"], "subscribed")
                socks.append((sock, stream))
            first = self.client("tech").request(
                {"op": "post", "channel": "general", "body": "one"}
            )["message"]
            second = self.client("bino").request(
                {"op": "post", "channel": "general", "body": "two"}
            )["message"]
            for _, stream in socks:
                e1 = json.loads(stream.readline())
                e2 = json.loads(stream.readline())
                self.assertEqual([e1["message"]["id"], e2["message"]["id"]], [first["id"], second["id"]])
        finally:
            for sock, _ in socks:
                sock.close()

    def test_channel_filter_delivers_only_subscribed_channels(self) -> None:
        sock, stream, ack = watch_socket(
            self.store, "rookie", {"op": "watch", "channels": ["general"]}
        )
        try:
            self.assertEqual(ack["event"], "subscribed")
            self.client("tech").request(
                {"op": "post", "channel": "adversarial-review", "body": "not for you"}
            )
            hit = self.client("tech").request(
                {"op": "post", "channel": "general", "body": "for you"}
            )["message"]
            event = json.loads(stream.readline())
            self.assertEqual(event["message"]["channel"], "general")
            self.assertEqual(event["message"]["id"], hit["id"])
        finally:
            sock.close()

    def test_offline_recipient_catches_up_from_durable_cursor(self) -> None:
        missed = [
            self.client("tech").request(
                {"op": "post", "channel": "general", "body": f"while away {i}"}
            )["message"]["id"]
            for i in range(3)
        ]
        # Recipient was offline for all of the above; now subscribes and reads.
        sock, stream, ack = watch_socket(self.store, "rookie", {"op": "watch"})
        try:
            self.assertEqual(ack["watermark"], missed[-1])
            catch_up = self.client("rookie").request(
                {"op": "read", "channel": "general", "after": 0, "limit": 50}
            )["messages"]
            self.assertEqual([m["id"] for m in catch_up], missed)
            # And the next post rings the doorbell live.
            live = self.client("tech").request(
                {"op": "post", "channel": "general", "body": "back online"}
            )["message"]
            event = json.loads(stream.readline())
            self.assertEqual(event["message"]["id"], live["id"])
        finally:
            sock.close()

    def test_watch_requires_authentication(self) -> None:
        # The server refuses ANY first frame that is not a well-formed auth
        # request; an unauthenticated watch is therefore rejected as
        # invalid_request before seat credentials are ever considered. The
        # security property under test is the refusal itself.
        sock = socket.socket(socket.AF_UNIX, socket.SOCK_STREAM)
        sock.settimeout(5.0)
        try:
            sock.connect(str(self.store.socket_path))
            stream = sock.makefile("rwb", buffering=0)
            stream.write(encode_frame({"op": "watch"}))
            response = json.loads(stream.readline())
            self.assertFalse(response["ok"])
            self.assertEqual(response["error"], "invalid_request")
        finally:
            sock.close()

    def test_watch_rejects_spoofed_and_unknown_fields(self) -> None:
        for payload in (
            {"op": "watch", "sender": "bino"},
            {"op": "watch", "authority": "owner"},
            {"op": "watch", "channels": []},
            {"op": "watch", "channels": ["ok", "BAD NAME!"]},
            {"op": "watch", "timeout_ms": 0},
            {"op": "watch", "timeout_ms": 3_600_001},
        ):
            with self.subTest(payload=payload):
                sock, stream, response = watch_socket(self.store, "tech", payload)
                sock.close()
                self.assertFalse(response["ok"])
                self.assertEqual(response["error"], "invalid_request")

    def test_events_never_advance_cursors(self) -> None:
        sock, stream, ack = watch_socket(self.store, "rookie", {"op": "watch"})
        try:
            self.client("tech").request(
                {"op": "post", "channel": "general", "body": "ring"}
            )
            event = json.loads(stream.readline())
            self.assertEqual(event["event"], "message")
            cursor = self.client("rookie").request({"op": "cursor", "channel": "general"})
            self.assertEqual(cursor["last_message_id"], 0)
        finally:
            sock.close()

    def test_timeout_ends_subscription_and_connection_stays_usable(self) -> None:
        sock, stream, ack = watch_socket(
            self.store, "rookie", {"op": "watch", "timeout_ms": 300}
        )
        try:
            self.assertEqual(ack["event"], "subscribed")
            timeout_event = json.loads(stream.readline())
            self.assertEqual(timeout_event["event"], "timeout")
            # Subscription ended: a later post must NOT ring this bell...
            self.client("tech").request(
                {"op": "post", "channel": "general", "body": "after timeout"}
            )
            with self.assertRaises(socket.timeout):
                sock.settimeout(0.6)
                sock.recv(4096)
            # ...but the connection itself remains a working wire connection.
            stream.write(encode_frame({"op": "ping"}))
            pong = json.loads(recv_line(sock, 5.0))
            self.assertTrue(pong["ok"])
            self.assertEqual(pong["status"], "ready")
        finally:
            sock.close()

    def test_duplicate_delivery_dedupes_by_canonical_id(self) -> None:
        # Watcher subscribes, then reads overlapping history while events flow.
        first = self.client("tech").request(
            {"op": "post", "channel": "general", "body": "overlap me"}
        )["message"]
        sock, stream, ack = watch_socket(self.store, "rookie", {"op": "watch"})
        try:
            read_ids = [
                m["id"]
                for m in self.client("rookie").request(
                    {"op": "read", "channel": "general", "after": 0, "limit": 50}
                )["messages"]
            ]
            second = self.client("tech").request(
                {"op": "post", "channel": "general", "body": "event only"}
            )["message"]
            event_ids = [json.loads(stream.readline())["message"]["id"]]
            seen = read_ids + event_ids
            self.assertEqual(len(seen), len(set(seen)))
            self.assertEqual(set(seen), {first["id"], second["id"]})
        finally:
            sock.close()

    def test_slow_listener_is_contained_without_harming_others(self) -> None:
        import threading

        fast_sock, fast_stream, ack = watch_socket(self.store, "rookie", {"op": "watch"})
        slow_sock, _, _ = watch_socket(self.store, "bino-gateway", {"op": "watch"})
        try:
            self.assertEqual(ack["event"], "subscribed")
            # The fast listener must actively drain during the pump — that is
            # the property under test: a draining listener survives, a
            # non-draining one gets contained. Reader thread collects events.
            fast_events: list[dict[str, Any]] = []
            done = threading.Event()

            def drain() -> None:
                while len(fast_events) < 60 and not done.is_set():
                    try:
                        line = fast_stream.readline()
                    except OSError:
                        return
                    if not line:
                        return
                    frame = json.loads(line)
                    fast_events.append(frame)

            reader = threading.Thread(target=drain, daemon=True)
            reader.start()
            # The slow listener genuinely stops reading from here on. Pump
            # enough oversized posts to exceed kernel queue + transport buffer
            # capacity many times over (60 x ~30KB = ~1.8MB vs ~0.5MB pipeline).
            big_body = "x" * 30_000
            for i in range(60):
                self.client("tech").request(
                    {"op": "post", "channel": "general", "body": f"{i}:{big_body}"}
                )
            # Containment shows up as an overflow event and/or connection
            # close on the slow socket; read (draining stale queued frames)
            # until EOF with a bounded budget. EOF is the guaranteed signal —
            # the overflow announcement is best-effort only.
            slow_sock.settimeout(5.0)
            saw_eof = False
            for _ in range(200):
                line = recv_line(slow_sock, 5.0)
                if line == b"":
                    saw_eof = True
                    break
            self.assertTrue(
                saw_eof, "slow listener connection was never closed by containment"
            )
            done.set()
            reader.join(timeout=5)
            # The draining listener received every event; the server is healthy.
            self.assertEqual(len(fast_events), 60)
            for i, event in enumerate(fast_events):
                self.assertEqual(event["event"], "message")
                self.assertTrue(event["message"]["body"].startswith(f"{i}:"))
            ping = self.client("tech").request({"op": "ping"})
            self.assertTrue(ping["ok"])
        finally:
            done.set()
            fast_sock.close()
            slow_sock.close()

    def test_rewatch_on_same_connection_replaces_subscription(self) -> None:
        sock, stream, ack = watch_socket(
            self.store, "rookie", {"op": "watch", "timeout_ms": 10_000}
        )
        try:
            self.assertEqual(ack["event"], "subscribed")
            # Re-watch with a shorter timeout: the ack replaces the old state.
            stream.write(encode_frame({"op": "watch", "timeout_ms": 200}))
            ack2 = json.loads(stream.readline())
            self.assertEqual(ack2["event"], "subscribed")
            timeout_event = json.loads(stream.readline())
            self.assertEqual(timeout_event["event"], "timeout")
        finally:
            sock.close()

    def test_shutdown_and_restart_preserve_history_and_watermark(self) -> None:
        pre = self.client("tech").request(
            {"op": "post", "channel": "general", "body": "before restart"}
        )["message"]
        self.server.send_signal(__import__("signal").SIGINT)
        self.server.wait(timeout=5)
        self.server = None  # type: ignore[assignment]
        import subprocess
        import sys
        from pathlib import Path as _Path

        project_root = _Path(__file__).resolve().parents[1]
        env = os.environ.copy()
        env["PYTHONPATH"] = str(project_root / "src")
        self.server = subprocess.Popen(  # type: ignore[assignment]
            [sys.executable, "-m", "aivengers_wire", "--state-dir", str(self.state_dir), "serve"],
            cwd=project_root,
            env=env,
            stdout=subprocess.DEVNULL,
            stderr=subprocess.PIPE,
            text=True,
        )
        deadline = time.monotonic() + 5
        while time.monotonic() < deadline:
            try:
                self.client("tech").request({"op": "ping"})
                break
            except WireError:
                time.sleep(0.02)
        else:
            self.fail("restarted server did not become ready")
        sock, stream, ack = watch_socket(self.store, "rookie", {"op": "watch"})
        try:
            self.assertGreaterEqual(ack["watermark"], pre["id"])
            history = self.client("rookie").request(
                {"op": "read", "channel": "general", "after": 0, "limit": 50}
            )["messages"]
            self.assertIn(pre["id"], [m["id"] for m in history])
        finally:
            sock.close()

    # ── R1 correction regressions (ported from Goblin's independent review
    # reproductions, AIVENGERS-CROSS-REVIEW-2026-10-23_GOBLIN-WIRE; his
    # originals remain untouched in the review receipt directory) ──────────

    def _server_socket_fds(self) -> int:
        """Count socket fds held by the relay subprocess (test-only seam)."""
        count = 0
        for entry in Path(f"/proc/{self.server.pid}/fd").iterdir():
            try:
                target = os.readlink(entry)
            except OSError:
                continue  # fd vanished mid-scan; not one we are counting
            if target.startswith("socket:["):
                count += 1
        return count

    def _wait_server_socket_fds(self, expected: int, description: str) -> None:
        deadline = time.monotonic() + 5
        while time.monotonic() < deadline:
            if self._server_socket_fds() == expected:
                return
            time.sleep(0.01)
        self.fail(f"{description}: relay socket-fd count never reached {expected}")

    def test_poster_disconnect_cannot_suppress_persisted_wakeup(self) -> None:
        """R1/F1 regression, R2-hardened: the poster genuinely disconnects.

        Ported from adversarial_review.py::test_01_poster_disconnect_cannot_
        suppress_persisted_wakeup. A poster that disconnects right after
        writing a valid post must not silence the doorbell for the message
        it just committed.

        R2: Goblin's re-review proved the R1 port left the poster's
        makefile() object open, so poster.close() never released the
        underlying connection (CPython keeps the socket open while makefile
        objects exist) and this regression passed against the known-broken
        parent aef2d97. The poster now closes its makefile and hard-closes
        the socket with SO_LINGER(1,0) — the raw-socket equivalent of the
        transport.abort() in the original probe — and the test verifies the
        disconnect actually happened server-side (the relay's accepted fd
        for the poster connection is released) before asserting that the
        committed message still rang the bell.
        """
        sock, stream, ack = watch_socket(self.store, "rookie", {"op": "watch"})
        try:
            self.assertEqual(ack["watermark"], 0)
            token = (self.store.seat_dir / "tech.token").read_text(encoding="utf-8").strip()
            baseline = self._server_socket_fds()
            poster = socket.socket(socket.AF_UNIX, socket.SOCK_STREAM)
            poster.settimeout(5.0)
            poster.connect(str(self.store.socket_path))
            self._wait_server_socket_fds(
                baseline + 1, "server accepted poster connection"
            )
            pstream = poster.makefile("rwb", buffering=0)
            pstream.write(encode_frame({"op": "auth", "seat": "tech", "token": token}))
            self.assertTrue(json.loads(pstream.readline())["ok"])
            pstream.write(
                encode_frame({"op": "post", "channel": "general", "body": "persist-then-peer-gone"})
            )
            # Genuine client disconnect immediately after the request frame:
            # release the makefile first (socket.close() alone cannot close a
            # socket with an open makefile object), then hard-close with
            # SO_LINGER(1,0) so the relay observes a real disconnect instead
            # of a lingering half-open fd.
            pstream.close()
            poster.setsockopt(
                socket.SOL_SOCKET, socket.SO_LINGER, struct.pack("ii", 1, 0)
            )
            poster.close()
            # Verify the disconnect actually occurred: the relay's accepted
            # fd for the poster connection is released once its handler
            # observes the reset and runs its cleanup path.
            self._wait_server_socket_fds(
                baseline, "server released poster connection"
            )
            deadline = time.monotonic() + 5
            while time.monotonic() < deadline and self.store.max_message_id() < 1:
                time.sleep(0.01)
            self.assertEqual(self.store.max_message_id(), 1, "post never persisted")
            second = self.client("bino").request(
                {"op": "post", "channel": "general", "body": "poster-still-healthy"}
            )["message"]
            # Collect the next two message events with a bounded raw-recv
            # poll (makefile streams are poisoned by mid-read timeouts; see
            # recv_line). On the broken parent the committed message's event
            # never arrives, so this assertion — not a bare socket timeout —
            # is what fails, naming the defect.
            events: list[dict[str, Any]] = []
            deadline = time.monotonic() + 5
            while len(events) < 2 and time.monotonic() < deadline:
                remaining = deadline - time.monotonic()
                try:
                    chunk = recv_line(sock, min(2.0, max(0.1, remaining)))
                except socket.timeout:
                    continue
                if not chunk:
                    break
                # One recv can deliver multiple newline-framed events at once.
                for part in chunk.split(b"\n"):
                    if not part:
                        continue
                    frame = json.loads(part)
                    if frame.get("event") == "message":
                        events.append(frame["message"])
                    if len(events) >= 2:
                        break
            self.assertEqual(
                [message["id"] for message in events],
                [1, second["id"]],
                "committed message's wakeup was lost when the poster "
                "disconnected; healthy watcher must still be rung",
            )
            # The watch connection is alive and usable after the gap.
            stream.write(encode_frame({"op": "ping"}))
            self.assertTrue(json.loads(stream.readline())["ok"])
        finally:
            sock.close()

    def test_notification_envelope_equals_read_envelope_exactly(self) -> None:
        """R1/F3: full-object equality, not selected fields.

        Ported from adversarial_review.py::test_04_notification_matches_read_
        envelope_exactly. The event message object must equal the `read`
        envelope for the same message, key for key.
        """
        sock, stream, ack = watch_socket(self.store, "rookie", {"op": "watch"})
        try:
            posted = self.client("tech").request(
                {"op": "post", "channel": "general", "body": "canonical-envelope"}
            )["message"]
            event = json.loads(stream.readline())
            stored = self.client("rookie").request(
                {"op": "read", "channel": "general", "after": 0, "limit": 10}
            )["messages"][0]
            self.assertEqual(event["message"], stored)
            self.assertNotIn("duplicate", event["message"])
        finally:
            sock.close()

    def test_client_id_retry_does_not_ring_the_bell_twice(self) -> None:
        """R1/F3 behavioral side: a retry persists nothing new and publishes
        nothing new; the subscriber sees exactly one event for the message."""
        sock, stream, ack = watch_socket(self.store, "rookie", {"op": "watch"})
        try:
            payload = {
                "op": "post", "channel": "general", "body": "send exactly once",
                "client_id": "retry-check-1",
            }
            first = self.client("tech").request(payload)["message"]
            first_event = json.loads(stream.readline())
            self.assertEqual(first_event["message"]["id"], first["id"])
            retry = self.client("tech").request(payload)["message"]
            self.assertTrue(retry["duplicate"])
            self.assertEqual(retry["id"], first["id"])
            # No second event for the retry.
            sock.settimeout(0.5)
            with self.assertRaises(socket.timeout):
                stream.readline()
        finally:
            sock.close()

    def test_public_client_ends_promptly_after_timeout_event(self) -> None:
        """R1/F2: normal timeout terminates the public iterator cleanly.

        Ported from adversarial_review.py::test_03_public_client_ends_after_
        timeout_event. Frames must be exactly [subscribed, timeout], no
        error, and the iterator must finish promptly instead of waiting out
        the idle timeout.
        """
        import threading as _threading

        result: dict[str, object] = {}

        def run() -> None:
            started = time.monotonic()
            frames, error = [], None
            try:
                for frame in WireClient(self.store, seat="rookie").watch(
                    timeout_ms=50, idle_timeout_seconds=3.0
                ):
                    frames.append(frame)
            except WireError as exc:
                error = str(exc)
            result["frames"] = [f["event"] for f in frames]
            result["error"] = error
            result["elapsed"] = time.monotonic() - started

        thread = _threading.Thread(target=run)
        thread.start()
        thread.join(timeout=10)
        self.assertFalse(thread.is_alive(), "watch iterator did not terminate promptly")
        self.assertEqual(result["frames"], ["subscribed", "timeout"])
        self.assertIsNone(result["error"])
        self.assertLess(result["elapsed"], 5.0)


class _GatedFlushServer(WireServer):
    """Controlled scheduling seam for the commit-order regression.

    Parks ONLY the gated writer's post-fanout response drain. Framing,
    persistence, subscription handling and fanout remain product code.
    """

    def __init__(self, store: WireStore, harness: "CommitOrderUnderHeldDrain") -> None:
        super().__init__(store)
        self._harness = harness

    async def _flush_response(self, writer: Any, committed: dict[str, Any] | None) -> bool:
        if (
            committed is not None
            and committed.get("body") == self._harness.gated_body
        ):
            self._harness.entered.set()
            await self._harness.release.wait()
        return await super()._flush_response(writer, committed)


class CommitOrderUnderHeldDrain(unittest.IsolatedAsyncioTestCase):
    """R1/F1 ordering: notification order must match commit order even when a
    poster's response drain is held.

    Ported from adversarial_review.py::test_02_concurrent_posts_preserve_
    commit_order. Controlled scheduling seam only.
    """

    async def asyncSetUp(self) -> None:
        self.temp = tempfile.TemporaryDirectory()
        self.store = WireStore(Path(self.temp.name) / "state")
        self.store.initialize()
        for seat in ("poster", "second", "listener"):
            self.store.add_seat(seat)
        self.gated_body = "gate-first"
        self.entered = asyncio.Event()
        self.release = asyncio.Event()
        self.server = _GatedFlushServer(self.store, self)
        await self.server.start()
        self.connections: list[tuple[asyncio.StreamReader, asyncio.StreamWriter]] = []

    async def asyncTearDown(self) -> None:
        self.release.set()
        for _, writer in self.connections:
            writer.close()
        for _, writer in self.connections:
            with contextlib.suppress(ConnectionError):
                await writer.wait_closed()
        await asyncio.sleep(0)
        await self.server.close()
        self.temp.cleanup()

    async def connect(self, seat: str):
        token = (self.store.seat_dir / f"{seat}.token").read_text(encoding="utf-8").strip()
        reader, writer = await asyncio.open_unix_connection(str(self.store.socket_path))
        self.connections.append((reader, writer))
        writer.write(encode_frame({"op": "auth", "seat": seat, "token": token}))
        ack = json.loads(await asyncio.wait_for(reader.readline(), 2))
        self.assertTrue(ack["ok"])
        return reader, writer

    async def test_commit_order_preserved_when_first_poster_drain_is_held(self) -> None:
        l_reader, l_writer = await self.connect("listener")
        l_writer.write(encode_frame({"op": "watch"}))
        ack = json.loads(await asyncio.wait_for(l_reader.readline(), 2))
        self.assertEqual(ack["event"], "subscribed")
        p_reader, p_writer = await self.connect("poster")
        p_writer.write(encode_frame({"op": "post", "channel": "general", "body": "gate-first"}))
        await asyncio.wait_for(self.entered.wait(), 2)
        first_ack = json.loads(await asyncio.wait_for(p_reader.readline(), 2))
        s_reader, s_writer = await self.connect("second")
        s_writer.write(encode_frame({"op": "post", "channel": "general", "body": "second-overtakes"}))
        second_ack = json.loads(await asyncio.wait_for(s_reader.readline(), 2))
        # With publication decoupled from the held drain, the watcher already
        # received message 1 BEFORE message 2, despite the first poster's
        # response drain still being parked.
        e1 = json.loads(await asyncio.wait_for(l_reader.readline(), 2))
        self.release.set()
        e2 = json.loads(await asyncio.wait_for(l_reader.readline(), 2))
        ids = [e1["message"]["id"], e2["message"]["id"]]
        self.assertEqual(ids, sorted(ids), "newer notification overtook older persisted message")
        self.assertEqual(ids, [first_ack["message"]["id"], second_ack["message"]["id"]])
