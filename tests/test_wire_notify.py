"""Adversarial verification for AIvengers Wire real-time notifications.

Every test runs against a real relay subprocess with a disposable state
directory (inherited from RunningWireTestCase), so receipts are end-to-end
socket truth, not in-process mocks.
"""

from __future__ import annotations

import json
import os
import socket
import time
from typing import Any

from aivengers_wire.client import WireClient
from aivengers_wire.errors import WireError
from aivengers_wire.protocol import encode_frame
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

    def test_multiple_subscribers_and_simultaneous_posts_in_canonical_order(self) -> None:
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
