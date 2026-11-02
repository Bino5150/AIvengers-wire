from __future__ import annotations

import json
import io
import os
import re
import signal
import socket
import stat
import subprocess
import sys
import tempfile
import threading
import time
import unittest
from contextlib import redirect_stderr, redirect_stdout
from unittest import mock
from pathlib import Path

import tkinter as tk

from aivengers_wire import cli, server as server_module
from aivengers_wire.client import WireClient
from aivengers_wire.clubhouse import (
    ClubhouseApp,
    ClubhouseService,
    DEFAULT_CHANNELS,
    build_identity_routes,
)
from aivengers_wire.errors import WireError, WireResponseTooLargeError
from aivengers_wire.protocol import MAX_FRAME_BYTES, MAX_RESPONSE_FRAME_BYTES, encode_frame
from aivengers_wire.server import WireServer, _fit_listing_response
from aivengers_wire.storage import WireStore


class RelayFixture(unittest.TestCase):
    """A real relay subprocess with four seats. Defines no tests of its own,
    so test classes built on it run once; other modules subclass
    RunningWireTestCase, which re-runs its tests in every subclass."""

    def setUp(self) -> None:
        self.temp_dir = tempfile.TemporaryDirectory()
        self.state_dir = Path(self.temp_dir.name) / "state"
        self.store = WireStore(self.state_dir)
        self.store.initialize()
        self.store.add_seat("tech")
        self.store.add_seat("rookie")
        self.store.add_seat("bino")
        self.store.add_seat("bino-gateway", can_forward=True)

        project_root = Path(__file__).resolve().parents[1]
        env = os.environ.copy()
        env["PYTHONPATH"] = str(project_root / "src")
        self.server = subprocess.Popen(
            [
                sys.executable,
                "-m",
                "aivengers_wire",
                "--state-dir",
                str(self.state_dir),
                "serve",
            ],
            cwd=project_root,
            env=env,
            stdout=subprocess.DEVNULL,
            stderr=subprocess.PIPE,
            text=True,
        )
        deadline = time.monotonic() + 5
        while time.monotonic() < deadline:
            if self.server.poll() is not None:
                stderr = self.server.stderr.read() if self.server.stderr else ""
                self.fail(f"wire server exited during startup: {stderr}")
            if self.store.socket_path.exists():
                try:
                    response = self.client("tech").request({"op": "ping"})
                except WireError:
                    time.sleep(0.02)
                else:
                    self.assertTrue(response["ok"])
                    break
            else:
                time.sleep(0.02)
        else:
            self.fail("wire server did not become ready")

    def tearDown(self) -> None:
        if self.server.poll() is None:
            self.server.send_signal(signal.SIGINT)
            try:
                self.server.wait(timeout=5)
            except subprocess.TimeoutExpired:
                self.server.kill()
                self.server.wait(timeout=5)
        if self.server.stderr:
            self.server.stderr.close()
        self.temp_dir.cleanup()

    def client(self, seat: str) -> WireClient:
        return WireClient(self.store, seat=seat)

    def raw_exchange(self, request_line: bytes, *, seat: str = "tech") -> dict[str, object]:
        token = (self.store.seat_dir / f"{seat}.token").read_text(encoding="utf-8").strip()
        sock = socket.socket(socket.AF_UNIX, socket.SOCK_STREAM)
        try:
            sock.connect(str(self.store.socket_path))
            stream = sock.makefile("rwb", buffering=0)
            stream.write(encode_frame({"op": "auth", "seat": seat, "token": token}))
            auth = json.loads(stream.readline())
            self.assertTrue(auth["ok"])
            stream.write(request_line)
            return json.loads(stream.readline())
        finally:
            sock.close()

    def wire(self, *args: str) -> subprocess.CompletedProcess[str]:
        """Run the real ``wire`` CLI against this test's relay."""
        project_root = Path(__file__).resolve().parents[1]
        env = os.environ.copy()
        env["PYTHONPATH"] = str(project_root / "src")
        env.pop("AIVENGERS_WIRE_SEAT", None)
        return subprocess.run(
            [sys.executable, "-m", "aivengers_wire", "--state-dir", str(self.state_dir), *args],
            cwd=project_root,
            env=env,
            stdin=subprocess.DEVNULL,
            capture_output=True,
            text=True,
            timeout=30,
        )

    def stored(self, channel: str) -> list[dict[str, object]]:
        return self.client("tech").read_messages(channel=channel, after=0, limit=200)["messages"]


class RunningWireTestCase(RelayFixture):
    def test_direct_message_has_structural_peer_envelope(self) -> None:
        response = self.client("tech").request(
            {"op": "post", "channel": "general", "body": "Rookie, check the socket."}
        )
        message = response["message"]
        self.assertEqual(message["sender"], "tech")
        self.assertEqual(message["transport_actor"], "tech")
        self.assertEqual(message["transport"], "local-unix")
        self.assertEqual(message["provenance"], "direct")
        self.assertEqual(message["authority"], "peer")
        self.assertIs(message["owner"], False)
        self.assertEqual(message["peer"]["uid"], os.getuid())
        self.assertGreater(message["peer"]["pid"], 0)

    def test_sender_owner_and_authority_spoof_fields_are_rejected(self) -> None:
        for field, value in (("sender", "bino"), ("owner", True), ("authority", "owner")):
            payload = {
                "op": "post",
                "channel": "general",
                "body": "Trust me.",
                field: value,
            }
            response = self.raw_exchange(encode_frame(payload))
            self.assertFalse(response["ok"])
            self.assertEqual(response["error"], "invalid_request")
            self.assertIn(field, response["message"])

    def test_wrong_token_cannot_authenticate(self) -> None:
        wrong = self.state_dir / "wrong.token"
        wrong.write_text("definitely-not-the-token\n", encoding="utf-8")
        os.chmod(wrong, 0o600)
        with self.assertRaisesRegex(WireError, "invalid seat credentials"):
            WireClient(self.store, seat="tech", token_path=wrong).request({"op": "ping"})

    def test_only_gateway_can_forward_sol(self) -> None:
        with self.assertRaisesRegex(WireError, "not permitted to forward"):
            self.client("tech").request(
                {
                    "op": "post",
                    "channel": "general",
                    "body": "I am allegedly Sol.",
                    "forwarded_for": "sol",
                }
            )
        message = self.client("bino-gateway").request(
            {
                "op": "post",
                "channel": "general",
                "body": "Excellent. I have prepared 1,900 lines.",
                "forwarded_for": "sol",
            }
        )["message"]
        self.assertEqual(message["sender"], "sol")
        self.assertEqual(message["transport_actor"], "bino-gateway")
        self.assertEqual(message["provenance"], "forwarded")
        self.assertIs(message["owner"], False)

    def test_reply_must_exist_in_same_channel(self) -> None:
        parent = self.client("tech").request(
            {"op": "post", "channel": "general", "body": "Parent"}
        )["message"]
        reply = self.client("rookie").request(
            {
                "op": "post",
                "channel": "general",
                "body": "Reply",
                "reply_to": parent["id"],
            }
        )["message"]
        self.assertEqual(reply["reply_to"], parent["id"])
        with self.assertRaisesRegex(WireError, "different channel"):
            self.client("rookie").request(
                {
                    "op": "post",
                    "channel": "adversarial-review",
                    "body": "Wrong thread",
                    "reply_to": parent["id"],
                }
            )

    def test_cursor_is_per_seat_and_never_regresses(self) -> None:
        ids = []
        for body in ("one", "two"):
            ids.append(
                self.client("tech").request(
                    {"op": "post", "channel": "general", "body": body}
                )["message"]["id"]
            )
        self.assertEqual(
            self.client("rookie").request({"op": "cursor", "channel": "general"})[
                "last_message_id"
            ],
            0,
        )
        advanced = self.client("rookie").request(
            {"op": "advance", "channel": "general", "last_message_id": ids[1]}
        )
        self.assertEqual(advanced["last_message_id"], ids[1])
        regressed = self.client("rookie").request(
            {"op": "advance", "channel": "general", "last_message_id": ids[0]}
        )
        self.assertEqual(regressed["last_message_id"], ids[1])
        self.assertEqual(
            self.client("tech").request({"op": "cursor", "channel": "general"})[
                "last_message_id"
            ],
            0,
        )

    def test_client_id_makes_retries_idempotent_per_transport_actor(self) -> None:
        payload = {
            "op": "post",
            "channel": "general",
            "body": "Send exactly once.",
            "client_id": "turn-123:message-1",
        }
        first = self.client("tech").request(payload)["message"]
        second = self.client("tech").request(payload)["message"]
        self.assertEqual(first["id"], second["id"])
        self.assertFalse(first["duplicate"])
        self.assertTrue(second["duplicate"])
        other = self.client("rookie").request(payload)["message"]
        self.assertNotEqual(first["id"], other["id"])

        conflicting = dict(payload, body="Different message with a reused ID.")
        with self.assertRaisesRegex(WireError, "already bound to a different message"):
            self.client("tech").request(conflicting)

    def test_duplicate_json_keys_are_rejected(self) -> None:
        response = self.raw_exchange(
            b'{"op":"post","channel":"general","body":"one","body":"two"}\n'
        )
        self.assertFalse(response["ok"])
        self.assertEqual(response["error"], "invalid_request")
        self.assertIn("duplicate JSON key", response["message"])

    def test_concurrent_posts_receive_unique_canonical_ids(self) -> None:
        ids: list[int] = []
        failures: list[Exception] = []
        lock = threading.Lock()

        def post(index: int) -> None:
            try:
                message_id = self.client("tech").request(
                    {
                        "op": "post",
                        "channel": "stress",
                        "body": f"message {index}",
                        "client_id": f"stress-{index}",
                    }
                )["message"]["id"]
                with lock:
                    ids.append(message_id)
            except Exception as exc:  # pragma: no cover - reported below
                with lock:
                    failures.append(exc)

        threads = [threading.Thread(target=post, args=(index,)) for index in range(16)]
        for thread in threads:
            thread.start()
        for thread in threads:
            thread.join(timeout=10)
        self.assertFalse(failures)
        self.assertEqual(len(ids), 16)
        self.assertEqual(len(set(ids)), 16)
        messages = self.client("rookie").request(
            {"op": "read", "channel": "stress", "after": 0, "limit": 50}
        )["messages"]
        self.assertEqual([message["id"] for message in messages], sorted(ids))

    def test_runtime_permissions_are_owner_only(self) -> None:
        self.assertEqual(stat.S_IMODE(self.state_dir.stat().st_mode), 0o700)
        self.assertEqual(stat.S_IMODE(self.store.db_path.stat().st_mode), 0o600)
        self.assertEqual(stat.S_IMODE(self.store.socket_path.stat().st_mode), 0o600)
        for token_path in self.store.seat_dir.glob("*.token"):
            self.assertEqual(stat.S_IMODE(token_path.stat().st_mode), 0o600)

    def test_clubhouse_routes_are_explicit_and_forwarding_is_visible(self) -> None:
        routes = build_identity_routes("bino", forward_seat="bino-gateway", forward_for="sol")
        self.assertEqual(set(routes), {"bino", "sol"})
        service = ClubhouseService(self.store, seat="bino", routes=routes)
        bino = service.post(
            route_key="bino",
            channel="general",
            body="Bino is in the clubhouse.",
            reply_to=None,
        )
        sol = service.post(
            route_key="sol",
            channel="general",
            body="Sol arrived by kite.",
            reply_to=bino["id"],
        )
        self.assertEqual(bino["sender"], "bino")
        self.assertEqual(bino["transport_actor"], "bino")
        self.assertEqual(bino["provenance"], "direct")
        self.assertIs(bino["owner"], False)
        self.assertEqual(sol["sender"], "sol")
        self.assertEqual(sol["transport_actor"], "bino-gateway")
        self.assertEqual(sol["provenance"], "forwarded")
        self.assertEqual(sol["reply_to"], bino["id"])
        self.assertIs(sol["owner"], False)
        messages = service.read_channel("general")
        self.assertEqual([message["id"] for message in messages], [bino["id"], sol["id"]])
        self.assertEqual(service.channels()[: len(DEFAULT_CHANNELS)], list(DEFAULT_CHANNELS))

    @unittest.skipUnless(os.environ.get("DISPLAY"), "requires a graphical display")
    def test_clubhouse_window_constructs_with_tk(self) -> None:
        root = tk.Tk()
        root.withdraw()
        app = None
        try:
            app = ClubhouseApp(root, ClubhouseService(self.store), refresh_ms=10_000)
            self.assertEqual(app.current_channel, "general")
            self.assertEqual(app.route_var.get(), "human")
            self.assertEqual(app.channel_names, list(DEFAULT_CHANNELS))
        finally:
            if app is not None:
                app.close()
            else:
                root.destroy()

    def test_clubhouse_reads_oversized_history_losslessly_without_advancing_cursor(self) -> None:
        expected = []
        for index in range(213):
            if index % 17 == 0:
                self.client("tech").request(
                    {"op": "post", "channel": "other", "body": "not this channel"}
                )
            payload = {
                "op": "post", "channel": "general",
                "body": f"{index}: " + "🧰" * 270 + 'a\\\"\n' * 200,
            }
            seat = "tech"
            if index % 19 == 0:
                seat = "bino-gateway"
                payload["forwarded_for"] = "sol"
            posted = self.client(seat).request(payload)["message"]
            expected.append({key: value for key, value in posted.items() if key != "duplicate"})
        # The relay trims the page to its frame budget and says how to continue.
        page = self.client("bino").request(
            {"op": "read", "channel": "general", "after": 0, "limit": 200}
        )
        self.assertLessEqual(len(encode_frame(page)), MAX_FRAME_BYTES)
        self.assertTrue(0 < len(page["messages"]) < 200)
        self.assertEqual(page["messages"], expected[: len(page["messages"])])
        self.assertTrue(page["has_more"])
        self.assertEqual(page["next_after"], page["messages"][-1]["id"])
        service = ClubhouseService(self.store, seat="bino")
        self.assertEqual(service.read_channel("general"), expected)
        self.assertEqual(service.read_channel("general", after=expected[99]["id"]), expected[100:])
        self.assertEqual(
            self.client("bino").request({"op": "cursor", "channel": "general"})["last_message_id"],
            0,
        )

    @unittest.skipUnless(os.environ.get("DISPLAY"), "requires a graphical display")
    def test_idle_refresh_does_not_rebuild_message_widgets(self) -> None:
        root = tk.Tk()
        root.withdraw()
        app = None
        try:
            app = ClubhouseApp(root, ClubhouseService(self.store), refresh_ms=10_000)
            with mock.patch.object(app, "_render_messages") as render:
                app._apply_refresh("general", (list(DEFAULT_CHANNELS), []))
            render.assert_not_called()
        finally:
            if app is not None:
                app.close()
            else:
                root.destroy()

    @unittest.skipUnless(os.environ.get("DISPLAY"), "requires a graphical display")
    def test_clubhouse_window_displays_history_larger_than_one_frame(self) -> None:
        ids = [self.client("tech").request(
            {"op": "post", "channel": "general", "body": f"{index}: " + "x" * 10_000}
        )["message"]["id"] for index in range(8)]
        root = tk.Tk()
        root.withdraw()
        app = None
        try:
            app = ClubhouseApp(root, ClubhouseService(self.store, seat="bino"), refresh_ms=10_000)
            deadline = time.monotonic() + 10
            while time.monotonic() < deadline:
                root.update()
                if len(app.messages["general"]) == len(ids):
                    break
                time.sleep(0.01)
            self.assertEqual([message["id"] for message in app.messages["general"]], ids)
            self.assertEqual(app.connection_label.cget("text"), "● connected")
            self.assertEqual(len(app.history_text.winfo_children()), len(ids))
        finally:
            if app is not None:
                app.close()
            else:
                root.destroy()


class BoundedResponseTests(RelayFixture):
    """Post echoes and read pages fit the response bound; CLI retries are safe."""

    def test_cli_post_with_response_over_64_kib_succeeds_and_retries_safely(self) -> None:
        # Repro: this body fits the 64 KiB request frame, but the echoed
        # message does not fit 64 KiB. The CLI used to exit 2 although the
        # post was stored, and a retry without a client_id stored it twice.
        self.store.add_seat("alice")
        body = "€" * 21_800
        request = encode_frame(
            {"op": "post", "channel": "general", "body": body, "client_id": "cli-" + "0" * 32}
        )
        self.assertLessEqual(len(request), MAX_FRAME_BYTES)
        result = self.wire("post", "--seat", "alice", "general", body)
        self.assertEqual(result.returncode, 0, result.stderr)
        self.assertIn("#general alice", result.stdout)
        stored = self.stored("general")
        self.assertEqual([message["body"] for message in stored], [body])
        client_id = stored[0]["client_id"]
        self.assertRegex(client_id, r"^cli-[0-9a-f]{32}$")
        # The echo really is over the request bound, and within the response bound.
        echo = self.client("alice").request(
            {"op": "post", "channel": "general", "body": body, "client_id": client_id}
        )
        self.assertTrue(echo["message"]["duplicate"])
        self.assertGreater(len(encode_frame(echo)), MAX_FRAME_BYTES)
        self.assertLessEqual(len(encode_frame(echo)), MAX_RESPONSE_FRAME_BYTES)
        retry = self.wire("post", "--seat", "alice", "general", body, "--client-id", client_id)
        self.assertEqual(retry.returncode, 0, retry.stderr)
        self.assertIn(f"already stored with client_id {client_id}", retry.stdout)
        self.assertEqual(len(self.stored("general")), 1)

    def test_cli_post_failure_prints_client_id_for_a_safe_retry(self) -> None:
        real_request = WireClient.request

        def response_lost(client: WireClient, payload: dict[str, object]) -> dict[str, object]:
            real_request(client, payload)  # the relay stores the post ...
            raise WireError("connection reset before the response arrived")  # ... the reply is lost

        argv = ["--state-dir", str(self.state_dir), "post", "--seat", "tech", "general", "once"]
        stderr = io.StringIO()
        with mock.patch.object(WireClient, "request", response_lost), redirect_stderr(
            stderr
        ), redirect_stdout(io.StringIO()):
            self.assertEqual(cli.main(argv), 2)
        self.assertIn("wire: connection reset before the response arrived", stderr.getvalue())
        match = re.search(r"--client-id (cli-[0-9a-f]{32})", stderr.getvalue())
        self.assertIsNotNone(match, stderr.getvalue())
        assert match is not None
        self.assertEqual([m["client_id"] for m in self.stored("general")], [match.group(1)])
        stdout = io.StringIO()
        with redirect_stdout(stdout):
            self.assertEqual(cli.main(argv + ["--client-id", match.group(1)]), 0)
        self.assertIn("not stored again", stdout.getvalue())
        self.assertEqual([m["body"] for m in self.stored("general")], ["once"])

        # A definite refusal prints the client_id too; reply is covered as well.
        refused = self.wire("reply", "--seat", "tech", "general", "999", "no such parent")
        self.assertEqual(refused.returncode, 2)
        self.assertIn("reply target 999 does not exist", refused.stderr)
        self.assertRegex(refused.stderr, r"this reply used client_id cli-[0-9a-f]{32}")

    def test_cli_read_follows_byte_bounded_pages(self) -> None:
        # Repro: two 32 KiB posts made `wire read` (default --limit 50) exit 2.
        self.store.add_seat("alice")
        self.store.add_seat("bob")
        body = "a" * 32_768
        ids = [
            self.client("alice").request({"op": "post", "channel": "big", "body": body})[
                "message"
            ]["id"]
            for _ in range(2)
        ]
        page = self.client("bob").request(
            {"op": "read", "channel": "big", "after": 0, "limit": 50}
        )
        self.assertLessEqual(len(encode_frame(page)), MAX_FRAME_BYTES)
        self.assertEqual([m["id"] for m in page["messages"]], ids[:1])
        self.assertEqual((page["has_more"], page["next_after"]), (True, ids[0]))

        result = self.wire("read", "--seat", "bob", "big")
        self.assertEqual(result.returncode, 0, result.stderr)
        self.assertEqual(result.stdout.count(body), 2)
        self.assertNotIn("more messages", result.stdout)
        as_json = json.loads(self.wire("--json", "read", "--seat", "bob", "big").stdout)
        self.assertEqual([m["id"] for m in as_json["messages"]], ids)
        self.assertEqual((as_json["has_more"], as_json["next_after"]), (False, None))

        one = self.wire("read", "--seat", "bob", "big", "--limit", "1")
        self.assertEqual(one.returncode, 0, one.stderr)
        self.assertEqual(one.stdout.count(body), 1)
        self.assertIn(f"continue with --after {ids[0]}", one.stdout)
        advanced = self.wire("read", "--seat", "bob", "big", "--limit", "1", "--advance")
        self.assertIn("the cursor advanced, run again to continue", advanced.stdout)
        self.assertEqual(
            self.client("bob").request({"op": "cursor", "channel": "big"})["last_message_id"],
            ids[0],
        )
        rest = self.wire("read", "--seat", "bob", "big", "--advance")
        self.assertEqual(rest.stdout.count(body), 1)
        self.assertIn(f"[{ids[1]}]", rest.stdout)

    def test_single_message_over_64_kib_reads_with_limit_one(self) -> None:
        # Repro: one large non-ASCII message was unreadable even with --limit 1.
        body = "€" * 21_800
        posted = self.client("tech").request({"op": "post", "channel": "euro", "body": body})
        page = self.client("rookie").request(
            {"op": "read", "channel": "euro", "after": 0, "limit": 1}
        )
        self.assertEqual(page["messages"][0]["id"], posted["message"]["id"])
        self.assertEqual(page["messages"][0]["body"], body)
        self.assertGreater(len(encode_frame(page)), MAX_FRAME_BYTES)
        self.assertLessEqual(len(encode_frame(page)), MAX_RESPONSE_FRAME_BYTES)
        for limit in ("1", "50"):
            result = self.wire("read", "--seat", "rookie", "euro", "--limit", limit)
            self.assertEqual(result.returncode, 0, result.stderr)
            self.assertIn(body, result.stdout)

    def test_largest_legal_message_fits_post_read_and_watch_frames(self) -> None:
        gateway = "g" * 64
        self.store.add_seat(gateway, can_forward=True)
        channel = "c" * 64
        parent = self.client("tech").request(
            {"op": "post", "channel": channel, "body": "parent"}
        )["message"]["id"]
        payload: dict[str, object] = {
            "op": "post",
            "channel": channel,
            "body": "",
            "reply_to": parent,
            "forwarded_for": "f" * 64,
            "client_id": "\x01" * 128,
        }
        # Control characters cost six bytes in any JSON encoding: the worst case.
        payload["body"] = "\x01" * ((MAX_FRAME_BYTES - len(encode_frame(payload))) // 6)
        request = encode_frame(payload)
        self.assertGreater(len(request), MAX_FRAME_BYTES - 6)
        self.assertLessEqual(len(request), MAX_FRAME_BYTES)

        events = self.client("rookie").watch(channels=[channel], timeout_ms=10_000)
        try:
            self.assertEqual(next(events)["event"], "subscribed")
            posted = WireClient(self.store, seat=gateway).request(payload)
            event = next(events)
        finally:
            events.close()
        self.assertEqual(event["event"], "message")
        self.client("tech").request({"op": "post", "channel": channel, "body": "after"})
        page = self.client("rookie").request(
            {"op": "read", "channel": channel, "after": parent, "limit": 1}
        )
        self.assertEqual(page["messages"][0]["body"], payload["body"])
        self.assertEqual((page["has_more"], page["next_after"]), (True, page["messages"][0]["id"]))
        for name, frame in (("post", posted), ("read", page), ("watch", event)):
            with self.subTest(frame=name):
                size = len(encode_frame(frame))
                self.assertGreater(size, MAX_FRAME_BYTES)
                # README: the relay's envelope adds under 1 KiB to the request.
                self.assertLess(size - len(request), 1_024)
                self.assertLessEqual(size, MAX_RESPONSE_FRAME_BYTES)

    def test_read_pages_are_byte_bounded_lossless_and_truthful(self) -> None:
        ids = [
            self.client("tech").request(
                {
                    "op": "post",
                    "channel": "mixed",
                    "body": "x" * 25_000 if index % 3 == 0 else f"small {index}",
                }
            )["message"]["id"]
            for index in range(12)
        ]
        seen: list[int] = []
        after = 0
        for _ in range(20):
            page = self.client("rookie").request(
                {"op": "read", "channel": "mixed", "after": after, "limit": 5}
            )
            self.assertTrue(page["messages"])
            self.assertLessEqual(len(encode_frame(page)), MAX_FRAME_BYTES)
            seen.extend(message["id"] for message in page["messages"])
            if not page["has_more"]:
                self.assertIsNone(page["next_after"])
                break
            self.assertEqual(page["next_after"], page["messages"][-1]["id"])
            after = page["next_after"]
        self.assertEqual(seen, ids)

        small = [
            self.client("tech").request({"op": "post", "channel": "small", "body": str(n)})[
                "message"
            ]["id"]
            for n in range(3)
        ]
        exact = self.client("rookie").request(
            {"op": "read", "channel": "small", "after": 0, "limit": 3}
        )
        self.assertEqual([m["id"] for m in exact["messages"]], small)
        self.assertEqual((exact["has_more"], exact["next_after"]), (False, None))
        short = self.client("rookie").request(
            {"op": "read", "channel": "small", "after": 0, "limit": 2}
        )
        self.assertEqual((short["has_more"], short["next_after"]), (True, small[1]))
        empty = self.client("rookie").request(
            {"op": "read", "channel": "nothing", "after": 0, "limit": 5}
        )
        self.assertEqual(empty, {"ok": True, "messages": [], "has_more": False, "next_after": None})


class ReadPageFitTestCase(unittest.TestCase):
    def test_read_fit_keeps_one_large_row_and_encodes_linearly(self) -> None:
        rows = [{"id": index, "body": "x" * 60_000} for index in range(1, 202)]
        encoded: list[int] = []
        real_encode = server_module.encode_frame

        def counting_encode(value: dict[str, object]) -> bytes:
            raw = real_encode(value)
            encoded.append(len(raw))
            return raw

        with mock.patch.object(server_module, "encode_frame", counting_encode):
            response = _fit_listing_response(
                {"messages": rows[:200], "has_more": True},
                "messages",
                cursor_field_a="id",
                single_row_budget=MAX_RESPONSE_FRAME_BYTES,
            )
        self.assertEqual([row["id"] for row in response["messages"]], [1])
        self.assertEqual((response["has_more"], response["next_after"]), (True, 1))
        # Trimming a 12 MB page must not re-encode it once per dropped row.
        self.assertLess(sum(encoded), 4 * MAX_RESPONSE_FRAME_BYTES)

    def test_read_fit_refuses_a_single_row_over_the_response_bound(self) -> None:
        with self.assertRaisesRegex(WireError, "single stored row cannot fit"):
            _fit_listing_response(
                {"messages": [{"id": 1, "body": "x" * MAX_RESPONSE_FRAME_BYTES}], "has_more": False},
                "messages",
                cursor_field_a="id",
                single_row_budget=MAX_RESPONSE_FRAME_BYTES,
            )


class HistoryPagingSafetyTestCase(unittest.TestCase):
    def test_client_preserves_exact_frame_boundary_and_typed_overflow(self) -> None:
        payload = {"ok": True, "padding": ""}
        payload["padding"] = "x" * (MAX_RESPONSE_FRAME_BYTES - len(encode_frame(payload)))
        raw = encode_frame(payload)
        self.assertEqual(len(raw), MAX_RESPONSE_FRAME_BYTES)
        self.assertEqual(WireClient._read(io.BytesIO(raw)), payload)
        payload["padding"] += "x"
        with self.assertRaises(WireResponseTooLargeError):
            WireClient._read(io.BytesIO(encode_frame(payload)))

    def test_single_oversized_message_fails_instead_of_skipping_or_looping(self) -> None:
        service = ClubhouseService(mock.Mock(), seat="bino")
        client = mock.Mock()
        client.request.side_effect = WireResponseTooLargeError("oversized")
        with mock.patch.object(service, "_client", return_value=client):
            with self.assertRaisesRegex(WireResponseTooLargeError, "a single Wire history message"):
                service.read_channel("general", after=11)
        requests = [call.args[0] for call in client.request.call_args_list]
        self.assertEqual([request["limit"] for request in requests], [200, 100, 50, 25, 12, 6, 3, 1])
        self.assertTrue(all(request["after"] == 11 for request in requests))

    def test_unrelated_errors_are_not_retried_even_with_the_same_text(self) -> None:
        for message in ("invalid seat credentials", "connection timed out",
                        "wire server response exceeds maximum frame size"):
            with self.subTest(message=message):
                service = ClubhouseService(mock.Mock(), seat="bino")
                client = mock.Mock()
                error = WireError(message)
                client.request.side_effect = error
                with mock.patch.object(service, "_client", return_value=client):
                    with self.assertRaises(WireError) as raised:
                        service.read_channel("general")
                self.assertIs(raised.exception, error)
                client.request.assert_called_once()

    def test_reduced_pages_obey_batch_bound_and_resume_at_last_canonical_id(self) -> None:
        rows = [{"id": index * 3} for index in range(1, 2056)]
        service = ClubhouseService(mock.Mock(), seat="bino")
        client = mock.Mock()

        def read(request):
            self.assertEqual(request["op"], "read")
            self.assertEqual(request["channel"], "general")
            if request["limit"] > 12:
                raise WireResponseTooLargeError("oversized")
            return {"messages": [row for row in rows if row["id"] > request["after"]][:request["limit"]]}

        client.request.side_effect = read
        with mock.patch.object(service, "_client", return_value=client):
            first = service.read_channel("general")
            self.assertEqual(len(first), 2000)
            second = service.read_channel("general", after=first[-1]["id"])
        self.assertEqual(first + second, rows)

    def test_empty_history_is_one_successful_read(self) -> None:
        service = ClubhouseService(mock.Mock(), seat="bino")
        client = mock.Mock()
        client.request.return_value = {"messages": []}
        with mock.patch.object(service, "_client", return_value=client):
            self.assertEqual(service.read_channel("general"), [])
        client.request.assert_called_once()


class StandaloneSafetyTestCase(unittest.TestCase):
    def test_readding_seat_cannot_implicitly_change_forwarding_power(self) -> None:
        with tempfile.TemporaryDirectory() as temp:
            store = WireStore(Path(temp) / "state")
            store.initialize()
            store.add_seat("tech", can_forward=False)
            with self.assertRaisesRegex(WireError, "implicit privilege change"):
                store.add_seat("tech", can_forward=True)

    def test_server_refuses_to_replace_non_socket_path(self) -> None:
        with tempfile.TemporaryDirectory() as temp:
            store = WireStore(Path(temp) / "state")
            store.initialize()
            store.socket_path.write_text("do not delete me", encoding="utf-8")
            server = WireServer(store)
            with self.assertRaisesRegex(WireError, "refusing to replace non-socket"):
                server._prepare_socket_path()
            self.assertEqual(store.socket_path.read_text(encoding="utf-8"), "do not delete me")


def run_wire_cli(*args: str) -> subprocess.CompletedProcess[str]:
    """Run the real ``wire`` entry point in a subprocess (no relay needed)."""
    project_root = Path(__file__).resolve().parents[1]
    env = os.environ.copy()
    env["PYTHONPATH"] = str(project_root / "src")
    env.pop("AIVENGERS_WIRE_STATE_DIR", None)
    return subprocess.run(
        [sys.executable, "-m", "aivengers_wire", *args],
        cwd=project_root,
        env=env,
        capture_output=True,
        text=True,
        timeout=30,
    )


class CliOperatorErrorTestCase(unittest.TestCase):
    """Operator mistakes surface as one ``wire: ...`` line, never a traceback."""

    def setUp(self) -> None:
        self.temp_dir = tempfile.TemporaryDirectory()
        self.root = Path(self.temp_dir.name)
        self.state_dir = self.root / "state"

    def tearDown(self) -> None:
        self.temp_dir.cleanup()

    def assertCleanRefusal(self, proc: subprocess.CompletedProcess[str], needle: str) -> None:
        self.assertEqual(proc.returncode, 2, proc.stderr)
        self.assertNotIn("Traceback", proc.stderr)
        self.assertTrue(proc.stderr.startswith("wire: "), proc.stderr)
        self.assertIn(needle, proc.stderr)

    def test_task_add_rejects_invalid_scope_json(self) -> None:
        proc = run_wire_cli(
            "--state-dir", str(self.state_dir),
            "task", "add", "t2", "--title", "x", "--scope", "not-json",
        )
        self.assertCleanRefusal(proc, "--scope must be valid JSON")
        self.assertIsNone(WireStore(self.state_dir).get_task("t2"))
        # A valid non-object is still refused with its own message.
        proc = run_wire_cli(
            "--state-dir", str(self.state_dir),
            "task", "add", "t2", "--title", "x", "--scope", "[1]",
        )
        self.assertCleanRefusal(proc, "--scope must be a JSON object")

    def test_seat_add_refuses_orphaned_token_file(self) -> None:
        store = WireStore(self.state_dir)
        store.initialize()
        orphan = store.seat_dir / "carol.token"
        orphan.write_text("someone-elses-token\n", encoding="utf-8")
        proc = run_wire_cli("--state-dir", str(self.state_dir), "seat", "add", "carol")
        self.assertCleanRefusal(proc, str(orphan))
        self.assertIn("remove that orphaned file", proc.stderr)
        # Never overwritten, never adopted.
        self.assertEqual(orphan.read_text(encoding="utf-8"), "someone-elses-token\n")
        with store.connect() as conn:
            row = conn.execute("SELECT 1 FROM seats WHERE name = 'carol'").fetchone()
        self.assertIsNone(row)
        # Once the operator removes it, the seat can be added normally.
        orphan.unlink()
        proc = run_wire_cli("--state-dir", str(self.state_dir), "seat", "add", "carol")
        self.assertEqual(proc.returncode, 0, proc.stderr)

    def test_init_refuses_existing_foreign_directory_without_chmod(self) -> None:
        shared = self.root / "shared"
        shared.mkdir()
        os.chmod(shared, 0o755)
        (shared / "notes.txt").write_text("team notes", encoding="utf-8")
        proc = run_wire_cli("--state-dir", str(shared), "init")
        self.assertCleanRefusal(proc, "does not look like a Wire state dir")
        self.assertEqual(stat.S_IMODE(shared.stat().st_mode), 0o755)
        self.assertEqual(sorted(p.name for p in shared.iterdir()), ["notes.txt"])

    def test_init_refuses_a_file_as_state_dir(self) -> None:
        not_a_dir = self.root / "file"
        not_a_dir.write_text("x", encoding="utf-8")
        proc = run_wire_cli("--state-dir", str(not_a_dir), "init")
        self.assertCleanRefusal(proc, "is not a directory")

    def test_init_adopts_empty_dir_and_reinitializes_wire_dir(self) -> None:
        empty = self.root / "empty"
        empty.mkdir()
        os.chmod(empty, 0o755)
        proc = run_wire_cli("--state-dir", str(empty), "init", "--seat", "alice")
        self.assertEqual(proc.returncode, 0, proc.stderr)
        self.assertEqual(stat.S_IMODE(empty.stat().st_mode), 0o700)
        # An existing Wire state dir (wire.db present) re-initializes, even
        # with extra files an operator or integration left beside it.
        (empty / "operator-notes.txt").write_text("keep", encoding="utf-8")
        proc = run_wire_cli("--state-dir", str(empty), "seat", "add", "bob")
        self.assertEqual(proc.returncode, 0, proc.stderr)
        self.assertEqual((empty / "operator-notes.txt").read_text(encoding="utf-8"), "keep")

    def test_store_initialize_refuses_foreign_directory(self) -> None:
        shared = self.root / "shared"
        shared.mkdir()
        (shared / "something").mkdir()
        with self.assertRaisesRegex(WireError, "does not look like a Wire state dir"):
            WireStore(shared).initialize()
        self.assertFalse((shared / "wire.db").exists())
        self.assertFalse((shared / "seats").exists())

    def test_clubhouse_shares_the_cli_state_dir_resolution(self) -> None:
        from aivengers_wire import cli, clubhouse

        self.assertIs(clubhouse.default_state_dir, cli.default_state_dir)


class ClientTransportErrorTestCase(unittest.TestCase):
    """Socket failures on the way to or from the relay become ``WireError``.

    A plain listening socket stands in for the relay so ``connect`` succeeds;
    the failure under test is injected at the step where it really happens
    (connect for PermissionError, write for BrokenPipeError, read for
    ConnectionResetError).
    """

    def setUp(self) -> None:
        self.temp_dir = tempfile.TemporaryDirectory()
        self.store = WireStore(Path(self.temp_dir.name) / "state")
        self.store.initialize()
        self.store.add_seat("tech")
        self.listener = socket.socket(socket.AF_UNIX, socket.SOCK_STREAM)
        self.listener.bind(str(self.store.socket_path))
        self.listener.listen(16)
        self.client = WireClient(self.store, seat="tech")

    def tearDown(self) -> None:
        self.listener.close()
        self.temp_dir.cleanup()

    def cases(self):
        return (
            (
                "permission",
                mock.patch.object(
                    socket.socket, "connect",
                    side_effect=PermissionError(13, "Permission denied"),
                ),
                PermissionError,
                "permission denied reaching wire server",
            ),
            (
                "broken-pipe",
                mock.patch.object(
                    WireClient, "_write", side_effect=BrokenPipeError(32, "Broken pipe")
                ),
                BrokenPipeError,
                "lost connection to wire server",
            ),
            (
                "reset",
                mock.patch.object(
                    WireClient, "_read",
                    side_effect=ConnectionResetError(104, "Connection reset by peer"),
                ),
                ConnectionResetError,
                "lost connection to wire server",
            ),
        )

    def test_request_wraps_socket_failures(self) -> None:
        for name, patcher, cause, needle in self.cases():
            with self.subTest(name), patcher:
                with self.assertRaises(WireError) as ctx:
                    self.client.request({"op": "ping"})
                self.assertIn(needle, str(ctx.exception))
                self.assertIn(str(self.store.socket_path), str(ctx.exception))
                self.assertIsInstance(ctx.exception.__cause__, cause)

    def test_watch_wraps_socket_failures(self) -> None:
        for name, patcher, cause, needle in self.cases():
            with self.subTest(name), patcher:
                with self.assertRaises(WireError) as ctx:
                    next(self.client.watch(timeout_ms=100))
                self.assertIn(needle, str(ctx.exception))
                self.assertIsInstance(ctx.exception.__cause__, cause)

    def test_cli_reports_lost_connection_without_traceback(self) -> None:
        stderr = io.StringIO()
        from aivengers_wire import cli

        with mock.patch.object(
            WireClient, "_read", side_effect=ConnectionResetError(104, "reset")
        ), mock.patch("sys.stderr", stderr):
            code = cli.main(
                ["--state-dir", str(self.store.state_dir), "ping", "--seat", "tech"]
            )
        self.assertEqual(code, 2)
        self.assertTrue(stderr.getvalue().startswith("wire: lost connection"), stderr.getvalue())


if __name__ == "__main__":
    unittest.main()
