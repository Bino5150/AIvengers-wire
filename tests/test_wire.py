from __future__ import annotations

import json
import os
import signal
import socket
import stat
import subprocess
import sys
import tempfile
import threading
import time
import unittest
from unittest import mock
from pathlib import Path

import tkinter as tk

from aivengers_wire.client import WireClient
from aivengers_wire.clubhouse import (
    ClubhouseApp,
    ClubhouseService,
    DEFAULT_CHANNELS,
    build_identity_routes,
)
from aivengers_wire.errors import WireError
from aivengers_wire.protocol import encode_frame
from aivengers_wire.server import WireServer
from aivengers_wire.storage import WireStore


class RunningWireTestCase(unittest.TestCase):
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


if __name__ == "__main__":
    unittest.main()
