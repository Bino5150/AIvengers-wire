"""Release-portability proofs for AIvengers Wire v0.1.0.

Proves the candidate is agent-agnostic and portable: import purity, explicit
seat creation, a generic two-seat end-to-end flow with restart durability,
structural impossibility of owner elevation at the database layer, and absence
of machine-specific residue in product code and release docs.
"""

from __future__ import annotations

import json
import os
import signal
import sqlite3
import stat
import subprocess
import sys
import tempfile
import time
import unittest
from pathlib import Path

PROJECT_ROOT = Path(__file__).resolve().parents[1]
SRC = PROJECT_ROOT / "src"


def run_cli(args: list[str], state_dir: Path, *, check: bool = True) -> subprocess.CompletedProcess:
    # The CLI's global options (e.g. --json) precede the subcommand; normalize here so
    # call sites can list flags in any order.
    ordered = [a for a in args if a != "--json"]
    if "--json" in args:
        ordered = ["--json", *ordered]
    env = os.environ.copy()
    env["PYTHONPATH"] = str(SRC)
    env["AIVENGERS_WIRE_STATE_DIR"] = str(state_dir)
    proc = subprocess.run(
        [sys.executable, "-m", "aivengers_wire", *ordered],
        cwd=PROJECT_ROOT,
        env=env,
        capture_output=True,
        text=True,
        timeout=30,
    )
    if check and proc.returncode != 0:
        raise AssertionError(f"wire {' '.join(args)} failed: {proc.stderr}")
    return proc


class ImportPurityTestCase(unittest.TestCase):
    def test_importing_package_creates_no_state_or_services(self) -> None:
        with tempfile.TemporaryDirectory() as temp:
            state_dir = Path(temp) / "state"
            env = os.environ.copy()
            env["PYTHONPATH"] = str(SRC)
            env["AIVENGERS_WIRE_STATE_DIR"] = str(state_dir)
            code = (
                "import aivengers_wire, aivengers_wire.cli, aivengers_wire.client, "
                "aivengers_wire.clubhouse, aivengers_wire.errors, aivengers_wire.protocol, "
                "aivengers_wire.server, aivengers_wire.storage; "
                "print(aivengers_wire.__version__)"
            )
            proc = subprocess.run(
                [sys.executable, "-c", code],
                env=env,
                capture_output=True,
                text=True,
                timeout=30,
            )
            self.assertEqual(proc.returncode, 0, proc.stderr)
            self.assertEqual(proc.stdout.strip(), "0.1.0")
            self.assertFalse(state_dir.exists(), "import must not create runtime state")


class ExplicitSeatInitTestCase(unittest.TestCase):
    def test_default_init_creates_no_seats(self) -> None:
        with tempfile.TemporaryDirectory() as temp:
            state = Path(temp) / "state"
            run_cli(["init"], state)
            self.assertEqual(list((state / "seats").iterdir()), [])
            self.assertEqual(stat.S_IMODE(state.stat().st_mode), 0o700)
            self.assertEqual(stat.S_IMODE((state / "seats").stat().st_mode), 0o700)

    def test_explicit_seats_have_no_forwarding_by_default(self) -> None:
        with tempfile.TemporaryDirectory() as temp:
            state = Path(temp) / "state"
            proc = run_cli(["init", "--seat", "alice", "--seat", "bob", "--json"], state)
            payload = json.loads(proc.stdout)
            self.assertEqual(
                [(seat["seat"], seat["can_forward"]) for seat in payload["seats"]],
                [("alice", False), ("bob", False)],
            )
            for token in (state / "seats").glob("*.token"):
                self.assertEqual(stat.S_IMODE(token.stat().st_mode), 0o600)

    def test_demo_roster_is_explicit_opt_in(self) -> None:
        with tempfile.TemporaryDirectory() as temp:
            state = Path(temp) / "state"
            proc = run_cli(["init", "--demo-roster", "--json"], state)
            payload = json.loads(proc.stdout)
            self.assertEqual(
                [seat["seat"] for seat in payload["seats"]],
                ["bino", "lumina", "tech", "rookie", "goblin", "other-claude", "bino-gateway"],
            )
            forwarding = {seat["seat"]: seat["can_forward"] for seat in payload["seats"]}
            self.assertTrue(forwarding["bino-gateway"])
            self.assertFalse(any(v for k, v in forwarding.items() if k != "bino-gateway"))

    def test_demo_roster_and_explicit_seats_are_mutually_exclusive(self) -> None:
        with tempfile.TemporaryDirectory() as temp:
            proc = run_cli(
                ["init", "--demo-roster", "--seat", "x"],
                Path(temp) / "state",
                check=False,
            )
            self.assertEqual(proc.returncode, 2)
            self.assertIn("either", proc.stderr)


class GenericTwoSeatEndToEndTestCase(unittest.TestCase):
    """Fresh init, two generic seats, full message cycle, restart durability."""

    def setUp(self) -> None:
        self.temp_dir = tempfile.TemporaryDirectory()
        self.state_dir = Path(self.temp_dir.name) / "state"
        run_cli(["init", "--seat", "alice", "--seat", "bob"], self.state_dir)
        self.server = None
        self.start_server()

    def tearDown(self) -> None:
        if self.server is not None and self.server.poll() is None:
            self.server.send_signal(signal.SIGINT)
            try:
                self.server.wait(timeout=5)
            except subprocess.TimeoutExpired:
                self.server.kill()
                self.server.wait(timeout=5)
        self.temp_dir.cleanup()

    def start_server(self) -> None:
        env = os.environ.copy()
        env["PYTHONPATH"] = str(SRC)
        env["AIVENGERS_WIRE_STATE_DIR"] = str(self.state_dir)
        self.server = subprocess.Popen(
            [sys.executable, "-m", "aivengers_wire", "serve"],
            cwd=PROJECT_ROOT,
            env=env,
            stdout=subprocess.DEVNULL,
            stderr=subprocess.DEVNULL,
        )
        deadline = time.monotonic() + 5
        while time.monotonic() < deadline:
            probe = run_cli(["ping", "--seat", "alice"], self.state_dir, check=False)
            if probe.returncode == 0:
                return
            time.sleep(0.05)
        self.fail("relay did not become ready")

    def test_generic_seats_exchange_durable_messages_across_restart(self) -> None:
        posted = json.loads(
            run_cli(
                ["post", "--seat", "alice", "general", "hello from alice", "--json"],
                self.state_dir,
            ).stdout
        )["message"]
        reply = json.loads(
            run_cli(
                [
                    "reply",
                    "--seat",
                    "bob",
                    "general",
                    str(posted["id"]),
                    "hi alice",
                    "--json",
                ],
                self.state_dir,
            ).stdout
        )["message"]
        self.assertEqual(reply["reply_to"], posted["id"])
        self.assertEqual(reply["authority"], "peer")
        self.assertIs(reply["owner"], False)

        messages = json.loads(
            run_cli(
                ["read", "--seat", "bob", "general", "--advance", "--json"], self.state_dir
            ).stdout
        )["messages"]
        self.assertEqual([m["id"] for m in messages], [posted["id"], reply["id"]])
        cursor = json.loads(
            run_cli(["cursor", "--seat", "bob", "general", "--json"], self.state_dir).stdout
        )
        self.assertEqual(cursor["last_message_id"], reply["id"])

        # Restart the relay; durable history and cursors must survive.
        self.server.send_signal(signal.SIGINT)
        self.server.wait(timeout=5)
        self.start_server()
        again = json.loads(
            run_cli(
                ["read", "--seat", "bob", "general", "--after", "0", "--json"],
                self.state_dir,
            ).stdout
        )["messages"]
        self.assertEqual([m["id"] for m in again], [posted["id"], reply["id"]])
        cursor_after_restart = json.loads(
            run_cli(["cursor", "--seat", "bob", "general", "--json"], self.state_dir).stdout
        )
        self.assertEqual(cursor_after_restart["last_message_id"], reply["id"])


class DatabaseOwnerElevationTestCase(unittest.TestCase):
    def test_database_cannot_represent_owner_or_nonpeer_authority(self) -> None:
        with tempfile.TemporaryDirectory() as temp:
            state = Path(temp) / "state"
            run_cli(["init"], state)
            db = sqlite3.connect(state / "wire.db")
            try:
                base = {
                    "message_uuid": "uuid-1",
                    "channel": "general",
                    "logical_sender": "alice",
                    "transport_actor": "alice",
                    "transport": "local-unix",
                    "provenance": "direct",
                    "body": "elevation attempt",
                    "created_at": "2026-09-11T00:00:00Z",
                    "peer_pid": 1,
                    "peer_uid": 1000,
                    "peer_gid": 1000,
                }
                insert = (
                    "INSERT INTO messages (message_uuid, channel, logical_sender,"
                    " transport_actor, transport, provenance, authority, owner, body,"
                    " created_at, peer_pid, peer_uid, peer_gid)"
                    " VALUES (:message_uuid, :channel, :logical_sender, :transport_actor,"
                    " :transport, :provenance, :authority, :owner, :body, :created_at,"
                    " :peer_pid, :peer_uid, :peer_gid)"
                )
                with self.assertRaises(sqlite3.IntegrityError):
                    db.execute(insert, {**base, "authority": "owner", "owner": 0})
                with self.assertRaises(sqlite3.IntegrityError):
                    db.execute(insert, {**base, "authority": "peer", "owner": 1})
            finally:
                db.close()


class NoMachineResidueTestCase(unittest.TestCase):
    FORBIDDEN = ("/home/bino", "Bino5150", "therealagentlumina", "@gmail.", "bino-skynet")

    def release_files(self) -> list[Path]:
        targets = list((SRC / "aivengers_wire").glob("*.py"))
        targets += [
            PROJECT_ROOT / name
            for name in (
                "README.md",
                "SECURITY.md",
                "CLUBHOUSE.md",
                "pyproject.toml",
                "wire",
                "clubhouse",
            )
        ]
        return targets

    def test_product_code_and_docs_carry_no_machine_residue(self) -> None:
        for path in self.release_files():
            text = path.read_text(encoding="utf-8")
            for needle in self.FORBIDDEN:
                self.assertNotIn(needle, text, f"{path.name} contains {needle!r}")

    def test_product_code_is_agent_agnostic(self) -> None:
        for path in (SRC / "aivengers_wire").glob("*.py"):
            if path.name == "cli.py":
                # cli.py carries the explicitly documented demo roster only.
                continue
            text = path.read_text(encoding="utf-8").lower()
            self.assertNotIn("bino", text, f"{path.name} hardcodes AIvengers identities")
            self.assertNotIn("lumina", text, f"{path.name} hardcodes AIvengers identities")


if __name__ == "__main__":
    unittest.main()