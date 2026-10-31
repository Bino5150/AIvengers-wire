"""R3 adversarial regressions for AIvengers Wire wake (WIRE-WAKE-01).

Ports the independent reviewer's R2 counterexamples (Goblin/Codex, review
of candidate 3717412/tree 970f35c) into permanent regressions, adapted to
the repaired architecture, and adds the owner-dispatched T1-T4 coverage:

- T1 temporal admission: lease and wake TTL are revalidated INSIDE the
  final write transaction, after a real lock wait (store-level injection
  plus authenticated-RPC relay counterexamples, with live-grant controls).
- T2 pagination completeness: default/max pages report truthful
  continuation; full cursor walks retrieve every record; tied cursor
  timestamps advance by wake_id.
- T3 frame-size safety: every listing page fits the transport frame budget
  measured as the exact serialized response, and every valid record stays
  retrievable through the cursors.
- T4 idempotency: the stable request identity includes the explicit target
  session; conflicting reuse is rejected; exact retries return the
  persisted wake without re-resolving implicit targets.

All sessions are synthetic test fakes: no real Claude Code, Codex or
Lumina runtime is contacted, woken, resumed or billed anywhere here.
"""

from __future__ import annotations

import json
import os
import signal
import subprocess
import sys
import tempfile
import threading
import time
import unittest
from contextlib import contextmanager
from datetime import datetime, timezone
from pathlib import Path
from typing import Any

from aivengers_wire.client import WireClient
from aivengers_wire.errors import WireError
from aivengers_wire.protocol import MAX_FRAME_BYTES, encode_frame
from aivengers_wire.storage import Seat, WireStore

from test_wire import RunningWireTestCase

RELAY_SCRIPT = Path(__file__).resolve().parent / "r3_rpc_temporal_relay.py"
PROJECT_ROOT = Path(__file__).resolve().parents[1]


def _long_task_ref() -> str:
    # 128 characters, 384 UTF-8 bytes — the largest legal task_ref.
    return "任务" * 64


def _long_key(index: int) -> str:
    # 128 characters — the largest legal idempotency_key.
    return "k" * 120 + f"{index:08d}"


def _max_meta() -> dict[str, Any]:
    # Serializes to <= 2,048 characters (the largest legal session meta).
    return {"blob": "x" * 2036}


def _max_meta_cjk() -> dict[str, Any]:
    # Multi-byte UTF-8 payload: 680 CJK characters plus envelope <= 2,048
    # characters, ~2 KB when encoded.
    return {"blob": "任" * 680}


class LockWaitMixin:
    """Real SQLite writer contention between the preflight and main
    transaction of one admission operation (ported from the reviewer's
    ``block_second_transaction``; no clock, SQL or predicate is replaced).

    Callers must perform ALL setup BEFORE entering the context: the
    instrumentation counts every ``connect()`` while active, so a setup
    call inside the window would consume the counted preflight slot and
    shift the hold off the T1 seam.
    """

    @contextmanager
    def block_second_transaction(self, seconds: float = 1.4):
        original = self.store.connect
        other = WireStore(self.store.state_dir)
        calls = 0
        locked = threading.Event()
        release = threading.Event()
        errors: list[str] = []
        observed: dict[str, Any] = {}
        thread: threading.Thread | None = None
        timer: threading.Timer | None = None

        def hold() -> None:
            try:
                with other.connect() as c:
                    c.execute("BEGIN IMMEDIATE")
                    observed["locker_acquired_utc"] = datetime.now(
                        timezone.utc
                    ).isoformat()
                    locked.set()
                    release.wait(4)
            except Exception as exc:  # pragma: no cover - defensive
                errors.append(repr(exc))
                locked.set()

        @contextmanager
        def instrumented():
            nonlocal calls, thread, timer
            calls += 1
            with original() as c:
                if calls == 2:
                    observed["main_pre_begin_transaction"] = c.in_transaction
                    thread = threading.Thread(target=hold)
                    thread.start()
                    assert locked.wait(2) and not errors, errors
                    timer = threading.Timer(seconds, release.set)
                    timer.start()
                yield c

        self.store.connect = instrumented
        start = time.monotonic()
        try:
            yield observed
        finally:
            self.store.connect = original
            release.set()
            if thread is not None:
                thread.join(5)
            if timer is not None:
                timer.cancel()
            observed["operation_elapsed_seconds"] = time.monotonic() - start
            assert not errors, errors


class R3TemporalStoreTests(LockWaitMixin, RunningWireTestCase):
    """T1 at the store seam: admission revalidates inside the final lock."""

    def make_wake(self, *, lease: int = 300, ttl: int | None = None):
        session = self.client("tech").register_session(
            adapter="synthetic-r3", lease_seconds=lease
        )
        self.store.add_task(task_id="T", title="synthetic task")
        wake = self.client("bino").wake(
            target_seat="tech",
            target_session_id=session["session_id"],
            task_ref="T",
            ttl_seconds=ttl,
        )["wake"]
        return session, wake

    def test_claim_rechecks_session_lease_after_write_lock_wait(self):
        session, wake = self.make_wake(lease=1)
        self.assertTrue(self.store.get_session(session["session_id"])["lease_live"])
        with self.block_second_transaction():
            with self.assertRaises(WireError) as ctx:
                self.store.claim_wake(
                    seat=Seat("tech", False),
                    wake_id=wake["wake_id"],
                    session_id=session["session_id"],
                )
            error = str(ctx.exception)
        self.assertIn("lease", error)
        # Durable terminal outcome, not just an exception.
        raw = self.store.get_wake(wake["wake_id"])
        self.assertEqual(raw["state"], "expired")
        self.assertIn("lease expired", raw["last_error"] or "")
        self.assertEqual(self.store.get_task("T")["state"], "open")
        self.assertFalse(self.store.get_session(session["session_id"])["lease_live"])

    def test_claim_rechecks_wake_ttl_after_write_lock_wait(self):
        session, wake = self.make_wake(ttl=1)
        with self.block_second_transaction():
            with self.assertRaises(WireError) as ctx:
                self.store.claim_wake(
                    seat=Seat("tech", False),
                    wake_id=wake["wake_id"],
                    session_id=session["session_id"],
                )
            error = str(ctx.exception)
        self.assertIn("ttl", error.lower())
        raw = self.store.get_wake(wake["wake_id"])
        self.assertEqual(raw["state"], "expired")
        self.assertIn("ttl expired at claim admission", raw["last_error"] or "")
        self.assertEqual(self.store.get_task("T")["state"], "open")

    def test_ack_rechecks_wake_ttl_after_write_lock_wait(self):
        session, wake = self.make_wake(ttl=1)
        with self.block_second_transaction():
            with self.assertRaises(WireError) as ctx:
                self.store.acknowledge_wake(
                    seat=Seat("tech", False),
                    wake_id=wake["wake_id"],
                    session_id=session["session_id"],
                )
            error = str(ctx.exception)
        self.assertIn("ttl", error.lower())
        raw = self.store.get_wake(wake["wake_id"])
        self.assertEqual(raw["state"], "expired")
        self.assertIn("ttl expired at ack admission", raw["last_error"] or "")
        self.assertEqual(self.store.get_task("T")["state"], "open")

    def test_control_wait_with_live_grants_succeeds(self):
        session, wake = self.make_wake()  # default 300s lease, no TTL
        with self.block_second_transaction():
            result = self.store.claim_wake(
                seat=Seat("tech", False),
                wake_id=wake["wake_id"],
                session_id=session["session_id"],
            )
        self.assertEqual(result["task"]["state"], "claimed")
        self.assertEqual(result["wake"]["state"], "claimed")


class RelayTestCase(unittest.TestCase):
    """Store + seats + the temporal relay subprocess (no standard server:
    the relay itself binds the socket, so the stock fixture's server would
    collide with it on the same state dir)."""

    def setUp(self) -> None:
        self.temp_dir = tempfile.TemporaryDirectory()
        self.state_dir = Path(self.temp_dir.name) / "state"
        self.store = WireStore(self.state_dir)
        self.store.initialize()
        self.store.add_seat("tech")
        self.store.add_seat("rookie")
        self.store.add_seat("bino")
        self.relay, self.relay_records = self._start_relay()

    def tearDown(self) -> None:
        self._stop_relay()
        self.temp_dir.cleanup()

    def client(self, seat: str) -> WireClient:
        return WireClient(self.store, seat=seat)

    def _start_relay(self, hold_seconds: float = 1.4):
        records_path = Path(self.temp_dir.name) / "relay-records.json"
        env = os.environ.copy()
        env["PYTHONPATH"] = str(PROJECT_ROOT / "src")
        env["PYTHONDONTWRITEBYTECODE"] = "1"
        relay = subprocess.Popen(
            [
                sys.executable,
                str(RELAY_SCRIPT),
                str(self.state_dir),
                str(records_path),
                str(hold_seconds),
            ],
            cwd=PROJECT_ROOT,
            env=env,
            stdout=subprocess.DEVNULL,
            stderr=subprocess.PIPE,
            text=True,
        )
        deadline = time.monotonic() + 10
        while time.monotonic() < deadline:
            if relay.poll() is not None:
                stderr = relay.stderr.read() if relay.stderr else ""
                self.fail(f"temporal relay exited during startup: {stderr}")
            if self.store.socket_path.exists():
                try:
                    self.client("tech").request({"op": "ping"})
                except WireError:
                    time.sleep(0.02)
                else:
                    break
            else:
                time.sleep(0.02)
        else:
            self.fail("temporal relay did not become ready")
        return relay, records_path

    def _stop_relay(self) -> None:
        if self.relay.poll() is None:
            self.relay.send_signal(signal.SIGINT)
            try:
                self.relay.wait(timeout=5)
            except subprocess.TimeoutExpired:  # pragma: no cover - defensive
                self.relay.kill()
                self.relay.wait()
        if self.relay.stderr:
            self.relay.stderr.close()


class R3RpcTemporalTests(RelayTestCase):
    """T1 over authenticated RPC: the reviewer's relay counterexamples.

    A real WireServer subprocess runs with real writer contention injected
    between the expiry preflight and the admitting transaction (see
    ``r3_rpc_temporal_relay.py``). On the reviewed R2 candidate all three
    expired admissions were accepted over the socket; the repaired
    admission must refuse every one while the live-grant control succeeds.
    """

    def test_expired_lease_and_ttl_refused_over_authenticated_rpc(self):
        cases: dict[str, dict[str, Any]] = {}
        errors: dict[str, str] = {}

        def prepare(name: str, *, lease: int | None, ttl: int | None) -> None:
            session = self.client("tech").register_session(
                adapter="synthetic-temporal",
                lease_seconds=lease if lease is not None else 300,
            )
            self.store.add_task(task_id=name, title="expiry probe")
            wake = self.client("bino").wake(
                target_seat="tech",
                target_session_id=session["session_id"],
                task_ref=name,
                ttl_seconds=ttl,
            )["wake"]
            cases[name] = {"session": session, "wake": wake}

        # Each case prepares IMMEDIATELY before its own attempt (the
        # reviewer's probe shape): a previous case's 1.4s lock hold must not
        # age the next case's 1s TTL past expiry before its claim starts,
        # or the refusal takes the preflight path instead of the T1 seam.
        prepare("session-lease", lease=1, ttl=None)
        with self.assertRaises(WireError) as ctx:
            self.client("tech").claim(
                wake_id=cases["session-lease"]["wake"]["wake_id"],
                session_id=cases["session-lease"]["session"]["session_id"],
            )
        errors["session-lease"] = str(ctx.exception)

        prepare("wake-ttl-claim", lease=None, ttl=1)
        with self.assertRaises(WireError) as ctx:
            self.client("tech").claim(
                wake_id=cases["wake-ttl-claim"]["wake"]["wake_id"],
                session_id=cases["wake-ttl-claim"]["session"]["session_id"],
            )
        errors["wake-ttl-claim"] = str(ctx.exception)

        prepare("wake-ttl-ack", lease=None, ttl=1)
        with self.assertRaises(WireError) as ctx:
            self.client("tech").wake_ack(
                wake_id=cases["wake-ttl-ack"]["wake"]["wake_id"],
                session_id=cases["wake-ttl-ack"]["session"]["session_id"],
            )
        errors["wake-ttl-ack"] = str(ctx.exception)

        # Control: live grants succeed under the identical wait.
        prepare("live-control", lease=None, ttl=None)
        result = self.client("tech").claim(
            wake_id=cases["live-control"]["wake"]["wake_id"],
            session_id=cases["live-control"]["session"]["session_id"],
        )
        self.assertEqual(result["task"]["state"], "claimed")

        for name, error in errors.items():
            self.assertIn("expired", error, name)
        # Durable refusal outcomes: the three expired wakes are terminal
        # 'expired' with admission-time last_error; their tasks stay open.
        with self.store.connect() as c:
            for name in ("session-lease", "wake-ttl-claim", "wake-ttl-ack"):
                row = c.execute(
                    "SELECT state, last_error FROM wire_wakes WHERE wake_id = ?",
                    (cases[name]["wake"]["wake_id"],),
                ).fetchone()
                self.assertEqual(row["state"], "expired", name)
                expected_error = (
                    "session lease expired at claim time"
                    if name == "session-lease"
                    else f"wake ttl expired at {('claim' if name == 'wake-ttl-claim' else 'ack')} admission"
                )
                self.assertEqual(row["last_error"], expected_error, name)
                task_state = c.execute(
                    "SELECT state FROM wire_tasks WHERE task_id = ?", (name,)
                ).fetchone()[0]
                self.assertEqual(task_state, "open", name)
        records = json.loads(self.relay_records.read_text())
        self.assertEqual(len(records), 4)
        for record in records:
            self.assertFalse(record["pre_begin_transaction"])


class R3PaginationTests(RunningWireTestCase):
    """T2: truthful continuation at the default/max cap, full walks."""

    def _backlog(self, count: int, *, long_fields: bool = False) -> list[str]:
        task = _long_task_ref() if long_fields else "queue"
        self.store.add_task(task_id=task, title="synthetic queue")
        ids = []
        for i in range(count):
            key = _long_key(i) if long_fields else f"key-{i}"
            ids.append(
                self.client("bino")
                .wake(target_seat="tech", task_ref=task, idempotency_key=key)["wake"][
                    "wake_id"
                ]
            )
        return ids

    def _walk_wakes(self, client: WireClient, *, limit: int | None = None):
        ids: list[str] = []
        pages: list[dict[str, Any]] = []
        for page in client.wake_list_iter(state="pending", limit=limit):
            ids.extend(w["wake_id"] for w in page["wakes"])
            pages.append(
                {
                    "count": len(page["wakes"]),
                    "has_more": page["has_more"],
                    "next_after": page["next_after"],
                    "bytes": len(encode_frame(page)),
                }
            )
        return ids, pages

    def test_default_page_reports_truthful_continuation_and_full_walk(self):
        ids = self._backlog(200)
        client = self.client("tech")
        first = client.wake_list_page(state="pending")  # default limit = cap
        self.assertEqual(len(first["wakes"]), 100)
        self.assertTrue(first["has_more"], "default page must not claim completeness")
        self.assertIsNotNone(first["next_after"])
        walked, pages = self._walk_wakes(client)
        self.assertEqual(sorted(walked), sorted(ids))
        self.assertEqual(len(set(walked)), 200)
        self.assertFalse(pages[-1]["has_more"])
        # limit=99 also walks the whole queue (reviewer's control shape).
        walked99, _ = self._walk_wakes(client, limit=99)
        self.assertEqual(sorted(walked99), sorted(ids))

    def test_exactly_full_page_reports_complete(self):
        self._backlog(100)
        page = self.client("tech").wake_list_page(state="pending")
        self.assertEqual(len(page["wakes"]), 100)
        self.assertFalse(page["has_more"])
        self.assertIsNone(page["next_after"])

    def test_one_extra_reports_more(self):
        self._backlog(101)
        page = self.client("tech").wake_list_page(state="pending")
        self.assertEqual(len(page["wakes"]), 100)
        self.assertTrue(page["has_more"])
        self.assertIsNotNone(page["next_after"])

    def test_tied_cursor_timestamps_advance_by_wake_id(self):
        ids = self._backlog(5)
        tied = "2026-10-31T00:00:00.000000Z"
        with self.store.connect() as c:
            c.execute("BEGIN IMMEDIATE")
            marks = ",".join("?" for _ in ids)
            c.execute(
                f"UPDATE wire_wakes SET created_at = ? WHERE wake_id IN ({marks})",
                (tied, *ids),
            )
        walked, pages = self._walk_wakes(self.client("tech"), limit=2)
        self.assertEqual(sorted(walked), sorted(ids))
        self.assertEqual(len(pages), 3)
        self.assertEqual([p["count"] for p in pages], [2, 2, 1])

    def test_session_listing_truthful_continuation_and_full_walk(self):
        client = self.client("tech")
        for i in range(101):
            client.register_session(adapter=f"synthetic-{i}")
        first = client.sessions_page()
        self.assertEqual(len(first["sessions"]), 100)
        self.assertTrue(first["has_more"], "session cap must not claim completeness")
        self.assertIsNotNone(first["next_after"])
        seen: list[str] = []
        cursor = first
        for _ in range(10):
            seen.extend(s["session_id"] for s in cursor["sessions"])
            if not cursor["has_more"]:
                break
            cursor = client.sessions_page(
                after_registered_at=cursor["next_after"]["registered_at"],
                after_session_id=cursor["next_after"]["session_id"],
            )
        self.assertEqual(len(seen), 101)
        self.assertEqual(len(set(seen)), 101)
        self.assertFalse(cursor["has_more"])


class R3FrameBudgetTests(RunningWireTestCase):
    """T3: every page fits the transport frame budget; nothing is lost."""

    def _long_backlog(self, count: int) -> list[str]:
        task = _long_task_ref()
        self.store.add_task(task_id=task, title="synthetic queue")
        created = []
        for i in range(count):
            created.append(
                self.client("bino")
                .wake(
                    target_seat="tech",
                    task_ref=task,
                    idempotency_key=_long_key(i),
                )["wake"]["wake_id"]
            )
        return created

    def test_wake_pages_fit_frame_budget_with_valid_long_identifiers(self):
        created = self._long_backlog(200)
        client = self.client("tech")
        walked: list[str] = []
        worst_page_bytes = 0
        for page in client.wake_list_iter(state="pending"):
            size = len(encode_frame(page))
            worst_page_bytes = max(worst_page_bytes, size)
            self.assertLessEqual(size, MAX_FRAME_BYTES)
            walked.extend(w["wake_id"] for w in page["wakes"])
        self.assertEqual(sorted(walked), sorted(created))
        self.assertGreater(worst_page_bytes, 0)

    def test_session_pages_fit_frame_budget_with_valid_metadata(self):
        client = self.client("tech")
        for i in range(101):
            client.register_session(
                adapter=f"synthetic-{i}",
                meta=_max_meta() if i % 2 else _max_meta_cjk(),
            )
        seen: list[str] = []
        cursor = client.sessions_page()
        worst_page_bytes = len(encode_frame(cursor))
        self.assertLessEqual(worst_page_bytes, MAX_FRAME_BYTES)
        for _ in range(10):
            seen.extend(s["session_id"] for s in cursor["sessions"])
            if not cursor["has_more"]:
                break
            cursor = client.sessions_page(
                after_registered_at=cursor["next_after"]["registered_at"],
                after_session_id=cursor["next_after"]["session_id"],
            )
            size = len(encode_frame(cursor))
            worst_page_bytes = max(worst_page_bytes, size)
            self.assertLessEqual(size, MAX_FRAME_BYTES)
        self.assertEqual(len(seen), 101)
        self.assertEqual(len(set(seen)), 101)

    def test_single_maximum_size_rows_fit_one_page(self):
        task = _long_task_ref()
        self.store.add_task(task_id=task, title="max row")
        self.client("bino").wake(
            target_seat="tech", task_ref=task, idempotency_key=_long_key(0)
        )
        self.client("tech").register_session(adapter="max-meta", meta=_max_meta_cjk())
        wake_page = self.client("tech").wake_list_page()
        self.assertLessEqual(len(encode_frame(wake_page)), MAX_FRAME_BYTES)
        self.assertEqual(len(wake_page["wakes"]), 1)
        session_page = self.client("tech").sessions_page()
        self.assertLessEqual(len(encode_frame(session_page)), MAX_FRAME_BYTES)
        self.assertEqual(len(session_page["sessions"]), 1)


class R3IdempotencyTests(RunningWireTestCase):
    """T4: stable request identity includes the explicit target session."""

    def test_key_reuse_cannot_silently_select_a_different_explicit_session(self):
        first = self.client("tech").register_session(adapter="first-fake")
        second = self.client("tech").register_session(adapter="second-fake")
        self.store.add_task(task_id="I", title="synthetic target")
        original = self.client("bino").wake(
            target_seat="tech",
            task_ref="I",
            target_session_id=first["session_id"],
            idempotency_key="same-key",
        )
        with self.assertRaises(WireError) as ctx:
            self.client("bino").wake(
                target_seat="tech",
                task_ref="I",
                target_session_id=second["session_id"],
                idempotency_key="same-key",
            )
        self.assertIn("different explicit target session", str(ctx.exception))
        # The persisted receipt is unchanged by the rejected conflict.
        again = self.client("bino").wake(
            target_seat="tech",
            task_ref="I",
            target_session_id=first["session_id"],
            idempotency_key="same-key",
        )
        self.assertEqual(again["resolution"], "duplicate")
        self.assertEqual(again["wake"]["wake_id"], original["wake"]["wake_id"])

    def test_exact_retry_returns_persisted_wake_without_reresolution(self):
        first = self.client("tech").register_session(adapter="first-fake")
        self.store.add_task(task_id="R", title="synthetic retry")
        original = self.client("bino").wake(
            target_seat="tech",
            task_ref="R",
            target_session_id=first["session_id"],
            idempotency_key="retry-key",
        )
        # A second session registers AFTER the original request; a legitimate
        # retry must return the persisted wake, not re-resolve against
        # today's sessions.
        self.client("tech").register_session(adapter="second-fake")
        retry = self.client("bino").wake(
            target_seat="tech",
            task_ref="R",
            target_session_id=first["session_id"],
            idempotency_key="retry-key",
        )
        self.assertEqual(retry["resolution"], "duplicate")
        self.assertEqual(retry["wake"]["wake_id"], original["wake"]["wake_id"])
        self.assertEqual(retry["wake"]["target_session_id"], first["session_id"])
        self.assertEqual(
            retry["wake"]["requested_target_session_id"], first["session_id"]
        )

    def test_implicit_retry_returns_persisted_resolution(self):
        self.store.add_task(task_id="M", title="implicit retry")
        original = self.client("bino").wake(
            target_seat="tech", task_ref="M", idempotency_key="implicit-key"
        )
        # Another live capable session appears; the retry must NOT re-resolve.
        self.client("tech").register_session(adapter="late-fake")
        retry = self.client("bino").wake(
            target_seat="tech", task_ref="M", idempotency_key="implicit-key"
        )
        self.assertEqual(retry["resolution"], "duplicate")
        self.assertEqual(retry["wake"]["wake_id"], original["wake"]["wake_id"])
        self.assertIsNone(retry["wake"]["requested_target_session_id"])

    def test_explicit_then_implicit_retry_conflicts(self):
        first = self.client("tech").register_session(adapter="first-fake")
        self.store.add_task(task_id="C", title="conflict probe")
        self.client("bino").wake(
            target_seat="tech",
            task_ref="C",
            target_session_id=first["session_id"],
            idempotency_key="conflict-key",
        )
        with self.assertRaises(WireError) as ctx:
            self.client("bino").wake(
                target_seat="tech", task_ref="C", idempotency_key="conflict-key"
            )
        self.assertIn("different explicit target session", str(ctx.exception))

    def test_identity_ignores_delivery_hints(self):
        first = self.client("tech").register_session(adapter="first-fake")
        self.store.add_task(task_id="H", title="hint probe")
        original = self.client("bino").wake(
            target_seat="tech",
            task_ref="H",
            target_session_id=first["session_id"],
            idempotency_key="hint-key",
            priority=3,
            ttl_seconds=600,
            note="first",
        )
        # Deliberate spec: priority/ttl/note are delivery hints, not identity.
        retry = self.client("bino").wake(
            target_seat="tech",
            task_ref="H",
            target_session_id=first["session_id"],
            idempotency_key="hint-key",
            priority=7,
            ttl_seconds=60,
            note="second",
        )
        self.assertEqual(retry["resolution"], "duplicate")
        self.assertEqual(retry["wake"]["wake_id"], original["wake"]["wake_id"])
        self.assertEqual(retry["wake"]["priority"], 3)


if __name__ == "__main__":  # pragma: no cover
    unittest.main()
