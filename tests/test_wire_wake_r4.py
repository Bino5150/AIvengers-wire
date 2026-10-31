"""R4 listing-boundary regressions for AIvengers Wire wake (WIRE-WAKE-01).

Ports the independent reviewer's R3 boundary probes (Goblin/Codex, review
of candidate f4edfff/tree 0a12d647) into permanent regressions, and adds
final-frame boundary receipts at just-below / exactly-at / just-above the
transport budget:

- R3-A: empty listings (fresh empty queue, filtered query, cursor exactly
  past the last record) are SUCCESSFUL correctly shaped pages — empty
  items, has_more=false, no continuation cursor — never frame-budget
  refusals.
- R3-B: the byte budget is enforced against the COMPLETE final transport
  frame — continuation cursor and the server's ``ok`` envelope included —
  with the cursor recomputed after every trim. No silent truncation: a
  trimmed page reports has_more=true and the dropped rows stay reachable
  through the cursor.

Boundary fixtures derive exact metadata lengths from real returned rows
(the reviewer's derivation technique), so the just-below/equal/above
targets are exact arithmetic on real encoded bytes, not estimates.

All sessions are synthetic test fakes: no real Claude Code, Codex or
Lumina runtime is contacted, woken, resumed or billed anywhere here.
"""

from __future__ import annotations

import json
import socket
import unittest
from typing import Any

from aivengers_wire.errors import WireError
from aivengers_wire.protocol import MAX_FRAME_BYTES, encode_frame
from aivengers_wire.server import _fit_listing_response

from test_wire import RunningWireTestCase


class R4BoundaryTests(RunningWireTestCase):
    """Reviewer's R3-A/R3-B probes + final-frame boundary receipts."""

    def request_result(self, payload: dict[str, Any]):
        try:
            return self.client("tech").request(payload), None
        except WireError as exc:
            return None, str(exc)

    def raw_wire_result(self, payload: dict[str, Any]):
        """Read the server's actual COMPLETE frame with a wide buffer.

        Ordinary WireClient stays unmodified and rejects oversized frames;
        this helper observes what the server actually put on the wire.
        (Ported from the reviewer's probe.)"""
        token = (self.store.seat_dir / "tech.token").read_text().strip()
        with socket.socket(socket.AF_UNIX, socket.SOCK_STREAM) as sock:
            sock.settimeout(5)
            sock.connect(str(self.store.socket_path))
            with sock.makefile("rwb", buffering=0) as stream:
                stream.write(encode_frame({"op": "auth", "seat": "tech", "token": token}))
                self.assertTrue(json.loads(stream.readline())["ok"])
                stream.write(encode_frame(payload))
                raw = stream.readline(131_073)
                return len(raw), json.loads(raw)

    # ── R3-A: empty listings are successful pages ────────────────────────

    def test_empty_wakes_return_successful_empty_page(self):
        response, error = self.request_result({"op": "wake_list"})
        self.assertIsNone(error)
        self.assertEqual(response["wakes"], [])
        self.assertFalse(response["has_more"])
        self.assertIsNone(response["next_after"])

    def test_filtered_empty_wake_query_returns_successful_empty_page(self):
        self.store.add_task(task_id="F", title="filtered probe")
        self.client("bino").wake(target_seat="tech", task_ref="F")
        # pending wakes exist, but the claimed filter matches nothing.
        response, error = self.request_result({"op": "wake_list", "state": "claimed"})
        self.assertIsNone(error)
        self.assertEqual(response["wakes"], [])
        self.assertFalse(response["has_more"])
        self.assertIsNone(response["next_after"])

    def test_empty_sessions_return_successful_empty_page(self):
        response, error = self.request_result({"op": "sessions"})
        self.assertIsNone(error)
        self.assertEqual(response["sessions"], [])
        self.assertFalse(response["has_more"])
        self.assertIsNone(response["next_after"])

    def test_cursor_past_last_session_returns_empty_page(self):
        session = self.client("tech").register_session(adapter="synthetic-boundary")
        response, error = self.request_result(
            {
                "op": "sessions",
                "after_registered_at": session["registered_at"],
                "after_session_id": session["session_id"],
            }
        )
        self.assertIsNone(error)
        self.assertEqual(response["sessions"], [])
        self.assertFalse(response["has_more"])
        self.assertIsNone(response["next_after"])

    # ── R3-B: the budget counts the COMPLETE final transport frame ───────

    def _populate_tuned_sessions(
        self, count: int, target_final_bytes: int, *, with_continuation: bool
    ):
        """Register ``count`` sessions whose COMPLETE final wire size
        (ok envelope + cursor when present) equals ``target_final_bytes``.

        Derivation technique ported from the reviewer: all identity and
        timestamp fields have fixed lengths, so per-row size varies only
        with the metadata length; the first real row anchors the base size
        and the remaining rows' metadata lengths are solved exactly."""
        client = self.client("tech")
        first = client.register_session(
            adapter="synthetic-boundary", meta={"data": "m" * 1500}
        )
        shape: dict[str, Any] = {
            "sessions": [first] * count,
            "has_more": False,
            # The server's final response ALWAYS carries next_after (null
            # when has_more is false) — the derivation must match that
            # complete shape exactly or the server will honestly trim a
            # page the fixture believed to be at/below budget.
            "next_after": None,
        }
        if with_continuation:
            shape["has_more"] = True
            shape["next_after"] = {
                "registered_at": first["registered_at"],
                "session_id": first["session_id"],
            }
        initial = len(encode_frame({"ok": True, **shape}))
        delta = target_final_bytes - initial
        per, rem = divmod(delta, count - 1)
        lengths = [1500]
        for i in range(count - 1):
            size = 1500 + per + (rem if i == count - 2 else 0)
            self.assertGreaterEqual(size, 0)
            self.assertLessEqual(len(json.dumps({"data": "m" * size})), 2048)
            client.register_session(
                adapter="synthetic-boundary", meta={"data": "m" * size}
            )
            lengths.append(size)
        rows = self.store.list_sessions(seat="tech", limit=count)
        actual = len(encode_frame({"ok": True, **shape | {"sessions": rows}}))
        self.assertEqual(
            actual,
            target_final_bytes,
            "fixture derivation must be exact",
        )
        return rows, lengths

    def test_final_page_just_below_budget_is_delivered_whole(self):
        target = MAX_FRAME_BYTES - 5
        rows, _ = self._populate_tuned_sessions(
            30, target, with_continuation=False
        )
        raw_size, raw = self.raw_wire_result({"op": "sessions"})
        self.assertEqual(raw_size, target)
        self.assertLessEqual(raw_size, MAX_FRAME_BYTES)
        self.assertTrue(raw["ok"])
        self.assertEqual(len(raw["sessions"]), 30)
        self.assertFalse(raw["has_more"])
        self.assertIsNone(raw["next_after"])
        del rows

    def test_final_page_exactly_at_budget_is_delivered_whole(self):
        target = MAX_FRAME_BYTES
        rows, _ = self._populate_tuned_sessions(
            30, target, with_continuation=False
        )
        raw_size, raw = self.raw_wire_result({"op": "sessions"})
        self.assertEqual(raw_size, MAX_FRAME_BYTES)
        self.assertTrue(raw["ok"])
        self.assertEqual(len(raw["sessions"]), 30)
        del rows

    def test_final_page_just_above_budget_trims_losslessly(self):
        target = MAX_FRAME_BYTES + 20  # server must trim to fit
        rows, _ = self._populate_tuned_sessions(
            30, target, with_continuation=False
        )
        raw_size, raw = self.raw_wire_result({"op": "sessions"})
        self.assertLessEqual(raw_size, MAX_FRAME_BYTES)
        self.assertTrue(raw["ok"])
        # No silent truncation: has_more is forced true and the dropped
        # rows stay reachable through the truthful cursor.
        self.assertTrue(raw["has_more"])
        self.assertIsNotNone(raw["next_after"])
        delivered = {s["session_id"] for s in raw["sessions"]}
        self.assertLess(len(delivered), 30)
        cursor = raw["next_after"]
        seen = set(delivered)
        for _ in range(10):
            page = self.client("tech").sessions_page(
                after_registered_at=cursor["registered_at"],
                after_session_id=cursor["session_id"],
            )
            seen.update(s["session_id"] for s in page["sessions"])
            if not page["has_more"]:
                break
            cursor = page["next_after"]
        self.assertEqual(len(seen), 30)
        del rows

    def test_final_page_counts_complete_transport_envelope(self):
        """Ported reviewer probe #4: 30 sessions tuned to a bare 65,530B
        response — under the OLD bare-byte admission but over budget once
        the ok envelope is added. The repaired helper must trim so the
        actual wire frame fits, and the RPC must succeed."""
        rows, lengths = self._populate_tuned_sessions(
            30, 65_530, with_continuation=False
        )
        response = _fit_listing_response(
            {"sessions": rows, "has_more": False},
            "sessions",
            cursor_field_a="registered_at",
            cursor_field_b="session_id",
        )
        final_size = len(encode_frame({"ok": True, **response}))
        small, small_error = self.request_result({"op": "sessions", "limit": 1})
        self.assertIsNone(small_error)
        self.assertLessEqual(len(encode_frame(small)), MAX_FRAME_BYTES)
        result, error = self.request_result({"op": "sessions"})
        raw_size, raw_result = self.raw_wire_result({"op": "sessions"})
        self.assertEqual(raw_size, final_size)
        self.assertIsNone(error, "metadata and ok envelope added after byte admission")
        self.assertLessEqual(final_size, MAX_FRAME_BYTES)
        self.assertTrue(raw_result["ok"])

    def test_continuation_page_counts_cursor_before_byte_admission(self):
        """Ported reviewer probe #5: 100 sessions tuned to a bare 65,529B
        response with has_more=true — the cursor and ok envelope push the
        real frame over budget. The repaired helper must count them before
        admission and trim losslessly."""
        rows, lengths = self._populate_tuned_sessions(
            100, 65_529, with_continuation=True
        )
        self.client("tech").register_session(adapter="synthetic-boundary")
        response = _fit_listing_response(
            {"sessions": rows, "has_more": True},
            "sessions",
            cursor_field_a="registered_at",
            cursor_field_b="session_id",
        )
        final_size = len(encode_frame({"ok": True, **response}))
        small, small_error = self.request_result({"op": "sessions", "limit": 1})
        self.assertIsNone(small_error)
        self.assertLessEqual(len(encode_frame(small)), MAX_FRAME_BYTES)
        result, error = self.request_result({"op": "sessions"})
        raw_size, raw_result = self.raw_wire_result({"op": "sessions"})
        self.assertEqual(raw_size, final_size)
        self.assertIsNone(error, "cursor added after byte admission")
        self.assertLessEqual(final_size, MAX_FRAME_BYTES)
        self.assertTrue(raw_result["ok"])
        self.assertTrue(raw_result["has_more"])
        self.assertIsNotNone(raw_result["next_after"])
        del lengths


if __name__ == "__main__":  # pragma: no cover
    unittest.main()
