"""Regressions for WIRE-SECURITY-HARDENING-01.

A. Bounded admission of unauthenticated connections: a finite authentication
   deadline, global and per-peer caps, cleanup on every exit path, and no harm
   to authenticated connections (watch, post, heartbeat, seat attribution).
B. ``channels`` paging: stable name order, lossless keyset continuation,
   truthful ``has_more``, final-frame size accounting, and unchanged behavior
   for the unpaged request every installed adapter sends.
C. Lone UTF-16 surrogates are refused as ``invalid_request`` at the request
   decoder, so they can neither reach storage nor crash a handler.

Admission tests run the real ``WireServer`` on a real Unix socket in-process
with small limits, so they are deterministic and fast; the remaining tests use
the standard relay-subprocess fixture. No test touches a live relay.
"""

from __future__ import annotations

import asyncio
import json
import os
import socket
import subprocess
import sys
import tempfile
import threading
import time
import unittest
from pathlib import Path
from typing import Any, Callable

from aivengers_wire.client import WireClient
from aivengers_wire.errors import ProtocolError, WireError
from aivengers_wire.protocol import (
    MAX_CHANNELS_LIMIT,
    MAX_FRAME_BYTES,
    MAX_UNPAGED_CHANNELS_FRAME_BYTES,
    decode_frame,
    encode_frame,
)
from aivengers_wire.server import (
    AUTH_TIMEOUT_SECONDS,
    MAX_UNAUTHENTICATED_CONNECTIONS,
    MAX_UNAUTHENTICATED_PER_PEER,
    UNAUTH_EVICTION_GRACE_SECONDS,
    WireServer,
)
from aivengers_wire.storage import WireStore

from test_wire import RelayFixture


def open_fd_count() -> int:
    return len(os.listdir("/dev/fd"))


# ── A. unauthenticated admission ────────────────────────────────────────────


class AdmissionCase(unittest.IsolatedAsyncioTestCase):
    """Real WireServer, real Unix socket, one event loop with the test."""

    async def asyncSetUp(self) -> None:
        self._tmp = tempfile.TemporaryDirectory()
        self.store = WireStore(Path(self._tmp.name) / "state")
        self.store.initialize()
        self.store.add_seat("tech")
        self.store.add_seat("rookie")
        self.server: WireServer | None = None
        self._pairs: list[tuple[asyncio.StreamReader, asyncio.StreamWriter]] = []

    async def asyncTearDown(self) -> None:
        for _, writer in self._pairs:
            writer.close()
        if self.server is not None:
            await asyncio.wait_for(self.server.close(), 10)
        self._tmp.cleanup()

    async def start(self, **limits: Any) -> WireServer:
        self.server = WireServer(self.store, **limits)
        await self.server.start()
        return self.server

    async def connect(self) -> tuple[asyncio.StreamReader, asyncio.StreamWriter]:
        pair = await asyncio.open_unix_connection(str(self.store.socket_path))
        self._pairs.append(pair)
        return pair

    @staticmethod
    async def frame(reader: asyncio.StreamReader, timeout: float = 5.0) -> dict[str, Any] | None:
        raw = await asyncio.wait_for(reader.readline(), timeout)
        return json.loads(raw) if raw else None

    async def until(self, predicate: Callable[[], bool], timeout: float = 5.0) -> None:
        deadline = time.monotonic() + timeout
        while not predicate():
            if time.monotonic() > deadline:
                self.fail("condition not reached in time")
            await asyncio.sleep(0.01)

    def token(self, seat: str) -> str:
        return (self.store.seat_dir / f"{seat}.token").read_text(encoding="utf-8").strip()

    async def authenticate(self, seat: str = "tech") -> tuple[asyncio.StreamReader, asyncio.StreamWriter]:
        reader, writer = await self.connect()
        writer.write(encode_frame({"op": "auth", "seat": seat, "token": self.token(seat)}))
        reply = await self.frame(reader)
        self.assertEqual(reply, {"ok": True, "seat": seat})
        return reader, writer

    async def assertStaysOpen(self, reader: asyncio.StreamReader, seconds: float = 0.2) -> None:
        with self.assertRaises(asyncio.TimeoutError):
            await self.frame(reader, seconds)

    async def assertRefusedThenClosed(
        self, reader: asyncio.StreamReader, *, code: str, containing: str
    ) -> dict[str, Any]:
        reply = await self.frame(reader)
        self.assertIsNotNone(reply)
        self.assertFalse(reply["ok"])
        self.assertEqual(reply["error"], code)
        self.assertIn(containing, reply["message"])
        self.assertIsNone(await self.frame(reader), "connection must be closed after the refusal")
        return reply

    # deadline ---------------------------------------------------------------

    async def test_silent_connection_is_closed_at_the_deadline(self) -> None:
        server = await self.start(auth_timeout=0.3)
        reader, _ = await self.connect()
        started = time.monotonic()
        await self.assertRefusedThenClosed(
            reader, code="authentication_failed", containing="not completed within"
        )
        elapsed = time.monotonic() - started
        self.assertGreaterEqual(elapsed, 0.25, "must not be refused before the deadline")
        self.assertLess(elapsed, 4.0)
        await self.until(lambda: server.unauthenticated_connections == 0)
        self.assertEqual(server.auth_timeouts, 1)

    async def test_partial_frame_is_closed_at_the_deadline(self) -> None:
        server = await self.start(auth_timeout=0.3)
        reader, writer = await self.connect()
        writer.write(b'{"op":"auth","seat":"tech"')  # no token, no newline
        await self.assertRefusedThenClosed(
            reader, code="authentication_failed", containing="not completed within"
        )
        await self.until(lambda: server.unauthenticated_connections == 0)

    async def test_drip_feeding_bytes_cannot_extend_the_deadline(self) -> None:
        server = await self.start(auth_timeout=0.4)
        reader, writer = await self.connect()
        stop = asyncio.Event()

        async def drip() -> None:
            # One byte every 50 ms would keep a per-read idle timeout alive forever.
            while not stop.is_set() and not writer.is_closing():
                writer.write(b" ")
                await asyncio.sleep(0.05)

        task = asyncio.create_task(drip())
        started = time.monotonic()
        try:
            await self.assertRefusedThenClosed(
                reader, code="authentication_failed", containing="not completed within"
            )
        finally:
            stop.set()
            await task
        self.assertLess(time.monotonic() - started, 3.0)
        await self.until(lambda: server.unauthenticated_connections == 0)

    async def test_completing_authentication_in_time_is_unaffected(self) -> None:
        server = await self.start(auth_timeout=5.0)
        reader, writer = await self.authenticate("tech")
        self.assertEqual(server.unauthenticated_connections, 0)
        writer.write(encode_frame({"op": "ping"}))
        self.assertEqual(await self.frame(reader), {"ok": True, "status": "ready", "seat": "tech"})
        self.assertEqual(server.auth_timeouts, 0)

    # cleanup on every exit path --------------------------------------------

    async def test_malformed_first_frames_are_refused_and_release_their_slot(self) -> None:
        server = await self.start()
        token = self.token("tech")
        cases: list[tuple[str, bytes, str]] = [
            ("not json", b"not json\n", "invalid_request"),
            ("oversize", b"x" * (MAX_FRAME_BYTES + 10) + b"\n", "invalid_request"),
            ("invalid utf-8", b"\xff\xfe\n", "invalid_request"),
            ("not an object", b"[]\n", "invalid_request"),
            ("missing keys", b'{"op":"auth"}\n', "invalid_request"),
            ("extra keys", b'{"op":"auth","seat":"tech","token":"t","x":1}\n', "invalid_request"),
            ("nesting bomb", b"[" * 30_000 + b"\n", "invalid_request"),
            (
                "lone surrogate in seat",
                b'{"op":"auth","seat":"te\\ud800ch","token":"t"}\n',
                "invalid_request",
            ),
            ("first op is not auth", b'{"op":"post","seat":"tech","token":"t"}\n', "authentication_failed"),
            ("unknown seat", b'{"op":"auth","seat":"nobody","token":"t"}\n', "authentication_failed"),
            (
                "wrong token",
                json.dumps({"op": "auth", "seat": "tech", "token": token[::-1]}).encode() + b"\n",
                "authentication_failed",
            ),
        ]
        for label, raw, code in cases:
            with self.subTest(label):
                reader, writer = await self.connect()
                writer.write(raw)
                reply = await self.frame(reader)
                self.assertIsNotNone(reply)
                self.assertEqual((reply["ok"], reply["error"]), (False, code), reply)
                self.assertIsNone(await self.frame(reader))
        await self.until(lambda: server.unauthenticated_connections == 0)

    async def test_disconnects_before_authentication_release_slots_and_descriptors(self) -> None:
        server = await self.start()
        await self.until(lambda: server.unauthenticated_connections == 0)
        baseline = open_fd_count()
        for index in range(20):
            reader, writer = await self.connect()
            if index % 2:
                writer.write(b'{"op":"auth","seat":"tech"')  # partial frame, then vanish
                await writer.drain()
            writer.close()
        await self.until(lambda: server.unauthenticated_connections == 0)
        self._pairs.clear()
        await self.until(lambda: open_fd_count() <= baseline)
        self.assertEqual(server.admission_evictions + server.admission_refusals, 0)

    async def test_deadline_expiry_releases_descriptors(self) -> None:
        server = await self.start(auth_timeout=0.3)
        baseline = open_fd_count()
        pairs = [await self.connect() for _ in range(10)]
        for reader, _ in pairs:
            await self.assertRefusedThenClosed(
                reader, code="authentication_failed", containing="not completed within"
            )
        await self.until(lambda: server.unauthenticated_connections == 0)
        for _, writer in pairs:
            writer.close()
        self._pairs.clear()
        await self.until(lambda: open_fd_count() <= baseline)
        self.assertEqual(server.auth_timeouts, 10)

    async def test_shutdown_does_not_wait_for_stalled_unauthenticated_connections(self) -> None:
        server = await self.start(auth_timeout=60.0)
        pairs = [await self.connect() for _ in range(5)]
        await self.until(lambda: server.unauthenticated_connections == 5)
        await asyncio.wait_for(server.close(), 5.0)
        self.assertEqual(server.unauthenticated_connections, 0)
        for reader, _ in pairs:
            await self.assertRefusedThenClosed(
                reader, code="operation_failed", containing="shutting down"
            )
        self.server = None

    # caps -----------------------------------------------------------------

    async def test_a_full_pool_evicts_the_oldest_stalled_connection_for_a_legitimate_client(self) -> None:
        server = await self.start(
            auth_timeout=30.0,
            max_unauthenticated=4,
            max_unauthenticated_per_peer=100,
            eviction_grace=0.0,
        )
        stalled = []
        for _ in range(4):
            stalled.append(await self.connect())
            await self.until(lambda: server.unauthenticated_connections == len(stalled))
        # The legitimate client is a separate synchronous process-style client.
        reply = await asyncio.to_thread(WireClient(self.store, seat="tech").request, {"op": "ping"})
        self.assertEqual(reply["status"], "ready")
        self.assertEqual(server.admission_evictions, 1)
        self.assertEqual(server.admission_refusals, 0)
        await self.assertRefusedThenClosed(
            stalled[0][0], code="operation_failed", containing="limit of unauthenticated"
        )
        for reader, _ in stalled[1:]:
            await self.assertStaysOpen(reader)
        self.assertLessEqual(server.unauthenticated_connections, 4)

    async def test_young_connections_are_never_evicted_the_newcomer_is_refused(self) -> None:
        server = await self.start(
            auth_timeout=30.0,
            max_unauthenticated=4,
            max_unauthenticated_per_peer=100,
            eviction_grace=60.0,
        )
        young = []
        for _ in range(4):
            young.append(await self.connect())
            await self.until(lambda: server.unauthenticated_connections == len(young))
        reader, _ = await self.connect()
        await self.assertRefusedThenClosed(reader, code="operation_failed", containing="retry shortly")
        for kept, _ in young:
            await self.assertStaysOpen(kept)
        self.assertEqual((server.admission_evictions, server.admission_refusals), (0, 1))
        self.assertEqual(server.unauthenticated_connections, 4)

    async def test_one_peer_cannot_take_every_slot(self) -> None:
        server = await self.start(
            auth_timeout=30.0,
            max_unauthenticated=10,
            max_unauthenticated_per_peer=2,
            eviction_grace=0.0,
        )
        first = await self.connect()
        await self.until(lambda: server.unauthenticated_connections == 1)
        second = await self.connect()
        await self.until(lambda: server.unauthenticated_connections == 2)
        third = await self.connect()
        await self.assertRefusedThenClosed(
            first[0], code="operation_failed", containing="limit of unauthenticated"
        )
        await self.assertStaysOpen(second[0])
        await self.assertStaysOpen(third[0])
        self.assertEqual(server.unauthenticated_connections, 2)

    async def test_burst_of_legitimate_handshakes_is_never_evicted(self) -> None:
        """More simultaneous handshakes than one peer may hold: the excess is
        refused with a retryable error; none already in flight is killed."""
        server = await self.start(eviction_grace=60.0)
        # 70 simultaneous handshakes is above the per-peer limit (32) yet keeps
        # this process's descriptor use (two per connection) under macOS's 256.
        outcomes = await asyncio.gather(*(self._handshake() for _ in range(70)))
        succeeded = [o for o in outcomes if o == "ok"]
        refused = [o for o in outcomes if o == "refused"]
        self.assertEqual(len(succeeded) + len(refused), 70, outcomes)
        self.assertGreaterEqual(len(succeeded), MAX_UNAUTHENTICATED_PER_PEER)
        self.assertEqual(server.admission_evictions, 0)
        await self.until(lambda: server.unauthenticated_connections == 0)

    async def _handshake(self) -> str:
        reader, writer = await self.connect()
        writer.write(encode_frame({"op": "auth", "seat": "tech", "token": self.token("tech")}))
        reply = await self.frame(reader, 15.0)
        if reply == {"ok": True, "seat": "tech"}:
            return "ok"
        if reply and reply.get("error") == "operation_failed" and "retry shortly" in reply["message"]:
            return "refused"
        return f"unexpected: {reply!r}"

    def test_defaults_sit_below_common_descriptor_limits(self) -> None:
        self.assertEqual(AUTH_TIMEOUT_SECONDS, 10.0)
        self.assertLessEqual(MAX_UNAUTHENTICATED_PER_PEER, MAX_UNAUTHENTICATED_CONNECTIONS)
        self.assertLessEqual(MAX_UNAUTHENTICATED_CONNECTIONS, 64)  # macOS default soft limit is 256
        self.assertGreater(UNAUTH_EVICTION_GRACE_SECONDS, 0)

    def test_invalid_limits_are_rejected(self) -> None:
        store = WireStore(Path(self._tmp.name) / "unused")
        for kwargs in (
            {"auth_timeout": 0},
            {"auth_timeout": -1},
            {"max_unauthenticated": 0},
            {"max_unauthenticated_per_peer": 0},
            {"eviction_grace": -0.1},
        ):
            with self.subTest(**kwargs), self.assertRaises(ValueError):
                WireServer(store, **kwargs)

    # authenticated connections are untouched --------------------------------

    async def test_authenticated_idle_connections_outlive_the_deadline_and_watch_still_delivers(self) -> None:
        server = await self.start(auth_timeout=0.3, max_unauthenticated=4, eviction_grace=0.0)
        watch_reader, watch_writer = await self.authenticate("tech")
        watch_writer.write(encode_frame({"op": "watch", "channels": ["general"]}))
        subscribed = await self.frame(watch_reader)
        self.assertEqual(subscribed["event"], "subscribed")
        idle_reader, idle_writer = await self.authenticate("rookie")
        # Well past the authentication deadline, then flood the pending pool.
        await asyncio.sleep(0.9)
        flood = [await self.connect() for _ in range(12)]
        await self.until(lambda: server.admission_evictions + server.auth_timeouts >= 12)
        for _, writer in flood:
            writer.close()
        # The idle authenticated connection still works...
        idle_writer.write(encode_frame({"op": "ping"}))
        self.assertEqual((await self.frame(idle_reader))["status"], "ready")
        # ...and the watcher still receives events for a post made by another seat.
        posted = await asyncio.to_thread(
            WireClient(self.store, seat="rookie").request,
            {"op": "post", "channel": "general", "body": "hello after the deadline", "client_id": "sh-1"},
        )
        event = await self.frame(watch_reader)
        self.assertEqual(event["event"], "message")
        self.assertEqual(event["message"]["id"], posted["message"]["id"])

    async def test_seat_attribution_is_fixed_by_the_first_authentication(self) -> None:
        await self.start()
        reader, writer = await self.authenticate("rookie")
        writer.write(
            encode_frame({"op": "post", "channel": "general", "body": "who am i", "client_id": "sa-1"})
        )
        message = (await self.frame(reader))["message"]
        self.assertEqual(
            (message["transport_actor"], message["sender"], message["authority"], message["owner"]),
            ("rookie", "rookie", "peer", False),
        )
        self.assertEqual(message["peer"]["pid"], os.getpid())
        # A second auth frame cannot switch the seat; it is refused and the connection ends.
        writer.write(encode_frame({"op": "auth", "seat": "tech", "token": self.token("tech")}))
        reply = await self.frame(reader)
        self.assertEqual((reply["ok"], reply["error"]), (False, "invalid_request"))
        self.assertIsNone(await self.frame(reader))

    async def test_a_connection_evicted_while_its_auth_frame_is_in_flight_is_not_authenticated(self) -> None:
        class EvictedMidFlight(WireServer):
            authentications = 0

            async def _read_auth_request(self, reader: asyncio.StreamReader) -> dict[str, Any] | None:
                frame = await super()._read_auth_request(reader)
                # Another admission evicts this connection after its whole frame
                # arrived but before the handler acts on it.
                for writer in list(self._pending):
                    del self._pending[writer]
                    self._refuse(writer, "evicted for room")
                return frame

            def _authenticate_request(self, request: dict[str, Any]) -> Any:
                type(self).authentications += 1
                return super()._authenticate_request(request)

        self.server = EvictedMidFlight(self.store)
        await self.server.start()
        reader, writer = await self.connect()
        writer.write(encode_frame({"op": "auth", "seat": "tech", "token": self.token("tech")}))
        await self.assertRefusedThenClosed(reader, code="operation_failed", containing="evicted")
        self.assertEqual(EvictedMidFlight.authentications, 0, "an evicted connection must not authenticate")


class RelayAdmissionTests(RelayFixture):
    """The shipped relay process with its default limits."""

    def test_a_stalled_flood_from_one_process_does_not_block_another_process(self) -> None:
        flood: list[socket.socket] = []
        try:
            for _ in range(MAX_UNAUTHENTICATED_PER_PEER + 20):
                sock = socket.socket(socket.AF_UNIX, socket.SOCK_STREAM)
                sock.connect(str(self.store.socket_path))
                sock.setblocking(False)
                flood.append(sock)
            # Within the per-peer limit the connections just wait; each excess
            # one is refused at once with a retryable error frame, then closed.
            refused: set[int] = set()
            deadline = time.monotonic() + 10
            while len(refused) < 20 and time.monotonic() < deadline:
                for index, sock in enumerate(flood):
                    if index in refused:
                        continue
                    try:
                        data = sock.recv(4096)
                    except BlockingIOError:
                        continue
                    frame = json.loads(data.split(b"\n")[0])
                    self.assertEqual(frame["error"], "operation_failed")
                    self.assertIn("retry shortly", frame["message"])
                    refused.add(index)
                time.sleep(0.02)
            self.assertEqual(len(refused), 20)
            self.assertTrue(all(index >= MAX_UNAUTHENTICATED_PER_PEER for index in refused))
            # Another process (the CLI) is a different peer and is served.
            result = self.wire("ping", "--seat", "tech")
            self.assertEqual(result.returncode, 0, result.stderr)
            self.assertIn("ready as tech", result.stdout)
        finally:
            for sock in flood:
                sock.close()
        # Slots are released once the stalled peers disconnect.
        deadline = time.monotonic() + 5
        while True:
            try:
                self.assertEqual(self.client("tech").request({"op": "ping"})["status"], "ready")
                break
            except WireError:
                if time.monotonic() > deadline:
                    raise
                time.sleep(0.05)


# ── B. channels paging ──────────────────────────────────────────────────────


def seed_channels(store: WireStore, names: list[str], *, first_id: int = 1) -> None:
    """One message per channel, in a single transaction (test seeding only)."""
    rows = [
        (first_id + index, f"{first_id + index:032x}", name)
        for index, name in enumerate(names)
    ]
    with store.connect() as conn:
        conn.executemany(
            """
            INSERT INTO messages(
                id, message_uuid, channel, logical_sender, transport_actor, transport,
                provenance, authority, owner, body, reply_to, client_id, created_at,
                peer_pid, peer_uid, peer_gid
            ) VALUES (?, ?, ?, 'tech', 'tech', 'local-unix', 'direct', 'peer', 0, 'x',
                      NULL, NULL, '2026-01-01T00:00:00.000000Z', 1, 1, 1)
            """,
            rows,
        )


def long_names(count: int, *, start: int = 0) -> list[str]:
    return [f"c{index:06d}".ljust(64, "x") for index in range(start, start + count)]


class ChannelPagingTests(RelayFixture):
    def raw_line(self, payload: dict[str, Any]) -> bytes:
        token = (self.store.seat_dir / "tech.token").read_text(encoding="utf-8").strip()
        sock = socket.socket(socket.AF_UNIX, socket.SOCK_STREAM)
        try:
            sock.connect(str(self.store.socket_path))
            stream = sock.makefile("rwb", buffering=0)
            stream.write(encode_frame({"op": "auth", "seat": "tech", "token": token}))
            stream.readline()
            stream.write(encode_frame(payload))
            return stream.readline()
        finally:
            sock.close()

    def page(self, **fields: Any) -> dict[str, Any]:
        return json.loads(self.raw_line({"op": "channels", **fields}))

    def walk(self, limit: int) -> tuple[list[str], list[dict[str, Any]]]:
        names: list[str] = []
        pages: list[dict[str, Any]] = []
        after = None
        for _ in range(5_000):  # a stuck cursor must fail the test, not hang it
            fields: dict[str, Any] = {"limit": limit}
            if after is not None:
                fields["after"] = after
            page = self.page(**fields)
            self.assertTrue(page["ok"], page)
            pages.append(page)
            names.extend(row["channel"] for row in page["channels"])
            if not page["has_more"]:
                self.assertIsNone(page["next_after"])
                return names, pages
            self.assertEqual(page["next_after"], page["channels"][-1]["channel"])
            after = page["next_after"]
        self.fail("channel listing did not terminate")

    def test_paged_walk_is_stable_lossless_and_ordered(self) -> None:
        names = [f"c{index:04d}" for index in range(1, 1_001)] + ["a", "a.b", "a-b", "a_b", "z9", "0"]
        seed_channels(self.store, names)
        expected = sorted(names)
        for limit in (1, 7, 200, MAX_CHANNELS_LIMIT):
            with self.subTest(limit=limit):
                got, pages = self.walk(limit)
                self.assertEqual(got, expected)
                self.assertTrue(all(len(page["channels"]) <= limit for page in pages))
                self.assertEqual(sum(page["has_more"] for page in pages), len(pages) - 1)

    def test_exact_multiple_of_the_limit_ends_without_a_phantom_page(self) -> None:
        seed_channels(self.store, [f"c{index:02d}" for index in range(10)])
        got, pages = self.walk(5)
        self.assertEqual(len(got), 10)
        self.assertEqual([page["has_more"] for page in pages], [True, False])
        self.assertEqual(len(pages[1]["channels"]), 5)

    def test_continuation_from_a_name_that_is_not_a_channel_is_exclusive(self) -> None:
        seed_channels(self.store, ["b", "d", "f"])
        self.assertEqual([r["channel"] for r in self.page(limit=10, after="c")["channels"]], ["d", "f"])
        self.assertEqual([r["channel"] for r in self.page(limit=10, after="d")["channels"]], ["f"])
        empty = self.page(limit=10, after="f")
        self.assertEqual((empty["channels"], empty["has_more"], empty["next_after"]), ([], False, None))

    def test_new_channels_appear_without_duplicating_or_skipping_existing_ones(self) -> None:
        seed_channels(self.store, ["c1", "c3", "c5", "c7"])
        first = self.page(limit=2)
        self.assertEqual([r["channel"] for r in first["channels"]], ["c1", "c3"])
        seed_channels(self.store, ["c4", "c6", "c0"], first_id=100)
        rest = self.page(limit=10, after=first["next_after"])
        # c4 and c6 sort after the cursor and arrive once; c0 sorts before it and
        # belongs to the next fresh walk. Nothing that existed throughout is lost.
        self.assertEqual([r["channel"] for r in rest["channels"]], ["c4", "c5", "c6", "c7"])

    def test_pages_are_trimmed_to_the_complete_frame_and_stay_lossless(self) -> None:
        # Large ids make rows long enough that a maximal page exceeds the frame.
        names = long_names(700)
        seed_channels(self.store, names, first_id=9_000_000_000_000_000)
        got, pages = self.walk(MAX_CHANNELS_LIMIT)
        self.assertEqual(got, names)
        self.assertTrue(
            any(len(page["channels"]) < MAX_CHANNELS_LIMIT and page["has_more"] for page in pages[:-1]),
            "expected at least one page to be trimmed by bytes",
        )
        for page in pages:
            self.assertLessEqual(len(encode_frame(page)), MAX_FRAME_BYTES)

    def test_unpaged_request_is_the_complete_listing_with_the_historical_shape(self) -> None:
        seed_channels(self.store, [f"c{index:03d}" for index in range(300)])
        reply = json.loads(self.raw_line({"op": "channels"}))
        self.assertTrue(reply["ok"])
        self.assertEqual([r["channel"] for r in reply["channels"]], [f"c{index:03d}" for index in range(300)])
        self.assertEqual(set(reply["channels"][0]), {"channel", "message_count", "latest_message_id"})
        self.assertEqual((reply["has_more"], reply["next_after"]), (False, None))
        # The paged walk returns exactly the same rows.
        paged = [row for page in self.walk(50)[1] for row in page["channels"]]
        self.assertEqual(paged, reply["channels"])

    def test_empty_inventory_is_a_complete_empty_listing_either_way(self) -> None:
        unpaged = json.loads(self.raw_line({"op": "channels"}))
        self.assertEqual((unpaged["channels"], unpaged["has_more"]), ([], False))
        paged = self.page(limit=10)
        self.assertEqual((paged["channels"], paged["has_more"], paged["next_after"]), ([], False, None))

    def test_oversized_inventory_is_refused_unpaged_never_truncated_and_stays_pageable(self) -> None:
        names = long_names(8_000)
        seed_channels(self.store, names, first_id=9_000_000_000_000_000)
        raw = self.raw_line({"op": "channels"})
        reply = json.loads(raw)
        self.assertLess(len(raw), 1_024, "the refusal must be small, not a giant frame")
        self.assertEqual((reply["ok"], reply["error"]), (False, "operation_failed"))
        self.assertIn("pages", reply["message"])
        self.assertNotIn("channels", reply)
        self.assertEqual(self.walk(MAX_CHANNELS_LIMIT)[0], names)
        self.assertEqual([c["channel"] for c in self.client("tech").list_channels()], names)

    def test_a_vast_inventory_is_refused_without_building_it(self) -> None:
        most = MAX_UNPAGED_CHANNELS_FRAME_BYTES // 50
        seed_channels(self.store, [f"c{index:06d}" for index in range(most + 500)])
        started = time.monotonic()
        reply = json.loads(self.raw_line({"op": "channels"}))
        self.assertEqual((reply["ok"], reply["error"]), (False, "operation_failed"))
        self.assertLess(time.monotonic() - started, 10)
        self.assertEqual(len(self.walk(MAX_CHANNELS_LIMIT)[0]), most + 500)

    def test_invalid_paging_requests_are_clear_refusals(self) -> None:
        seed_channels(self.store, ["a"])
        bad: list[dict[str, Any]] = [
            {"limit": 0},
            {"limit": -1},
            {"limit": True},
            {"limit": "5"},
            {"limit": 1.5},
            {"limit": MAX_CHANNELS_LIMIT + 1},
            {"limit": None},
            {"after": "a"},  # after without limit
            {"limit": 5, "after": "UPPER"},
            {"limit": 5, "after": ""},
            {"limit": 5, "after": 3},
            {"limit": 5, "after": "x" * 65},
            {"limit": 5, "offset": 1},
        ]
        for fields in bad:
            with self.subTest(fields=fields):
                reply = self.page(**fields)
                self.assertEqual((reply["ok"], reply["error"]), (False, "invalid_request"), reply)
        self.assertTrue(self.page(limit=5, after=None)["ok"])

    def test_relay_stays_healthy_after_paging_refusals(self) -> None:
        self.page(limit=0)
        self.assertEqual(self.client("tech").request({"op": "ping"})["status"], "ready")

    # consumers ------------------------------------------------------------

    def test_cli_lists_every_channel_beyond_one_page(self) -> None:
        names = [f"c{index:04d}" for index in range(450)]
        seed_channels(self.store, names)
        plain = self.wire("channels", "--seat", "tech")
        self.assertEqual(plain.returncode, 0, plain.stderr)
        self.assertEqual(
            [line.split(":")[0] for line in plain.stdout.splitlines()], [f"#{n}" for n in names]
        )
        as_json = self.wire("--json", "channels", "--seat", "tech")
        self.assertEqual(as_json.returncode, 0, as_json.stderr)
        payload = json.loads(as_json.stdout)
        self.assertEqual(set(payload), {"ok", "channels"})
        self.assertEqual([row["channel"] for row in payload["channels"]], names)

    def test_clubhouse_service_discovers_channels_beyond_one_page(self) -> None:
        from aivengers_wire.clubhouse import DEFAULT_CHANNELS, ClubhouseService

        names = [f"c{index:04d}" for index in range(450)]
        seed_channels(self.store, names)
        service = ClubhouseService(self.store, seat="tech")
        found = service.channels()
        self.assertEqual(found[: len(DEFAULT_CHANNELS)], list(DEFAULT_CHANNELS))
        self.assertTrue(set(names) <= set(found))


class FakeRelay:
    """A scripted relay speaking just enough protocol for client-side tests."""

    def __init__(self, directory: Path, script: Callable[[dict[str, Any]], dict[str, Any]]):
        self.store = WireStore(directory)
        self.store.seat_dir.mkdir(parents=True)
        (self.store.seat_dir / "tech.token").write_text("synthetic-token\n", encoding="utf-8")
        self.requests: list[dict[str, Any]] = []
        self._script = script
        self._sock = socket.socket(socket.AF_UNIX, socket.SOCK_STREAM)
        self._sock.bind(str(self.store.socket_path))
        self._sock.listen(8)
        self._sock.settimeout(0.2)
        self._stop = threading.Event()
        self._thread = threading.Thread(target=self._serve, daemon=True)
        self._thread.start()

    def _serve(self) -> None:
        while not self._stop.is_set():
            try:
                conn, _ = self._sock.accept()
            except (socket.timeout, OSError):
                continue
            with conn, conn.makefile("rwb", buffering=0) as stream:
                try:
                    stream.readline()
                    stream.write(encode_frame({"ok": True, "seat": "tech"}))
                    request = json.loads(stream.readline())
                    self.requests.append(request)
                    stream.write(encode_frame(self._script(request)))
                except (OSError, ValueError):
                    continue

    def close(self) -> None:
        self._stop.set()
        self._thread.join(timeout=2)
        self._sock.close()


class ChannelClientCompatibilityTests(unittest.TestCase):
    def setUp(self) -> None:
        self._tmp = tempfile.TemporaryDirectory()
        self.addCleanup(self._tmp.cleanup)

    def relay(self, script: Callable[[dict[str, Any]], dict[str, Any]]) -> FakeRelay:
        relay = FakeRelay(Path(self._tmp.name) / "fake", script)
        self.addCleanup(relay.close)
        return relay

    @staticmethod
    def rows(*names: str) -> list[dict[str, Any]]:
        return [{"channel": n, "message_count": 1, "latest_message_id": 1} for n in names]

    def test_a_relay_that_predates_paging_gets_the_unpaged_request_it_understands(self) -> None:
        def old_relay(request: dict[str, Any]) -> dict[str, Any]:
            if set(request) != {"op"}:
                return {"ok": False, "error": "invalid_request", "message": "unknown fields: limit"}
            return {"ok": True, "channels": self.rows("a", "b", "c")}

        relay = self.relay(old_relay)
        got = WireClient(relay.store, seat="tech").list_channels()
        self.assertEqual([r["channel"] for r in got], ["a", "b", "c"])
        self.assertEqual(relay.requests, [{"op": "channels", "limit": 200}, {"op": "channels"}])

    def test_other_invalid_requests_are_not_mistaken_for_an_old_relay(self) -> None:
        relay = self.relay(
            lambda request: {"ok": False, "error": "invalid_request", "message": "limit must not exceed 5"}
        )
        with self.assertRaises(WireError) as caught:
            WireClient(relay.store, seat="tech").list_channels()
        self.assertEqual(caught.exception.code, "invalid_request")
        self.assertEqual(len(relay.requests), 1)

    def test_a_cursor_that_does_not_advance_raises_instead_of_looping(self) -> None:
        relay = self.relay(
            lambda request: {"ok": True, "channels": self.rows("a"), "has_more": True, "next_after": "a"}
        )
        with self.assertRaisesRegex(WireError, "did not advance"):
            WireClient(relay.store, seat="tech").list_channels()
        self.assertEqual(len(relay.requests), 2)

    def test_has_more_without_a_cursor_raises(self) -> None:
        relay = self.relay(
            lambda request: {"ok": True, "channels": self.rows("a"), "has_more": True, "next_after": None}
        )
        with self.assertRaisesRegex(WireError, "did not advance"):
            WireClient(relay.store, seat="tech").list_channels()

    def test_pages_are_followed_with_the_previous_last_name(self) -> None:
        def paged(request: dict[str, Any]) -> dict[str, Any]:
            names = ["a", "b", "c", "d", "e"]
            after = request.get("after")
            rest = [n for n in names if after is None or n > after]
            page = rest[:2]
            more = len(rest) > 2
            return {
                "ok": True,
                "channels": self.rows(*page),
                "has_more": more,
                "next_after": page[-1] if more else None,
            }

        relay = self.relay(paged)
        got = WireClient(relay.store, seat="tech").list_channels(page_size=2)
        self.assertEqual([r["channel"] for r in got], ["a", "b", "c", "d", "e"])
        self.assertEqual([r.get("after") for r in relay.requests], [None, "b", "d"])


# ── C. lone surrogates ──────────────────────────────────────────────────────


class DecodeFrameUnicodeTests(unittest.TestCase):
    def decode(self, text: str) -> dict[str, Any]:
        return decode_frame(text.encode("utf-8"))

    def test_lone_surrogates_are_refused_wherever_they_appear(self) -> None:
        for label, text in {
            "string value": r'{"op":"post","body":"a\ud800b"}',
            "low surrogate alone": r'{"op":"post","body":"\udc00"}',
            "object key": r'{"op":"post","b\ud800":"x"}',
            "nested object": r'{"op":"x","meta":{"deep":{"k":"\ud83d"}}}',
            "list element": r'{"op":"x","items":["fine",["\ud800"]]}',
            "reversed pair": r'{"body":"\ude00\ud83d"}',
            "high then ascii": r'{"body":"\ud83dz"}',
            "high then high": r'{"body":"\ud83d\ud83d"}',
        }.items():
            with self.subTest(label), self.assertRaisesRegex(ProtocolError, "unpaired UTF-16 surrogate"):
                self.decode(text)

    def test_the_refusal_does_not_echo_the_offending_text(self) -> None:
        with self.assertRaises(ProtocolError) as caught:
            self.decode(r'{"body":"SECRET-MARKER\ud800"}')
        self.assertNotIn("SECRET-MARKER", str(caught.exception))

    def test_valid_unicode_is_accepted_and_round_trips(self) -> None:
        text = r'{"body":"h\u00e9llo \ud83d\ude00 \u4e2d\u6587 😀 \\ud800 \ud83d\ude00"}'
        value = self.decode(text)
        self.assertEqual(value["body"], "héllo 😀 中文 😀 \\ud800 😀")
        # The escaped backslash form spells six literal characters, not a surrogate.
        self.assertEqual(self.decode(r'{"body":"\\ud800"}')["body"], "\\ud800")
        encode_frame(value)  # and it can be encoded again

    def test_raw_utf8_encoded_surrogates_remain_invalid_utf8(self) -> None:
        with self.assertRaisesRegex(ProtocolError, "not valid UTF-8"):
            decode_frame(b'{"body":"\xed\xa0\x80"}')

    def test_deeply_nested_requests_are_a_clear_refusal_not_a_crash(self) -> None:
        # 30,000 levels fit a 64 KiB frame and exceed the parser's recursion
        # limit on the supported interpreters (3.12, 3.13). Whatever a future
        # interpreter allows, nothing but a ProtocolError may escape.
        try:
            decode_frame(b'{"x":' + b"[" * 30_000 + b"]" * 30_000 + b"}")
        except ProtocolError as exc:
            self.assertIn("nested too deeply", str(exc))

    def test_moderate_nesting_still_decodes(self) -> None:
        value = decode_frame(b'{"x":' + b"[" * 50 + b"]" * 50 + b"}")
        self.assertIn("x", value)

    def test_scan_is_linear_enough_for_a_maximal_frame(self) -> None:
        body = "é" * 30_000
        started = time.monotonic()
        decode_frame(json.dumps({"body": body}, ensure_ascii=False).encode("utf-8"))
        self.assertLess(time.monotonic() - started, 1.0)


class LoneSurrogateRelayTests(RelayFixture):
    def reply(self, line: bytes) -> dict[str, Any]:
        return self.raw_exchange(line + b"\n")

    def messages(self) -> list[dict[str, Any]]:
        return self.client("tech").read_messages(channel=None, after=0, limit=200)["messages"]

    def test_every_text_field_with_a_lone_surrogate_is_an_invalid_request_and_stores_nothing(self) -> None:
        requests = {
            "post body": b'{"op":"post","channel":"general","body":"a\\ud800b"}',
            "post client_id": b'{"op":"post","channel":"general","body":"ok","client_id":"x\\udc00"}',
            "post channel": b'{"op":"post","channel":"gen\\ud800","body":"ok"}',
            "post forwarded_for": b'{"op":"post","channel":"general","body":"ok","forwarded_for":"s\\ud800"}',
            "read channel": b'{"op":"read","channel":"g\\ud800","after":0,"limit":1}',
            "wake note": b'{"op":"wake","target_seat":"rookie","task_ref":"t","note":"n\\ud800"}',
            "register adapter": b'{"op":"register","adapter":"a\\ud800"}',
            "register nested meta": b'{"op":"register","adapter":"a","meta":{"k":"\\ud800"}}',
            "surrogate in an unknown key": b'{"op":"ping","k\\ud800":1}',
            "channels after": b'{"op":"channels","limit":5,"after":"a\\ud800"}',
        }
        for label, line in requests.items():
            with self.subTest(label):
                reply = self.reply(line)
                self.assertEqual((reply["ok"], reply["error"]), (False, "invalid_request"), reply)
                self.assertIn("surrogate", reply["message"])
        self.assertEqual(self.messages(), [])
        self.assertIsNone(self.server.poll(), "relay must survive")
        self.assertEqual(self.client("tech").request({"op": "ping"})["status"], "ready")

    def test_the_authentication_frame_is_screened_too(self) -> None:
        sock = socket.socket(socket.AF_UNIX, socket.SOCK_STREAM)
        try:
            sock.connect(str(self.store.socket_path))
            stream = sock.makefile("rwb", buffering=0)
            stream.write(b'{"op":"auth","seat":"tech","token":"\\ud800"}\n')
            reply = json.loads(stream.readline())
        finally:
            sock.close()
        self.assertEqual((reply["ok"], reply["error"]), (False, "invalid_request"))

    def test_retrying_a_refused_post_never_duplicates_and_a_corrected_retry_stores_once(self) -> None:
        bad = b'{"op":"post","channel":"general","body":"a\\ud800","client_id":"retry-1"}'
        good = b'{"op":"post","channel":"general","body":"a","client_id":"retry-1"}'
        for _ in range(3):
            self.assertEqual(self.reply(bad)["error"], "invalid_request")
        self.assertEqual(self.messages(), [])
        first = self.reply(good)
        self.assertTrue(first["ok"])
        self.assertFalse(first["message"]["duplicate"])
        again = self.reply(good)
        self.assertTrue(again["message"]["duplicate"])
        self.assertEqual(again["message"]["id"], first["message"]["id"])
        self.assertEqual(len(self.messages()), 1)

    def test_valid_astral_and_non_latin_text_is_stored_and_read_back_intact(self) -> None:
        line = b'{"op":"post","channel":"general","body":"h\\u00e9llo \\ud83d\\ude00 \\u4e2d\\u6587","client_id":"u-1"}'
        stored = self.reply(line)["message"]
        self.assertEqual(stored["body"], "héllo 😀 中文")
        self.assertEqual(self.messages()[0]["body"], "héllo 😀 中文")
        listing = self.client("tech").read_messages(channel="general", after=0, limit=5)
        self.assertEqual(listing["messages"][0]["body"], "héllo 😀 中文")

    def test_nesting_bomb_is_an_invalid_request_and_the_handler_survives(self) -> None:
        reply = self.reply(b'{"op":"ping","x":' + b"[" * 20_000 + b"]" * 20_000 + b"}")
        self.assertEqual((reply["ok"], reply["error"]), (False, "invalid_request"))
        self.assertEqual(self.client("tech").request({"op": "ping"})["status"], "ready")

    def test_refusals_leave_no_traceback_in_the_relay_log(self) -> None:
        self.reply(b'{"op":"post","channel":"general","body":"a\\ud800b"}')
        self.reply(b'{"op":"ping","x":' + b"[" * 20_000 + b"]" * 20_000 + b"}")
        self.server.terminate()
        self.server.wait(timeout=10)
        log = self.server.stderr.read() if self.server.stderr else ""
        self.assertNotIn("Traceback", log)
        self.assertNotIn("internal_error", log)

    def run_cli(self, *args: str | bytes, stdin: bytes | None = None) -> subprocess.CompletedProcess[bytes]:
        project_root = Path(__file__).resolve().parents[1]
        env = os.environ.copy()
        env["PYTHONPATH"] = str(project_root / "src")
        env["PYTHONIOENCODING"] = "utf-8"
        env.pop("AIVENGERS_WIRE_SEAT", None)
        return subprocess.run(
            [sys.executable, "-m", "aivengers_wire", "--state-dir", str(self.state_dir), *args],
            cwd=project_root,
            env=env,
            input=stdin,
            stdin=None if stdin is not None else subprocess.DEVNULL,
            capture_output=True,
            timeout=30,
        )

    def test_cli_refuses_non_utf8_arguments_cleanly_before_sending(self) -> None:
        result = self.run_cli("post", "--seat", "tech", "general", b"a\xffb")
        self.assertEqual(result.returncode, 2, result.stderr)
        self.assertNotIn(b"Traceback", result.stderr)
        self.assertIn(b"nothing was sent", result.stderr)
        self.assertEqual(self.messages(), [])

    def test_cli_refuses_non_utf8_stdin_cleanly(self) -> None:
        result = self.run_cli("post", "--seat", "tech", "general", stdin=b"a\xffb")
        self.assertEqual(result.returncode, 2, result.stderr)
        self.assertNotIn(b"Traceback", result.stderr)
        self.assertIn(b"nothing was sent", result.stderr)
        self.assertEqual(self.messages(), [])


if __name__ == "__main__":
    unittest.main()
