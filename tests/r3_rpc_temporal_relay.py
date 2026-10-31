"""R3 test relay: real writer contention injected at the admission seam.

Adapted from the independent reviewer's ``temporal_relay.py`` (Goblin/Codex,
WIRE-WAKE-01 R2 review evidence) into a permanent test asset. The REAL
``WireServer`` runs unmodified; a store subclass holds a genuine
``BEGIN IMMEDIATE`` write lock for a fixed interval when the second
connection of an armed ``claim``/``wake_ack`` operation opens — the exact
window between the expiry-preflight transaction and the admitting
transaction (T1). No SQL, clock, predicate or admission logic is replaced;
only scheduling is injected. The reviewer's original probe scheduled the
same contention against the frozen R2 candidate and observed three expired
admissions accepted over authenticated RPC.
"""

from __future__ import annotations

import asyncio
import json
import sys
import threading
from contextlib import contextmanager
from datetime import datetime, timezone
from pathlib import Path

sys.path.insert(0, str(Path(__file__).resolve().parents[1] / "src"))

from aivengers_wire.server import WireServer  # noqa: E402
from aivengers_wire.storage import WireStore  # noqa: E402

RECORDS: list[dict] = []


class ScheduledStore(WireStore):
    """WireStore that holds a real write lock during the admission window."""

    armed: str | None = None
    calls = 0
    hold_seconds = 1.4

    @contextmanager
    def connect(self):
        if self.armed is not None:
            self.calls += 1
        release = threading.Event()
        locked = threading.Event()
        errors: list[str] = []
        thread: threading.Thread | None = None
        timer: threading.Timer | None = None
        with super().connect() as c:
            if self.armed is not None and self.calls == 2:
                record = {
                    "op": self.armed,
                    "pre_begin_transaction": c.in_transaction,
                    "lock_start_utc": datetime.now(timezone.utc).isoformat(),
                }
                independent = WireStore(self.state_dir)

                def hold() -> None:
                    try:
                        with independent.connect() as other:
                            other.execute("BEGIN IMMEDIATE")
                            locked.set()
                            release.wait(4)
                    except Exception as exc:  # pragma: no cover - defensive
                        errors.append(str(exc))
                        locked.set()

                thread = threading.Thread(target=hold)
                thread.start()
                assert locked.wait(2) and not errors, errors
                timer = threading.Timer(self.hold_seconds, release.set)
                timer.start()
                RECORDS.append(record)
            try:
                yield c
            finally:
                release.set()
                if thread is not None:
                    thread.join(5)
                if timer is not None:
                    timer.cancel()
                assert not errors, errors


class ScheduledServer(WireServer):
    """WireServer that arms the contention window per claim/ack dispatch."""

    def _dispatch(self, request, **kwargs):
        if request.get("op") in ("claim", "wake_ack"):
            self.store.armed = request["op"]
            self.store.calls = 0
        try:
            return super()._dispatch(request, **kwargs)
        finally:
            self.store.armed = None
            Path(sys.argv[2]).write_text(json.dumps(RECORDS, indent=2))


async def run() -> None:
    store = ScheduledStore(Path(sys.argv[1]))
    if len(sys.argv) > 3:
        store.hold_seconds = float(sys.argv[3])
    server = ScheduledServer(store)
    await server.start()
    try:
        await asyncio.Future()
    finally:
        await server.close()


if __name__ == "__main__":
    asyncio.run(run())
