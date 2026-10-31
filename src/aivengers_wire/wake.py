"""Session registry, wake queue and task ledger for AIvengers Wire (W0).

W0 scope: fake sessions and disposable state only. This module adds the
smallest durable foundation for agent wake:

- ``wire_sessions``  — registered runtime sessions owned by an authenticated
  seat. A session is a specific adapter instance (a GUI runtime, a coding
  agent's background session, a gateway relay, a test fake), never a model
  claim and never an OS process id.
- ``wire_tasks``     — owner-granted work items. Rows can only be created
  locally by the operator (``wire task add``) or by test fixtures; there is
  deliberately NO socket operation that creates tasks, so a peer message can
  never mint owner authority.
- ``wire_wakes``     — durable wake intents referencing an existing task.
  A wake is a notification with a target, not an authorization and not a
  resume. The state machine records exactly what happened and nothing more.

State-machine honesty contract (mirrors SECURITY.md and the Chapter 06
baseline "notification is not cognitive activation"), REVISED per owner
dispatch after the Goblin R2 review:

    pending -> delivered -> claimed -> completed | failed
    pending/delivered -> expired | cancelled | ambiguous

``pending`` means the durable intent exists and NO adapter has receipted it.
A successful socket frame write is a TRANSMISSION ATTEMPT (attempts counter)
and never a state transition: a frame handed to a socket buffer does not
prove any receiver read it, so it must not durably mark delivery. Unacked
wakes stay pending and are replayed on every eligible watch subscription;
redelivery is safe because task claims are exactly-once.
``delivered`` means an authenticated, live, capable session of the target
seat acknowledged the intent (``wake_ack``). It is an adapter receipt —
NOT a model resume and NOT task progress. There is deliberately no
``resumed`` state: only platform evidence could mint that, and no platform
offers it yet. ``claimed`` means one session atomically won the task.
Every other outcome is recorded as exactly what it is: expired, cancelled,
ambiguous, failed. Unknown stays unknown; offline stays offline;
unsupported stays unsupported.

Schema policy: the three tables are added with ``CREATE TABLE IF NOT EXISTS``
and ``SCHEMA_VERSION`` is deliberately left at 1 for W0. Old relay code
ignores unknown tables; new code creates them on first run; rolling the
daemon back to pre-wake code keeps working. A real release must decide the
version/migration story explicitly (see the W0 report) — silently bumping
the version here would strand a rollback.
"""

from __future__ import annotations

import json
import sqlite3
import uuid
from datetime import datetime, timedelta, timezone
from typing import TYPE_CHECKING, Any

from .errors import WireError
from .protocol import validate_name

if TYPE_CHECKING:  # pragma: no cover - annotations only, avoids a circular import
    from .storage import Seat

#: Lease bounds. A registration without an explicit lease gets the default;
#: heartbeats renew it. Leases bound how long a session may claim work after
#: its last sign of life, which is what defeats a delayed wake.
DEFAULT_LEASE_SECONDS = 300
MAX_LEASE_SECONDS = 86_400

#: Wake intent time-to-live bounds. An unclaimed wake older than its TTL is
#: marked expired (lazily, on access) instead of lurking forever.
MAX_WAKE_TTL_SECONDS = 86_400

#: R8: wake_list pagination bound. Worst-case serialized wake rows observed
#: during review were ~410 bytes; 100 rows ≈ 41 KB, safely under the 65,536-
#: byte transport frame limit including envelope overhead.
MAX_WAKE_LIST_LIMIT = 100

#: R2: maximum pending wakes offered to one watch subscription per catch-up
#: cycle. Anything beyond stays pending for the next cycle; replay is safe
#: because claims are exactly-once and ack is the durable receipt.
PENDING_REPLAY_BATCH = 32

#: Advisory metadata only. Never an identity, never an authority input.
MAX_MODEL_CLAIM_CHARS = 128
MAX_TASK_REF_CHARS = 128
MAX_NOTE_CHARS = 2_048
MAX_META_CHARS = 2_048

#: Session lifecycle states. ``ready``/``busy`` are live states; the rest are
#: recorded-but-not-live. Liveness additionally requires a valid lease.
SESSION_STATES = ("ready", "busy", "draining", "offline", "expired", "revoked")
LIVE_SESSION_STATES = ("ready", "busy")

#: Wake pipeline states, in honest order. R2 revision: ``delivered`` is the
#: adapter-receipt state (set only by ``wake_ack``); the former separate
#: ``acknowledged`` state was redundant with it and is retired. Frame writes
#: are attempts, never transitions.
WAKE_STATES = (
    "pending",
    "delivered",
    "claimed",
    "completed",
    "failed",
    "expired",
    "cancelled",
    "ambiguous",
)

#: States from which a claim may proceed. Claiming directly from ``pending``
#: is the poll catch-up path (the adapter learned of the wake via wake_list);
#: the transition to ``claimed`` itself records receipt of the intent.
CLAIMABLE_WAKE_STATES = ("pending", "delivered")

#: Task states. Only the operator (local CLI) or a test fixture creates rows,
#: and only with ``owner_granted = 1`` — the CHECK constraint makes a
#: peer-minted task structurally impossible through this layer.
TASK_STATES = ("open", "claimed", "completed", "failed", "cancelled", "revoked")

DEFAULT_CAPABILITIES = {"notify": True, "wake_ack": True, "claim": True}

WAKE_SCHEMA_SQL = """
CREATE TABLE IF NOT EXISTS wire_sessions (
    session_id TEXT PRIMARY KEY,
    seat TEXT NOT NULL REFERENCES seats(name),
    adapter TEXT NOT NULL,
    platform_pid INTEGER,
    model_claim TEXT,
    model_evidence TEXT NOT NULL DEFAULT 'unverified'
        CHECK (model_evidence IN ('unverified', 'adapter-attested', 'operator-attested')),
    state TEXT NOT NULL CHECK (state IN ('ready', 'busy', 'draining', 'offline', 'expired', 'revoked')),
    capabilities TEXT NOT NULL,
    registered_at TEXT NOT NULL,
    last_heartbeat TEXT NOT NULL,
    lease_seconds INTEGER NOT NULL CHECK (lease_seconds > 0),
    lease_expires_at TEXT,
    generation INTEGER NOT NULL DEFAULT 1 CHECK (generation >= 1),
    meta TEXT
);

CREATE TABLE IF NOT EXISTS wire_tasks (
    task_id TEXT PRIMARY KEY,
    title TEXT NOT NULL,
    scope TEXT,
    owner_granted INTEGER NOT NULL CHECK (owner_granted = 1),
    created_by TEXT NOT NULL,
    created_at TEXT NOT NULL,
    state TEXT NOT NULL CHECK (state IN ('open', 'claimed', 'completed', 'failed', 'cancelled', 'revoked')),
    claimed_by_session TEXT REFERENCES wire_sessions(session_id),
    claimed_at TEXT,
    completed_at TEXT,
    outcome_note TEXT
);

CREATE TABLE IF NOT EXISTS wire_wakes (
    wake_id TEXT PRIMARY KEY,
    target_seat TEXT NOT NULL REFERENCES seats(name),
    target_session_id TEXT REFERENCES wire_sessions(session_id),
    task_ref TEXT NOT NULL REFERENCES wire_tasks(task_id),
    requested_by TEXT NOT NULL REFERENCES seats(name),
    idempotency_key TEXT,
    priority INTEGER NOT NULL DEFAULT 5 CHECK (priority BETWEEN 1 AND 9),
    ttl_expires_at TEXT,
    state TEXT NOT NULL CHECK (state IN (
        'pending', 'delivered', 'claimed',
        'completed', 'failed', 'expired', 'cancelled', 'ambiguous')),
    created_at TEXT NOT NULL,
    delivered_at TEXT,
    acknowledged_at TEXT,
    claimed_at TEXT,
    claimed_by_session TEXT,
    attempts INTEGER NOT NULL DEFAULT 0 CHECK (attempts >= 0),
    last_error TEXT,
    UNIQUE (requested_by, idempotency_key)
);

CREATE INDEX IF NOT EXISTS idx_wire_sessions_seat ON wire_sessions(seat);
CREATE INDEX IF NOT EXISTS idx_wire_wakes_target ON wire_wakes(target_seat, state);
CREATE INDEX IF NOT EXISTS idx_wire_wakes_task ON wire_wakes(task_ref);
"""


def _utc_now() -> str:
    # Same format as storage.utc_now (ISO-8601 UTC, microseconds, Z suffix)
    # so timestamps sort lexicographically. Duplicated here rather than
    # imported: storage imports this module, so this module must not import
    # storage back (circular import).
    return datetime.now(timezone.utc).isoformat(timespec="microseconds").replace("+00:00", "Z")


def _iso_now() -> datetime:
    return datetime.now(timezone.utc)


def _parse_iso(value: str) -> datetime:
    return datetime.fromisoformat(value.replace("Z", "+00:00"))


def _lease_is_live(lease_expires_at: str | None, now: datetime) -> bool:
    if lease_expires_at is None:
        return True
    try:
        return _parse_iso(lease_expires_at) > now
    except ValueError:
        return False


def _begin_immediate(conn: sqlite3.Connection) -> None:
    """Open a real SQLite write transaction BEFORE any mutable-state read.

    Python's legacy sqlite3 mode does NOT begin a transaction for SELECT
    statements (Goblin R1: ``conn.in_transaction`` is false before the first
    write), so read-check-write sequences built on plain SELECTs are racy:
    another writer can commit between the check and this connection's first
    write. Every wake read-check-write path calls this first so the whole
    sequence is serialized against other writers from its first read.
    """

    conn.execute("BEGIN IMMEDIATE")


def _validated_capabilities(value: Any) -> dict[str, bool]:
    if value is None:
        return dict(DEFAULT_CAPABILITIES)
    if not isinstance(value, dict):
        raise WireError("capabilities must be an object")
    unknown = set(value) - set(DEFAULT_CAPABILITIES)
    if unknown:
        raise WireError(f"unknown capabilities: {', '.join(sorted(unknown))}")
    result = dict(DEFAULT_CAPABILITIES)
    for key, item in value.items():
        if not isinstance(item, bool):
            raise WireError(f"capability {key!r} must be a boolean")
        result[key] = item
    return result


def _validated_optional_text(value: Any, *, field: str, limit: int) -> str | None:
    if value is None:
        return None
    if not isinstance(value, str) or not value.strip() or len(value) > limit:
        raise WireError(f"{field} must be a non-empty string up to {limit} characters")
    return value.strip()


def _validated_task_ref(value: Any) -> str:
    result = _validated_optional_text(value, field="task_ref", limit=MAX_TASK_REF_CHARS)
    assert result is not None
    return result


def _session_dict(row: sqlite3.Row, *, now: datetime | None = None) -> dict[str, Any]:
    lease_expires_at = row["lease_expires_at"]
    live = row["state"] in LIVE_SESSION_STATES and _lease_is_live(lease_expires_at, now or _iso_now())
    return {
        "session_id": row["session_id"],
        "seat": row["seat"],
        "adapter": row["adapter"],
        "platform_pid": row["platform_pid"],
        "model_claim": row["model_claim"],
        "model_evidence": row["model_evidence"],
        "state": row["state"],
        "capabilities": json.loads(row["capabilities"]),
        "registered_at": row["registered_at"],
        "last_heartbeat": row["last_heartbeat"],
        "lease_seconds": row["lease_seconds"],
        "lease_expires_at": lease_expires_at,
        "lease_live": live,
        "generation": row["generation"],
        "meta": json.loads(row["meta"]) if row["meta"] else None,
    }


def _wake_dict(row: sqlite3.Row) -> dict[str, Any]:
    return {
        "wake_id": row["wake_id"],
        "target_seat": row["target_seat"],
        "target_session_id": row["target_session_id"],
        "task_ref": row["task_ref"],
        "requested_by": row["requested_by"],
        "idempotency_key": row["idempotency_key"],
        "priority": row["priority"],
        "ttl_expires_at": row["ttl_expires_at"],
        "state": row["state"],
        "created_at": row["created_at"],
        "delivered_at": row["delivered_at"],
        "acknowledged_at": row["acknowledged_at"],
        "claimed_at": row["claimed_at"],
        "claimed_by_session": row["claimed_by_session"],
        "attempts": row["attempts"],
        "last_error": row["last_error"],
    }


def _task_dict(row: sqlite3.Row) -> dict[str, Any]:
    return {
        "task_id": row["task_id"],
        "title": row["title"],
        "scope": json.loads(row["scope"]) if row["scope"] else None,
        "owner_granted": bool(row["owner_granted"]),
        "created_by": row["created_by"],
        "created_at": row["created_at"],
        "state": row["state"],
        "claimed_by_session": row["claimed_by_session"],
        "claimed_at": row["claimed_at"],
        "completed_at": row["completed_at"],
        "outcome_note": row["outcome_note"],
    }


class WakeStoreMixin:
    """Session/wake/task storage added to :class:`WireStore` for W0.

    Every method opens its own short ``connect()`` transaction, matching the
    existing store style. Claim atomicity lives in a single transaction with
    a guarded ``UPDATE ... WHERE state = 'open'`` so exactly one claimant can
    win; losers receive an honest refusal, never a duplicate claim.
    """

    def initialize_wake_tables(self, conn: sqlite3.Connection | None = None) -> None:
        """Create the additive wake tables.

        Pass the caller's open connection when called from within another
        transaction (e.g. ``WireStore.initialize``) so the DDL runs on the
        same connection instead of opening a nested one that could contend
        for the write lock.
        """

        def run(c: sqlite3.Connection) -> None:
            c.executescript(WAKE_SCHEMA_SQL)

        if conn is not None:
            run(conn)
            return
        with self.connect() as owned:
            run(owned)

    # ── sessions ──────────────────────────────────────────────────────────

    def register_session(
        self,
        *,
        seat: Seat,
        adapter: str,
        capabilities: dict[str, bool] | None = None,
        model_claim: str | None = None,
        lease_seconds: int | None = None,
        platform_pid: int | None = None,
        meta: dict[str, Any] | None = None,
    ) -> dict[str, Any]:
        adapter_name = validate_name(adapter, field="adapter")
        caps = _validated_capabilities(capabilities)
        claim = _validated_optional_text(
            model_claim, field="model_claim", limit=MAX_MODEL_CLAIM_CHARS
        )
        if lease_seconds is None:
            lease_seconds = DEFAULT_LEASE_SECONDS
        if isinstance(lease_seconds, bool) or not isinstance(lease_seconds, int):
            raise WireError("lease_seconds must be an integer")
        if not 1 <= lease_seconds <= MAX_LEASE_SECONDS:
            raise WireError(
                f"lease_seconds must be between 1 and {MAX_LEASE_SECONDS}"
            )
        if platform_pid is not None:
            if isinstance(platform_pid, bool) or not isinstance(platform_pid, int) or platform_pid <= 0:
                raise WireError("platform_pid must be a positive integer when present")
        meta_text = None
        if meta is not None:
            if not isinstance(meta, dict):
                raise WireError("meta must be an object")
            meta_text = json.dumps(meta, ensure_ascii=False)
            if len(meta_text) > MAX_META_CHARS:
                raise WireError(f"meta exceeds {MAX_META_CHARS} characters")

        now = _iso_now()
        expires = (now + timedelta(seconds=lease_seconds)).isoformat(
            timespec="microseconds"
        ).replace("+00:00", "Z")
        session_id = uuid.uuid4().hex
        registered_at = _utc_now()
        with self.connect() as conn:
            _begin_immediate(conn)
            conn.execute(
                """
                INSERT INTO wire_sessions(
                    session_id, seat, adapter, platform_pid, model_claim,
                    model_evidence, state, capabilities, registered_at,
                    last_heartbeat, lease_seconds, lease_expires_at,
                    generation, meta
                ) VALUES (?, ?, ?, ?, ?, 'unverified', 'ready', ?, ?, ?, ?, ?, 1, ?)
                """,
                (
                    session_id,
                    seat.name,
                    adapter_name,
                    platform_pid,
                    claim,
                    json.dumps(caps),
                    registered_at,
                    registered_at,
                    lease_seconds,
                    expires,
                    meta_text,
                ),
            )
        row = self._fetch_session(conn=None, session_id=session_id)
        assert row is not None
        return _session_dict(row, now=now)

    def _fetch_session(
        self, *, conn: sqlite3.Connection | None, session_id: str
    ) -> sqlite3.Row | None:
        def query(c: sqlite3.Connection) -> sqlite3.Row | None:
            return c.execute(
                "SELECT * FROM wire_sessions WHERE session_id = ?", (session_id,)
            ).fetchone()

        if conn is not None:
            return query(conn)
        with self.connect() as owned:
            return query(owned)

    def get_session(self, session_id: str) -> dict[str, Any] | None:
        row = self._fetch_session(conn=None, session_id=session_id)
        return None if row is None else _session_dict(row)

    def heartbeat_session(
        self, *, seat: Seat, session_id: str, state: str | None = None
    ) -> dict[str, Any]:
        if state is not None and state not in ("ready", "busy", "draining"):
            raise WireError("state must be one of ready, busy, draining")
        now = _iso_now()
        with self.connect() as conn:
            _begin_immediate(conn)
            row = conn.execute(
                "SELECT * FROM wire_sessions WHERE session_id = ?", (session_id,)
            ).fetchone()
            if row is None:
                raise WireError("unknown session")
            if row["seat"] != seat.name:
                raise WireError("session belongs to a different seat")
            if row["state"] in ("offline", "expired", "revoked"):
                raise WireError(
                    f"session is {row['state']}; register a new session instead"
                )
            expires = (
                now + timedelta(seconds=int(row["lease_seconds"]))
            ).isoformat(timespec="microseconds").replace("+00:00", "Z")
            new_state = state if state is not None else row["state"]
            conn.execute(
                """
                UPDATE wire_sessions
                SET last_heartbeat = ?, lease_expires_at = ?, state = ?
                WHERE session_id = ?
                """,
                (_utc_now(), expires, new_state, session_id),
            )
            refreshed = conn.execute(
                "SELECT * FROM wire_sessions WHERE session_id = ?", (session_id,)
            ).fetchone()
        assert refreshed is not None
        return _session_dict(refreshed, now=now)

    def unregister_session(self, *, seat: Seat, session_id: str) -> dict[str, Any]:
        with self.connect() as conn:
            _begin_immediate(conn)
            row = conn.execute(
                "SELECT * FROM wire_sessions WHERE session_id = ?", (session_id,)
            ).fetchone()
            if row is None:
                raise WireError("unknown session")
            if row["seat"] != seat.name:
                raise WireError("session belongs to a different seat")
            conn.execute(
                """
                UPDATE wire_sessions
                SET state = 'offline', lease_expires_at = NULL, generation = generation + 1
                WHERE session_id = ?
                """,
                (session_id,),
            )
            refreshed = conn.execute(
                "SELECT * FROM wire_sessions WHERE session_id = ?", (session_id,)
            ).fetchone()
        assert refreshed is not None
        return _session_dict(refreshed)

    def list_sessions(
        self, *, seat: str | None = None, limit: int = MAX_WAKE_LIST_LIMIT
    ) -> list[dict[str, Any]]:
        """Registered sessions, bounded (R8: listing responses stay under the
        transport frame limit)."""
        if isinstance(limit, bool) or not isinstance(limit, int) or limit <= 0:
            raise WireError("limit must be a positive integer")
        if limit > MAX_WAKE_LIST_LIMIT:
            raise WireError(f"limit must not exceed {MAX_WAKE_LIST_LIMIT}")
        now = _iso_now()
        with self.connect() as conn:
            if seat is None:
                rows = conn.execute(
                    "SELECT * FROM wire_sessions ORDER BY registered_at ASC, session_id ASC LIMIT ?",
                    (limit,),
                ).fetchall()
            else:
                rows = conn.execute(
                    """SELECT * FROM wire_sessions WHERE seat = ?
                       ORDER BY registered_at ASC, session_id ASC LIMIT ?""",
                    (seat, limit),
                ).fetchall()
        return [_session_dict(row, now=now) for row in rows]

    def live_sessions_for_seat(
        self, conn: sqlite3.Connection, seat: str, *, now: datetime
    ) -> list[sqlite3.Row]:
        """Live sessions of a seat regardless of wake capability.

        R3 revision: capability filtering lives with the callers that need it
        (auto-resolution candidates, ack/claim enforcement), so a live session
        that cannot acknowledge is classified ``unsupported`` rather than
        silently disappearing into ``queued_offline``.
        """
        rows = conn.execute(
            "SELECT * FROM wire_sessions WHERE seat = ? ORDER BY registered_at ASC",
            (seat,),
        ).fetchall()
        return [
            row
            for row in rows
            if row["state"] in LIVE_SESSION_STATES
            and _lease_is_live(row["lease_expires_at"], now)
        ]

    # ── tasks (operator-local creation only; no socket op creates these) ──

    def add_task(
        self,
        *,
        task_id: str,
        title: str,
        scope: dict[str, Any] | None = None,
        created_by: str = "operator-local",
    ) -> dict[str, Any]:
        ref = _validated_task_ref(task_id)
        title_text = _validated_optional_text(title, field="title", limit=MAX_NOTE_CHARS)
        assert title_text is not None
        scope_text = (
            json.dumps(scope, ensure_ascii=False) if scope is not None else None
        )
        if scope_text is not None and len(scope_text) > MAX_META_CHARS:
            raise WireError(f"scope exceeds {MAX_META_CHARS} characters")
        with self.connect() as conn:
            _begin_immediate(conn)
            existing = conn.execute(
                "SELECT task_id FROM wire_tasks WHERE task_id = ?", (ref,)
            ).fetchone()
            if existing is not None:
                raise WireError(f"task {ref!r} already exists")
            conn.execute(
                """
                INSERT INTO wire_tasks(
                    task_id, title, scope, owner_granted, created_by, created_at, state
                ) VALUES (?, ?, ?, 1, ?, ?, 'open')
                """,
                (ref, title_text, scope_text, created_by, _utc_now()),
            )
            row = conn.execute(
                "SELECT * FROM wire_tasks WHERE task_id = ?", (ref,)
            ).fetchone()
        assert row is not None
        return _task_dict(row)

    def get_task(self, task_id: str) -> dict[str, Any] | None:
        with self.connect() as conn:
            row = conn.execute(
                "SELECT * FROM wire_tasks WHERE task_id = ?", (task_id,)
            ).fetchone()
        return None if row is None else _task_dict(row)

    def list_tasks(self) -> list[dict[str, Any]]:
        with self.connect() as conn:
            rows = conn.execute(
                "SELECT * FROM wire_tasks ORDER BY created_at ASC"
            ).fetchall()
        return [_task_dict(row) for row in rows]

    def cancel_task(self, *, task_id: str, reason: str | None = None) -> dict[str, Any]:
        """Operator-local revocation. Defeats pending wakes at claim time."""
        note = _validated_optional_text(reason, field="reason", limit=MAX_NOTE_CHARS)
        with self.connect() as conn:
            _begin_immediate(conn)
            row = conn.execute(
                "SELECT * FROM wire_tasks WHERE task_id = ?", (task_id,)
            ).fetchone()
            if row is None:
                raise WireError("unknown task")
            if row["state"] in ("completed", "failed", "cancelled", "revoked"):
                raise WireError(f"task already {row['state']}")
            conn.execute(
                """
                UPDATE wire_tasks
                SET state = 'cancelled', completed_at = ?, outcome_note = ?
                WHERE task_id = ?
                """,
                (_utc_now(), note, task_id),
            )
            conn.execute(
                """
                UPDATE wire_wakes
                SET state = 'cancelled',
                    last_error = COALESCE(?, last_error)
                WHERE task_ref = ? AND state IN ('pending', 'delivered')
                """,
                ("task cancelled by operator", task_id),
            )
            refreshed = conn.execute(
                "SELECT * FROM wire_tasks WHERE task_id = ?", (task_id,)
            ).fetchone()
        assert refreshed is not None
        return _task_dict(refreshed)

    # ── wakes ─────────────────────────────────────────────────────────────

    def request_wake(
        self,
        *,
        requester: Seat,
        target_seat: str,
        task_ref: str,
        target_session_id: str | None = None,
        idempotency_key: str | None = None,
        priority: int = 5,
        ttl_seconds: int | None = None,
        note: str | None = None,
    ) -> tuple[dict[str, Any], str, list[str]]:
        """Create a durable wake intent and resolve its target.

        Returns ``(wake_record, resolution, candidate_session_ids)``.
        Resolution vocabulary:

        - ``deliverable``   — exactly one live, wake-capable target session
          was resolved; the server should now attempt live fanout.
        - ``queued``        — target session is registered and live but the
          server has no bound watcher for it yet; the wake stays pending and
          is delivered on the next eligible watch subscription.
        - ``queued_offline``— no live session exists for the target; the wake
          stays pending as durable intent.
        - ``ambiguous``     — several live sessions match and no explicit
          target was given; nothing is delivered and the dispatcher must
          choose. Recorded with state ``ambiguous``.
        - ``unsupported``   — the resolved session did not declare the
          ``wake_ack`` capability. Recorded with state ``failed``.

        The wake references an existing, open task or the request is refused:
        a wake can point at owner-granted work, never create it.

        R7 revision: the authenticated idempotency lookup runs BEFORE new-intent
        eligibility checks, so a retry after the task was claimed/cancelled
        returns the persisted duplicate receipt instead of a task-state error;
        conflicting reuse of a key (same key, different wake) is rejected
        explicitly. R3 revision: live and wake-capable are decided separately
        — a live session that did not declare ``wake_ack`` is ``unsupported``,
        never mislabeled offline.
        """
        target = validate_name(target_seat, field="target_seat")
        ref = _validated_task_ref(task_ref)
        if idempotency_key is not None:
            if (
                not isinstance(idempotency_key, str)
                or not idempotency_key
                or len(idempotency_key) > 128
            ):
                raise WireError("idempotency_key must be a non-empty string up to 128 characters")
        if isinstance(priority, bool) or not isinstance(priority, int) or not 1 <= priority <= 9:
            raise WireError("priority must be an integer between 1 and 9")
        if ttl_seconds is not None:
            if isinstance(ttl_seconds, bool) or not isinstance(ttl_seconds, int) or ttl_seconds <= 0:
                raise WireError("ttl_seconds must be a positive integer")
            if ttl_seconds > MAX_WAKE_TTL_SECONDS:
                raise WireError(f"ttl_seconds must not exceed {MAX_WAKE_TTL_SECONDS}")
        note_text = _validated_optional_text(note, field="note", limit=MAX_NOTE_CHARS)

        now = _iso_now()
        ttl_expires_at = None
        if ttl_seconds is not None:
            ttl_expires_at = (
                now + timedelta(seconds=ttl_seconds)
            ).isoformat(timespec="microseconds").replace("+00:00", "Z")

        with self.connect() as conn:
            _begin_immediate(conn)
            seat_row = conn.execute(
                "SELECT name FROM seats WHERE name = ?", (target,)
            ).fetchone()
            if seat_row is None:
                raise WireError(f"unknown target seat {target!r}")
            task_row = conn.execute(
                "SELECT * FROM wire_tasks WHERE task_id = ?", (ref,)
            ).fetchone()
            if task_row is None:
                raise WireError(f"unknown task_ref {ref!r}; a wake cannot create a task")

            # R7: idempotent retries resolve against the persisted intent
            # before any new-intent eligibility check, and conflicting reuse
            # of a key is rejected instead of silently returning the old wake.
            if idempotency_key is not None:
                existing = conn.execute(
                    """
                    SELECT * FROM wire_wakes
                    WHERE requested_by = ? AND idempotency_key = ?
                    """,
                    (requester.name, idempotency_key),
                ).fetchone()
                if existing is not None:
                    if (
                        existing["task_ref"] != ref
                        or existing["target_seat"] != target
                    ):
                        raise WireError(
                            "idempotency_key is already bound to a different wake"
                        )
                    return _wake_dict(existing), "duplicate", []

            if task_row["state"] != "open":
                raise WireError(f"task {ref!r} is {task_row['state']}, not open")

            candidates: list[str] = []
            resolved_session: sqlite3.Row | None = None
            if target_session_id is not None:
                session_row = conn.execute(
                    "SELECT * FROM wire_sessions WHERE session_id = ?",
                    (target_session_id,),
                ).fetchone()
                if session_row is None:
                    raise WireError("unknown target session")
                if session_row["seat"] != target:
                    raise WireError("target session belongs to a different seat")
                resolved_session = session_row
            else:
                # R3: liveness and wake capability are independent facts. A
                # live session that cannot acknowledge is unsupported, not
                # offline; only wake-capable sessions are auto-resolution
                # candidates.
                live = self.live_sessions_for_seat(conn, target, now=now)
                capable = [
                    row
                    for row in live
                    if json.loads(row["capabilities"]).get("wake_ack", True)
                ]
                if len(capable) == 1:
                    resolved_session = capable[0]
                elif len(capable) > 1:
                    candidates = [row["session_id"] for row in capable]
                elif live:
                    candidates = [row["session_id"] for row in live]

            if resolved_session is not None:
                caps = json.loads(resolved_session["capabilities"])
                if not caps.get("wake_ack", True):
                    state = "failed"
                    resolution = "unsupported"
                    last_error = "target session did not declare the wake_ack capability"
                elif resolved_session["state"] in LIVE_SESSION_STATES and _lease_is_live(
                    resolved_session["lease_expires_at"], now
                ):
                    state = "pending"
                    resolution = "deliverable"
                    last_error = None
                else:
                    state = "pending"
                    resolution = "queued_offline"
                    last_error = "target session is registered but not live"
                target_session_id = resolved_session["session_id"]
            elif candidates:
                live_capable = [
                    row
                    for row in self.live_sessions_for_seat(conn, target, now=now)
                    if json.loads(row["capabilities"]).get("wake_ack", True)
                ]
                if len(live_capable) > 1:
                    state = "ambiguous"
                    resolution = "ambiguous"
                    last_error = (
                        f"ambiguous: {len(candidates)} live wake-capable sessions; "
                        "name a target_session_id"
                    )
                else:
                    # Live sessions exist but none can acknowledge the wake.
                    state = "failed"
                    resolution = "unsupported"
                    last_error = (
                        f"no live session of seat {target!r} declared the "
                        "wake_ack capability"
                    )
            else:
                state = "pending"
                resolution = "queued_offline"
                last_error = "no live session for the target seat"
                target_session_id = None

            wake_id = uuid.uuid4().hex
            conn.execute(
                """
                INSERT INTO wire_wakes(
                    wake_id, target_seat, target_session_id, task_ref,
                    requested_by, idempotency_key, priority, ttl_expires_at,
                    state, created_at, attempts, last_error
                ) VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, 0, ?)
                """,
                (
                    wake_id,
                    target,
                    target_session_id,
                    ref,
                    requester.name,
                    idempotency_key,
                    priority,
                    ttl_expires_at,
                    state,
                    _utc_now(),
                    last_error,
                ),
            )
            row = conn.execute(
                "SELECT * FROM wire_wakes WHERE wake_id = ?", (wake_id,)
            ).fetchone()
        assert row is not None
        return _wake_dict(row), resolution, candidates

    def _expire_stale_wake(self, wake_id: str) -> None:
        """Durably expire a wake whose TTL passed, in its own transaction.

        R6 revision: expiry is a terminal state change, so it commits in its
        own transaction BEFORE any caller's read-check-write transaction runs.
        A later refusal (wrong seat, wrong session, already-claimed) can then
        raise freely without rolling the honest expiry record back, and raw
        stored state always agrees with what the API reports.
        """
        with self.connect() as conn:
            _begin_immediate(conn)
            row = conn.execute(
                "SELECT state, ttl_expires_at FROM wire_wakes WHERE wake_id = ?",
                (wake_id,),
            ).fetchone()
            if row is None:
                return
            if row["state"] in ("pending", "delivered"):
                ttl = row["ttl_expires_at"]
                if ttl is not None and _parse_iso(ttl) <= _iso_now():
                    conn.execute(
                        """
                        UPDATE wire_wakes
                        SET state = 'expired', last_error = 'wake ttl expired'
                        WHERE wake_id = ? AND state IN ('pending', 'delivered')
                        """,
                        (wake_id,),
                    )

    def get_wake(self, wake_id: str) -> dict[str, Any] | None:
        self._expire_stale_wake(wake_id)
        with self.connect() as conn:
            row = conn.execute(
                "SELECT * FROM wire_wakes WHERE wake_id = ?", (wake_id,)
            ).fetchone()
        return None if row is None else _wake_dict(row)

    def list_wakes(
        self,
        *,
        seat: str,
        state: str | None = None,
        limit: int = MAX_WAKE_LIST_LIMIT,
        after_created_at: str | None = None,
        after_wake_id: str | None = None,
    ) -> list[dict[str, Any]]:
        """Wakes addressed to this seat (as target) or requested by it.

        R8 revision: keyset-paginated on (created_at, wake_id) and bounded —
        a growing durable queue must never make its own recovery interface
        unreadable through the transport frame limit. Callers wanting
        has-more detection pass ``limit + 1`` and trim.
        """
        if state is not None and state not in WAKE_STATES:
            raise WireError(f"unknown wake state {state!r}")
        if isinstance(limit, bool) or not isinstance(limit, int) or limit <= 0:
            raise WireError("limit must be a positive integer")
        if limit > MAX_WAKE_LIST_LIMIT:
            raise WireError(f"limit must not exceed {MAX_WAKE_LIST_LIMIT}")
        if (after_created_at is None) != (after_wake_id is None):
            raise WireError("after_created_at and after_wake_id must be given together")
        self._expire_stale_batch(seat)
        with self.connect() as conn:
            if after_created_at is not None:
                cursor_sql = """
                    AND (created_at > ? OR (created_at = ? AND wake_id > ?))
                """
                cursor_args: tuple[Any, ...] = (
                    after_created_at,
                    after_created_at,
                    after_wake_id,
                )
            else:
                cursor_sql = ""
                cursor_args = ()
            if state is None:
                rows = conn.execute(
                    f"""
                    SELECT * FROM wire_wakes
                    WHERE (target_seat = ? OR requested_by = ?){cursor_sql}
                    ORDER BY created_at ASC, wake_id ASC
                    LIMIT ?
                    """,
                    (seat, seat, *cursor_args, limit),
                ).fetchall()
            else:
                rows = conn.execute(
                    f"""
                    SELECT * FROM wire_wakes
                    WHERE (target_seat = ? OR requested_by = ?) AND state = ?{cursor_sql}
                    ORDER BY created_at ASC, wake_id ASC
                    LIMIT ?
                    """,
                    (seat, seat, state, *cursor_args, limit),
                ).fetchall()
        return [_wake_dict(row) for row in rows]

    def _expire_stale_batch(self, seat: str) -> None:
        """Expire every stale wake visible to this seat, in one transaction.

        Keeps listing/catch-up honest without per-row transactions; the batch
        is bounded by the seat's own wake population and runs only on explicit
        list/catch-up calls, never on the message fast path.
        """
        with self.connect() as conn:
            _begin_immediate(conn)
            conn.execute(
                """
                UPDATE wire_wakes
                SET state = 'expired', last_error = 'wake ttl expired'
                WHERE (target_seat = ? OR requested_by = ?)
                  AND state IN ('pending', 'delivered')
                  AND ttl_expires_at IS NOT NULL
                  AND ttl_expires_at <= ?
                """,
                (seat, seat, _utc_now()),
            )

    def pending_wakes_for_watch(
        self,
        *,
        seat: str,
        session_id: str | None,
        limit: int = PENDING_REPLAY_BATCH,
    ) -> list[dict[str, Any]]:
        """Pending wakes this watcher should receive on subscribe (READ-ONLY).

        R2 revision: this method no longer marks anything delivered — a
        durable delivery mark requires an adapter receipt (``wake_ack``), and
        a frame write is only a transmission attempt recorded by the server
        via :meth:`record_wake_attempt`. Unacknowledged wakes stay pending
        and replay on every eligible subscription; redelivery is safe because
        task claims are exactly-once.

        A bound watcher (``session_id`` set) receives seat-level wakes and
        wakes targeted at its own session. An unbound watcher receives only
        seat-level wakes; session-targeted wakes wait for a bound watch.
        The batch is bounded (``limit``, default :data:`PENDING_REPLAY_BATCH`)
        so one subscribe cycle cannot produce an unbounded frame burst;
        anything beyond the batch remains pending for the next cycle.

        Task state is re-checked here: a wake whose task was cancelled or
        completed while pending is marked failed (terminal mark, own
        committed transaction) instead of being offered for delivery.
        """
        if isinstance(limit, bool) or not isinstance(limit, int) or limit <= 0:
            raise WireError("limit must be a positive integer")
        if limit > PENDING_REPLAY_BATCH:
            raise WireError(f"limit must not exceed {PENDING_REPLAY_BATCH}")
        self._expire_stale_batch(seat)
        with self.connect() as conn:
            if session_id is None:
                rows = conn.execute(
                    """
                    SELECT * FROM wire_wakes
                    WHERE target_seat = ? AND target_session_id IS NULL AND state = 'pending'
                    ORDER BY priority ASC, created_at ASC, wake_id ASC
                    LIMIT ?
                    """,
                    (seat, limit),
                ).fetchall()
            else:
                rows = conn.execute(
                    """
                    SELECT * FROM wire_wakes
                    WHERE target_seat = ?
                      AND (target_session_id IS NULL OR target_session_id = ?)
                      AND state = 'pending'
                    ORDER BY priority ASC, created_at ASC, wake_id ASC
                    LIMIT ?
                    """,
                    (seat, session_id, limit),
                ).fetchall()
            candidates: list[dict[str, Any]] = []
            for row in rows:
                task_row = conn.execute(
                    "SELECT state FROM wire_tasks WHERE task_id = ?", (row["task_ref"],)
                ).fetchone()
                if task_row is None or task_row["state"] != "open":
                    # Terminal refusal mark: committed in its own transaction
                    # so it survives anything the caller does next (R6 rule).
                    self._fail_pending_wake(
                        row["wake_id"],
                        f"task no longer open ({'missing' if task_row is None else task_row['state']})",
                    )
                    continue
                candidates.append(_wake_dict(row))
        return candidates

    def _fail_pending_wake(self, wake_id: str, error: str) -> None:
        with self.connect() as conn:
            _begin_immediate(conn)
            conn.execute(
                """
                UPDATE wire_wakes
                SET state = 'failed', last_error = ?
                WHERE wake_id = ? AND state = 'pending'
                """,
                (error, wake_id),
            )

    def record_wake_attempt(self, wake_id: str) -> dict[str, Any]:
        """Record one transmission attempt of a pending wake.

        R2/Sol revision: a successful socket frame write bumps ``attempts``
        and nothing else. The durable state stays ``pending`` until an
        authenticated adapter receipt (``wake_ack``) transitions it to
        ``delivered`` — a frame handed to a socket buffer is a transport
        observation, never a receiver acknowledgment.
        """
        with self.connect() as conn:
            _begin_immediate(conn)
            row = conn.execute(
                "SELECT * FROM wire_wakes WHERE wake_id = ?", (wake_id,)
            ).fetchone()
            if row is None:
                raise WireError("unknown wake")
            if row["state"] == "pending":
                conn.execute(
                    "UPDATE wire_wakes SET attempts = attempts + 1 WHERE wake_id = ?",
                    (wake_id,),
                )
            refreshed = conn.execute(
                "SELECT * FROM wire_wakes WHERE wake_id = ?", (wake_id,)
            ).fetchone()
        assert refreshed is not None
        return _wake_dict(refreshed)

    def acknowledge_wake(
        self, *, seat: Seat, wake_id: str, session_id: str
    ) -> dict[str, Any]:
        """Adapter-level receipt of the wake intent. NOT a resume proof.

        R2/Sol revision: this is the ONLY transition into ``delivered``. A
        successful frame write never reaches here — delivery is what an
        authenticated adapter says it received, nothing less and nothing more.

        R3/R4 revisions enforced at this boundary: the session must belong to
        the seat, be the wake's addressed target, be live with a valid lease,
        and have declared the ``wake_ack`` capability. Idempotent: acking an
        already-delivered wake returns its record unchanged.
        """
        self._expire_stale_wake(wake_id)
        with self.connect() as conn:
            _begin_immediate(conn)
            row = conn.execute(
                "SELECT * FROM wire_wakes WHERE wake_id = ?", (wake_id,)
            ).fetchone()
            if row is None:
                raise WireError("unknown wake")
            if row["target_seat"] != seat.name:
                raise WireError("wake targets a different seat")
            if (
                row["target_session_id"] is not None
                and row["target_session_id"] != session_id
            ):
                raise WireError("wake targets a different session of this seat")
            session_row = conn.execute(
                "SELECT * FROM wire_sessions WHERE session_id = ?", (session_id,)
            ).fetchone()
            if session_row is None:
                raise WireError("unknown session")
            if session_row["seat"] != seat.name:
                raise WireError("session belongs to a different seat")
            if session_row["state"] in ("offline", "expired", "revoked"):
                raise WireError(
                    f"session is {session_row['state']}; an offline session cannot acknowledge"
                )
            if session_row["state"] not in LIVE_SESSION_STATES or not _lease_is_live(
                session_row["lease_expires_at"], _iso_now()
            ):
                # R4: an ack is a live-adapter receipt; a session whose lease
                # has lapsed must heartbeat (or re-register) first.
                raise WireError(
                    "session lease expired; heartbeat or re-register before acknowledging"
                )
            if not json.loads(session_row["capabilities"]).get("wake_ack", True):
                # R3: a session that declared wake_ack=false cannot receipt
                # wakes; honoring the declaration is the point of declaring it.
                raise WireError(
                    "session did not declare the wake_ack capability; cannot acknowledge"
                )
            if row["state"] == "delivered":
                refreshed = row
            elif row["state"] == "pending":
                conn.execute(
                    """
                    UPDATE wire_wakes
                    SET state = 'delivered', delivered_at = ?, acknowledged_at = ?
                    WHERE wake_id = ? AND state = 'pending'
                    """,
                    (_utc_now(), _utc_now(), wake_id),
                )
                refreshed = conn.execute(
                    "SELECT * FROM wire_wakes WHERE wake_id = ?", (wake_id,)
                ).fetchone()
            else:
                raise WireError(f"wake is {row['state']}; cannot acknowledge")
        assert refreshed is not None
        return _wake_dict(refreshed)

    def claim_wake(
        self, *, seat: Seat, wake_id: str, session_id: str
    ) -> dict[str, Any]:
        """Atomically claim the wake's task for one session. Exactly once.

        Fencing applied at claim time, inside one BEGIN IMMEDIATE transaction
        (R1/M1 revision: the whole read-check-write sequence is serialized
        from its first read — legacy sqlite3 SELECTs do not open transactions,
        which is exactly the gap the original M1 analysis missed):
        - the session must exist, belong to the seat, be live, hold a valid
          lease, and have declared the ``claim`` capability (R3);
        - the task must still be ``open`` (cancellation/revocation defeats
          the claim) and the guarded ``UPDATE ... WHERE state = 'open'`` wins
          for exactly one caller — the guard is LOAD-BEARING, not redundant:
          with it removed, a cancellation committing between the precheck read
          and the write revives the cancelled task (Goblin's paired mutant
          test kills the mutant; classification corrected from the original
          W0 report's "equivalent survivor" to "coverage gap");
        - a delayed wake arriving after lease expiry is defeated here and the
          wake is durably marked expired (expiry committed in its own
          transaction first, so the refusal cannot roll it back — R6).
        """
        self._expire_stale_wake(wake_id)
        now = _iso_now()
        failure: str | None = None
        with self.connect() as conn:
            _begin_immediate(conn)
            row = conn.execute(
                "SELECT * FROM wire_wakes WHERE wake_id = ?", (wake_id,)
            ).fetchone()
            if row is None:
                raise WireError("unknown wake")
            if row["target_seat"] != seat.name:
                raise WireError("wake targets a different seat")
            if row["state"] not in CLAIMABLE_WAKE_STATES:
                raise WireError(f"wake is {row['state']}; cannot claim")
            if (
                row["target_session_id"] is not None
                and row["target_session_id"] != session_id
            ):
                raise WireError("wake targets a different session of this seat")
            session_row = conn.execute(
                "SELECT * FROM wire_sessions WHERE session_id = ?", (session_id,)
            ).fetchone()
            if session_row is None:
                raise WireError("unknown session")
            if session_row["seat"] != seat.name:
                raise WireError("session belongs to a different seat")
            if session_row["state"] not in LIVE_SESSION_STATES:
                raise WireError(f"session is {session_row['state']}; cannot claim")
            if not json.loads(session_row["capabilities"]).get("claim", True):
                # R3: a session that declared claim=false cannot claim work;
                # the declaration is honored at the boundary that matters.
                raise WireError(
                    "session did not declare the claim capability; cannot claim"
                )
            if not _lease_is_live(session_row["lease_expires_at"], now):
                # Fencing marks are recorded in THIS transaction and must
                # survive the refusal, so the refusal is raised after the
                # transaction commits (raising inside the ``with`` block
                # would roll the honest expiry record back).
                conn.execute(
                    """
                    UPDATE wire_wakes
                    SET state = 'expired', last_error = 'session lease expired at claim time'
                    WHERE wake_id = ?
                    """,
                    (wake_id,),
                )
                failure = "session lease expired; delayed wake defeated"
            if failure is None:
                task_row = conn.execute(
                    "SELECT * FROM wire_tasks WHERE task_id = ?", (row["task_ref"],)
                ).fetchone()
                if task_row is None:
                    failure = "wake references a missing task"
                elif task_row["state"] != "open":
                    conn.execute(
                        """
                        UPDATE wire_wakes
                        SET state = 'failed', last_error = ?
                        WHERE wake_id = ? AND state IN ('pending', 'delivered')
                        """,
                        (
                            f"task no longer open ({task_row['state']}) at claim time",
                            wake_id,
                        ),
                    )
                    failure = f"task is {task_row['state']}; cannot claim"
            if failure is None:
                claimed_at = _utc_now()
                cursor = conn.execute(
                    """
                    UPDATE wire_tasks
                    SET state = 'claimed', claimed_by_session = ?, claimed_at = ?
                    WHERE task_id = ? AND state = 'open'
                    """,
                    (session_id, claimed_at, row["task_ref"]),
                )
                if cursor.rowcount != 1:
                    conn.execute(
                        """
                        UPDATE wire_wakes
                        SET state = 'failed', last_error = 'lost claim race; task claimed elsewhere'
                        WHERE wake_id = ? AND state IN ('pending', 'delivered')
                        """,
                        (wake_id,),
                    )
                    failure = "task no longer open; another claimant won"
            if failure is not None:
                # Transaction commits here (context exits normally), then the
                # honest refusal raises with the durable fencing record in place.
                pass
            else:
                conn.execute(
                    """
                    UPDATE wire_wakes
                    SET state = 'claimed', claimed_at = ?, claimed_by_session = ?
                    WHERE wake_id = ? AND state IN ('pending', 'delivered')
                    """,
                    (claimed_at, session_id, wake_id),
                )
                wake_row = conn.execute(
                    "SELECT * FROM wire_wakes WHERE wake_id = ?", (wake_id,)
                ).fetchone()
                task_row = conn.execute(
                    "SELECT * FROM wire_tasks WHERE task_id = ?", (row["task_ref"],)
                ).fetchone()
        if failure is not None:
            raise WireError(failure)
        assert wake_row is not None and task_row is not None
        return {
            "wake": _wake_dict(wake_row),
            "task": _task_dict(task_row),
            "fencing": {
                "session_id": session_id,
                "session_generation": session_row["generation"],
                "claimed_at": claimed_at,
            },
        }

    def complete_task(
        self,
        *,
        seat: Seat,
        task_ref: str,
        session_id: str,
        outcome: str,
        note: str | None = None,
    ) -> dict[str, Any]:
        if outcome not in ("completed", "failed"):
            raise WireError("outcome must be 'completed' or 'failed'")
        note_text = _validated_optional_text(note, field="note", limit=MAX_NOTE_CHARS)
        ref = _validated_task_ref(task_ref)
        failure: str | None = None
        with self.connect() as conn:
            # R1 revision: BEGIN IMMEDIATE serializes the read-check-write
            # sequence from its first read (legacy SELECTs open no
            # transaction), and the completion UPDATE is guarded on
            # ``state = 'claimed'`` with a rowcount check. An operator
            # cancellation that commits before the write therefore wins:
            # the guarded UPDATE matches zero rows, the refusal is honest,
            # and the cancellation reason survives. The old unguarded
            # ``WHERE task_id = ?`` overwrote committed cancellations
            # (Goblin's real-CLI race reproduction).
            _begin_immediate(conn)
            task_row = conn.execute(
                "SELECT * FROM wire_tasks WHERE task_id = ?", (ref,)
            ).fetchone()
            if task_row is None:
                raise WireError("unknown task")
            if task_row["claimed_by_session"] != session_id:
                raise WireError("task was not claimed by this session")
            session_row = conn.execute(
                "SELECT * FROM wire_sessions WHERE session_id = ?", (session_id,)
            ).fetchone()
            if session_row is None or session_row["seat"] != seat.name:
                raise WireError("session belongs to a different seat")
            if task_row["state"] in ("cancelled", "revoked"):
                # Terminal refusal with a durable receipt: the claimant's wake
                # is marked failed so the pipeline records why completion
                # never happened. Mark + commit, raise after (R6 rule).
                conn.execute(
                    """
                    UPDATE wire_wakes
                    SET state = 'failed', last_error = ?
                    WHERE task_ref = ? AND claimed_by_session = ?
                      AND state = 'claimed'
                    """,
                    (f"task was {task_row['state']} before completion", ref, session_id),
                )
                failure = f"task was {task_row['state']}; completion refused"
            elif task_row["state"] != "claimed":
                failure = f"task is {task_row['state']}; cannot complete"
            if failure is None:
                completed_at = _utc_now()
                cursor = conn.execute(
                    """
                    UPDATE wire_tasks
                    SET state = ?, completed_at = ?, outcome_note = ?
                    WHERE task_id = ? AND state = 'claimed'
                    """,
                    (outcome, completed_at, note_text, ref),
                )
                if cursor.rowcount != 1:
                    # Lost the race against a committed cancellation between
                    # our read and our write (possible only without BEGIN
                    # IMMEDIATE; kept as second-layer defense).
                    conn.execute(
                        """
                        UPDATE wire_wakes
                        SET state = 'failed', last_error = 'task no longer claimed at completion; operator cancellation won'
                        WHERE task_ref = ? AND claimed_by_session = ?
                          AND state = 'claimed'
                        """,
                        (ref, session_id),
                    )
                    failure = "task no longer claimed; completion refused"
            if failure is None:
                conn.execute(
                    """
                    UPDATE wire_wakes
                    SET state = ?
                    WHERE task_ref = ? AND claimed_by_session = ?
                      AND state = 'claimed'
                    """,
                    (outcome, ref, session_id),
                )
                task_row = conn.execute(
                    "SELECT * FROM wire_tasks WHERE task_id = ?", (ref,)
                ).fetchone()
                wake_row = conn.execute(
                    """
                    SELECT * FROM wire_wakes
                    WHERE task_ref = ? AND claimed_by_session = ?
                    ORDER BY created_at ASC LIMIT 1
                    """,
                    (ref, session_id),
                ).fetchone()
        if failure is not None:
            raise WireError(failure)
        assert task_row is not None
        return {
            "task": _task_dict(task_row),
            "wake": _wake_dict(wake_row) if wake_row is not None else None,
        }
