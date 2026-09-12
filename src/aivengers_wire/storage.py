"""SQLite storage and seat credential management."""

from __future__ import annotations

import hashlib
import hmac
import json
import os
import secrets
import sqlite3
from contextlib import contextmanager
from dataclasses import dataclass
from datetime import datetime, timezone
from pathlib import Path
from typing import Any, Iterator

from .errors import AuthenticationError, WireError

SCHEMA_VERSION = 1


def utc_now() -> str:
    return datetime.now(timezone.utc).isoformat(timespec="microseconds").replace("+00:00", "Z")


def token_digest(token: str) -> str:
    return hashlib.sha256(token.encode("utf-8")).hexdigest()


@dataclass(frozen=True)
class PeerCredentials:
    pid: int
    uid: int
    gid: int


@dataclass(frozen=True)
class Seat:
    name: str
    can_forward: bool


class WireStore:
    def __init__(self, state_dir: Path):
        self.state_dir = state_dir
        self.db_path = state_dir / "wire.db"
        self.seat_dir = state_dir / "seats"
        self.socket_path = state_dir / "wire.sock"

    def initialize(self) -> None:
        self.state_dir.mkdir(mode=0o700, parents=True, exist_ok=True)
        os.chmod(self.state_dir, 0o700)
        self.seat_dir.mkdir(mode=0o700, exist_ok=True)
        os.chmod(self.seat_dir, 0o700)
        with self.connect() as conn:
            conn.executescript(
                """
                CREATE TABLE IF NOT EXISTS metadata (
                    key TEXT PRIMARY KEY,
                    value TEXT NOT NULL
                );

                CREATE TABLE IF NOT EXISTS seats (
                    name TEXT PRIMARY KEY,
                    token_digest TEXT NOT NULL,
                    can_forward INTEGER NOT NULL DEFAULT 0 CHECK (can_forward IN (0, 1)),
                    enabled INTEGER NOT NULL DEFAULT 1 CHECK (enabled IN (0, 1)),
                    created_at TEXT NOT NULL
                );

                CREATE TABLE IF NOT EXISTS messages (
                    id INTEGER PRIMARY KEY AUTOINCREMENT,
                    message_uuid TEXT NOT NULL UNIQUE,
                    channel TEXT NOT NULL,
                    logical_sender TEXT NOT NULL,
                    transport_actor TEXT NOT NULL,
                    transport TEXT NOT NULL CHECK (transport = 'local-unix'),
                    provenance TEXT NOT NULL CHECK (provenance IN ('direct', 'forwarded')),
                    authority TEXT NOT NULL CHECK (authority = 'peer'),
                    owner INTEGER NOT NULL CHECK (owner = 0),
                    body TEXT NOT NULL,
                    reply_to INTEGER REFERENCES messages(id),
                    client_id TEXT,
                    created_at TEXT NOT NULL,
                    peer_pid INTEGER NOT NULL,
                    peer_uid INTEGER NOT NULL,
                    peer_gid INTEGER NOT NULL
                );

                CREATE UNIQUE INDEX IF NOT EXISTS idx_messages_actor_client_id
                    ON messages(transport_actor, client_id)
                    WHERE client_id IS NOT NULL;
                CREATE INDEX IF NOT EXISTS idx_messages_channel_id
                    ON messages(channel, id);
                CREATE INDEX IF NOT EXISTS idx_messages_reply_to
                    ON messages(reply_to);

                CREATE TABLE IF NOT EXISTS cursors (
                    seat TEXT NOT NULL REFERENCES seats(name),
                    channel TEXT NOT NULL,
                    last_message_id INTEGER NOT NULL CHECK (last_message_id >= 0),
                    updated_at TEXT NOT NULL,
                    PRIMARY KEY (seat, channel)
                );
                """
            )
            existing = conn.execute(
                "SELECT value FROM metadata WHERE key = 'schema_version'"
            ).fetchone()
            if existing is None:
                conn.execute(
                    "INSERT INTO metadata(key, value) VALUES ('schema_version', ?)",
                    (str(SCHEMA_VERSION),),
                )
            elif int(existing[0]) != SCHEMA_VERSION:
                raise WireError(
                    f"unsupported database schema {existing[0]}; expected {SCHEMA_VERSION}"
                )
        os.chmod(self.db_path, 0o600)

    @contextmanager
    def connect(self) -> Iterator[sqlite3.Connection]:
        conn = sqlite3.connect(self.db_path, timeout=5.0)
        conn.row_factory = sqlite3.Row
        conn.execute("PRAGMA foreign_keys = ON")
        conn.execute("PRAGMA journal_mode = WAL")
        conn.execute("PRAGMA synchronous = FULL")
        try:
            yield conn
            conn.commit()
        except Exception:
            conn.rollback()
            raise
        finally:
            conn.close()

    def add_seat(self, name: str, *, can_forward: bool = False) -> Path:
        token_path = self.seat_dir / f"{name}.token"
        with self.connect() as conn:
            row = conn.execute(
                "SELECT name, can_forward FROM seats WHERE name = ?", (name,)
            ).fetchone()
            if row is not None:
                if not token_path.is_file():
                    raise WireError(
                        f"seat {name!r} exists but its token file is missing; rotate it explicitly"
                    )
                if bool(row["can_forward"]) != can_forward:
                    raise WireError(
                        f"seat {name!r} already exists with can_forward="
                        f"{bool(row['can_forward'])}; refusing an implicit privilege change"
                    )
                return token_path

            token = secrets.token_urlsafe(32)
            flags = os.O_WRONLY | os.O_CREAT | os.O_EXCL
            fd = os.open(token_path, flags, 0o600)
            try:
                os.write(fd, (token + "\n").encode("utf-8"))
                os.fsync(fd)
            finally:
                os.close(fd)
            try:
                conn.execute(
                    """
                    INSERT INTO seats(name, token_digest, can_forward, enabled, created_at)
                    VALUES (?, ?, ?, 1, ?)
                    """,
                    (name, token_digest(token), int(can_forward), utc_now()),
                )
            except Exception:
                token_path.unlink(missing_ok=True)
                raise
        os.chmod(token_path, 0o600)
        return token_path

    def authenticate(self, name: str, token: str) -> Seat:
        with self.connect() as conn:
            row = conn.execute(
                "SELECT token_digest, can_forward, enabled FROM seats WHERE name = ?", (name,)
            ).fetchone()
        supplied = token_digest(token)
        if row is None:
            hmac.compare_digest(supplied, "0" * 64)
            raise AuthenticationError("invalid seat credentials")
        if not hmac.compare_digest(supplied, row["token_digest"]):
            raise AuthenticationError("invalid seat credentials")
        if not row["enabled"]:
            raise AuthenticationError("seat is disabled")
        return Seat(name=name, can_forward=bool(row["can_forward"]))

    def post_message(
        self,
        *,
        seat: Seat,
        peer: PeerCredentials,
        channel: str,
        body: str,
        reply_to: int | None,
        forwarded_for: str | None,
        client_id: str | None,
    ) -> dict[str, Any]:
        if forwarded_for is not None and not seat.can_forward:
            raise WireError(f"seat {seat.name!r} is not permitted to forward messages")
        logical_sender = forwarded_for or seat.name
        provenance = "forwarded" if forwarded_for else "direct"
        message_uuid = secrets.token_hex(16)
        created_at = utc_now()
        with self.connect() as conn:
            if reply_to is not None:
                parent = conn.execute(
                    "SELECT id, channel FROM messages WHERE id = ?", (reply_to,)
                ).fetchone()
                if parent is None:
                    raise WireError(f"reply target {reply_to} does not exist")
                if parent["channel"] != channel:
                    raise WireError("reply target belongs to a different channel")
            try:
                cursor = conn.execute(
                    """
                    INSERT INTO messages(
                        message_uuid, channel, logical_sender, transport_actor,
                        transport, provenance, authority, owner, body, reply_to,
                        client_id, created_at, peer_pid, peer_uid, peer_gid
                    ) VALUES (?, ?, ?, ?, 'local-unix', ?, 'peer', 0, ?, ?, ?, ?, ?, ?, ?)
                    """,
                    (
                        message_uuid,
                        channel,
                        logical_sender,
                        seat.name,
                        provenance,
                        body,
                        reply_to,
                        client_id,
                        created_at,
                        peer.pid,
                        peer.uid,
                        peer.gid,
                    ),
                )
            except sqlite3.IntegrityError as exc:
                if client_id is not None and "client_id" in str(exc):
                    existing = conn.execute(
                        """
                        SELECT * FROM messages
                        WHERE transport_actor = ? AND client_id = ?
                        """,
                        (seat.name, client_id),
                    ).fetchone()
                    if existing is not None:
                        matches = (
                            existing["channel"] == channel
                            and existing["logical_sender"] == logical_sender
                            and existing["provenance"] == provenance
                            and existing["body"] == body
                            and existing["reply_to"] == reply_to
                        )
                        if not matches:
                            raise WireError(
                                "client_id is already bound to a different message"
                            ) from exc
                        return self._message_dict(existing, duplicate=True)
                raise
            row = conn.execute("SELECT * FROM messages WHERE id = ?", (cursor.lastrowid,)).fetchone()
        assert row is not None
        return self._message_dict(row, duplicate=False)

    def read_messages(
        self, *, channel: str | None, after: int, limit: int
    ) -> list[dict[str, Any]]:
        with self.connect() as conn:
            if channel is None:
                rows = conn.execute(
                    "SELECT * FROM messages WHERE id > ? ORDER BY id ASC LIMIT ?",
                    (after, limit),
                ).fetchall()
            else:
                rows = conn.execute(
                    """
                    SELECT * FROM messages
                    WHERE channel = ? AND id > ?
                    ORDER BY id ASC LIMIT ?
                    """,
                    (channel, after, limit),
                ).fetchall()
        return [self._message_dict(row) for row in rows]

    def get_cursor(self, *, seat: Seat, channel: str) -> int:
        with self.connect() as conn:
            row = conn.execute(
                "SELECT last_message_id FROM cursors WHERE seat = ? AND channel = ?",
                (seat.name, channel),
            ).fetchone()
        return 0 if row is None else int(row["last_message_id"])

    def advance_cursor(self, *, seat: Seat, channel: str, last_message_id: int) -> int:
        with self.connect() as conn:
            visible = conn.execute(
                "SELECT 1 FROM messages WHERE id = ? AND channel = ?",
                (last_message_id, channel),
            ).fetchone()
            if last_message_id != 0 and visible is None:
                raise WireError("cursor target does not exist in that channel")
            conn.execute(
                """
                INSERT INTO cursors(seat, channel, last_message_id, updated_at)
                VALUES (?, ?, ?, ?)
                ON CONFLICT(seat, channel) DO UPDATE SET
                    last_message_id = MAX(cursors.last_message_id, excluded.last_message_id),
                    updated_at = CASE
                        WHEN excluded.last_message_id > cursors.last_message_id
                        THEN excluded.updated_at ELSE cursors.updated_at END
                """,
                (seat.name, channel, last_message_id, utc_now()),
            )
            row = conn.execute(
                "SELECT last_message_id FROM cursors WHERE seat = ? AND channel = ?",
                (seat.name, channel),
            ).fetchone()
        assert row is not None
        return int(row["last_message_id"])

    def list_channels(self) -> list[dict[str, Any]]:
        with self.connect() as conn:
            rows = conn.execute(
                """
                SELECT channel, COUNT(*) AS message_count, MAX(id) AS latest_message_id
                FROM messages GROUP BY channel ORDER BY channel
                """
            ).fetchall()
        return [dict(row) for row in rows]

    @staticmethod
    def _message_dict(row: sqlite3.Row, *, duplicate: bool | None = None) -> dict[str, Any]:
        result = {
            "id": row["id"],
            "message_uuid": row["message_uuid"],
            "channel": row["channel"],
            "sender": row["logical_sender"],
            "transport_actor": row["transport_actor"],
            "transport": row["transport"],
            "provenance": row["provenance"],
            "authority": row["authority"],
            "owner": bool(row["owner"]),
            "body": row["body"],
            "reply_to": row["reply_to"],
            "client_id": row["client_id"],
            "created_at": row["created_at"],
            "peer": {
                "pid": row["peer_pid"],
                "uid": row["peer_uid"],
                "gid": row["peer_gid"],
            },
        }
        if duplicate is not None:
            result["duplicate"] = duplicate
        return result


def load_token(path: Path) -> str:
    try:
        value = path.read_text(encoding="utf-8").strip()
    except OSError as exc:
        raise WireError(f"cannot read seat token {path}: {exc}") from exc
    if not value:
        raise WireError(f"seat token {path} is empty")
    return value


def format_message(message: dict[str, Any]) -> str:
    forwarded = ""
    if message["provenance"] == "forwarded":
        forwarded = f" via {message['transport_actor']}"
    reply = "" if message["reply_to"] is None else f" ↪{message['reply_to']}"
    return (
        f"[{message['id']}] #{message['channel']} {message['sender']}{forwarded}{reply}\n"
        f"    {message['body']}"
    )
