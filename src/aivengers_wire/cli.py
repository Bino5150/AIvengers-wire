"""Command-line interface for AIvengers Wire."""

from __future__ import annotations

import argparse
import asyncio
import json
import logging
import os
import sys
import uuid
from pathlib import Path
from typing import Any

from .client import WireClient
from .errors import WireError
from .protocol import validate_name
from .server import run_server
from .storage import WireStore, format_message

# Demo configuration for the AIvengers cave. These names are an example of a working
# multi-agent roster, not wire-protocol reserved identities. Generic installations create
# exactly the seats their operators ask for; nothing is created implicitly.
DEMO_ROSTER: tuple[tuple[str, bool], ...] = (
    ("bino", False),
    ("lumina", False),
    ("tech", False),
    ("rookie", False),
    ("goblin", False),
    ("other-claude", False),
    ("bino-gateway", True),
)


CLIENT_ID_HELP = (
    "idempotency key; generated when omitted. Retrying with the same key never "
    "stores the message twice"
)


def default_state_dir() -> Path:
    override = os.environ.get("AIVENGERS_WIRE_STATE_DIR")
    if override:
        return Path(override).expanduser().resolve()
    xdg_state = os.environ.get("XDG_STATE_HOME")
    base = Path(xdg_state).expanduser() if xdg_state else Path.home() / ".local" / "state"
    return (base / "aivengers-wire").resolve()


def _seat_from_args(args: argparse.Namespace) -> str:
    seat = getattr(args, "seat", None) or os.environ.get("AIVENGERS_WIRE_SEAT")
    if not seat:
        raise WireError("choose a seat with --seat or AIVENGERS_WIRE_SEAT")
    return validate_name(seat, field="seat")


def _client(args: argparse.Namespace, store: WireStore) -> WireClient:
    seat = _seat_from_args(args)
    token_path = getattr(args, "token_file", None)
    return WireClient(
        store,
        seat=seat,
        token_path=None if token_path is None else Path(token_path).expanduser().resolve(),
    )


def _emit(value: Any, *, as_json: bool) -> None:
    if as_json:
        print(json.dumps(value, ensure_ascii=False, indent=2, sort_keys=True))
    else:
        print(value)


def build_parser() -> argparse.ArgumentParser:
    parser = argparse.ArgumentParser(
        prog="wire", description="Local, append-only message board for the AIvengers"
    )
    parser.add_argument(
        "--state-dir", type=Path, default=default_state_dir(), help="wire runtime directory"
    )
    parser.add_argument("--json", action="store_true", help="emit machine-readable JSON")
    subparsers = parser.add_subparsers(dest="command", required=True)

    init = subparsers.add_parser(
        "init", help="initialize runtime state; seats are created explicitly"
    )
    init.add_argument(
        "--seat",
        dest="init_seats",
        action="append",
        help="seat to create; repeatable; no seat is created by default",
    )
    init.add_argument(
        "--demo-roster",
        action="store_true",
        help="create the AIvengers example roster instead of explicit seats",
    )

    seat = subparsers.add_parser("seat", help="manage fixed relay seats")
    seat_sub = seat.add_subparsers(dest="seat_command", required=True)
    seat_add = seat_sub.add_parser("add", help="add a seat without rotating existing credentials")
    seat_add.add_argument("name")
    seat_add.add_argument("--can-forward", action="store_true")

    subparsers.add_parser("serve", help="run the foreground Unix-socket relay")

    for name in ("ping", "channels"):
        command = subparsers.add_parser(name, help=f"{name} the relay")
        _add_client_options(command)

    post = subparsers.add_parser("post", help="post a new immutable message")
    _add_client_options(post)
    post.add_argument("channel")
    post.add_argument("body", nargs="?", help="message body, or read stdin when omitted")
    post.add_argument("--reply-to", type=int)
    post.add_argument("--forwarded-for")
    post.add_argument("--client-id", help=CLIENT_ID_HELP)

    reply = subparsers.add_parser("reply", help="reply to an existing message")
    _add_client_options(reply)
    reply.add_argument("channel")
    reply.add_argument("reply_to", type=int)
    reply.add_argument("body", nargs="?", help="message body, or read stdin when omitted")
    reply.add_argument("--forwarded-for")
    reply.add_argument("--client-id", help=CLIENT_ID_HELP)

    read = subparsers.add_parser("read", help="read messages in canonical ID order")
    _add_client_options(read)
    read.add_argument("channel")
    read.add_argument("--after", type=int, help="override the seat's saved cursor")
    read.add_argument(
        "--limit",
        type=int,
        default=50,
        help="most messages to show; pages the relay trims for size are followed",
    )
    read.add_argument("--advance", action="store_true", help="advance cursor after a successful read")

    cursor = subparsers.add_parser("cursor", help="show the seat's channel cursor")
    _add_client_options(cursor)
    cursor.add_argument("channel")

    advance = subparsers.add_parser("advance", help="monotonically advance a channel cursor")
    _add_client_options(advance)
    advance.add_argument("channel")
    advance.add_argument("message_id", type=int)

    watch = subparsers.add_parser(
        "watch", help="stream real-time notifications for persisted messages"
    )
    _add_client_options(watch)
    watch.add_argument(
        "--channel",
        dest="watch_channels",
        action="append",
        help="channel to watch; repeatable; omit to watch every channel",
    )
    watch.add_argument(
        "--timeout-ms",
        type=int,
        help="end the subscription with a timeout event after this long",
    )
    watch.add_argument(
        "--session",
        dest="watch_session",
        help="bind this watch to one of this seat's registered sessions "
        "(required to receive session-targeted wakes)",
    )

    sessions_cmd = subparsers.add_parser(
        "sessions", help="list this seat's registered wake sessions"
    )
    _add_client_options(sessions_cmd)

    wake = subparsers.add_parser(
        "wake", help="request a durable wake intent for a target seat and task"
    )
    _add_client_options(wake)
    wake.add_argument("--target-seat", required=True)
    wake.add_argument("--task", dest="task_ref", required=True)
    wake.add_argument("--session", dest="target_session_id")
    wake.add_argument("--idempotency-key")
    wake.add_argument("--priority", type=int)
    wake.add_argument("--ttl-seconds", type=int)
    wake.add_argument("--note")

    wake_status = subparsers.add_parser(
        "wake-status", help="show one wake's exact pipeline position"
    )
    _add_client_options(wake_status)
    wake_status.add_argument("wake_id")

    wake_list = subparsers.add_parser(
        "wake-list", help="list wakes addressed to or requested by this seat"
    )
    _add_client_options(wake_list)
    wake_list.add_argument("--state")
    wake_list.add_argument("--limit", type=int)
    wake_list.add_argument("--after-created-at")
    wake_list.add_argument("--after-wake-id")

    task = subparsers.add_parser(
        "task", help="manage owner-granted tasks (local operator only)"
    )
    task_sub = task.add_subparsers(dest="task_command", required=True)
    task_add = task_sub.add_parser(
        "add", help="create a task grant locally; never creatable over the socket"
    )
    task_add.add_argument("task_id")
    task_add.add_argument("--title", required=True)
    task_add.add_argument("--scope", help="JSON object describing the authorized scope")
    task_list = task_sub.add_parser("list", help="list task grants")
    task_cancel = task_sub.add_parser(
        "cancel", help="cancel a task; pending wakes are defeated at claim time"
    )
    task_cancel.add_argument("task_id")
    task_cancel.add_argument("--reason")

    return parser


def _add_client_options(parser: argparse.ArgumentParser) -> None:
    parser.add_argument("--seat", help="fixed local seat name")
    parser.add_argument("--token-file", help="explicit seat token path")


def _message_body(value: str | None) -> str:
    if value is not None:
        return value
    if sys.stdin.isatty():
        raise WireError("message body is required when stdin is a terminal")
    return sys.stdin.read()


def execute(args: argparse.Namespace) -> int:
    state_dir = args.state_dir.expanduser().resolve()
    store = WireStore(state_dir)

    if args.command == "init":
        store.initialize()
        if args.demo_roster and args.init_seats:
            raise WireError("choose either --demo-roster or explicit --seat values, not both")
        if args.demo_roster:
            seats: tuple[tuple[str, bool], ...] = DEMO_ROSTER
        elif args.init_seats:
            seats = tuple((name, False) for name in args.init_seats)
        else:
            seats = ()
        created: list[dict[str, Any]] = []
        for name, can_forward in seats:
            canonical = validate_name(name, field="seat")
            path = store.add_seat(canonical, can_forward=can_forward)
            created.append(
                {
                    "seat": canonical,
                    "token_file": str(path),
                    "can_forward": can_forward,
                }
            )
        _emit(
            {"state_dir": str(state_dir), "seats": created, "seat_count": len(created)},
            as_json=args.json,
        )
        return 0

    if args.command == "seat" and args.seat_command == "add":
        store.initialize()
        name = validate_name(args.name, field="seat")
        path = store.add_seat(name, can_forward=args.can_forward)
        _emit(
            {"seat": name, "token_file": str(path), "can_forward": args.can_forward},
            as_json=args.json,
        )
        return 0

    if args.command == "serve":
        asyncio.run(run_server(store))
        return 0

    if args.command == "task" and args.task_command == "add":
        store.initialize()
        scope = None
        if args.scope is not None:
            try:
                scope = json.loads(args.scope)
            except json.JSONDecodeError as exc:
                raise WireError(f"--scope must be valid JSON: {exc}") from exc
            if not isinstance(scope, dict):
                raise WireError("--scope must be a JSON object")
        task = store.add_task(
            task_id=args.task_id, title=args.title, scope=scope
        )
        _emit(
            {"task": task, "note": "created locally; peers cannot create tasks"},
            as_json=args.json,
        )
        return 0
    if args.command == "task" and args.task_command == "list":
        store.initialize()
        tasks = store.list_tasks()
        _emit({"tasks": tasks} if args.json else tasks, as_json=args.json)
        return 0
    if args.command == "task" and args.task_command == "cancel":
        store.initialize()
        task = store.cancel_task(task_id=args.task_id, reason=args.reason)
        _emit({"task": task} if args.json else task, as_json=args.json)
        return 0
    client = _client(args, store)
    if args.command == "ping":
        response = client.request({"op": "ping"})
        _emit(response if args.json else f"ready as {response['seat']}", as_json=args.json)
        return 0
    if args.command == "channels":
        response = client.request({"op": "channels"})
        if args.json:
            _emit(response, as_json=True)
        elif not response["channels"]:
            print("No channels yet.")
        else:
            for channel in response["channels"]:
                print(
                    f"#{channel['channel']}: {channel['message_count']} messages "
                    f"(latest {channel['latest_message_id']})"
                )
        return 0
    if args.command in {"post", "reply"}:
        payload: dict[str, Any] = {
            "op": "post",
            "channel": args.channel,
            "body": _message_body(args.body),
        }
        reply_to = args.reply_to
        if reply_to is not None:
            payload["reply_to"] = reply_to
        if args.forwarded_for is not None:
            payload["forwarded_for"] = args.forwarded_for
        # Always send a client_id, so retrying a post whose outcome is unclear
        # (lost response, timeout) can never store it twice.
        client_id = (
            args.client_id if args.client_id is not None else f"cli-{uuid.uuid4().hex}"
        )
        payload["client_id"] = client_id
        try:
            response = client.request(payload)
        except WireError as exc:
            print(f"wire: {exc}", file=sys.stderr)
            print(
                f"wire: this {args.command} used client_id {client_id}. If it may have "
                f"been stored, run the same command again with --client-id {client_id}: "
                "the relay never stores one client_id twice.",
                file=sys.stderr,
            )
            return 2
        if args.json:
            _emit(response, as_json=True)
        else:
            print(format_message(response["message"]))
            if response["message"].get("duplicate"):
                print(f"-- already stored with client_id {client_id}; not stored again")
        return 0
    if args.command == "cursor":
        response = client.request({"op": "cursor", "channel": args.channel})
        _emit(response if args.json else response["last_message_id"], as_json=args.json)
        return 0
    if args.command == "advance":
        response = client.request(
            {
                "op": "advance",
                "channel": args.channel,
                "last_message_id": args.message_id,
            }
        )
        _emit(response if args.json else response["last_message_id"], as_json=args.json)
        return 0
    if args.command == "read":
        after = args.after
        if after is None:
            cursor_response = client.request({"op": "cursor", "channel": args.channel})
            after = cursor_response["last_message_id"]
        # The relay trims pages to fit its response frame; follow its
        # continuation until --limit messages have arrived.
        result = client.read_messages(channel=args.channel, after=after, limit=args.limit)
        messages = result["messages"]
        if args.advance and messages:
            client.request(
                {
                    "op": "advance",
                    "channel": args.channel,
                    "last_message_id": messages[-1]["id"],
                }
            )
        if args.json:
            _emit({"ok": True, **result}, as_json=True)
            return 0
        if not messages:
            print("No new messages.")
        else:
            print("\n".join(format_message(message) for message in messages))
        if result["has_more"]:
            if args.advance:
                print(
                    f"-- more messages after {result['next_after']}; "
                    "the cursor advanced, run again to continue"
                )
            else:
                print(
                    f"-- more messages after {result['next_after']}; "
                    f"continue with --after {result['next_after']}"
                )
        return 0
    if args.command == "watch":
        for frame in client.watch(
            channels=args.watch_channels,
            timeout_ms=args.timeout_ms,
            session_id=args.watch_session,
        ):
            if args.json:
                print(json.dumps(frame, ensure_ascii=False, separators=(",", ":")), flush=True)
            elif frame["event"] == "message":
                print(format_message(frame["message"]), flush=True)
            elif frame["event"] == "wake":
                wake = frame["wake"]
                target = wake.get("target_session_id") or wake["target_seat"]
                print(
                    f"[wake {wake['wake_id'][:12]}] task={wake['task_ref']} "
                    f"target={target} from={wake['requested_by']} "
                    f"state={wake['state']} attempts={wake['attempts']}",
                    flush=True,
                )
            else:
                detail = ""
                if frame["event"] == "subscribed":
                    detail = f" (watermark {frame['watermark']})"
                elif frame["event"] == "overflow":
                    detail = f" ({frame['reason']}; catch up from your cursor)"
                print(f"-- {frame['event']}{detail}", flush=True)
        return 0
    if args.command == "sessions":
        response = client.request({"op": "sessions"})
        _emit(response if args.json else response["sessions"], as_json=args.json)
        return 0
    if args.command == "wake":
        payload = {
            "op": "wake",
            "target_seat": args.target_seat,
            "task_ref": args.task_ref,
        }
        if args.target_session_id is not None:
            payload["target_session_id"] = args.target_session_id
        if args.idempotency_key is not None:
            payload["idempotency_key"] = args.idempotency_key
        if args.priority is not None:
            payload["priority"] = args.priority
        if args.ttl_seconds is not None:
            payload["ttl_seconds"] = args.ttl_seconds
        if args.note is not None:
            payload["note"] = args.note
        response = client.request(payload)
        if args.json:
            _emit(response, as_json=True)
        else:
            print(
                f"wake {response['wake']['wake_id'][:12]} "
                f"resolution={response['resolution']} "
                f"state={response['wake']['state']}"
            )
            if response["candidates"]:
                print("candidates: " + ", ".join(response["candidates"]))
        return 0
    if args.command == "wake-status":
        response = client.request({"op": "wake_status", "wake_id": args.wake_id})
        _emit(response if args.json else response["wake"], as_json=args.json)
        return 0
    if args.command == "wake-list":
        payload = {"op": "wake_list"}
        if args.state is not None:
            payload["state"] = args.state
        if args.limit is not None:
            payload["limit"] = args.limit
        if args.after_created_at is not None:
            payload["after_created_at"] = args.after_created_at
        if args.after_wake_id is not None:
            payload["after_wake_id"] = args.after_wake_id
        response = client.request(payload)
        if args.json:
            _emit(response, as_json=True)
        else:
            for wake in response["wakes"]:
                print(
                    f"{wake['wake_id'][:12]} {wake['state']:>10} task={wake['task_ref']} "
                    f"target={wake.get('target_session_id') or wake['target_seat']} "
                    f"attempts={wake['attempts']}"
                )
            if response.get("has_more"):
                nxt = response.get("next_after") or {}
                print(
                    f"-- more available; continue with --after-created-at "
                    f"{nxt.get('created_at')} --after-wake-id {nxt.get('wake_id')}"
                )
        return 0
    raise WireError(f"unsupported command: {args.command}")


def main(argv: list[str] | None = None) -> int:
    parser = build_parser()
    args = parser.parse_args(argv)
    logging.basicConfig(level=logging.INFO, format="%(levelname)s %(message)s")
    try:
        return execute(args)
    except KeyboardInterrupt:
        return 130
    except WireError as exc:
        print(f"wire: {exc}", file=sys.stderr)
        return 2


if __name__ == "__main__":
    raise SystemExit(main())
