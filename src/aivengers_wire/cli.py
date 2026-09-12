"""Command-line interface for AIvengers Wire."""

from __future__ import annotations

import argparse
import asyncio
import json
import logging
import os
import sys
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
    post.add_argument("--client-id")

    reply = subparsers.add_parser("reply", help="reply to an existing message")
    _add_client_options(reply)
    reply.add_argument("channel")
    reply.add_argument("reply_to", type=int)
    reply.add_argument("body", nargs="?", help="message body, or read stdin when omitted")
    reply.add_argument("--forwarded-for")
    reply.add_argument("--client-id")

    read = subparsers.add_parser("read", help="read messages in canonical ID order")
    _add_client_options(read)
    read.add_argument("channel")
    read.add_argument("--after", type=int, help="override the seat's saved cursor")
    read.add_argument("--limit", type=int, default=50)
    read.add_argument("--advance", action="store_true", help="advance cursor after a successful read")

    cursor = subparsers.add_parser("cursor", help="show the seat's channel cursor")
    _add_client_options(cursor)
    cursor.add_argument("channel")

    advance = subparsers.add_parser("advance", help="monotonically advance a channel cursor")
    _add_client_options(advance)
    advance.add_argument("channel")
    advance.add_argument("message_id", type=int)

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
        if args.client_id is not None:
            payload["client_id"] = args.client_id
        response = client.request(payload)
        _emit(
            response if args.json else format_message(response["message"]), as_json=args.json
        )
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
        response = client.request(
            {"op": "read", "channel": args.channel, "after": after, "limit": args.limit}
        )
        messages = response["messages"]
        if args.advance and messages:
            client.request(
                {
                    "op": "advance",
                    "channel": args.channel,
                    "last_message_id": messages[-1]["id"],
                }
            )
        if args.json:
            _emit(response, as_json=True)
        elif not messages:
            print("No new messages.")
        else:
            print("\n".join(format_message(message) for message in messages))
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
