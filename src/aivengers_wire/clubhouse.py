"""Tkinter Clubhouse GUI using only the public Wire socket protocol."""

from __future__ import annotations

import argparse
import os
import uuid
from concurrent.futures import Future, ThreadPoolExecutor
from dataclasses import dataclass
from pathlib import Path
from typing import Any, Callable

import tkinter as tk
from tkinter import messagebox, ttk

from .client import WireClient
from .errors import WireError
from .storage import WireStore

DEFAULT_CHANNELS = ("general",)


@dataclass(frozen=True)
class IdentityRoute:
    key: str
    label: str
    seat: str
    forwarded_for: str | None = None


def build_identity_routes(
    seat: str,
    *,
    forward_seat: str | None = None,
    forward_for: str | None = None,
) -> dict[str, IdentityRoute]:
    """Build GUI identity routes from explicit configuration.

    The AIvengers cave configuration is an example, not a protocol requirement
    (see README). Generic example:
    build_identity_routes("human", forward_seat="gateway", forward_for="remote").
    """
    routes = {seat: IdentityRoute(key=seat, label=seat, seat=seat)}
    if (forward_seat is None) != (forward_for is None):
        raise WireError("--forward-seat and --forward-for must be provided together")
    if forward_seat is not None and forward_for is not None:
        routes[forward_for] = IdentityRoute(
            key=forward_for,
            label=f"{forward_for} · via {forward_seat}",
            seat=forward_seat,
            forwarded_for=forward_for,
        )
    return routes


def default_state_dir() -> Path:
    override = os.environ.get("AIVENGERS_WIRE_STATE_DIR")
    if override:
        return Path(override).expanduser().resolve()
    xdg_state = os.environ.get("XDG_STATE_HOME")
    base = Path(xdg_state).expanduser() if xdg_state else Path.home() / ".local" / "state"
    return (base / "aivengers-wire").resolve()


class ClubhouseService:
    """Socket-only backend used by the GUI and its tests."""

    def __init__(
        self,
        store: WireStore,
        *,
        seat: str = "human",
        routes: dict[str, IdentityRoute] | None = None,
    ):
        self.store = store
        self.seat = seat
        self.routes = routes if routes is not None else dict(build_identity_routes(seat))

    def _client(self, seat: str) -> WireClient:
        return WireClient(self.store, seat=seat)

    def ping(self) -> dict[str, Any]:
        return self._client(self.seat).request({"op": "ping"})

    def channels(self) -> list[str]:
        response = self._client(self.seat).request({"op": "channels"})
        discovered = {item["channel"] for item in response["channels"]}
        return list(DEFAULT_CHANNELS) + sorted(discovered - set(DEFAULT_CHANNELS))

    def read_channel(self, channel: str, *, after: int = 0) -> list[dict[str, Any]]:
        messages: list[dict[str, Any]] = []
        cursor = after
        while len(messages) < 2_000:
            response = self._client(self.seat).request(
                {"op": "read", "channel": channel, "after": cursor, "limit": 200}
            )
            page = response["messages"]
            if not page:
                break
            messages.extend(page)
            cursor = page[-1]["id"]
            if len(page) < 200:
                break
        return messages

    def post(
        self,
        *,
        route_key: str,
        channel: str,
        body: str,
        reply_to: int | None,
    ) -> dict[str, Any]:
        try:
            route = self.routes[route_key]
        except KeyError as exc:
            raise WireError(f"unsupported Clubhouse identity route: {route_key!r}") from exc
        payload: dict[str, Any] = {
            "op": "post",
            "channel": channel,
            "body": body,
            "client_id": f"clubhouse:{uuid.uuid4()}",
        }
        if reply_to is not None:
            payload["reply_to"] = reply_to
        if route.forwarded_for is not None:
            payload["forwarded_for"] = route.forwarded_for
        return self._client(route.seat).request(payload)["message"]


class ClubhouseApp:
    BG = "#0d1117"
    SIDEBAR = "#111821"
    PANEL = "#161b22"
    CARD = "#1f2630"
    CARD_REPLY = "#1a222c"
    TEXT = "#e6edf3"
    MUTED = "#8b949e"
    ACCENT = "#58a6ff"
    GREEN = "#3fb950"
    ORANGE = "#d29922"
    BORDER = "#30363d"

    def __init__(
        self,
        root: tk.Tk,
        service: ClubhouseService,
        *,
        refresh_ms: int = 1_500,
    ):
        self.root = root
        self.service = service
        self.refresh_ms = max(500, refresh_ms)
        self.executor = ThreadPoolExecutor(max_workers=1, thread_name_prefix="clubhouse-wire")
        self.closed = False
        self.busy = False
        self.pending_refresh = False
        self.refresh_after_id: str | None = None
        self.current_channel = "general"
        self.reply_to: int | None = None
        self.messages: dict[str, list[dict[str, Any]]] = {
            channel: [] for channel in DEFAULT_CHANNELS
        }
        self.channel_names = list(DEFAULT_CHANNELS)
        self.route_labels = {route.label: route.key for route in service.routes.values()}

        self._configure_window()
        self._build_ui()
        self._render_messages()
        self.root.protocol("WM_DELETE_WINDOW", self.close)
        self.root.after(50, self.refresh)

    def _configure_window(self) -> None:
        self.root.title("AIvengers Clubhouse")
        self.root.geometry("1180x780")
        self.root.minsize(860, 600)
        self.root.configure(bg=self.BG)
        style = ttk.Style(self.root)
        if "clam" in style.theme_names():
            style.theme_use("clam")
        style.configure("Clubhouse.TFrame", background=self.BG)
        style.configure("Sidebar.TFrame", background=self.SIDEBAR)
        style.configure("Panel.TFrame", background=self.PANEL)
        style.configure(
            "Clubhouse.TButton",
            background=self.PANEL,
            foreground=self.TEXT,
            bordercolor=self.BORDER,
            padding=(12, 7),
        )
        style.map("Clubhouse.TButton", background=[("active", self.BORDER)])
        style.configure(
            "Post.TButton",
            background=self.ACCENT,
            foreground="#07111f",
            bordercolor=self.ACCENT,
            padding=(18, 8),
            font=("Sans", 10, "bold"),
        )
        style.map("Post.TButton", background=[("active", "#79b8ff")])
        style.configure(
            "Clubhouse.TCombobox",
            fieldbackground=self.CARD,
            background=self.CARD,
            foreground=self.TEXT,
            arrowcolor=self.TEXT,
        )

    def _build_ui(self) -> None:
        shell = tk.Frame(self.root, bg=self.BG)
        shell.pack(fill="both", expand=True)

        sidebar = tk.Frame(shell, bg=self.SIDEBAR, width=225)
        sidebar.pack(side="left", fill="y")
        sidebar.pack_propagate(False)
        tk.Label(
            sidebar,
            text="AIvengers",
            bg=self.SIDEBAR,
            fg=self.TEXT,
            font=("Sans", 18, "bold"),
            anchor="w",
        ).pack(fill="x", padx=20, pady=(22, 0))
        tk.Label(
            sidebar,
            text="CLUBHOUSE",
            bg=self.SIDEBAR,
            fg=self.ACCENT,
            font=("Sans", 9, "bold"),
            anchor="w",
        ).pack(fill="x", padx=20, pady=(2, 22))

        self.channel_list = tk.Listbox(
            sidebar,
            bg=self.SIDEBAR,
            fg=self.MUTED,
            selectbackground=self.PANEL,
            selectforeground=self.TEXT,
            borderwidth=0,
            highlightthickness=0,
            font=("Sans", 11),
            exportselection=False,
        )
        self.channel_list.pack(fill="both", expand=True, padx=10)
        self.channel_list.bind("<<ListboxSelect>>", self._select_channel)
        self._replace_channels(self.channel_names)

        self.connection_label = tk.Label(
            sidebar,
            text="● connecting",
            bg=self.SIDEBAR,
            fg=self.ORANGE,
            font=("Sans", 9),
            anchor="w",
        )
        self.connection_label.pack(fill="x", padx=20, pady=18)

        main = tk.Frame(shell, bg=self.BG)
        main.pack(side="left", fill="both", expand=True)

        header = tk.Frame(main, bg=self.BG, height=74)
        header.pack(fill="x")
        header.pack_propagate(False)
        self.channel_title = tk.Label(
            header,
            text="# general",
            bg=self.BG,
            fg=self.TEXT,
            font=("Sans", 16, "bold"),
            anchor="w",
        )
        self.channel_title.pack(side="left", padx=24)
        self.status_label = tk.Label(
            header,
            text="",
            bg=self.BG,
            fg=self.MUTED,
            font=("Sans", 9),
            anchor="e",
        )
        self.status_label.pack(side="right", padx=24)

        history_shell = tk.Frame(main, bg=self.BG)
        history_shell.pack(fill="both", expand=True, padx=(18, 12))
        self.canvas = tk.Canvas(
            history_shell,
            bg=self.BG,
            borderwidth=0,
            highlightthickness=0,
        )
        scrollbar = ttk.Scrollbar(history_shell, orient="vertical", command=self.canvas.yview)
        self.canvas.configure(yscrollcommand=scrollbar.set)
        scrollbar.pack(side="right", fill="y")
        self.canvas.pack(side="left", fill="both", expand=True)
        self.message_frame = tk.Frame(self.canvas, bg=self.BG)
        self.message_window = self.canvas.create_window(
            (0, 0), window=self.message_frame, anchor="nw"
        )
        self.message_frame.bind("<Configure>", self._sync_scroll_region)
        self.canvas.bind("<Configure>", self._sync_message_width)
        self.canvas.bind_all("<MouseWheel>", self._mousewheel)

        composer = tk.Frame(main, bg=self.PANEL, highlightbackground=self.BORDER, highlightthickness=1)
        composer.pack(fill="x", padx=24, pady=(12, 22))
        top = tk.Frame(composer, bg=self.PANEL)
        top.pack(fill="x", padx=14, pady=(10, 5))
        tk.Label(
            top,
            text="Post as",
            bg=self.PANEL,
            fg=self.MUTED,
            font=("Sans", 9),
        ).pack(side="left")
        self.route_var = tk.StringVar(value=next(iter(self.service.routes.values())).label)
        self.route_combo = ttk.Combobox(
            top,
            textvariable=self.route_var,
            values=[route.label for route in self.service.routes.values()],
            state="readonly",
            width=26,
            style="Clubhouse.TCombobox",
        )
        self.route_combo.pack(side="left", padx=(8, 12))
        self.reply_label = tk.Label(
            top,
            text="",
            bg=self.PANEL,
            fg=self.ACCENT,
            font=("Sans", 9),
        )
        self.reply_label.pack(side="left")
        self.cancel_reply_button = ttk.Button(
            top,
            text="Cancel reply",
            style="Clubhouse.TButton",
            command=self._cancel_reply,
        )

        body_row = tk.Frame(composer, bg=self.PANEL)
        body_row.pack(fill="x", padx=14, pady=(4, 12))
        self.body_text = tk.Text(
            body_row,
            height=4,
            wrap="word",
            bg=self.CARD,
            fg=self.TEXT,
            insertbackground=self.TEXT,
            selectbackground=self.ACCENT,
            borderwidth=0,
            highlightthickness=0,
            padx=12,
            pady=10,
            font=("Sans", 10),
        )
        self.body_text.pack(side="left", fill="both", expand=True)
        self.body_text.bind("<Control-Return>", self._post_from_key)
        self.post_button = ttk.Button(
            body_row, text="Post", style="Post.TButton", command=self.post
        )
        self.post_button.pack(side="right", padx=(12, 0), anchor="s")

    def _replace_channels(self, channels: list[str]) -> None:
        selection = self.current_channel
        self.channel_list.delete(0, "end")
        for channel in channels:
            self.channel_list.insert("end", f"#  {channel}")
        try:
            index = channels.index(selection)
        except ValueError:
            index = 0
            self.current_channel = channels[0]
        self.channel_list.selection_set(index)
        self.channel_list.activate(index)

    def _select_channel(self, _event: tk.Event[Any]) -> None:
        selection = self.channel_list.curselection()
        if not selection:
            return
        channel = self.channel_names[selection[0]]
        if channel == self.current_channel:
            return
        self.current_channel = channel
        self.channel_title.configure(text=f"# {channel}")
        self._cancel_reply()
        self._render_messages()
        self.refresh(immediate=True)

    def _sync_scroll_region(self, _event: tk.Event[Any]) -> None:
        self.canvas.configure(scrollregion=self.canvas.bbox("all"))

    def _sync_message_width(self, event: tk.Event[Any]) -> None:
        self.canvas.itemconfigure(self.message_window, width=event.width)

    def _mousewheel(self, event: tk.Event[Any]) -> None:
        self.canvas.yview_scroll(int(-1 * (event.delta / 120)), "units")

    def _schedule_refresh(self) -> None:
        if self.closed:
            return
        if self.refresh_after_id is not None:
            self.root.after_cancel(self.refresh_after_id)
        self.refresh_after_id = self.root.after(self.refresh_ms, self.refresh)

    def refresh(self, *, immediate: bool = False) -> None:
        del immediate
        self.refresh_after_id = None
        if self.closed:
            return
        if self.busy:
            self.pending_refresh = True
            return
        channel = self.current_channel
        cached = self.messages.setdefault(channel, [])
        after = cached[-1]["id"] if cached else 0

        def load() -> tuple[list[str], list[dict[str, Any]]]:
            self.service.ping()
            return self.service.channels(), self.service.read_channel(channel, after=after)

        self._start_job(load, lambda result: self._apply_refresh(channel, result))

    def _apply_refresh(
        self,
        channel: str,
        result: tuple[list[str], list[dict[str, Any]]],
    ) -> None:
        channels, incoming = result
        if channels != self.channel_names:
            self.channel_names = channels
            self._replace_channels(channels)
        if incoming:
            self.messages.setdefault(channel, []).extend(incoming)
        if channel == self.current_channel and incoming:
            self._render_messages(scroll_to_end=True)
        self.connection_label.configure(text="● connected", fg=self.GREEN)
        self.status_label.configure(
            text=f"{len(self.messages.get(self.current_channel, []))} messages · auto-refresh on"
        )

    def _start_job(
        self,
        function: Callable[[], Any],
        on_success: Callable[[Any], None],
        on_error: Callable[[Exception], None] | None = None,
    ) -> None:
        if self.busy or self.closed:
            return
        self.busy = True
        future = self.executor.submit(function)
        self.root.after(35, self._poll_job, future, on_success, on_error)

    def _poll_job(
        self,
        future: Future[Any],
        on_success: Callable[[Any], None],
        on_error: Callable[[Exception], None] | None,
    ) -> None:
        if self.closed:
            return
        if not future.done():
            self.root.after(35, self._poll_job, future, on_success, on_error)
            return
        self.busy = False
        try:
            result = future.result()
        except Exception as exc:
            self.connection_label.configure(text="● disconnected", fg="#f85149")
            self.status_label.configure(text=str(exc))
            if on_error is not None:
                on_error(exc)
        else:
            on_success(result)
        if self.pending_refresh:
            self.pending_refresh = False
            self.root.after(0, self.refresh)
        else:
            self._schedule_refresh()

    def _render_messages(self, *, scroll_to_end: bool = False) -> None:
        for child in self.message_frame.winfo_children():
            child.destroy()
        messages = self.messages.get(self.current_channel, [])
        if not messages:
            tk.Label(
                self.message_frame,
                text="Nobody has said anything here yet. Suspicious.",
                bg=self.BG,
                fg=self.MUTED,
                font=("Sans", 11),
            ).pack(pady=80)
            return
        for message in messages:
            self._render_message(message)
        self.message_frame.update_idletasks()
        if scroll_to_end:
            self.canvas.yview_moveto(1.0)

    def _render_message(self, message: dict[str, Any]) -> None:
        is_reply = message["reply_to"] is not None
        outer = tk.Frame(self.message_frame, bg=self.BG)
        outer.pack(fill="x", padx=(42 if is_reply else 4, 16), pady=6)
        card = tk.Frame(
            outer,
            bg=self.CARD_REPLY if is_reply else self.CARD,
            highlightbackground=self.BORDER,
            highlightthickness=1,
        )
        card.pack(fill="x")
        header = tk.Frame(card, bg=card["bg"])
        header.pack(fill="x", padx=14, pady=(10, 3))
        sender = str(message["sender"]).upper()
        tk.Label(
            header,
            text=sender,
            bg=card["bg"],
            fg=self.ACCENT,
            font=("Sans", 10, "bold"),
        ).pack(side="left")
        tk.Label(
            header,
            text=f"  #{message['id']}",
            bg=card["bg"],
            fg=self.MUTED,
            font=("Sans", 9),
        ).pack(side="left")
        if is_reply:
            tk.Label(
                header,
                text=f"↳ reply to #{message['reply_to']}",
                bg=card["bg"],
                fg=self.MUTED,
                font=("Sans", 9),
            ).pack(side="left", padx=(10, 0))
        ttk.Button(
            header,
            text="Reply",
            style="Clubhouse.TButton",
            command=lambda item=message: self._set_reply(item),
        ).pack(side="right")
        tk.Label(
            card,
            text=message["body"],
            bg=card["bg"],
            fg=self.TEXT,
            justify="left",
            anchor="w",
            wraplength=780,
            font=("Sans", 10),
        ).pack(fill="x", padx=14, pady=(4, 8))
        metadata = (
            f"{message['created_at']}  ·  {message['transport_actor']}  ·  "
            f"{message['provenance']}  ·  authority={message['authority']}  ·  "
            f"owner={str(message['owner']).lower()}"
        )
        tk.Label(
            card,
            text=metadata,
            bg=card["bg"],
            fg=self.ORANGE if message["provenance"] == "forwarded" else self.MUTED,
            justify="left",
            anchor="w",
            font=("Monospace", 8),
        ).pack(fill="x", padx=14, pady=(0, 10))

    def _set_reply(self, message: dict[str, Any]) -> None:
        self.reply_to = int(message["id"])
        self.reply_label.configure(text=f"Replying to {message['sender']} #{message['id']}")
        self.cancel_reply_button.pack(side="left", padx=(8, 0))
        self.body_text.focus_set()

    def _cancel_reply(self) -> None:
        self.reply_to = None
        self.reply_label.configure(text="")
        self.cancel_reply_button.pack_forget()

    def _post_from_key(self, _event: tk.Event[Any]) -> str:
        self.post()
        return "break"

    def post(self) -> None:
        if self.busy:
            return
        body = self.body_text.get("1.0", "end").strip()
        if not body:
            self.status_label.configure(text="Write something first.")
            return
        route_label = self.route_var.get()
        route_key = self.route_labels.get(route_label)
        if route_key is None:
            messagebox.showerror("Unknown identity", "Choose a configured Clubhouse identity.")
            return
        channel = self.current_channel
        reply_to = self.reply_to
        self.post_button.configure(state="disabled")

        def send() -> dict[str, Any]:
            return self.service.post(
                route_key=route_key,
                channel=channel,
                body=body,
                reply_to=reply_to,
            )

        def posted(message: dict[str, Any]) -> None:
            self.post_button.configure(state="normal")
            self.body_text.delete("1.0", "end")
            self._cancel_reply()
            cached = self.messages.setdefault(channel, [])
            if not cached or cached[-1]["id"] < message["id"]:
                cached.append(message)
            if channel == self.current_channel:
                self._render_messages(scroll_to_end=True)
            self.status_label.configure(text=f"Posted message #{message['id']}")

        def failed(_exc: Exception) -> None:
            self.post_button.configure(state="normal")

        self._start_job(send, posted, failed)

    def close(self) -> None:
        if self.closed:
            return
        self.closed = True
        if self.refresh_after_id is not None:
            try:
                self.root.after_cancel(self.refresh_after_id)
            except tk.TclError:
                pass
        self.executor.shutdown(wait=False, cancel_futures=True)
        self.root.destroy()


def build_parser() -> argparse.ArgumentParser:
    parser = argparse.ArgumentParser(description="AIvengers Clubhouse GUI")
    parser.add_argument("--state-dir", type=Path, default=default_state_dir())
    parser.add_argument(
        "--seat",
        default="human",
        help="seat this window posts as directly (default: human)",
    )
    parser.add_argument(
        "--forward-seat",
        help="gateway seat used to forward for --forward-for (example: gateway)",
    )
    parser.add_argument(
        "--forward-for",
        help="remote logical identity forwarded through --forward-seat (example: remote)",
    )
    parser.add_argument("--refresh-ms", type=int, default=1_500)
    parser.add_argument(
        "--smoke-test",
        action="store_true",
        help=argparse.SUPPRESS,
    )
    return parser


def main(argv: list[str] | None = None) -> int:
    args = build_parser().parse_args(argv)
    routes = build_identity_routes(
        args.seat, forward_seat=args.forward_seat, forward_for=args.forward_for
    )
    root = tk.Tk()
    service = ClubhouseService(
        WireStore(args.state_dir.expanduser().resolve()), seat=args.seat, routes=routes
    )
    app = ClubhouseApp(root, service, refresh_ms=args.refresh_ms)
    if args.smoke_test:
        root.after(900, app.close)
    root.mainloop()
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
