"""A scripted stand-in for the Anthropic Messages API, for adapter end-to-end tests.

Serves on 127.0.0.1 only. Each response is decided by a script, so a real
Claude Code process can be driven through MCP tool calls with no model
inference and no network traffic. Every request is recorded.
"""

from __future__ import annotations

import json
import threading
from http.server import BaseHTTPRequestHandler, ThreadingHTTPServer
from typing import Any, Callable

#: A step receives the parsed JSON text of the previous tool result (or None)
#: and returns ``(short_tool_name, input)``.
Step = Callable[[Any], tuple[str, dict[str, Any]]]


class FakeMessagesApi:
    def __init__(self) -> None:
        self.requests: list[dict[str, Any]] = []
        self.unhandled: list[str] = []
        self.script: list[Step] = []
        self.calls = 0
        self._lock = threading.Lock()
        api = self

        class Handler(BaseHTTPRequestHandler):
            def log_message(self, *args: Any) -> None:
                pass

            def do_GET(self) -> None:  # noqa: N802
                api.unhandled.append(f"GET {self.path}")
                self._json(404, {"type": "error", "error": {"type": "not_found_error", "message": "fake"}})

            def do_HEAD(self) -> None:  # noqa: N802
                self.send_response(200)
                self.end_headers()

            def do_POST(self) -> None:  # noqa: N802
                length = int(self.headers.get("content-length") or 0)
                body = json.loads(self.rfile.read(length) or b"{}")
                if not self.path.split("?")[0].endswith("/v1/messages"):
                    api.unhandled.append(f"POST {self.path}")
                    if self.path.split("?")[0].endswith("/count_tokens"):
                        self._json(200, {"input_tokens": 1})
                    else:
                        self._json(404, {"type": "error", "error": {"type": "not_found_error", "message": "fake"}})
                    return
                with api._lock:
                    api.requests.append(body)
                    content = api._next(body)
                if body.get("stream"):
                    self._stream(body, content)
                else:
                    self._json(200, api._message(body, content))

            def _json(self, status: int, value: dict[str, Any]) -> None:
                data = json.dumps(value).encode()
                self.send_response(status)
                self.send_header("content-type", "application/json")
                self.send_header("content-length", str(len(data)))
                self.end_headers()
                self.wfile.write(data)

            def _stream(self, body: dict[str, Any], content: dict[str, Any]) -> None:
                self.send_response(200)
                self.send_header("content-type", "text/event-stream")
                self.send_header("cache-control", "no-cache")
                self.end_headers()
                message = api._message(body, content)
                start = dict(message, content=[], stop_reason=None)
                events: list[tuple[str, dict[str, Any]]] = [("message_start", {"type": "message_start", "message": start})]
                if content["type"] == "tool_use":
                    block = dict(content, input={})
                    delta = {"type": "input_json_delta", "partial_json": json.dumps(content["input"])}
                else:
                    block = {"type": "text", "text": ""}
                    delta = {"type": "text_delta", "text": content["text"]}
                events += [
                    ("content_block_start", {"type": "content_block_start", "index": 0, "content_block": block}),
                    ("content_block_delta", {"type": "content_block_delta", "index": 0, "delta": delta}),
                    ("content_block_stop", {"type": "content_block_stop", "index": 0}),
                    ("message_delta", {"type": "message_delta", "delta": {"stop_reason": message["stop_reason"], "stop_sequence": None}, "usage": {"output_tokens": 1}}),
                    ("message_stop", {"type": "message_stop"}),
                ]
                for name, data in events:
                    self.wfile.write(f"event: {name}\ndata: {json.dumps(data)}\n\n".encode())
                self.wfile.flush()

        self.server = ThreadingHTTPServer(("127.0.0.1", 0), Handler)
        self.url = f"http://127.0.0.1:{self.server.server_address[1]}"
        self.thread = threading.Thread(target=self.server.serve_forever, daemon=True)

    def __enter__(self) -> "FakeMessagesApi":
        self.thread.start()
        return self

    def __exit__(self, *exc: Any) -> None:
        self.server.shutdown()
        self.server.server_close()

    # ── scripting ──

    def run(self, script: list[Step]) -> None:
        with self._lock:
            self.script = list(script)
            self.calls = 0
            self.requests.clear()

    @staticmethod
    def tool_names(body: dict[str, Any]) -> list[str]:
        return [tool.get("name", "") for tool in body.get("tools", [])]

    @staticmethod
    def last_tool_result(body: dict[str, Any]) -> Any:
        for message in reversed(body.get("messages", [])):
            content = message.get("content")
            if not isinstance(content, list):
                continue
            for block in reversed(content):
                if block.get("type") == "tool_result":
                    inner = block.get("content")
                    text = inner if isinstance(inner, str) else "".join(
                        part.get("text", "") for part in inner or [] if part.get("type") == "text"
                    )
                    try:
                        return json.loads(text)
                    except ValueError:
                        return {"_raw": text, "_is_error": block.get("is_error")}
        return None

    def _next(self, body: dict[str, Any]) -> dict[str, Any]:
        # Only the main conversation (the one offered tools) follows the script;
        # anything else (titles, summaries) gets a plain answer.
        if not body.get("tools") or self.calls >= len(self.script):
            return {"type": "text", "text": "done"}
        short, args = self.script[self.calls](self.last_tool_result(body))
        names = [name for name in self.tool_names(body) if name.endswith("__" + short)]
        if len(names) != 1:
            return {"type": "text", "text": f"tool {short!r} not offered: {self.tool_names(body)}"}
        self.calls += 1
        return {"type": "tool_use", "id": f"toolu_fake_{self.calls:03d}", "name": names[0], "input": args}

    @staticmethod
    def _message(body: dict[str, Any], content: dict[str, Any]) -> dict[str, Any]:
        return {
            "id": "msg_fake",
            "type": "message",
            "role": "assistant",
            "model": body.get("model", "fake"),
            "content": [content],
            "stop_reason": "tool_use" if content["type"] == "tool_use" else "end_turn",
            "stop_sequence": None,
            "usage": {"input_tokens": 1, "output_tokens": 1},
        }
