"""Official MCP SDK over bounded stdio. This module never starts inference."""
from __future__ import annotations

import json
import logging
import sys

import anyio
from mcp import types
from mcp.server import Server
from mcp.server.stdio import stdio_server

from .adapter import Adapter, AdapterError, INSTRUCTIONS, TOOLS

MCP_INPUT_BYTES = 262_144


class BoundedInput:
    """Preserve SDK lifecycle while bounding/redacting its input parser errors."""
    def __aiter__(self):
        return self.lines()

    async def lines(self):
        from aivengers_wire.protocol import _strict_object
        while True:
            raw = await anyio.to_thread.run_sync(sys.stdin.buffer.readline, MCP_INPUT_BYTES + 1)
            if not raw:
                return
            if len(raw) > MCP_INPUT_BYTES or not raw.endswith(b"\n"):
                # Terminate rather than draining an attacker-controlled unbounded line.
                return
            try:
                obj = json.loads(raw.decode("utf-8"), object_pairs_hook=_strict_object)
                if not isinstance(obj, dict):
                    raise ValueError()
                if "method" in obj:
                    if "id" in obj:
                        types.ClientRequest.model_validate(obj)
                    else:
                        types.ClientNotification.model_validate(obj)
                yield json.dumps(obj, ensure_ascii=True)
            except Exception:
                # The SDK may format parser errors; never give it original input.
                yield '{"jsonrpc":"2.0","id":null,"method":null}'


async def serve(adapter: Adapter) -> None:
    # SDK diagnostics can contain request values. Tool errors below are fixed,
    # structured and actionable, so suppress SDK logging in this stdio process.
    logging.disable(logging.CRITICAL)
    server = Server("aivengers_wire", version="0.1.0", instructions=INSTRUCTIONS)
    lock = anyio.Lock()

    @server.list_tools()
    async def list_tools():
        return [types.Tool(name=name, description=description,
                           inputSchema=model.model_json_schema(),
                           annotations=types.ToolAnnotations(readOnlyHint=read_only,
                               destructiveHint=False, idempotentHint=True, openWorldHint=True))
                for name, (model, description, read_only) in TOOLS.items()]

    @server.call_tool(validate_input=False)
    async def call_tool(name: str, arguments: dict):
        async with lock:
            try:
                result = await anyio.to_thread.run_sync(adapter.call, name, arguments)
                failed = False
            except AdapterError as exc:
                result, failed = exc.payload, True
            except Exception:
                result = {"error": "adapter_error", "message": "Adapter could not complete this operation; check the relay and configuration."}
                failed = True
        return types.CallToolResult(content=[types.TextContent(type="text", text=json.dumps(result, ensure_ascii=True))], isError=failed)

    async with stdio_server(stdin=BoundedInput()) as (read_stream, write_stream):
        await server.run(read_stream, write_stream, server.create_initialization_options())
