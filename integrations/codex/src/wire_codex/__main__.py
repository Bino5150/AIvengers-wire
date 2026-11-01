"""Explicit configuration only; never discover live credentials or start a relay."""
from __future__ import annotations
import argparse
import json
import sys
from pathlib import Path


def main() -> int:
    parser = argparse.ArgumentParser(description="Wire messaging MCP server or synchronous Codex notice hook")
    parser.add_argument("mode", choices=["mcp", "notice"])
    parser.add_argument("--state-dir", type=Path, required=True)
    parser.add_argument("--seat", required=True)
    parser.add_argument("--token-path", type=Path)
    parser.add_argument("--notice-dir", type=Path)
    args = parser.parse_args()
    try:
        from .adapter import Adapter, Transport
        transport = Transport(args.state_dir, args.seat, args.token_path)
        if args.mode == "notice":
            from .notice import notice
            if args.notice_dir is None or not args.notice_dir.is_absolute():
                return 0
            raw = sys.stdin.buffer.read(65_537)
            if len(raw) > 65_536:
                return 0
            from aivengers_wire.protocol import _strict_object
            event = json.loads(raw.decode("utf-8"), object_pairs_hook=_strict_object)
            result = notice(transport, args.notice_dir, event)
            if result:
                print(json.dumps(result, ensure_ascii=True))
        else:
            import anyio
            from .server import serve
            anyio.run(serve, Adapter(transport))
        return 0
    except (KeyboardInterrupt, BrokenPipeError):
        return 0
    except Exception:
        if args.mode == "notice":
            return 0
        print("Wire adapter could not start; check absolute configuration paths and installed dependencies.", file=sys.stderr)
        return 1


if __name__ == "__main__":
    raise SystemExit(main())
