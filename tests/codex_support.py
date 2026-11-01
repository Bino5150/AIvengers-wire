"""Disposable-only fixtures shared by adapter and installed-Codex acceptance."""
from __future__ import annotations
import json
import os
import shutil
import signal
import socket
import subprocess
import sys
import tempfile
import threading
import time
from pathlib import Path

ROOT = Path(__file__).resolve().parents[1]
ADAPTER_SRC = ROOT / "integrations" / "codex" / "src"


class Relay:
    def __enter__(self):
        from aivengers_wire.client import WireClient
        from aivengers_wire.storage import WireStore
        self.temp = tempfile.TemporaryDirectory(prefix="w2-t-", dir="/tmp")
        self.path = Path(self.temp.name)
        self.store = WireStore(self.path / "s")
        self.store.initialize()
        for seat in ["codex-a", "codex-b", "ordinary"]:
            self.store.add_seat(seat)
        self.env = {**os.environ, "AIVENGERS_WIRE_STATE_DIR": str(self.store.state_dir),
                    "PYTHONPATH": os.pathsep.join([str(ROOT / "src"), str(ADAPTER_SRC)])}
        self.process = subprocess.Popen([sys.executable, "-m", "aivengers_wire", "--state-dir", str(self.store.state_dir), "serve"],
                                        cwd=self.path, env=self.env, stdout=subprocess.DEVNULL, stderr=subprocess.PIPE)
        try:
            for _ in range(200):
                if self.process.poll() is not None:
                    raise RuntimeError("disposable relay failed to start")
                try:
                    self.client("codex-a").request({"op": "ping"})
                    break
                except Exception:
                    time.sleep(.01)
            else:
                raise RuntimeError("disposable relay did not become ready")
        except BaseException:
            self.__exit__(None, None, None)
            raise
        return self

    def client(self, seat):
        from aivengers_wire.client import WireClient
        return WireClient(self.store, seat=seat)

    def __exit__(self, *args):
        if self.process.poll() is None:
            self.process.send_signal(signal.SIGINT)
            try:
                self.process.wait(timeout=5)
            except subprocess.TimeoutExpired:
                self.process.kill(); self.process.wait(timeout=5)
        self.process.stderr.close()
        self.temp.cleanup()


class DropProxy:
    """Forward real authenticated commits, then discard a bounded number of receipts."""
    def __init__(self, relay, drops):
        self.relay = relay
        self.drops = drops
        self.keys = []
        self.path = relay.path / "p"
        self.path.mkdir()
        shutil.copytree(relay.store.seat_dir, self.path / "seats")
        self.sock = socket.socket(socket.AF_UNIX)
        self.sock.bind(str(self.path / "wire.sock"))
        self.sock.listen(4); self.sock.settimeout(.05)
        self.stop = threading.Event()
        self.thread = threading.Thread(target=self.run)

    def __enter__(self):
        self.thread.start(); return self

    def run(self):
        while not self.stop.is_set():
            try:
                conn, _ = self.sock.accept()
            except socket.timeout:
                continue
            with conn, socket.socket(socket.AF_UNIX) as upstream:
                conn.settimeout(2); upstream.settimeout(2)
                upstream.connect(str(self.relay.store.socket_path))
                with conn.makefile("rwb", buffering=0) as incoming, upstream.makefile("rwb", buffering=0) as outgoing:
                    try:
                        outgoing.write(incoming.readline(65_537))
                        incoming.write(outgoing.readline(262_145))
                        raw = incoming.readline(65_537)
                        payload = json.loads(raw)
                        self.keys.append(payload.get("client_id"))
                        outgoing.write(raw)
                        reply = outgoing.readline(262_145)
                        if payload.get("op") == "post" and self.drops:
                            self.drops -= 1
                        else:
                            incoming.write(reply)
                    except (OSError, ValueError):
                        continue

    def __exit__(self, *args):
        self.stop.set(); self.thread.join(timeout=4); self.sock.close()
        if self.thread.is_alive():
            raise RuntimeError("test proxy failed to stop")
