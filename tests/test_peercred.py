"""Peer-credential capture on Linux (SO_PEERCRED) and macOS (LOCAL_PEERCRED).

The real-socket test runs the current platform's branch against the kernel.
The fake-socket tests cover every branch on any machine, so the macOS code is
exercised on Linux CI and the Linux code on macOS.
"""

from __future__ import annotations

import asyncio
import json
import os
import socket
import struct
import sys
import tempfile
import threading
import types
import unittest
from pathlib import Path
from unittest import mock

from aivengers_wire import peercred
from aivengers_wire.client import WireClient
from aivengers_wire.errors import WireError
from aivengers_wire.peercred import peer_credentials
from aivengers_wire.server import WireServer
from aivengers_wire.storage import WireStore

SUPPORTED = sys.platform.startswith("linux") or sys.platform == "darwin"


def _xucred(*, version: int = 0, uid: int = 501, groups: tuple[int, ...] = (20, 12, 61)) -> bytes:
    padded = list(groups) + [0] * (16 - len(groups))
    return struct.pack("=IIh2x16I", version, uid, len(groups), *padded)


class FakeSocket:
    """Answers getsockopt from a table keyed by (level, option)."""

    def __init__(self, replies: dict[tuple[int, int], bytes | Exception]):
        self.replies = replies
        self.calls: list[tuple[int, int, int]] = []

    def getsockopt(self, level: int, option: int, buflen: int) -> bytes:
        self.calls.append((level, option, buflen))
        reply = self.replies[(level, option)]
        if isinstance(reply, Exception):
            raise reply
        return reply


@unittest.skipUnless(SUPPORTED, "peer credentials are only supported on Linux and macOS")
class RealSocketTestCase(unittest.TestCase):
    def test_kernel_reports_this_process_through_a_listening_socket(self) -> None:
        # A real bind/listen/connect/accept, the way the relay receives clients
        # (socketpair is not used: platforms differ on whether it records creds).
        with tempfile.TemporaryDirectory() as temp:
            path = str(Path(temp) / "p.sock")
            server = socket.socket(socket.AF_UNIX, socket.SOCK_STREAM)
            client = socket.socket(socket.AF_UNIX, socket.SOCK_STREAM)
            try:
                server.bind(path)
                server.listen(1)
                connector = threading.Thread(target=client.connect, args=(path,))
                connector.start()
                accepted, _ = server.accept()
                connector.join(timeout=5)
                try:
                    creds = peer_credentials(accepted)
                finally:
                    accepted.close()
            finally:
                client.close()
                server.close()
        self.assertEqual(creds.pid, os.getpid())
        self.assertEqual(creds.uid, os.geteuid())
        self.assertEqual(creds.gid, os.getegid())


class DarwinBranchTestCase(unittest.TestCase):
    def test_reads_uid_and_effective_gid_from_xucred_and_pid_separately(self) -> None:
        sock = FakeSocket({(0, 0x001): _xucred(uid=501, groups=(20, 12)), (0, 0x002): struct.pack("@i", 4242)})
        creds = peer_credentials(sock, platform="darwin")
        self.assertEqual((creds.pid, creds.uid, creds.gid), (4242, 501, 20))
        self.assertEqual(sock.calls, [(0, 0x001, 76), (0, 0x002, 4)])

    def test_unexpected_xucred_version_is_refused(self) -> None:
        sock = FakeSocket({(0, 0x001): _xucred(version=1), (0, 0x002): struct.pack("@i", 1)})
        with self.assertRaisesRegex(WireError, "xucred version"):
            peer_credentials(sock, platform="darwin")

    def test_group_count_outside_one_to_sixteen_is_refused(self) -> None:
        empty = FakeSocket({(0, 0x001): _xucred(groups=()), (0, 0x002): struct.pack("@i", 1)})
        with self.assertRaisesRegex(WireError, "reports 0 groups"):
            peer_credentials(empty, platform="darwin")
        too_many = struct.pack("=IIh2x16I", 0, 501, 17, *([20] * 16))
        overfull = FakeSocket({(0, 0x001): too_many, (0, 0x002): struct.pack("@i", 1)})
        with self.assertRaisesRegex(WireError, "reports 17 groups"):
            peer_credentials(overfull, platform="darwin")
        self.assertEqual(overfull.calls, [(0, 0x001, 76)], "no pid lookup after a refused xucred")

    def test_xucred_layout_matches_the_c_struct(self) -> None:
        self.assertEqual(peercred._DARWIN_XUCRED.size, 76)
        self.assertEqual(peercred._DARWIN_PID.size, 4)

    def test_replies_of_the_wrong_size_are_refused(self) -> None:
        short_cred = FakeSocket({(0, 0x001): b"\0" * 8, (0, 0x002): struct.pack("@i", 1)})
        with self.assertRaisesRegex(WireError, "LOCAL_PEERCRED returned 8 bytes, expected 76"):
            peer_credentials(short_cred, platform="darwin")
        long_cred = FakeSocket({(0, 0x001): _xucred() + b"\0", (0, 0x002): struct.pack("@i", 1)})
        with self.assertRaisesRegex(WireError, "returned 77 bytes"):
            peer_credentials(long_cred, platform="darwin")
        short_pid = FakeSocket({(0, 0x001): _xucred(), (0, 0x002): b"\0"})
        with self.assertRaisesRegex(WireError, "LOCAL_PEERPID returned 1 bytes, expected 4"):
            peer_credentials(short_pid, platform="darwin")

    def test_kernel_errors_become_wire_errors(self) -> None:
        sock = FakeSocket({(0, 0x001): OSError(57, "Socket is not connected")})
        with self.assertRaisesRegex(WireError, "not connected"):
            peer_credentials(sock, platform="darwin")

    def test_unreadable_pid_is_refused_not_fabricated(self) -> None:
        sock = FakeSocket(
            {(0, 0x001): _xucred(), (0, 0x002): OSError(57, "Socket is not connected")}
        )
        with self.assertRaisesRegex(WireError, "not connected"):
            peer_credentials(sock, platform="darwin")

    def test_full_group_list_is_accepted(self) -> None:
        groups = tuple(range(100, 116))
        sock = FakeSocket({(0, 0x001): _xucred(uid=0, groups=groups), (0, 0x002): struct.pack("@i", 1)})
        creds = peer_credentials(sock, platform="darwin")
        self.assertEqual((creds.pid, creds.uid, creds.gid), (1, 0, 100))


class LinuxBranchTestCase(unittest.TestCase):
    def test_reads_pid_uid_gid_from_ucred(self) -> None:
        fake_socket_module = types.SimpleNamespace(SOL_SOCKET=1, SO_PEERCRED=17)
        sock = FakeSocket({(1, 17): struct.pack("3i", 99, 1000, 1001)})
        with mock.patch.object(peercred, "socket", fake_socket_module):
            creds = peer_credentials(sock, platform="linux")
        self.assertEqual((creds.pid, creds.uid, creds.gid), (99, 1000, 1001))
        self.assertEqual(sock.calls, [(1, 17, 12)])

    def test_missing_so_peercred_is_refused(self) -> None:
        with mock.patch.object(peercred, "socket", types.SimpleNamespace(SOL_SOCKET=1)):
            with self.assertRaisesRegex(WireError, "SO_PEERCRED is missing"):
                peer_credentials(FakeSocket({}), platform="linux")

    def test_short_reply_is_refused(self) -> None:
        fake_socket_module = types.SimpleNamespace(SOL_SOCKET=1, SO_PEERCRED=17)
        with mock.patch.object(peercred, "socket", fake_socket_module):
            with self.assertRaisesRegex(WireError, "SO_PEERCRED returned 4 bytes, expected 12"):
                peer_credentials(FakeSocket({(1, 17): b"\0" * 4}), platform="linux")

    def test_kernel_errors_become_wire_errors(self) -> None:
        fake_socket_module = types.SimpleNamespace(SOL_SOCKET=1, SO_PEERCRED=17)
        sock = FakeSocket({(1, 17): OSError(92, "Protocol not available")})
        with mock.patch.object(peercred, "socket", fake_socket_module):
            with self.assertRaisesRegex(WireError, "unavailable: .*Protocol not available"):
                peer_credentials(sock, platform="linux")


class UnsupportedPlatformTestCase(unittest.TestCase):
    def test_other_platforms_are_refused_not_guessed(self) -> None:
        for platform in ("win32", "freebsd14", "openbsd7", "cygwin"):
            with self.subTest(platform=platform):
                sock = FakeSocket({})
                with self.assertRaisesRegex(WireError, platform):
                    peer_credentials(sock, platform=platform)
                self.assertEqual(sock.calls, [])


class InProcessRelayTestCase(unittest.TestCase):
    """Runs WireServer on a background event loop and talks to it as a client."""

    def setUp(self) -> None:
        temp = tempfile.TemporaryDirectory()
        self.addCleanup(temp.cleanup)
        self.store = WireStore(Path(temp.name) / "state")
        self.store.initialize()
        self.store.add_seat("tech")
        loop = asyncio.new_event_loop()
        thread = threading.Thread(target=loop.run_forever, daemon=True)
        thread.start()
        server = WireServer(self.store)

        def stop() -> None:
            asyncio.run_coroutine_threadsafe(server.close(), loop).result(5)
            loop.call_soon_threadsafe(loop.stop)
            thread.join(5)
            loop.close()

        self.addCleanup(stop)
        asyncio.run_coroutine_threadsafe(server.start(), loop).result(5)
        self.client = WireClient(self.store, seat="tech")

    @unittest.skipUnless(SUPPORTED, "peer credentials are only supported on Linux and macOS")
    def test_ping_succeeds_and_posts_record_the_kernel_peer(self) -> None:
        self.assertTrue(self.client.request({"op": "ping"})["ok"])
        posted = self.client.request(
            {"op": "post", "channel": "general", "body": "hello"}
        )["message"]
        self.assertEqual(
            posted["peer"],
            {"pid": os.getpid(), "uid": os.geteuid(), "gid": os.getegid()},
        )

    def test_refusal_reaches_the_client_on_unsupported_platforms(self) -> None:
        # The relay reads credentials on accept, before any frame. The test
        # only reads, so the relay closing first cannot fail a client write.
        with mock.patch.object(sys, "platform", "sunos5"):
            with socket.socket(socket.AF_UNIX, socket.SOCK_STREAM) as sock:
                sock.settimeout(5)
                sock.connect(str(self.store.socket_path))
                with sock.makefile("rb") as stream:
                    frame = json.loads(stream.readline())
        self.assertEqual(
            frame,
            {
                "ok": False,
                "error": "operation_failed",
                "message": "Unix peer credentials are unavailable on platform 'sunos5'",
            },
        )


if __name__ == "__main__":
    unittest.main()
