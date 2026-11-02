"""Peer-credential capture on Linux (SO_PEERCRED) and macOS (LOCAL_PEERCRED).

The real-socket test runs the current platform's branch against the kernel.
The fake-socket tests cover every branch on any machine, so the macOS code is
exercised on Linux CI and the Linux code on macOS.
"""

from __future__ import annotations

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
from aivengers_wire.errors import WireError
from aivengers_wire.peercred import peer_credentials

SUPPORTED = sys.platform.startswith("linux") or sys.platform == "darwin"


def _xucred(*, version: int = 0, uid: int = 501, groups: tuple[int, ...] = (20, 12, 61)) -> bytes:
    padded = list(groups) + [0] * (16 - len(groups))
    return struct.pack("@IIh16I", version, uid, len(groups), *padded)


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

    def test_empty_group_list_is_refused(self) -> None:
        sock = FakeSocket({(0, 0x001): _xucred(groups=()), (0, 0x002): struct.pack("@i", 1)})
        with self.assertRaisesRegex(WireError, "no groups"):
            peer_credentials(sock, platform="darwin")

    def test_short_replies_are_refused(self) -> None:
        short_cred = FakeSocket({(0, 0x001): b"\0" * 8, (0, 0x002): struct.pack("@i", 1)})
        with self.assertRaisesRegex(WireError, "short LOCAL_PEERCRED"):
            peer_credentials(short_cred, platform="darwin")
        short_pid = FakeSocket({(0, 0x001): _xucred(), (0, 0x002): b"\0"})
        with self.assertRaisesRegex(WireError, "short LOCAL_PEERPID"):
            peer_credentials(short_pid, platform="darwin")

    def test_kernel_errors_become_wire_errors(self) -> None:
        sock = FakeSocket({(0, 0x001): OSError(57, "Socket is not connected")})
        with self.assertRaisesRegex(WireError, "not connected"):
            peer_credentials(sock, platform="darwin")


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
            with self.assertRaisesRegex(WireError, "short SO_PEERCRED"):
                peer_credentials(FakeSocket({(1, 17): b"\0" * 4}), platform="linux")


class UnsupportedPlatformTestCase(unittest.TestCase):
    def test_other_platforms_are_refused_not_guessed(self) -> None:
        for platform in ("win32", "freebsd14", "openbsd7", "cygwin"):
            with self.subTest(platform=platform):
                sock = FakeSocket({})
                with self.assertRaisesRegex(WireError, platform):
                    peer_credentials(sock, platform=platform)
                self.assertEqual(sock.calls, [])


if __name__ == "__main__":
    unittest.main()
