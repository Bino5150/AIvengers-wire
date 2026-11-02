"""Peer-credential capture on Linux (SO_PEERCRED) and macOS (LOCAL_PEERCRED).

The helper tests replace the platform and the socket module's constants and
answer getsockopt from a fake socket, so both branches run on any host. The
real-socket tests then check the branch for the host this suite runs on
against the kernel.
"""

from __future__ import annotations

import asyncio
import contextlib
import errno
import json
import os
import socket
import struct
import sys
import tempfile
import threading
import unittest
from pathlib import Path
from typing import Iterator
from unittest import mock

from aivengers_wire import peercred
from aivengers_wire.client import WireClient
from aivengers_wire.errors import WireError
from aivengers_wire.peercred import read_peer_credentials
from aivengers_wire.server import WireServer
from aivengers_wire.storage import PeerCredentials, WireStore

_MISSING = object()

# Every constant the helper may look up. Each test states which ones the
# simulated Python build exposes; the rest are absent.
_CONSTANTS = ("SO_PEERCRED", "SOL_LOCAL", "LOCAL_PEERCRED", "LOCAL_PEERPID")

# The header values, spelled independently of the module under test.
SOL_LOCAL = 0
LOCAL_PEERCRED = 0x001
LOCAL_PEERPID = 0x002
XUCRED_SIZE = 76  # sizeof(struct xucred) on macOS
LINUX_SO_PEERCRED = 17  # <asm-generic/socket.h>

SUPPORTED_HOST = sys.platform.startswith("linux") or sys.platform == "darwin"


@contextlib.contextmanager
def simulated_host(platform: str, **constants: int) -> Iterator[None]:
    """Run as ``platform`` with exactly ``constants`` defined on ``socket``."""
    saved = {name: getattr(socket, name, _MISSING) for name in _CONSTANTS}
    try:
        for name in _CONSTANTS:
            if name in constants:
                setattr(socket, name, constants[name])
            elif hasattr(socket, name):
                delattr(socket, name)
        with mock.patch.object(sys, "platform", platform):
            yield
    finally:
        for name, value in saved.items():
            if value is _MISSING:
                if hasattr(socket, name):
                    delattr(socket, name)
            else:
                setattr(socket, name, value)


class FakePeerSocket:
    """Answers getsockopt from a table and records every call."""

    def __init__(self, replies: dict[tuple[int, int], bytes | OSError]):
        self.replies = replies
        self.calls: list[tuple[int, int, int]] = []

    def getsockopt(self, level: int, optname: int, buflen: int) -> bytes:
        self.calls.append((level, optname, buflen))
        reply = self.replies[(level, optname)]
        if isinstance(reply, OSError):
            raise reply
        return reply


def ucred(pid: int, uid: int, gid: int) -> bytes:
    return struct.pack("=iII", pid, uid, gid)


def xucred(
    uid: int, groups: list[int], *, version: int = 0, ngroups: int | None = None
) -> bytes:
    padded = (groups + [0] * 16)[:16]
    count = len(groups) if ngroups is None else ngroups
    # u_int, uid_t, short, 2 pad bytes, gid_t[16]
    return struct.pack("=IIh2x16I", version, uid, count, *padded)


def pid_t(pid: int) -> bytes:
    return struct.pack("=i", pid)


class LinuxBranchTestCase(unittest.TestCase):
    def test_reads_ucred_through_so_peercred(self) -> None:
        sock = FakePeerSocket(
            {(socket.SOL_SOCKET, LINUX_SO_PEERCRED): ucred(4242, 1000, 1001)}
        )
        with simulated_host("linux", SO_PEERCRED=LINUX_SO_PEERCRED):
            peer = read_peer_credentials(sock)
        self.assertEqual(peer, PeerCredentials(pid=4242, uid=1000, gid=1001))
        self.assertEqual(sock.calls, [(socket.SOL_SOCKET, LINUX_SO_PEERCRED, 12)])

    def test_missing_so_peercred_is_refused_without_a_syscall(self) -> None:
        sock = FakePeerSocket({})
        with simulated_host("linux"):
            with self.assertRaisesRegex(WireError, "^Unix peer credentials are unavailable$"):
                read_peer_credentials(sock)
        self.assertEqual(sock.calls, [])

    def test_macos_constants_are_not_used_on_linux(self) -> None:
        sock = FakePeerSocket({})
        with simulated_host("linux", LOCAL_PEERCRED=LOCAL_PEERCRED):
            with self.assertRaisesRegex(WireError, "unavailable"):
                read_peer_credentials(sock)
        self.assertEqual(sock.calls, [])

    def test_truncated_reply_is_refused(self) -> None:
        sock = FakePeerSocket(
            {(socket.SOL_SOCKET, LINUX_SO_PEERCRED): ucred(1, 2, 3)[:8]}
        )
        with simulated_host("linux", SO_PEERCRED=LINUX_SO_PEERCRED):
            with self.assertRaisesRegex(WireError, "returned 8 bytes, expected 12"):
                read_peer_credentials(sock)

    def test_kernel_error_is_refused(self) -> None:
        sock = FakePeerSocket(
            {
                (socket.SOL_SOCKET, LINUX_SO_PEERCRED): OSError(
                    errno.ENOPROTOOPT, "Protocol not available"
                )
            }
        )
        with simulated_host("linux", SO_PEERCRED=LINUX_SO_PEERCRED):
            with self.assertRaisesRegex(WireError, "unavailable: .*Protocol not available"):
                read_peer_credentials(sock)


class DarwinBranchTestCase(unittest.TestCase):
    def test_header_values_are_used_when_python_exposes_none(self) -> None:
        sock = FakePeerSocket(
            {
                (SOL_LOCAL, LOCAL_PEERCRED): xucred(501, [20, 12, 61, 79]),
                (SOL_LOCAL, LOCAL_PEERPID): pid_t(31337),
            }
        )
        with simulated_host("darwin"):
            peer = read_peer_credentials(sock)
        # gid is cr_groups[0], the effective GID; the rest are supplementary.
        self.assertEqual(peer, PeerCredentials(pid=31337, uid=501, gid=20))
        self.assertEqual(
            sock.calls,
            [
                (SOL_LOCAL, LOCAL_PEERCRED, XUCRED_SIZE),
                (SOL_LOCAL, LOCAL_PEERPID, 4),
            ],
        )

    def test_constants_the_python_build_exposes_take_precedence(self) -> None:
        # Values deliberately differ from the header ones, so the call log
        # shows which source each option number came from.
        sock = FakePeerSocket(
            {
                (7, 0x101): xucred(502, [80]),
                (7, 0x102): pid_t(99),
            }
        )
        with simulated_host(
            "darwin", SOL_LOCAL=7, LOCAL_PEERCRED=0x101, LOCAL_PEERPID=0x102
        ):
            peer = read_peer_credentials(sock)
        self.assertEqual(peer, PeerCredentials(pid=99, uid=502, gid=80))
        self.assertEqual(sock.calls, [(7, 0x101, XUCRED_SIZE), (7, 0x102, 4)])

    def test_only_local_peercred_exposed_mixes_with_header_values(self) -> None:
        # The usual CPython-on-macOS case: LOCAL_PEERCRED is defined, while
        # SOL_LOCAL and LOCAL_PEERPID are not.
        sock = FakePeerSocket(
            {
                (SOL_LOCAL, LOCAL_PEERCRED): xucred(501, [20]),
                (SOL_LOCAL, LOCAL_PEERPID): pid_t(77),
            }
        )
        with simulated_host("darwin", LOCAL_PEERCRED=LOCAL_PEERCRED):
            peer = read_peer_credentials(sock)
        self.assertEqual(peer, PeerCredentials(pid=77, uid=501, gid=20))

    def test_full_group_list_is_accepted(self) -> None:
        sock = FakePeerSocket(
            {
                (SOL_LOCAL, LOCAL_PEERCRED): xucred(0, list(range(100, 116))),
                (SOL_LOCAL, LOCAL_PEERPID): pid_t(1),
            }
        )
        with simulated_host("darwin"):
            peer = read_peer_credentials(sock)
        self.assertEqual(peer, PeerCredentials(pid=1, uid=0, gid=100))

    def test_unknown_xucred_version_is_refused(self) -> None:
        sock = FakePeerSocket(
            {(SOL_LOCAL, LOCAL_PEERCRED): xucred(501, [20], version=1)}
        )
        with simulated_host("darwin"):
            with self.assertRaisesRegex(WireError, "unsupported xucred version 1"):
                read_peer_credentials(sock)
        self.assertEqual(len(sock.calls), 1, "no PID lookup after a refused xucred")

    def test_empty_group_list_is_refused(self) -> None:
        sock = FakePeerSocket({(SOL_LOCAL, LOCAL_PEERCRED): xucred(501, [])})
        with simulated_host("darwin"):
            with self.assertRaisesRegex(WireError, "xucred reports 0 groups"):
                read_peer_credentials(sock)

    def test_out_of_range_group_count_is_refused(self) -> None:
        for count in (-1, 17):
            with self.subTest(count=count):
                sock = FakePeerSocket(
                    {(SOL_LOCAL, LOCAL_PEERCRED): xucred(501, [20], ngroups=count)}
                )
                with simulated_host("darwin"):
                    with self.assertRaisesRegex(WireError, f"reports {count} groups"):
                        read_peer_credentials(sock)

    def test_truncated_xucred_is_refused(self) -> None:
        sock = FakePeerSocket(
            {(SOL_LOCAL, LOCAL_PEERCRED): xucred(501, [20])[:12]}
        )
        with simulated_host("darwin"):
            with self.assertRaisesRegex(WireError, "returned 12 bytes, expected 76"):
                read_peer_credentials(sock)

    def test_unreadable_pid_is_refused_not_fabricated(self) -> None:
        sock = FakePeerSocket(
            {
                (SOL_LOCAL, LOCAL_PEERCRED): xucred(501, [20]),
                (SOL_LOCAL, LOCAL_PEERPID): OSError(errno.ENOTCONN, "Socket is not connected"),
            }
        )
        with simulated_host("darwin"):
            with self.assertRaisesRegex(WireError, "unavailable: .*not connected"):
                read_peer_credentials(sock)

    def test_linux_option_is_not_used_on_macos(self) -> None:
        sock = FakePeerSocket(
            {
                (SOL_LOCAL, LOCAL_PEERCRED): xucred(501, [20]),
                (SOL_LOCAL, LOCAL_PEERPID): pid_t(5),
            }
        )
        with simulated_host("darwin", SO_PEERCRED=LINUX_SO_PEERCRED):
            read_peer_credentials(sock)
        self.assertNotIn(socket.SOL_SOCKET, [level for level, _, _ in sock.calls])

    def test_xucred_layout_matches_the_c_struct(self) -> None:
        self.assertEqual(peercred._DARWIN_XUCRED.size, XUCRED_SIZE)
        self.assertEqual(peercred._DARWIN_PID.size, 4)


class UnsupportedPlatformTestCase(unittest.TestCase):
    def test_other_platforms_refuse_even_when_so_peercred_exists(self) -> None:
        # OpenBSD defines SO_PEERCRED with a different field order, so reading
        # it as Linux's layout would record wrong values.
        for platform in ("openbsd7", "freebsd14", "sunos5", "win32", "cygwin"):
            with self.subTest(platform=platform):
                sock = FakePeerSocket({})
                with simulated_host(
                    platform, SO_PEERCRED=LINUX_SO_PEERCRED, LOCAL_PEERCRED=LOCAL_PEERCRED
                ):
                    with self.assertRaisesRegex(
                        WireError, "^Unix peer credentials are unavailable$"
                    ):
                        read_peer_credentials(sock)
                self.assertEqual(sock.calls, [])

    def test_missing_socket_is_refused(self) -> None:
        with self.assertRaisesRegex(WireError, "^Unix peer credentials are unavailable$"):
            read_peer_credentials(None)


@unittest.skipUnless(SUPPORTED_HOST, "peer credentials are supported on Linux and macOS")
class RealSocketTestCase(unittest.TestCase):
    def test_socketpair_reports_this_process(self) -> None:
        left, right = socket.socketpair(socket.AF_UNIX, socket.SOCK_STREAM)
        with left, right:
            peer = read_peer_credentials(left)
        self.assertEqual(
            peer,
            PeerCredentials(pid=os.getpid(), uid=os.geteuid(), gid=os.getegid()),
        )


class InProcessRelayTestCase(unittest.TestCase):
    """Runs the relay on a background event loop and talks to it as a client."""

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

    @unittest.skipUnless(SUPPORTED_HOST, "peer credentials are supported on Linux and macOS")
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
                "message": "Unix peer credentials are unavailable",
            },
        )


if __name__ == "__main__":
    unittest.main()
