"""Kernel-reported credentials of the process on the other end of a Unix socket.

The relay records who connected (PID, UID, GID) as provenance. The kernel
supplies these values, not the client, so a peer cannot choose them. Each
supported platform asks the kernel differently:

- Linux: ``SO_PEERCRED`` returns ``struct ucred { pid_t pid; uid_t uid;
  gid_t gid; }``.
- macOS: ``LOCAL_PEERCRED`` returns ``struct xucred`` (UID and groups; the
  first group is the effective GID) and ``LOCAL_PEERPID`` returns the PID.
  CPython does not always expose these constants, so the values from
  ``<sys/un.h>`` are used when it does not.

Any other platform is refused explicitly rather than given made-up values.
"""

from __future__ import annotations

import socket
import struct
import sys
from typing import Any

from .errors import WireError
from .storage import PeerCredentials

_LINUX_UCRED = struct.Struct("3i")

# macOS <sys/un.h> and <sys/ucred.h>. The socket module's values win when this
# Python build exposes them.
_DARWIN_SOL_LOCAL = getattr(socket, "SOL_LOCAL", 0)
_DARWIN_LOCAL_PEERCRED = getattr(socket, "LOCAL_PEERCRED", 0x001)
_DARWIN_LOCAL_PEERPID = getattr(socket, "LOCAL_PEERPID", 0x002)
_DARWIN_XUCRED_VERSION = 0
_DARWIN_NGROUPS = 16
# struct xucred { u_int cr_version; uid_t cr_uid; short cr_ngroups;
#                 gid_t cr_groups[16]; }  -- 2 pad bytes align cr_groups; 76 bytes.
_DARWIN_XUCRED = struct.Struct(f"=IIh2x{_DARWIN_NGROUPS}I")
_DARWIN_PID = struct.Struct("=i")


def peer_credentials(sock: Any, *, platform: str | None = None) -> PeerCredentials:
    """Return the connected peer's PID, UID and GID, or raise :class:`WireError`.

    ``sock`` is a connected ``AF_UNIX`` stream socket (or anything with a
    compatible ``getsockopt``). ``platform`` defaults to ``sys.platform`` and
    exists so tests can exercise every branch on one machine.
    """
    platform = sys.platform if platform is None else platform
    try:
        if platform.startswith("linux"):
            return _linux(sock)
        if platform == "darwin":
            return _darwin(sock)
    except OSError as exc:
        raise WireError(f"Unix peer credentials are unavailable: {exc}") from exc
    raise WireError(f"Unix peer credentials are unavailable on platform {platform!r}")


def _linux(sock: Any) -> PeerCredentials:
    option = getattr(socket, "SO_PEERCRED", None)
    if option is None:
        raise WireError("Unix peer credentials are unavailable: SO_PEERCRED is missing")
    pid, uid, gid = _getsockopt(sock, socket.SOL_SOCKET, option, _LINUX_UCRED, "SO_PEERCRED")
    return PeerCredentials(pid=pid, uid=uid, gid=gid)


def _darwin(sock: Any) -> PeerCredentials:
    version, uid, ngroups, *groups = _getsockopt(
        sock, _DARWIN_SOL_LOCAL, _DARWIN_LOCAL_PEERCRED, _DARWIN_XUCRED, "LOCAL_PEERCRED"
    )
    if version != _DARWIN_XUCRED_VERSION:
        raise WireError(
            f"Unix peer credentials are unavailable: unexpected xucred version {version}"
        )
    if not 1 <= ngroups <= _DARWIN_NGROUPS:
        # No group list means no effective GID to record.
        raise WireError(
            f"Unix peer credentials are unavailable: xucred reports {ngroups} groups"
        )
    (pid,) = _getsockopt(
        sock, _DARWIN_SOL_LOCAL, _DARWIN_LOCAL_PEERPID, _DARWIN_PID, "LOCAL_PEERPID"
    )
    return PeerCredentials(pid=pid, uid=uid, gid=groups[0])


def _getsockopt(
    sock: Any, level: int, option: int, layout: struct.Struct, name: str
) -> tuple[Any, ...]:
    """Read one option and decode it, refusing any reply that is not exactly ``layout``."""
    raw = sock.getsockopt(level, option, layout.size)
    if len(raw) != layout.size:
        raise WireError(
            f"Unix peer credentials are unavailable: {name} returned {len(raw)} bytes, "
            f"expected {layout.size}"
        )
    return layout.unpack(raw)
