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

# macOS <sys/un.h> and <sys/ucred.h>.
_DARWIN_SOL_LOCAL = 0
_DARWIN_LOCAL_PEERCRED = getattr(socket, "LOCAL_PEERCRED", 0x001)
_DARWIN_LOCAL_PEERPID = getattr(socket, "LOCAL_PEERPID", 0x002)
_DARWIN_XUCRED_VERSION = 0
# struct xucred { u_int cr_version; uid_t cr_uid; short cr_ngroups;
#                 gid_t cr_groups[16]; }  -- native alignment pads after the short.
_DARWIN_XUCRED = struct.Struct("@IIh16I")
_DARWIN_PID = struct.Struct("@i")


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
    raw = sock.getsockopt(socket.SOL_SOCKET, option, _LINUX_UCRED.size)
    if len(raw) < _LINUX_UCRED.size:
        raise WireError("Unix peer credentials are unavailable: short SO_PEERCRED reply")
    pid, uid, gid = _LINUX_UCRED.unpack_from(raw)
    return PeerCredentials(pid=pid, uid=uid, gid=gid)


def _darwin(sock: Any) -> PeerCredentials:
    raw = sock.getsockopt(_DARWIN_SOL_LOCAL, _DARWIN_LOCAL_PEERCRED, _DARWIN_XUCRED.size)
    if len(raw) < _DARWIN_XUCRED.size:
        raise WireError("Unix peer credentials are unavailable: short LOCAL_PEERCRED reply")
    version, uid, ngroups, *groups = _DARWIN_XUCRED.unpack_from(raw)
    if version != _DARWIN_XUCRED_VERSION:
        raise WireError(
            f"Unix peer credentials are unavailable: unexpected xucred version {version}"
        )
    if ngroups < 1:
        raise WireError("Unix peer credentials are unavailable: peer reported no groups")
    raw_pid = sock.getsockopt(_DARWIN_SOL_LOCAL, _DARWIN_LOCAL_PEERPID, _DARWIN_PID.size)
    if len(raw_pid) < _DARWIN_PID.size:
        raise WireError("Unix peer credentials are unavailable: short LOCAL_PEERPID reply")
    (pid,) = _DARWIN_PID.unpack_from(raw_pid)
    return PeerCredentials(pid=pid, uid=uid, gid=groups[0])
