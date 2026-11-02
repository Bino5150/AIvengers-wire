"""Kernel-reported credentials of the process on the far end of a Unix socket.

The relay records which local process connected (PID, UID, GID) as message
provenance. Each supported OS reports this through a different socket option:

- Linux: ``SO_PEERCRED`` returns ``struct ucred { pid_t pid; uid_t uid;
  gid_t gid; }``, captured when the peer connected.
- macOS: ``LOCAL_PEERCRED`` returns ``struct xucred``, captured when the peer
  connected. It carries the effective UID and the group list, whose first entry
  (``cr_groups[0]``) is the effective GID. ``LOCAL_PEERPID`` returns the PID.

On any other platform, or when the kernel's reply is not exactly the expected
shape, the helper raises ``WireError`` instead of guessing. A recorded
credential is either what the kernel reported or nothing at all.
"""

from __future__ import annotations

import socket
import struct
import sys
from typing import Any

from .errors import WireError
from .storage import PeerCredentials

UNAVAILABLE = "Unix peer credentials are unavailable"

# Linux <sys/socket.h>: struct ucred { pid_t pid; uid_t uid; gid_t gid; }.
_LINUX_UCRED = struct.Struct("3i")

# macOS <sys/un.h>. Each is read from the socket module when this Python build
# exposes it, and falls back to the header value otherwise.
_DARWIN_SOL_LOCAL = 0
_DARWIN_LOCAL_PEERCRED = 0x001
_DARWIN_LOCAL_PEERPID = 0x002

# macOS <sys/ucred.h>:
#   struct xucred { u_int cr_version; uid_t cr_uid; short cr_ngroups;
#                   gid_t cr_groups[NGROUPS]; };   with NGROUPS == 16
# The two pad bytes after cr_ngroups are the C alignment of cr_groups, so the
# struct is 76 bytes.
_DARWIN_XUCRED_VERSION = 0
_DARWIN_NGROUPS = 16
_DARWIN_XUCRED = struct.Struct(f"=IIh2x{_DARWIN_NGROUPS}I")
# LOCAL_PEERPID returns a bare pid_t.
_DARWIN_PID = struct.Struct("=i")


def read_peer_credentials(sock: Any) -> PeerCredentials:
    """Return the kernel-reported credentials of ``sock``'s connected peer.

    Raises ``WireError`` when this platform has no supported mechanism or the
    kernel's reply cannot be decoded exactly. It never returns placeholder
    values.
    """
    if sock is None:
        raise WireError(UNAVAILABLE)
    if sys.platform.startswith("linux"):
        return _linux_peer_credentials(sock)
    if sys.platform == "darwin":
        return _darwin_peer_credentials(sock)
    # Other systems either lack these options or use another layout (OpenBSD's
    # SO_PEERCRED orders uid, gid, pid), so decoding them as above would
    # record the wrong values.
    raise WireError(UNAVAILABLE)


def _linux_peer_credentials(sock: Any) -> PeerCredentials:
    optname = getattr(socket, "SO_PEERCRED", None)
    if optname is None:
        raise WireError(UNAVAILABLE)
    pid, uid, gid = _getsockopt(sock, socket.SOL_SOCKET, optname, _LINUX_UCRED)
    return PeerCredentials(pid=pid, uid=uid, gid=gid)


def _darwin_peer_credentials(sock: Any) -> PeerCredentials:
    level = getattr(socket, "SOL_LOCAL", _DARWIN_SOL_LOCAL)
    version, uid, ngroups, *groups = _getsockopt(
        sock,
        level,
        getattr(socket, "LOCAL_PEERCRED", _DARWIN_LOCAL_PEERCRED),
        _DARWIN_XUCRED,
    )
    if version != _DARWIN_XUCRED_VERSION:
        raise WireError(f"{UNAVAILABLE}: unsupported xucred version {version}")
    if not 1 <= ngroups <= _DARWIN_NGROUPS:
        # No group list means no effective GID to record.
        raise WireError(f"{UNAVAILABLE}: xucred reports {ngroups} groups")
    (pid,) = _getsockopt(
        sock,
        level,
        getattr(socket, "LOCAL_PEERPID", _DARWIN_LOCAL_PEERPID),
        _DARWIN_PID,
    )
    return PeerCredentials(pid=pid, uid=uid, gid=groups[0])


def _getsockopt(
    sock: Any, level: int, optname: int, layout: struct.Struct
) -> tuple[Any, ...]:
    try:
        raw = sock.getsockopt(level, optname, layout.size)
    except OSError as exc:
        raise WireError(f"{UNAVAILABLE}: {exc}") from exc
    if len(raw) != layout.size:
        raise WireError(
            f"{UNAVAILABLE}: kernel returned {len(raw)} bytes, expected {layout.size}"
        )
    return layout.unpack(raw)
