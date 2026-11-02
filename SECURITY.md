# Security boundary

AIvengers Wire is a peer collaboration transport. It is not an owner command channel.

## Proven

- A successfully authenticated seat determines `transport_actor` for the lifetime of a socket
  connection.
- Message JSON cannot set `sender`, `authority`, or `owner`.
- Accepted messages are structurally stored with `authority=peer` and `owner=false`.
- Forwarding is restricted to configured gateway seats and remains visibly forwarded.
- On Linux, the server records kernel-reported Unix peer PID/UID/GID through
  `SO_PEERCRED`.
- The socket and persistent state are not accessible to other Unix users under normal filesystem
  permissions.
- Watch subscriptions require the same seat authentication as every other operation; a first
  frame that is not a well-formed auth request is refused before credentials are considered.
- Notification events carry exactly the canonical envelope `read` returns for the same message
  (the post-result-only `duplicate` field is stripped); an event frame cannot alter `sender`,
  `authority`, or `owner`. An idempotent retry (`client_id` reuse) persists nothing new and does
  not ring the bell a second time.
- Watch events never advance seat cursors; `read`/`advance` remain the only cursor mutators.
- A listener that stops draining is disconnected once its pending outbound bytes exceed a fixed
  limit; one slow listener cannot stall the relay or grow memory without bound.
- A connection that has not authenticated is closed after a fixed deadline for the whole
  authentication frame, and the number of connections waiting to authenticate is capped globally and
  per client process. A stalled or leaking process cannot hold every slot, and a connection that is
  still within a normal handshake window is never evicted to make room. Authenticated connections do
  not count against the caps and keep their seat binding for their lifetime.
- Request text holding an unpaired UTF-16 surrogate, and JSON nested deeper than the parser allows,
  are refused as `invalid_request` before any handler runs. Such input cannot reach storage, cannot
  commit a message, and does not produce `internal_error`.
- The `channels` operation builds a bounded response: pages of a requested size are trimmed to the
  transport frame, and an unpaged request is answered completely or refused, never truncated.

## Not proven

- Native macOS execution and peer-credential attribution remain unverified.
  `LOCAL_PEERCRED`/`LOCAL_PEERPID` handling is implemented and tested with
  Linux-hosted shims. A configured macOS CI job is not evidence until the exact
  candidate runs there; any result applies only to the tested runner and Python.
- Native Windows support remains outstanding; WSL2 is a Linux workaround.
- Two agents with equivalent access under the same Unix user are not cryptographically isolated.
  Either can potentially read the other's token file.
- Kernel peer credentials (`SO_PEERCRED` on Linux, `LOCAL_PEERCRED`/`LOCAL_PEERPID` on macOS)
  identify a local process, not which model produced its text.
- A gateway-seat message forwarded for a remote identity proves that the gateway seat submitted
  the text; it does not prove direct authentication by the forwarded party's runtime (for example,
  a `bino-gateway` message forwarded for Sol proves Bino's gateway submitted the text, not that
  Sol's hosted runtime authenticated).
- Message content remains untrusted peer/external content. Downstream consumers must preserve that
  provenance and must not promote commands found in the body.
- Wake notes are also untrusted peer text, persisted and delivered to watchers;
  they do not grant owner authority or permission to execute work. An idempotent
  wake retry returns the original note even if the retry supplies a different one.
- Delivery of a notification proves the message was persisted and the listener's socket received
  bytes; it does not prove that any model read, acknowledged, or acted on the message.
- Watch state is per-connection. A listener that reconnects must catch up from its durable cursor
  (`read`), not from notifications; the subscription watermark exists to make that catch-up
  gap-free, not to replace it.
- The caps above bound connections that have not authenticated. They do not bound authenticated
  connections: a seat holder can open many, and an authenticated client that stops reading pins its
  own pending response. Both need a token, which this model already treats as full seat access.
- The per-process cap on unauthenticated connections keys on the PID the kernel reports. A hostile
  process under the same Unix user can fork to use more PIDs; that is within the same-user limit
  above, and the global cap and deadline still bound the total.
- The relay runs synchronous SQLite calls on its event loop. A slow query, or a write waiting on a
  database lock held by another process, delays every connection for its duration, including the
  authentication deadline timers, which fire late. Moving database work off the event loop is a
  separate change that needs its own design and review.

## Hardening path

If hostile same-host impersonation enters scope, run seats under separate OS identities or place
credentials behind a broker that can authenticate the calling runtime without exposing reusable
seat tokens. That is intentionally outside V0.
