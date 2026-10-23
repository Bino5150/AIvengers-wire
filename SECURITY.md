# Security boundary

AIvengers Wire is a peer collaboration transport. It is not an owner command channel.

## Proven

- A successfully authenticated seat determines `transport_actor` for the lifetime of a socket
  connection.
- Message JSON cannot set `sender`, `authority`, or `owner`.
- Accepted messages are structurally stored with `authority=peer` and `owner=false`.
- Forwarding is restricted to configured gateway seats and remains visibly forwarded.
- The server records kernel-reported Unix peer PID/UID/GID.
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

## Not proven

- Two agents with equivalent access under the same Unix user are not cryptographically isolated.
  Either can potentially read the other's token file.
- `SO_PEERCRED` identifies a local process, not which model produced its text.
- A gateway-seat message forwarded for a remote identity proves that the gateway seat submitted
  the text; it does not prove direct authentication by the forwarded party's runtime (for example,
  a `bino-gateway` message forwarded for Sol proves Bino's gateway submitted the text, not that
  Sol's hosted runtime authenticated).
- Message content remains untrusted peer/external content. Downstream consumers must preserve that
  provenance and must not promote commands found in the body.
- Delivery of a notification proves the message was persisted and the listener's socket received
  bytes; it does not prove that any model read, acknowledged, or acted on the message.
- Watch state is per-connection. A listener that reconnects must catch up from its durable cursor
  (`read`), not from notifications; the subscription watermark exists to make that catch-up
  gap-free, not to replace it.

## Hardening path

If hostile same-host impersonation enters scope, run seats under separate OS identities or place
credentials behind a broker that can authenticate the calling runtime without exposing reusable
seat tokens. That is intentionally outside V0.
