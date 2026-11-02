"""Wire error types."""


class WireError(Exception):
    """A request or local wire operation failed safely."""

    #: The relay's machine-readable error code (``invalid_request``,
    #: ``operation_failed``, ...) when this error reports a relay refusal, so
    #: callers can tell refusals apart without matching message text.
    code: str | None = None


class WireResponseTooLargeError(WireError):
    """A server response exceeded the client's bounded transport frame."""


class ProtocolError(WireError):
    """A peer sent a malformed or unsupported request."""


class AuthenticationError(WireError):
    """A peer could not authenticate as its requested seat."""
