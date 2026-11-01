"""Wire error types."""


class WireError(Exception):
    """A request or local wire operation failed safely."""


class WireResponseTooLargeError(WireError):
    """A server response exceeded the client's bounded transport frame."""


class ProtocolError(WireError):
    """A peer sent a malformed or unsupported request."""


class AuthenticationError(WireError):
    """A peer could not authenticate as its requested seat."""
