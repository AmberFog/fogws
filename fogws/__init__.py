"""FogWS public package surface."""

from . import _fogws
from .client import Connection, connect


DEFAULT_CLOSE_TIMEOUT = _fogws.DEFAULT_CLOSE_TIMEOUT
DEFAULT_MAX_BUFFERED_BYTES = _fogws.DEFAULT_MAX_BUFFERED_BYTES
DEFAULT_MAX_MESSAGE_SIZE = _fogws.DEFAULT_MAX_MESSAGE_SIZE
DEFAULT_MAX_QUEUE = _fogws.DEFAULT_MAX_QUEUE
ConcurrencyError = _fogws.ConcurrencyError
ConnectionClosed = _fogws.ConnectionClosed
ConnectionClosedError = _fogws.ConnectionClosedError
ConnectionClosedOK = _fogws.ConnectionClosedOK
ConnectionFailedError = _fogws.ConnectionFailedError
FogWSError = _fogws.FogWSError
InvalidURIError = _fogws.InvalidURIError
LoopAffinityError = _fogws.LoopAffinityError
ResourceLimitError = _fogws.ResourceLimitError
RuntimeContextError = _fogws.RuntimeContextError
__version__ = _fogws.__version__


__all__ = (
    "DEFAULT_CLOSE_TIMEOUT",
    "DEFAULT_MAX_BUFFERED_BYTES",
    "DEFAULT_MAX_MESSAGE_SIZE",
    "DEFAULT_MAX_QUEUE",
    "ConcurrencyError",
    "Connection",
    "ConnectionClosed",
    "ConnectionClosedError",
    "ConnectionClosedOK",
    "ConnectionFailedError",
    "FogWSError",
    "InvalidURIError",
    "LoopAffinityError",
    "ResourceLimitError",
    "RuntimeContextError",
    "__version__",
    "connect",
)
