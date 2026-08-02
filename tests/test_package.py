import importlib
from importlib.metadata import version
import pickle

import pytest

import fogws


NORMAL_CLOSE_CODE = 1000


def test_native_and_python_versions_match() -> None:
    assert fogws.__version__ == version("fogws")


def test_public_surface_is_explicit() -> None:
    assert fogws.__all__ == (
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


@pytest.mark.parametrize(
    "exception_type",
    [
        fogws.FogWSError,
        fogws.ConnectionClosed,
        fogws.ConnectionClosedError,
        fogws.ConnectionClosedOK,
        fogws.ConcurrencyError,
        fogws.ConnectionFailedError,
        fogws.InvalidURIError,
        fogws.LoopAffinityError,
        fogws.ResourceLimitError,
        fogws.RuntimeContextError,
    ],
)
def test_public_exceptions_have_importable_pickle_identity(
    exception_type: type[fogws.FogWSError],
) -> None:
    assert exception_type.__module__ == "fogws"
    assert getattr(importlib.import_module(exception_type.__module__), exception_type.__name__) is exception_type

    error = exception_type("diagnostic")
    restored = pickle.loads(pickle.dumps(error))  # noqa: S301  # Trusted local round-trip.
    assert type(restored) is exception_type
    assert restored.args == error.args


@pytest.mark.parametrize(
    "exception_type",
    [
        fogws.ConnectionClosed,
        fogws.ConnectionClosedError,
        fogws.ConnectionClosedOK,
    ],
)
def test_public_close_exceptions_have_typed_defaults(
    exception_type: type[fogws.ConnectionClosed],
) -> None:
    error = exception_type("diagnostic")
    assert error.code is None
    assert error.reason == ""
    assert error.initiated_by_local is None

    restored = pickle.loads(pickle.dumps(error))  # noqa: S301  # Trusted local round-trip.
    assert restored.code is None
    assert restored.reason == ""
    assert restored.initiated_by_local is None


def test_pickled_close_exception_preserves_typed_metadata() -> None:
    error = fogws.ConnectionClosedOK("clean close")
    error.code = NORMAL_CLOSE_CODE
    error.reason = "done"
    error.initiated_by_local = False

    restored = pickle.loads(pickle.dumps(error))  # noqa: S301  # Trusted local round-trip.
    assert type(restored) is fogws.ConnectionClosedOK
    assert restored.code == error.code
    assert restored.reason == error.reason
    assert restored.initiated_by_local is error.initiated_by_local
