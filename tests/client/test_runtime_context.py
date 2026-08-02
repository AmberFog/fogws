import asyncio
import contextlib
import gc
import importlib
import importlib.util
import multiprocessing
from multiprocessing.connection import Connection as PipeConnection
import subprocess
import sys
import textwrap
from typing import Any
import warnings

import pytest

import fogws

from .models import LoopbackEndpoint


_INHERITED_CONNECTION: fogws.Connection | None = None

_FIRST_SUBINTERPRETER_IMPORT_PROBE = textwrap.dedent(
    """
    import importlib
    import importlib.util

    module_name = (
        "_interpreters"
        if importlib.util.find_spec("_interpreters") is not None
        else "_xxsubinterpreters"
    )
    interpreters = importlib.import_module(module_name)
    interpreter = interpreters.create()
    try:
        if module_name == "_interpreters":
            result = interpreters.exec(interpreter, "import fogws")
            if result is None:
                raise AssertionError("subinterpreter import unexpectedly succeeded")
            if result.type.__module__ != "builtins" or result.type.__name__ != "ImportError":
                raise AssertionError(result)
            if "does not support loading in subinterpreters" not in result.msg:
                raise AssertionError(result.msg)
        else:
            try:
                interpreters.run_string(interpreter, "import fogws")
            except interpreters.RunFailedError as error:
                diagnostic = str(error)
                if "ImportError" not in diagnostic:
                    raise AssertionError(diagnostic) from error
                if "does not support loading in subinterpreters" not in diagnostic:
                    raise AssertionError(diagnostic) from error
            else:
                raise AssertionError("subinterpreter import unexpectedly succeeded")
    finally:
        interpreters.destroy(interpreter)

    import fogws
    import fogws._fogws as native

    assert fogws.FogWSError("main-interpreter-probe").args == ("main-interpreter-probe",)
    assert native._Connection.__module__ == "fogws._fogws"
    """,
)


def _run_fork_probe(sender: PipeConnection) -> None:
    try:
        asyncio.run(fogws.connect("ws://127.0.0.1:1/fork-probe"))
    except fogws.RuntimeContextError:
        sender.send("RuntimeContextError")
    else:
        sender.send("missing fail-closed guard")
    finally:
        sender.close()


def _run_inherited_receive_probe(sender: PipeConnection) -> None:
    global _INHERITED_CONNECTION  # noqa: PLW0603  # Fork fixture transfers one inherited owner.
    connection = _INHERITED_CONNECTION
    _INHERITED_CONNECTION = None
    if connection is None:
        sender.send("missing inherited connection")
        sender.close()
        return

    # Poll exactly once: the context guard must fail before the first native await.
    receive = connection.receive()
    try:
        receive.send(None)
    except fogws.RuntimeContextError:
        outcome = "RuntimeContextError"
    except BaseException as error:  # noqa: BLE001  # Child must report the exact fail-closed outcome.
        outcome = f"{type(error).__name__}: {error}"
    else:
        outcome = "missing fail-closed guard"
    finally:
        del connection
        gc.collect()
        sender.send(f"{outcome}; finalized")
        sender.close()


@pytest.mark.skipif(
    "fork" not in multiprocessing.get_all_start_methods(),
    reason="fork start method isn't available on this platform",
)
def test_runtime_use_after_fork_fails_closed() -> None:
    context = multiprocessing.get_context("fork")
    receiver, sender = context.Pipe(duplex=False)
    process = context.Process(target=_run_fork_probe, args=(sender,))
    process_started = False

    try:
        with warnings.catch_warnings():
            warnings.filterwarnings(
                "ignore",
                message=".*multi-threaded.*fork.*",
                category=DeprecationWarning,
            )
            process.start()
        process_started = True
        sender.close()
        process.join(timeout=3)

        assert receiver.poll(timeout=1)
        assert receiver.recv() == "RuntimeContextError"
        assert process.exitcode == 0
    finally:
        sender.close()
        receiver.close()
        if process_started:
            if process.is_alive():
                process.terminate()
                process.join(timeout=1)
            if process.is_alive():
                process.kill()
                process.join(timeout=1)
            if not process.is_alive():
                process.close()


@pytest.mark.asyncio
@pytest.mark.skipif(
    "fork" not in multiprocessing.get_all_start_methods(),
    reason="fork start method isn't available on this platform",
)
async def test_retained_receive_use_after_fork_fails_closed(
    loopback_endpoint: LoopbackEndpoint,
) -> None:
    global _INHERITED_CONNECTION  # noqa: PLW0603  # Cleared after the bounded fork probe.

    def create_retained_receive() -> tuple[asyncio.AbstractEventLoop, fogws.Connection]:
        owner_loop = asyncio.new_event_loop()
        asyncio.set_event_loop(owner_loop)
        try:
            connection = owner_loop.run_until_complete(fogws.connect(loopback_endpoint.uri))
            receive_task = owner_loop.create_task(connection.receive())
            owner_loop.run_until_complete(asyncio.sleep(0))
            receive_task.cancel()
            with contextlib.suppress(asyncio.CancelledError):
                owner_loop.run_until_complete(receive_task)
        finally:
            asyncio.set_event_loop(None)
        return owner_loop, connection

    owner_loop, connection = await asyncio.to_thread(create_retained_receive)
    context = multiprocessing.get_context("fork")
    receiver, sender = context.Pipe(duplex=False)
    _INHERITED_CONNECTION = connection
    process = context.Process(target=_run_inherited_receive_probe, args=(sender,))
    process_started = False

    try:
        with warnings.catch_warnings():
            warnings.filterwarnings(
                "ignore",
                message=".*multi-threaded.*fork.*",
                category=DeprecationWarning,
            )
            process.start()
        process_started = True
        sender.close()
        process.join(timeout=3)

        assert receiver.poll(timeout=1)
        assert receiver.recv() == "RuntimeContextError; finalized"
        assert process.exitcode == 0

        await asyncio.to_thread(
            owner_loop.run_until_complete,
            connection.send_text("parent-after-child-finalization"),
        )
        message = await asyncio.to_thread(
            owner_loop.run_until_complete,
            connection.receive(),
        )
        assert message == "parent-after-child-finalization"
    finally:
        _INHERITED_CONNECTION = None
        sender.close()
        receiver.close()
        if process_started:
            if process.is_alive():
                process.terminate()
                process.join(timeout=1)
            if process.is_alive():
                process.kill()
                process.join(timeout=1)
            if not process.is_alive():
                process.close()
        with contextlib.suppress(fogws.ConnectionClosed):
            await asyncio.to_thread(owner_loop.run_until_complete, connection.close())
        owner_loop.close()


@pytest.mark.asyncio
async def test_runtime_identity_survives_sys_modules_rebinding(
    loopback_endpoint: LoopbackEndpoint,
) -> None:
    connection = await fogws.connect(loopback_endpoint.uri)
    original_modules = sys.modules
    sys.modules = dict(original_modules)
    try:
        await connection.send_text("stable-interpreter-identity")
        assert await connection.receive() == "stable-interpreter-identity"
        await connection.close()
    finally:
        sys.modules = original_modules


def test_second_subinterpreter_is_rejected_at_import() -> None:
    module_name = "_interpreters" if importlib.util.find_spec("_interpreters") is not None else "_xxsubinterpreters"
    interpreters: Any = importlib.import_module(module_name)
    interpreter = interpreters.create()
    try:
        if module_name == "_interpreters":
            result = interpreters.exec(interpreter, "import fogws")
        else:
            with pytest.raises(
                interpreters.RunFailedError,
                match=r"ImportError.*does not support loading in subinterpreters",
            ):
                interpreters.run_string(interpreter, "import fogws")
            return
    finally:
        interpreters.destroy(interpreter)

    assert result is not None
    assert result.type.__module__ == "builtins"
    assert result.type.__name__ == "ImportError"
    assert "does not support loading in subinterpreters" in result.msg


def test_first_import_from_subinterpreter_is_rejected() -> None:
    result = subprocess.run(  # noqa: S603  # Exact current interpreter, fixed probe source.
        [sys.executable, "-c", _FIRST_SUBINTERPRETER_IMPORT_PROBE],
        check=False,
        capture_output=True,
        text=True,
        timeout=5,
    )

    assert result.returncode == 0, result.stderr
    assert result.stdout == ""
    assert result.stderr == ""
