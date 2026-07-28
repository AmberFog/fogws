import asyncio
import multiprocessing
from multiprocessing.connection import Connection as PipeConnection
import warnings

import pytest

import fogws


def _run_fork_probe(sender: PipeConnection) -> None:
    try:
        asyncio.run(fogws.connect("ws://127.0.0.1:1/fork-probe"))
    except fogws.RuntimeContextError:
        sender.send("RuntimeContextError")
    else:
        sender.send("missing fail-closed guard")
    finally:
        sender.close()


@pytest.mark.skipif(
    "fork" not in multiprocessing.get_all_start_methods(),
    reason="fork start method isn't available on this platform",
)
def test_runtime_use_after_fork_fails_closed() -> None:
    context = multiprocessing.get_context("fork")
    receiver, sender = context.Pipe(duplex=False)
    process = context.Process(target=_run_fork_probe, args=(sender,))

    with warnings.catch_warnings():
        warnings.filterwarnings(
            "ignore",
            message=".*multi-threaded.*fork.*",
            category=DeprecationWarning,
        )
        process.start()
    sender.close()
    process.join(timeout=3)

    assert process.exitcode == 0
    assert receiver.poll(timeout=1)
    assert receiver.recv() == "RuntimeContextError"
    receiver.close()
