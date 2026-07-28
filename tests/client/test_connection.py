import asyncio
import contextlib
import gc
import socket
import sys
from weakref import ReferenceType, ref

import pytest

import fogws

from .constants import (
    EXPECTED_CLOSE_TIMEOUT,
    EXPECTED_ERROR_CLOSE_CODE,
    EXPECTED_MAX_BUFFERED_BYTES,
    EXPECTED_MAX_MESSAGE_SIZE,
    EXPECTED_MAX_QUEUE,
    EXPECTED_NORMAL_CLOSE_CODE,
)
from .models import (
    BackpressuredEndpoint,
    LoopbackEndpoint,
    StalledHandshakeEndpoint,
    UnresponsiveCloseEndpoint,
)


pytestmark = pytest.mark.timeout(5)

BACKPRESSURED_MESSAGE_BYTES = 8 * 1024 * 1024
BACKPRESSURE_SETTLE_SECONDS = 0.05
BOUNDED_CLOSE_TIMEOUT_SECONDS = 0.05


@pytest.mark.asyncio
async def test_text_and_binary_round_trip_through_rust(
    loopback_endpoint: LoopbackEndpoint,
) -> None:
    connection = await fogws.connect(loopback_endpoint.uri)

    await connection.send_text("hello")
    assert await connection.receive() == "hello"
    await connection.send_bytes(b"\x00\x01\xff")
    assert await connection.receive() == b"\x00\x01\xff"

    await connection.close()
    await connection.close()


@pytest.mark.asyncio
async def test_context_manager_closes_native_transport(
    loopback_endpoint: LoopbackEndpoint,
) -> None:
    async with await fogws.connect(loopback_endpoint.uri) as connection:
        await connection.send_text("context")
        assert await connection.receive() == "context"

    await asyncio.wait_for(loopback_endpoint.disconnected.wait(), timeout=1)


@pytest.mark.asyncio
async def test_normal_peer_close_has_exact_type(
    loopback_endpoint: LoopbackEndpoint,
) -> None:
    connection = await fogws.connect(loopback_endpoint.uri)
    await connection.send_text("close-normal")

    with pytest.raises(fogws.ConnectionClosedOK, match="code 1000") as error:
        await connection.receive()
    assert error.value.code == EXPECTED_NORMAL_CLOSE_CODE
    assert error.value.reason == "done"
    assert error.value.initiated_by_local is False

    await connection.close()


@pytest.mark.asyncio
async def test_abnormal_peer_close_has_exact_type(
    loopback_endpoint: LoopbackEndpoint,
) -> None:
    connection = await fogws.connect(loopback_endpoint.uri)
    await connection.send_text("close-error")

    with pytest.raises(fogws.ConnectionClosedError, match="code 1011") as error:
        await connection.receive()
    assert error.value.code == EXPECTED_ERROR_CLOSE_CODE
    assert error.value.reason == "failed"
    assert error.value.initiated_by_local is False

    with pytest.raises(fogws.ConnectionClosedError, match="code 1011"):
        await connection.close()


@pytest.mark.asyncio
async def test_connection_refusal_has_exact_type() -> None:
    with socket.socket() as reserved_socket:
        reserved_socket.bind(("127.0.0.1", 0))
        _, port = reserved_socket.getsockname()

    with pytest.raises(fogws.ConnectionFailedError, match="connection failed"):
        await fogws.connect(f"ws://127.0.0.1:{port}/refused")


@pytest.mark.asyncio
async def test_connect_cancellation_drops_stalled_transport(
    stalled_handshake_endpoint: StalledHandshakeEndpoint,
) -> None:
    connect_task = asyncio.create_task(fogws.connect(stalled_handshake_endpoint.uri))
    await asyncio.wait_for(stalled_handshake_endpoint.accepted.wait(), timeout=1)

    connect_task.cancel()
    with pytest.raises(asyncio.CancelledError):
        await connect_task

    await asyncio.wait_for(stalled_handshake_endpoint.disconnected.wait(), timeout=1)


@pytest.mark.asyncio
async def test_close_cancellation_keeps_native_timeout_cleanup_running(
    unresponsive_close_endpoint: UnresponsiveCloseEndpoint,
) -> None:
    connection = await fogws.connect(
        unresponsive_close_endpoint.uri,
        close_timeout=0.05,
    )
    await asyncio.wait_for(unresponsive_close_endpoint.upgraded.wait(), timeout=1)

    close_task = asyncio.create_task(connection.close())
    await asyncio.sleep(0)
    close_task.cancel()
    with pytest.raises(asyncio.CancelledError):
        await close_task

    await asyncio.wait_for(unresponsive_close_endpoint.disconnected.wait(), timeout=1)
    with pytest.raises(fogws.ConnectionClosedError, match="close timed out") as error:
        await connection.close()
    assert error.value.code is None
    assert error.value.reason == ""
    assert error.value.initiated_by_local is None


@pytest.mark.asyncio
async def test_peer_close_bounds_backpressured_writer_cleanup(
    backpressured_close_endpoint: BackpressuredEndpoint,
) -> None:
    connection, send_task = await _start_backpressured_send(backpressured_close_endpoint)
    backpressured_close_endpoint.trigger.set()

    with pytest.raises(fogws.ConnectionClosedOK):
        await asyncio.wait_for(connection.receive(), timeout=1)
    with pytest.raises(fogws.ConnectionClosedOK):
        await asyncio.wait_for(send_task, timeout=1)
    await connection.close()


@pytest.mark.asyncio
async def test_protocol_error_aborts_backpressured_writer(
    backpressured_protocol_error_endpoint: BackpressuredEndpoint,
) -> None:
    connection, send_task = await _start_backpressured_send(
        backpressured_protocol_error_endpoint,
    )
    backpressured_protocol_error_endpoint.trigger.set()

    with pytest.raises(fogws.ConnectionClosedError, match="WebSocket receive failed"):
        await asyncio.wait_for(connection.receive(), timeout=1)
    with pytest.raises(fogws.ConnectionClosedError, match="WebSocket receive failed"):
        await asyncio.wait_for(send_task, timeout=1)
    with pytest.raises(fogws.ConnectionClosedError, match="WebSocket receive failed"):
        await connection.close()


@pytest.mark.asyncio
async def test_receive_cancellation_does_not_consume_next_message(
    loopback_endpoint: LoopbackEndpoint,
) -> None:
    connection = await fogws.connect(loopback_endpoint.uri)
    receive_task = asyncio.create_task(connection.receive())
    await asyncio.sleep(0)
    receive_task.cancel()
    with pytest.raises(asyncio.CancelledError):
        await receive_task

    await connection.send_text("after-cancel")
    assert await connection.receive() == "after-cancel"
    await connection.close()


@pytest.mark.asyncio
async def test_receive_cancellation_after_native_completion_is_lossless(
    loopback_endpoint: LoopbackEndpoint,
) -> None:
    connection = await fogws.connect(loopback_endpoint.uri)
    receive_task = asyncio.create_task(connection.receive())
    await asyncio.sleep(0)
    pending_receive = connection._pending_receive  # noqa: SLF001
    assert pending_receive is not None

    def cancel_waiter(_: asyncio.Future[str | bytes]) -> None:
        receive_task.cancel()

    pending_receive.add_done_callback(cancel_waiter)
    await connection.send_text("ready-during-cancel")
    with pytest.raises(asyncio.CancelledError):
        await receive_task

    assert await connection.receive() == "ready-during-cancel"
    await connection.close()


@pytest.mark.asyncio
async def test_native_receive_cancellation_releases_retained_slot(
    loopback_endpoint: LoopbackEndpoint,
) -> None:
    connection = await fogws.connect(loopback_endpoint.uri)
    receive_task = asyncio.create_task(connection.receive())
    await asyncio.sleep(0)
    pending_receive = connection._pending_receive  # noqa: SLF001
    assert pending_receive is not None
    pending_receive.cancel()

    with pytest.raises(asyncio.CancelledError):
        await receive_task

    await connection.send_text("after-native-cancel")
    assert await connection.receive() == "after-native-cancel"
    await connection.close()


@pytest.mark.asyncio
async def test_dropping_connection_cancels_retained_native_receive(
    loopback_endpoint: LoopbackEndpoint,
) -> None:
    connection = await fogws.connect(loopback_endpoint.uri)
    receive_task = asyncio.create_task(connection.receive())
    await asyncio.sleep(0)
    receive_task.cancel()
    with pytest.raises(asyncio.CancelledError):
        await receive_task

    del receive_task
    del connection
    gc.collect()
    await asyncio.wait_for(loopback_endpoint.disconnected.wait(), timeout=1)


@pytest.mark.asyncio
async def test_drop_aborts_receive_after_creator_loop_is_closed(
    loopback_endpoint: LoopbackEndpoint,
) -> None:
    def create_abandoned_connection() -> fogws.Connection:
        event_loop = asyncio.new_event_loop()
        asyncio.set_event_loop(event_loop)
        try:
            connection = event_loop.run_until_complete(
                fogws.connect(loopback_endpoint.uri),
            )
            receive_task = event_loop.create_task(connection.receive())
            event_loop.run_until_complete(asyncio.sleep(0))
            receive_task.cancel()
            with contextlib.suppress(asyncio.CancelledError):
                event_loop.run_until_complete(receive_task)
        finally:
            asyncio.set_event_loop(None)
            event_loop.close()
        return connection

    connection = await asyncio.to_thread(create_abandoned_connection)
    del connection
    gc.collect()

    await asyncio.wait_for(loopback_endpoint.disconnected.wait(), timeout=1)


@pytest.mark.asyncio
async def test_concurrent_receive_fails_explicitly(
    loopback_endpoint: LoopbackEndpoint,
) -> None:
    connection = await fogws.connect(loopback_endpoint.uri)
    first_receive = asyncio.create_task(connection.receive())
    await asyncio.sleep(0)

    with pytest.raises(fogws.ConcurrencyError, match="only one receive"):
        await connection.receive()

    first_receive.cancel()
    with pytest.raises(asyncio.CancelledError):
        await first_receive
    await connection.close()


@pytest.mark.asyncio
async def test_cross_loop_use_fails_explicitly(
    loopback_endpoint: LoopbackEndpoint,
) -> None:
    connection = await fogws.connect(loopback_endpoint.uri)

    async def send_from_new_loop() -> None:
        await connection.send_text("wrong-loop")

    with pytest.raises(fogws.LoopAffinityError, match="loop that created"):
        await asyncio.to_thread(asyncio.run, send_from_new_loop())

    await connection.close()


@pytest.mark.asyncio
async def test_connection_retains_exact_creator_loop_identity(
    loopback_endpoint: LoopbackEndpoint,
) -> None:
    def create_on_private_loop() -> tuple[
        fogws.Connection,
        ReferenceType[asyncio.AbstractEventLoop],
    ]:
        event_loop = asyncio.new_event_loop()
        asyncio.set_event_loop(event_loop)
        try:
            connection = event_loop.run_until_complete(
                fogws.connect(loopback_endpoint.uri),
            )
            event_loop.run_until_complete(connection.close())
            loop_reference = ref(event_loop)
        finally:
            asyncio.set_event_loop(None)
            event_loop.close()
        return connection, loop_reference

    connection, loop_reference = await asyncio.to_thread(create_on_private_loop)
    gc.collect()
    assert loop_reference() is not None

    with pytest.raises(fogws.LoopAffinityError, match="loop that created"):
        await connection.receive()


@pytest.mark.asyncio
async def test_local_close_preserves_typed_close_metadata(
    loopback_endpoint: LoopbackEndpoint,
) -> None:
    connection = await fogws.connect(loopback_endpoint.uri)
    await connection.close()

    with pytest.raises(fogws.ConnectionClosedOK) as error:
        await connection.receive()
    assert error.value.code == EXPECTED_NORMAL_CLOSE_CODE
    assert error.value.reason == ""
    assert error.value.initiated_by_local is True


@pytest.mark.asyncio
async def test_limits_are_validated_before_network_use(
    loopback_endpoint: LoopbackEndpoint,
) -> None:
    with pytest.raises(fogws.ResourceLimitError, match="at least max_message_size"):
        await fogws.connect(
            loopback_endpoint.uri,
            max_message_size=2,
            max_buffered_bytes=1,
        )

    with pytest.raises(fogws.ResourceLimitError, match="max_queue must not exceed"):
        await fogws.connect(
            loopback_endpoint.uri,
            max_queue=(sys.maxsize >> 2) + 1,
        )


@pytest.mark.asyncio
@pytest.mark.parametrize(
    "name",
    ["max_queue", "max_message_size", "max_buffered_bytes"],
)
@pytest.mark.parametrize("value", [-1, 1 << 100])
async def test_integer_limit_conversion_has_one_typed_error(
    loopback_endpoint: LoopbackEndpoint,
    name: str,
    value: int,
) -> None:
    with pytest.raises(
        fogws.ResourceLimitError,
        match=f"{name} must be a non-negative platform-sized integer",
    ):
        await fogws.connect(loopback_endpoint.uri, **{name: value})


@pytest.mark.asyncio
async def test_outbound_message_limit_is_enforced(
    loopback_endpoint: LoopbackEndpoint,
) -> None:
    connection = await fogws.connect(
        loopback_endpoint.uri,
        max_message_size=4,
        max_buffered_bytes=4,
    )

    with pytest.raises(fogws.ResourceLimitError, match="maximum is 4"):
        await connection.send_text("12345")

    await connection.close()


@pytest.mark.asyncio
async def test_inbound_message_limit_closes_connection(
    loopback_endpoint: LoopbackEndpoint,
) -> None:
    connection = await fogws.connect(
        loopback_endpoint.uri,
        max_message_size=4,
        max_buffered_bytes=4,
    )
    await connection.send_text("over")

    with pytest.raises(fogws.ConnectionClosedError, match="Message too long"):
        await connection.receive()


@pytest.mark.asyncio
async def test_non_ws_scheme_is_rejected(
    loopback_endpoint: LoopbackEndpoint,
) -> None:
    secure_uri = loopback_endpoint.uri.replace("ws://", "wss://", 1)

    with pytest.raises(fogws.InvalidURIError, match="only plain ws"):
        await fogws.connect(secure_uri)


def test_public_limit_defaults_are_named_and_finite() -> None:
    assert fogws.DEFAULT_MAX_QUEUE == EXPECTED_MAX_QUEUE
    assert fogws.DEFAULT_MAX_MESSAGE_SIZE == EXPECTED_MAX_MESSAGE_SIZE
    assert fogws.DEFAULT_MAX_BUFFERED_BYTES == EXPECTED_MAX_BUFFERED_BYTES
    assert fogws.DEFAULT_CLOSE_TIMEOUT == EXPECTED_CLOSE_TIMEOUT


async def _start_backpressured_send(
    endpoint: BackpressuredEndpoint,
) -> tuple[fogws.Connection, asyncio.Task[None]]:
    connection = await fogws.connect(
        endpoint.uri,
        max_message_size=BACKPRESSURED_MESSAGE_BYTES,
        max_buffered_bytes=BACKPRESSURED_MESSAGE_BYTES,
        close_timeout=BOUNDED_CLOSE_TIMEOUT_SECONDS,
    )
    await endpoint.upgraded.wait()
    send_task = asyncio.create_task(
        connection.send_bytes(b"x" * BACKPRESSURED_MESSAGE_BYTES),
    )
    await asyncio.sleep(BACKPRESSURE_SETTLE_SECONDS)
    assert not send_task.done(), "test peer didn't backpressure the client writer"
    return connection, send_task
