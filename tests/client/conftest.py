import asyncio
import base64
from collections.abc import AsyncIterator
from contextlib import asynccontextmanager
import hashlib
import socket

import pytest_asyncio
from websockets.asyncio.server import ServerConnection, serve

from .models import (
    BackpressuredEndpoint,
    LoopbackEndpoint,
    StalledHandshakeEndpoint,
    UnresponsiveCloseEndpoint,
)


NORMAL_CLOSE_FRAME = b"\x88\x02\x03\xe8"
RESERVED_OPCODE_FRAME = b"\x83\x00"


@pytest_asyncio.fixture
async def loopback_endpoint() -> AsyncIterator[LoopbackEndpoint]:
    disconnected = asyncio.Event()

    async def echo_handler(connection: ServerConnection) -> None:
        try:
            async for message in connection:
                if message == "close-normal":
                    await connection.close(code=1000, reason="done")
                elif message == "close-error":
                    await connection.close(code=1011, reason="failed")
                elif message == "over":
                    await connection.send(b"12345")
                else:
                    await connection.send(message)
        finally:
            disconnected.set()

    server = await serve(echo_handler, "127.0.0.1", 0, compression=None)
    socket = server.sockets[0]
    host, port = socket.getsockname()[:2]
    yield LoopbackEndpoint(
        disconnected=disconnected,
        uri=f"ws://{host}:{port}/socket",
    )
    server.close()
    await server.wait_closed()


@pytest_asyncio.fixture
async def backpressured_close_endpoint() -> AsyncIterator[BackpressuredEndpoint]:
    async with _backpressured_endpoint(NORMAL_CLOSE_FRAME, "peer-close") as endpoint:
        yield endpoint


@pytest_asyncio.fixture
async def backpressured_protocol_error_endpoint() -> AsyncIterator[BackpressuredEndpoint]:
    async with _backpressured_endpoint(RESERVED_OPCODE_FRAME, "protocol-error") as endpoint:
        yield endpoint


@pytest_asyncio.fixture
async def unresponsive_close_endpoint() -> AsyncIterator[UnresponsiveCloseEndpoint]:
    upgraded = asyncio.Event()
    disconnected = asyncio.Event()

    async def accept_without_close_reply(
        reader: asyncio.StreamReader,
        writer: asyncio.StreamWriter,
    ) -> None:
        request = await reader.readuntil(b"\r\n\r\n")
        key_header = next(line for line in request.split(b"\r\n") if line.lower().startswith(b"sec-websocket-key:"))
        key = key_header.split(b":", maxsplit=1)[1].strip()
        accept = base64.b64encode(
            hashlib.sha1(
                key + b"258EAFA5-E914-47DA-95CA-C5AB0DC85B11",
                usedforsecurity=False,
            ).digest(),
        )
        writer.write(
            b"HTTP/1.1 101 Switching Protocols\r\n"
            b"Upgrade: websocket\r\n"
            b"Connection: Upgrade\r\n"
            b"Sec-WebSocket-Accept: " + accept + b"\r\n\r\n",
        )
        await writer.drain()
        upgraded.set()
        try:
            while await reader.read(4096):
                continue
        finally:
            disconnected.set()
            writer.close()
            await writer.wait_closed()

    server = await asyncio.start_server(accept_without_close_reply, "127.0.0.1", 0)
    socket = server.sockets[0]
    host, port = socket.getsockname()[:2]
    yield UnresponsiveCloseEndpoint(
        disconnected=disconnected,
        upgraded=upgraded,
        uri=f"ws://{host}:{port}/unresponsive-close",
    )
    server.close()
    await server.wait_closed()


@asynccontextmanager
async def _backpressured_endpoint(
    terminal_frame: bytes,
    path: str,
) -> AsyncIterator[BackpressuredEndpoint]:
    trigger = asyncio.Event()
    upgraded = asyncio.Event()
    release = asyncio.Event()
    handler_finished = asyncio.Event()

    async def send_terminal_without_reading(
        reader: asyncio.StreamReader,
        writer: asyncio.StreamWriter,
    ) -> None:
        try:
            await _complete_handshake(reader, writer)
            peer_socket = writer.get_extra_info("socket")
            assert peer_socket is not None
            peer_socket.setsockopt(socket.SOL_SOCKET, socket.SO_RCVBUF, 4096)
            upgraded.set()
            await trigger.wait()
            writer.write(terminal_frame)
            await writer.drain()
            await release.wait()
        finally:
            writer.close()
            await writer.wait_closed()
            handler_finished.set()

    server = await asyncio.start_server(send_terminal_without_reading, "127.0.0.1", 0)
    server_socket = server.sockets[0]
    host, port = server_socket.getsockname()[:2]
    try:
        yield BackpressuredEndpoint(
            trigger=trigger,
            upgraded=upgraded,
            uri=f"ws://{host}:{port}/{path}",
        )
    finally:
        release.set()
        server.close()
        await server.wait_closed()
        await handler_finished.wait()


async def _complete_handshake(
    reader: asyncio.StreamReader,
    writer: asyncio.StreamWriter,
) -> None:
    request = await reader.readuntil(b"\r\n\r\n")
    key_header = next(line for line in request.split(b"\r\n") if line.lower().startswith(b"sec-websocket-key:"))
    key = key_header.split(b":", maxsplit=1)[1].strip()
    accept = base64.b64encode(
        hashlib.sha1(
            key + b"258EAFA5-E914-47DA-95CA-C5AB0DC85B11",
            usedforsecurity=False,
        ).digest(),
    )
    writer.write(
        b"HTTP/1.1 101 Switching Protocols\r\n"
        b"Upgrade: websocket\r\n"
        b"Connection: Upgrade\r\n"
        b"Sec-WebSocket-Accept: " + accept + b"\r\n\r\n",
    )
    await writer.drain()


@pytest_asyncio.fixture
async def stalled_handshake_endpoint() -> AsyncIterator[StalledHandshakeEndpoint]:
    accepted = asyncio.Event()
    disconnected = asyncio.Event()

    async def stall_handshake(
        reader: asyncio.StreamReader,
        writer: asyncio.StreamWriter,
    ) -> None:
        accepted.set()
        try:
            while await reader.read(4096):
                continue
        finally:
            disconnected.set()
            writer.close()
            await writer.wait_closed()

    server = await asyncio.start_server(stall_handshake, "127.0.0.1", 0)
    socket = server.sockets[0]
    host, port = socket.getsockname()[:2]
    yield StalledHandshakeEndpoint(
        accepted=accepted,
        disconnected=disconnected,
        uri=f"ws://{host}:{port}/stalled",
    )
    server.close()
    await server.wait_closed()
