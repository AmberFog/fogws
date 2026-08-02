"""Public asyncio client facade."""

import asyncio
from types import TracebackType
from typing import Self

from ._fogws import (
    DEFAULT_CLOSE_TIMEOUT,
    DEFAULT_MAX_BUFFERED_BYTES,
    DEFAULT_MAX_MESSAGE_SIZE,
    DEFAULT_MAX_QUEUE,
    ConcurrencyError,
    _connect,
    _Connection,
)


__all__ = ("Connection", "connect")


# The public methods are the complete minimal connection lifecycle surface.
class Connection:  # noqa: WPS214
    """A loop-bound, whole-message WebSocket connection backed by Rust."""

    __slots__ = (
        "_native",
        "_pending_receive",
        "_receive_waiter_active",
    )

    def __init__(self, native: _Connection) -> None:
        self._native = native
        self._pending_receive: asyncio.Future[str | bytes] | None = None
        self._receive_waiter_active = False

    # Native abort is required when asyncio is already unable to schedule cleanup.
    def __del__(self) -> None:  # noqa: WPS603
        self._native._abort(  # noqa: SLF001  # Private native lifecycle hook.
            self._pending_receive,
        )

    async def send_text(self, payload: str) -> None:
        """Send one complete UTF-8 text message."""
        await self._native.send_text(payload)

    async def send_bytes(self, payload: bytes) -> None:
        """Send one complete binary message."""
        await self._native.send_bytes(payload)

    async def receive(self) -> str | bytes:
        """Receive one complete text or binary message."""
        self._native._ensure_context()  # noqa: SLF001  # Required before retained-Future reuse.
        self._claim_receive_waiter()
        pending_receive = self._pending_receive
        try:
            if pending_receive is None:
                pending_receive = asyncio.ensure_future(self._native.receive())
                pending_receive.add_done_callback(_observe_receive_completion)
                self._pending_receive = pending_receive
            message = await asyncio.shield(pending_receive)
        except asyncio.CancelledError:
            if pending_receive is not None and pending_receive.cancelled():
                self._pending_receive = None
            raise
        except BaseException:
            self._pending_receive = None
            raise
        else:
            self._pending_receive = None
            return message
        finally:
            self._receive_waiter_active = False

    async def close(self) -> None:
        """Close the connection and wait for bounded native cleanup."""
        await self._native.close()

    async def __aenter__(self) -> Self:
        return self

    async def __aexit__(
        self,
        exc_type: type[BaseException] | None,
        exc_value: BaseException | None,
        traceback: TracebackType | None,
    ) -> None:
        await self.close()

    def _claim_receive_waiter(self) -> None:
        if self._receive_waiter_active:
            msg = "only one receive operation may be active per connection"
            raise ConcurrencyError(
                msg,
            )
        self._receive_waiter_active = True


async def connect(
    uri: str,
    *,
    max_queue: int = DEFAULT_MAX_QUEUE,
    max_message_size: int = DEFAULT_MAX_MESSAGE_SIZE,
    max_buffered_bytes: int = DEFAULT_MAX_BUFFERED_BYTES,
    close_timeout: float = DEFAULT_CLOSE_TIMEOUT,
) -> Connection:
    """Open one plain ``ws://`` connection attempt."""
    native = await _connect(
        uri,
        max_queue=max_queue,
        max_message_size=max_message_size,
        max_buffered_bytes=max_buffered_bytes,
        close_timeout=close_timeout,
    )
    return Connection(native)


def _observe_receive_completion(future: asyncio.Future[str | bytes]) -> None:
    if not future.cancelled():
        future.exception()
