# Minimal asyncio client

FogWS currently provides one explicit connection attempt for plain `ws://`
endpoints:

```python
import fogws

connection = await fogws.connect("ws://127.0.0.1:8765/socket")
async with connection:
    await connection.send_text("hello")
    await connection.send_bytes(b"payload")
    message = await connection.receive()
```

`Connection` is bound to the running asyncio loop that created it. Calling an
operation from another loop or thread raises `LoopAffinityError`. Only one
`receive()` may be active; a second raises `ConcurrencyError`. Canceling a
receive doesn't consume its next queued message: the one retained native
receive is resumed by the next call. Dropping the connection cancels that
retained receive so it can't keep the driver or socket alive.

## Runtime and lifecycle

The native module configures one lazily created, process-wide Tokio runtime
with two worker threads. Connections don't create or stop runtimes. Network
waits run without the GIL. The runtime lives until process exit and has no
public shutdown operation.

FogWS fails closed if a loaded module is used after `fork()` or from a second
Python subinterpreter. Start a fresh interpreter process instead. A connection
may only be used on its creating asyncio loop.

`close()` is idempotent. It publishes the closing state before awaiting the
peer and continues native cleanup if its Python waiter is canceled. Local and
peer-initiated close cleanup are bounded by `DEFAULT_CLOSE_TIMEOUT`; expiration
force-drops a backpressured transport. Fatal reader errors abort the writer
path immediately.
`ConnectionClosedOK` reports close codes 1000 and 1001, while
an empty close frame is also clean with `code=None`. `ConnectionClosedError`
reports abnormal close frames and transport failures. Both exception types
expose typed `code`, `reason` and `initiated_by_local` attributes; callers
don't need to parse their diagnostic text. Transport failures have `code=None`,
an empty `reason` and `initiated_by_local=None` because no close frame supplied
that metadata.
`close_timeout` may shorten or lengthen the bounded cleanup window per
connection.

## Resource limits

Defaults are intentionally finite:

- `DEFAULT_MAX_QUEUE = 16` accepted messages in each direction;
- `DEFAULT_MAX_MESSAGE_SIZE = 1 MiB` for a frame or complete message;
- `DEFAULT_MAX_BUFFERED_BYTES = 1 MiB` for each inbound and outbound queue;
- 16 KiB eager Tungstenite read buffer;
- zero-byte target write buffer and a 1 MiB payload budget plus one bounded
  RFC 6455 frame header for the maximum write buffer;
- `DEFAULT_CLOSE_TIMEOUT = 10 seconds`.

The inbound reader may hold one additional message, bounded by
`max_message_size`, while it waits for queue byte capacity. Outbound byte
permits remain attached through flush or terminal failure. Outbound queue or
byte saturation fails immediately with `ResourceLimitError`; inbound
saturation applies bounded backpressure to the Rust reader. Neither path
creates an unbounded waiter queue.

Canceling a send before native queue admission means it wasn't accepted. Once
admitted, the driver owns it through flush or terminal failure; canceling the
Python waiter makes the delivery outcome unknown. Closing remains the safe way
to abandon a backpressured connection.

## Operation matrix

| Operation or event | Public outcome | Native resource outcome |
|---|---|---|
| successful connect | `Connection` | one reader and one serialized writer path on the shared runtime |
| refused connect | `ConnectionFailedError` | connect future drops its socket; no driver starts |
| canceled connect | `CancelledError` | Rust connect future and partial transport are dropped |
| second active receive | `ConcurrencyError` | first receive and inbound queue are unchanged |
| canceled receive | `CancelledError` | no queued message is consumed |
| queue or byte saturation | `ResourceLimitError` | no unbounded waiter is registered |
| peer close 1000 or 1001 | `ConnectionClosedOK` from `receive()` | writer flushes the close reply when possible; both paths stop within `close_timeout` |
| abnormal close or transport failure | `ConnectionClosedError` | terminal state stops both paths and releases permits |
| explicit close | `None`; repeated close has the same successful result | close is sent on a separate control path and cleanup is bounded |
| canceled close waiter | `CancelledError` for that waiter | native close and timeout watchdog continue |
| cross-loop, post-fork or second-interpreter use | typed fail-closed error | no new Rust future or network operation starts |

Secure `wss://`, headers, subprotocols, redirects, proxies, reconnect,
keepalive, compression, fragments, metrics, telemetry, sync APIs and server
hosting aren't part of this slice.
