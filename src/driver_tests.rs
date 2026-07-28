use std::{
    io,
    pin::Pin,
    sync::{
        Arc,
        atomic::{AtomicBool, Ordering},
    },
    task::{Context, Poll},
};

use futures_util::{SinkExt, StreamExt};
use tokio::time::{Duration, timeout};
use tokio::{
    io::{AsyncRead, AsyncWrite, AsyncWriteExt, DuplexStream, ReadBuf},
    net::TcpListener,
    sync::Notify,
};
use tokio_tungstenite::{
    WebSocketStream, accept_async,
    tungstenite::{Message, protocol::Role},
};

use crate::{
    client,
    config::ConnectionConfig,
    driver::{ConnectionDriver, InboundMessage},
    error::DriverError,
};

const NORMAL_CLOSE_FRAME: &[u8] = b"\x88\x02\x03\xe8";

struct ObservedIo {
    dropped: Arc<AtomicBool>,
    dropped_notify: Arc<Notify>,
    fail_writes: Arc<AtomicBool>,
    inner: DuplexStream,
    write_blocked: Arc<AtomicBool>,
    write_blocked_notify: Arc<Notify>,
}

impl AsyncRead for ObservedIo {
    fn poll_read(
        mut self: Pin<&mut Self>,
        context: &mut Context<'_>,
        buffer: &mut ReadBuf<'_>,
    ) -> Poll<io::Result<()>> {
        Pin::new(&mut self.inner).poll_read(context, buffer)
    }
}

impl AsyncWrite for ObservedIo {
    fn poll_write(
        mut self: Pin<&mut Self>,
        context: &mut Context<'_>,
        buffer: &[u8],
    ) -> Poll<io::Result<usize>> {
        if self.fail_writes.load(Ordering::Acquire) {
            return Poll::Ready(Err(io::Error::new(
                io::ErrorKind::BrokenPipe,
                "injected write failure",
            )));
        }
        let result = Pin::new(&mut self.inner).poll_write(context, buffer);
        if result.is_pending() {
            self.write_blocked.store(true, Ordering::Release);
            self.write_blocked_notify.notify_one();
        }
        result
    }

    fn poll_flush(mut self: Pin<&mut Self>, context: &mut Context<'_>) -> Poll<io::Result<()>> {
        Pin::new(&mut self.inner).poll_flush(context)
    }

    fn poll_shutdown(mut self: Pin<&mut Self>, context: &mut Context<'_>) -> Poll<io::Result<()>> {
        Pin::new(&mut self.inner).poll_shutdown(context)
    }
}

impl Drop for ObservedIo {
    fn drop(&mut self) {
        self.dropped.store(true, Ordering::Release);
        self.dropped_notify.notify_one();
    }
}

struct IoObservation {
    dropped: Arc<AtomicBool>,
    dropped_notify: Arc<Notify>,
    fail_writes: Arc<AtomicBool>,
    write_blocked: Arc<AtomicBool>,
    write_blocked_notify: Arc<Notify>,
}

impl IoObservation {
    async fn wait_for_drop(&self) {
        wait_for_flag(&self.dropped, &self.dropped_notify).await;
    }

    async fn wait_for_write_blocked(&self) {
        wait_for_flag(&self.write_blocked, &self.write_blocked_notify).await;
    }
}

fn observed_duplex(capacity: usize) -> (ObservedIo, DuplexStream, IoObservation) {
    let (inner, peer) = tokio::io::duplex(capacity);
    let observation = IoObservation {
        dropped: Arc::new(AtomicBool::new(false)),
        dropped_notify: Arc::new(Notify::new()),
        fail_writes: Arc::new(AtomicBool::new(false)),
        write_blocked: Arc::new(AtomicBool::new(false)),
        write_blocked_notify: Arc::new(Notify::new()),
    };
    let io = ObservedIo {
        dropped: Arc::clone(&observation.dropped),
        dropped_notify: Arc::clone(&observation.dropped_notify),
        fail_writes: Arc::clone(&observation.fail_writes),
        inner,
        write_blocked: Arc::clone(&observation.write_blocked),
        write_blocked_notify: Arc::clone(&observation.write_blocked_notify),
    };
    (io, peer, observation)
}

async fn wait_for_flag(flag: &AtomicBool, notify: &Notify) {
    while !flag.load(Ordering::Acquire) {
        notify.notified().await;
    }
}

#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn concrete_driver_serves_both_loopback_roles() {
    timeout(Duration::from_secs(5), run_concrete_driver_loopback())
        .await
        .expect("driver loopback timed out");
}

async fn run_concrete_driver_loopback() {
    let listener = TcpListener::bind("127.0.0.1:0").await.unwrap();
    let address = listener.local_addr().unwrap();
    let server = tokio::spawn(async move {
        let (stream, _) = listener.accept().await.unwrap();
        let websocket = accept_async(stream).await.unwrap();
        let connection = ConnectionDriver::start(websocket, ConnectionConfig::default());
        let InboundMessage::Text(text) = connection.receive().await.unwrap() else {
            panic!("expected a text message");
        };
        assert_eq!(text, "hello");
        connection
            .send(Message::Binary(vec![1, 2, 3].into()))
            .await
            .unwrap();
        assert!(matches!(
            connection.receive().await,
            Err(DriverError::ClosedOk(_)),
        ));
        connection.close().await.unwrap();
    });

    let connection = client::connect(
        format!("ws://{address}/socket"),
        ConnectionConfig::default(),
    )
    .await
    .unwrap();
    connection
        .send(Message::Text("hello".into()))
        .await
        .unwrap();
    let InboundMessage::Binary(binary) = connection.receive().await.unwrap() else {
        panic!("expected a binary message");
    };
    assert_eq!(binary, vec![1, 2, 3]);
    connection.close().await.unwrap();

    server.await.unwrap();
}

#[tokio::test]
async fn tokio_tungstenite_loopback_fixture_exercises_raw_client_and_server_roles() {
    let listener = TcpListener::bind("127.0.0.1:0").await.unwrap();
    let address = listener.local_addr().unwrap();
    let server = tokio::spawn(async move {
        let (stream, _) = listener.accept().await.unwrap();
        let mut websocket = accept_async(stream).await.unwrap();
        let message = websocket.next().await.unwrap().unwrap();
        websocket.send(message).await.unwrap();
        websocket.close(None).await.unwrap();
    });

    let (mut client, _) = tokio_tungstenite::connect_async(format!("ws://{address}/echo"))
        .await
        .unwrap();
    client
        .send(Message::Text("round-trip".into()))
        .await
        .unwrap();
    assert_eq!(
        client.next().await.unwrap().unwrap(),
        Message::Text("round-trip".into()),
    );
    client.close(None).await.unwrap();
    server.await.unwrap();
}

#[tokio::test]
async fn peer_close_flush_failure_preserves_clean_outcome_and_drops_transport() {
    let (io, mut peer, observation) = observed_duplex(1024);
    let websocket = WebSocketStream::from_raw_socket(io, Role::Client, None).await;
    let connection = ConnectionDriver::start(websocket, ConnectionConfig::default());

    observation.fail_writes.store(true, Ordering::Release);
    peer.write_all(NORMAL_CLOSE_FRAME).await.unwrap();

    let receive_error = timeout(Duration::from_secs(1), connection.receive())
        .await
        .expect("receive did not observe the peer close")
        .unwrap_err();
    assert!(matches!(
        receive_error,
        DriverError::ClosedOk(ref info)
            if info.code == Some(1000) && !info.initiated_by_local
    ));
    timeout(Duration::from_secs(1), connection.close())
        .await
        .expect("close did not finish after the injected flush failure")
        .unwrap();
    timeout(Duration::from_secs(1), observation.wait_for_drop())
        .await
        .expect("transport survived the terminal close-flush failure");
}

#[tokio::test]
async fn peer_close_stops_a_proven_backpressured_writer_and_drops_transport() {
    const PAYLOAD_BYTES: usize = 4096;

    let (io, mut peer, observation) = observed_duplex(64);
    let config = ConnectionConfig::new(1, PAYLOAD_BYTES, PAYLOAD_BYTES, 0.05).unwrap();
    let websocket =
        WebSocketStream::from_raw_socket(io, Role::Client, Some(config.websocket_config())).await;
    let connection = ConnectionDriver::start(websocket, config);
    let sending = tokio::spawn({
        let connection = Arc::clone(&connection);
        async move {
            connection
                .send(Message::Binary(vec![b'x'; PAYLOAD_BYTES].into()))
                .await
        }
    });

    timeout(Duration::from_secs(1), observation.wait_for_write_blocked())
        .await
        .expect("writer never reached deterministic backpressure");
    peer.write_all(NORMAL_CLOSE_FRAME).await.unwrap();

    let send_error = timeout(Duration::from_secs(1), sending)
        .await
        .expect("backpressured send did not terminate")
        .unwrap()
        .unwrap_err();
    assert!(matches!(
        send_error,
        DriverError::ClosedOk(ref info)
            if info.code == Some(1000) && !info.initiated_by_local
    ));
    timeout(Duration::from_secs(1), connection.close())
        .await
        .expect("close did not finish after peer close")
        .unwrap();
    timeout(Duration::from_secs(1), observation.wait_for_drop())
        .await
        .expect("transport survived backpressured peer-close cleanup");
}
