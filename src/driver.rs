use std::sync::{
    Arc,
    atomic::{AtomicBool, Ordering},
};

use futures_util::{SinkExt, StreamExt, stream::SplitSink, stream::SplitStream};
use tokio::{
    io::{AsyncRead, AsyncWrite},
    sync::{Mutex, Semaphore, mpsc, oneshot, watch},
    task::AbortHandle,
};
use tokio_tungstenite::{
    WebSocketStream,
    tungstenite::{
        Message,
        protocol::frame::{CloseFrame, coding::CloseCode},
    },
};

use crate::{
    config::ConnectionConfig,
    error::{CloseInfo, DriverError},
};

#[derive(Clone, Debug, Eq, PartialEq)]
pub enum DriverState {
    Closed(DriverError),
    Closing,
    Open,
    PeerClosed(DriverError),
}

impl DriverState {
    fn is_terminal(&self) -> bool {
        matches!(self, Self::Closed(_))
    }
}

#[derive(Debug)]
pub enum InboundMessage {
    Binary(Vec<u8>),
    Text(String),
}

struct InboundEnvelope {
    message: InboundMessage,
    _permit: tokio::sync::OwnedSemaphorePermit,
}

struct OutboundCommand {
    message: Message,
    response: oneshot::Sender<Result<(), DriverError>>,
    _permit: tokio::sync::OwnedSemaphorePermit,
}

pub struct OutboundAdmission {
    byte_permit: tokio::sync::OwnedSemaphorePermit,
    item_permit: mpsc::OwnedPermit<OutboundCommand>,
    message_size: usize,
}

struct DriverTaskGuard {
    remaining: watch::Sender<usize>,
}

impl DriverTaskGuard {
    fn new(remaining: &watch::Sender<usize>) -> Self {
        remaining.send_modify(|count| *count += 1);
        Self {
            remaining: remaining.clone(),
        }
    }
}

impl Drop for DriverTaskGuard {
    fn drop(&mut self) {
        self.remaining
            .send_modify(|remaining| *remaining = remaining.saturating_sub(1));
    }
}

#[derive(Clone, Copy)]
enum WriterControl {
    Flush,
}

enum WriterEvent {
    Control(Option<WriterControl>),
    Outbound(Option<OutboundCommand>),
}

#[derive(Clone, Copy, Debug, Eq, PartialEq)]
enum ShutdownSignal {
    Abort,
    LocalClose,
    PeerClose,
    Running,
}

struct ReaderContext {
    close_timeout: std::time::Duration,
    closing: Arc<AtomicBool>,
    control: mpsc::Sender<WriterControl>,
    inbound: mpsc::Sender<InboundEnvelope>,
    inbound_budget: Arc<Semaphore>,
    shutdown: watch::Sender<ShutdownSignal>,
    shutdown_receiver: watch::Receiver<ShutdownSignal>,
    state: watch::Sender<DriverState>,
    writer_abort: AbortHandle,
}

pub struct ConnectionDriver {
    close_timeout_started: AtomicBool,
    closing: Arc<AtomicBool>,
    config: ConnectionConfig,
    inbound: Mutex<mpsc::Receiver<InboundEnvelope>>,
    outbound: mpsc::Sender<OutboundCommand>,
    outbound_budget: Arc<Semaphore>,
    reader_abort: AbortHandle,
    receive_active: Arc<AtomicBool>,
    shutdown: watch::Sender<ShutdownSignal>,
    state: watch::Receiver<DriverState>,
    state_sender: watch::Sender<DriverState>,
    tasks_remaining: watch::Receiver<usize>,
    tasks_remaining_sender: watch::Sender<usize>,
    writer_abort: AbortHandle,
}

impl ConnectionDriver {
    pub fn start<Socket>(websocket: WebSocketStream<Socket>, config: ConnectionConfig) -> Arc<Self>
    where
        Socket: AsyncRead + AsyncWrite + Unpin + Send + 'static,
    {
        let (writer, reader) = websocket.split();
        let (inbound_sender, inbound) = mpsc::channel(config.max_queue);
        let (outbound, outbound_receiver) = mpsc::channel(config.max_queue);
        let (control_sender, control_receiver) = mpsc::channel(4);
        let (shutdown, shutdown_receiver) = watch::channel(ShutdownSignal::Running);
        let (state_sender, state) = watch::channel(DriverState::Open);
        let (tasks_remaining_sender, tasks_remaining) = watch::channel(0);
        let closing = Arc::new(AtomicBool::new(false));
        let inbound_budget = Arc::new(Semaphore::new(config.max_buffered_bytes));
        let outbound_budget = Arc::new(Semaphore::new(config.max_buffered_bytes));

        let writer_task_guard = DriverTaskGuard::new(&tasks_remaining_sender);
        let writer_shutdown = shutdown.clone();
        let writer_shutdown_receiver = shutdown_receiver.clone();
        let writer_state = state_sender.clone();
        let writer_task = tokio::spawn(async move {
            run_writer(
                writer,
                outbound_receiver,
                control_receiver,
                writer_shutdown_receiver,
                writer_shutdown,
                writer_state,
            )
            .await;
            drop(writer_task_guard);
        });
        let writer_abort = writer_task.abort_handle();
        let reader_task_guard = DriverTaskGuard::new(&tasks_remaining_sender);
        let reader_context = ReaderContext {
            close_timeout: config.close_timeout,
            closing: Arc::clone(&closing),
            control: control_sender,
            inbound: inbound_sender,
            inbound_budget,
            shutdown: shutdown.clone(),
            shutdown_receiver,
            state: state_sender.clone(),
            writer_abort: writer_abort.clone(),
        };
        let reader_task = tokio::spawn(async move {
            run_reader(reader, reader_context).await;
            drop(reader_task_guard);
        });

        Arc::new(Self {
            close_timeout_started: AtomicBool::new(false),
            closing,
            config,
            inbound: Mutex::new(inbound),
            outbound,
            outbound_budget,
            reader_abort: reader_task.abort_handle(),
            receive_active: Arc::new(AtomicBool::new(false)),
            shutdown,
            state,
            state_sender,
            tasks_remaining,
            tasks_remaining_sender,
            writer_abort,
        })
    }

    #[cfg(test)]
    pub async fn send(self: &Arc<Self>, message: Message) -> Result<(), DriverError> {
        let message_size = message.len();
        let admission = self.try_admit_outbound(message_size)?;
        self.send_admitted(admission, message).await
    }

    #[cfg(test)]
    pub async fn wait_for_peer_close_for_test(&self) -> Result<(), DriverError> {
        let mut state = self.state.clone();
        loop {
            match state.borrow().clone() {
                DriverState::PeerClosed(_) => return Ok(()),
                DriverState::Closed(error) => return Err(error),
                DriverState::Closing | DriverState::Open => {}
            }
            if state.changed().await.is_err() {
                return Err(DriverError::Transport(
                    "connection driver stopped before publishing peer close".to_owned(),
                ));
            }
        }
    }

    #[cfg(test)]
    pub fn abort_reader_for_test(&self) {
        self.reader_abort.abort();
    }

    #[cfg(test)]
    pub async fn wait_for_terminal_for_test(&self) -> DriverError {
        let mut state = self.state.clone();
        loop {
            if let DriverState::Closed(error) = state.borrow().clone() {
                return error;
            }
            if state.changed().await.is_err() {
                return DriverError::Transport(
                    "connection driver stopped before publishing terminal state".to_owned(),
                );
            }
        }
    }

    #[cfg(test)]
    pub fn tasks_remaining_for_test(&self) -> usize {
        *self.tasks_remaining.borrow()
    }

    pub fn try_admit_outbound(
        self: &Arc<Self>,
        message_size: usize,
    ) -> Result<OutboundAdmission, DriverError> {
        self.validate_outbound_message(message_size)?;
        let permit_count = u32::try_from(message_size).map_err(|_| {
            DriverError::ResourceLimit("message byte count exceeds the supported budget".to_owned())
        })?;
        let byte_permit = Arc::clone(&self.outbound_budget)
            .try_acquire_many_owned(permit_count)
            .map_err(|_| {
                DriverError::ResourceLimit(
                    "outbound byte budget is exhausted; retry after an accepted send completes"
                        .to_owned(),
                )
            })?;
        let item_permit =
            self.outbound
                .clone()
                .try_reserve_owned()
                .map_err(|error| match error {
                    mpsc::error::TrySendError::Full(_) => DriverError::ResourceLimit(
                        "outbound item queue is full; retry after an accepted send completes"
                            .to_owned(),
                    ),
                    mpsc::error::TrySendError::Closed(_) => self.terminal_error(),
                })?;
        Ok(OutboundAdmission {
            byte_permit,
            item_permit,
            message_size,
        })
    }

    pub async fn send_admitted(
        self: &Arc<Self>,
        admission: OutboundAdmission,
        message: Message,
    ) -> Result<(), DriverError> {
        if message.len() != admission.message_size {
            return Err(DriverError::Transport(
                "outbound message changed after capacity admission".to_owned(),
            ));
        }
        let (response, response_receiver) = oneshot::channel();
        let command = OutboundCommand {
            message,
            response,
            _permit: admission.byte_permit,
        };
        admission.item_permit.send(command);

        response_receiver
            .await
            .unwrap_or_else(|_| Err(self.terminal_error()))
    }

    pub fn validate_outbound_message(&self, message_size: usize) -> Result<(), DriverError> {
        self.ensure_open()?;
        if message_size > self.config.max_message_size {
            return Err(DriverError::ResourceLimit(format!(
                "message contains {message_size} bytes; maximum is {}",
                self.config.max_message_size,
            )));
        }
        Ok(())
    }

    pub async fn receive(self: &Arc<Self>) -> Result<InboundMessage, DriverError> {
        let _guard = ReceiveGuard::claim(Arc::clone(&self.receive_active))?;
        let mut inbound = self.inbound.lock().await;
        match inbound.recv().await {
            Some(envelope) => Ok(envelope.message),
            None => Err(self.terminal_error()),
        }
    }

    pub async fn close(self: &Arc<Self>) -> Result<(), DriverError> {
        let terminal = if let DriverState::Closed(error) = self.state.borrow().clone() {
            error
        } else {
            self.begin_close();
            let mut state = self.state.clone();
            loop {
                if let DriverState::Closed(error) = state.borrow().clone() {
                    break error;
                }
                if state.changed().await.is_err() {
                    break DriverError::Transport(
                        "connection driver stopped before publishing a terminal state".to_owned(),
                    );
                }
            }
        };
        wait_for_driver_tasks(self.tasks_remaining.clone()).await?;
        close_result(terminal)
    }

    pub fn abort(&self) {
        self.closing.store(true, Ordering::Release);
        let terminal = DriverError::Transport("connection was abandoned".to_owned());
        publish_terminal(&self.state_sender, &terminal);
        signal_abort(&self.shutdown);
        self.reader_abort.abort();
        self.writer_abort.abort();
    }

    fn begin_close(self: &Arc<Self>) {
        let watchdog_guard = DriverTaskGuard::new(&self.tasks_remaining_sender);
        let first_close = !self.closing.swap(true, Ordering::AcqRel);
        if !first_close {
            return;
        }
        let started_local_close = self.state_sender.send_if_modified(|state| {
            if matches!(state, DriverState::Open) {
                *state = DriverState::Closing;
                true
            } else {
                false
            }
        });
        if !started_local_close {
            return;
        }
        signal_local_close(&self.shutdown);

        if self.close_timeout_started.swap(true, Ordering::AcqRel) {
            return;
        }
        let timeout = self.config.close_timeout;
        let state = self.state_sender.subscribe();
        let state_sender = self.state_sender.clone();
        let reader_abort = self.reader_abort.clone();
        let writer_abort = self.writer_abort.clone();
        tokio::spawn(async move {
            if close_timeout_expired(state, timeout).await {
                publish_close_timeout(&state_sender);
                reader_abort.abort();
                writer_abort.abort();
            }
            drop(watchdog_guard);
        });
    }

    fn ensure_open(&self) -> Result<(), DriverError> {
        match self.state.borrow().clone() {
            DriverState::Open => Ok(()),
            DriverState::Closing => Err(DriverError::ClosedOk(CloseInfo::local_normal())),
            DriverState::Closed(error) | DriverState::PeerClosed(error) => Err(error),
        }
    }

    fn terminal_error(&self) -> DriverError {
        match self.state.borrow().clone() {
            DriverState::Closed(error) | DriverState::PeerClosed(error) => error,
            DriverState::Closing => DriverError::ClosedOk(CloseInfo::local_normal()),
            DriverState::Open => {
                DriverError::Transport("connection driver stopped unexpectedly".to_owned())
            }
        }
    }
}

impl Drop for ConnectionDriver {
    fn drop(&mut self) {
        self.abort();
    }
}

struct ReceiveGuard {
    active: Arc<AtomicBool>,
}

impl ReceiveGuard {
    fn claim(active: Arc<AtomicBool>) -> Result<Self, DriverError> {
        active
            .compare_exchange(false, true, Ordering::AcqRel, Ordering::Acquire)
            .map_err(|_| {
                DriverError::Concurrency(
                    "only one receive operation may be active per connection".to_owned(),
                )
            })?;
        Ok(Self { active })
    }
}

impl Drop for ReceiveGuard {
    fn drop(&mut self) {
        self.active.store(false, Ordering::Release);
    }
}

async fn run_reader<Socket>(
    mut reader: SplitStream<WebSocketStream<Socket>>,
    mut context: ReaderContext,
) where
    Socket: AsyncRead + AsyncWrite + Unpin,
{
    loop {
        if *context.shutdown_receiver.borrow() == ShutdownSignal::Abort {
            return;
        }
        let result = tokio::select! {
            message = reader.next() => message,
            changed = context.shutdown_receiver.changed() => {
                if changed.is_err()
                    || *context.shutdown_receiver.borrow() == ShutdownSignal::Abort
                {
                    return;
                }
                continue;
            }
        };
        let Some(result) = result else {
            break;
        };
        match result {
            Ok(payload @ (Message::Text(_) | Message::Binary(_))) => {
                if queue_inbound(
                    payload,
                    &context.inbound,
                    &context.inbound_budget,
                    &mut context.shutdown_receiver,
                )
                .await
                .is_err()
                {
                    break;
                }
            }
            Ok(Message::Close(frame)) => {
                let initiated_by_local = context.closing.load(Ordering::Acquire);
                context.closing.store(true, Ordering::Release);
                finish_peer_close(&context, classify_close(frame, initiated_by_local)).await;
                return;
            }
            Ok(Message::Ping(_) | Message::Pong(_)) => {
                let _ = context.control.try_send(WriterControl::Flush);
            }
            Ok(Message::Frame(_)) => {}
            Err(error) => {
                let terminal = if context.closing.load(Ordering::Acquire)
                    && matches!(
                        error,
                        tokio_tungstenite::tungstenite::Error::ConnectionClosed
                    ) {
                    DriverError::ClosedOk(CloseInfo::local_normal())
                } else {
                    DriverError::Transport(format!("WebSocket receive failed: {error}"))
                };
                if matches!(terminal, DriverError::ClosedOk(_)) {
                    finish_peer_close(&context, terminal).await;
                } else {
                    finish_reader_failure(&context, &terminal);
                }
                return;
            }
        }
    }

    let terminal = if context.closing.load(Ordering::Acquire) {
        DriverError::ClosedOk(CloseInfo::local_normal())
    } else {
        DriverError::Transport("WebSocket transport ended without a close frame".to_owned())
    };
    if matches!(terminal, DriverError::ClosedOk(_)) {
        finish_peer_close(&context, terminal).await;
    } else {
        finish_reader_failure(&context, &terminal);
    }
}

async fn finish_peer_close(context: &ReaderContext, outcome: DriverError) {
    publish_peer_closed(&context.state, &outcome);
    signal_peer_close(&context.shutdown);
    if tokio::time::timeout(context.close_timeout, wait_for_terminal(&context.state))
        .await
        .is_err()
    {
        publish_terminal(&context.state, &outcome);
        context.writer_abort.abort();
    }
}

fn finish_reader_failure(context: &ReaderContext, terminal: &DriverError) {
    publish_terminal(&context.state, terminal);
    signal_abort(&context.shutdown);
    context.writer_abort.abort();
}

async fn queue_inbound(
    payload: Message,
    inbound: &mpsc::Sender<InboundEnvelope>,
    inbound_budget: &Arc<Semaphore>,
    shutdown: &mut watch::Receiver<ShutdownSignal>,
) -> Result<(), ()> {
    if *shutdown.borrow() != ShutdownSignal::Running {
        return Ok(());
    }
    let permit_count = u32::try_from(payload.len()).map_err(|_| ())?;
    let permit = tokio::select! {
        result = Arc::clone(inbound_budget).acquire_many_owned(permit_count) => {
            result.map_err(|_| ())?
        }
        result = shutdown.changed() => {
            result.map_err(|_| ())?;
            return Ok(());
        }
    };
    let message = match payload {
        Message::Text(payload) => InboundMessage::Text(payload.to_string()),
        Message::Binary(payload) => InboundMessage::Binary(payload.to_vec()),
        Message::Close(_) | Message::Ping(_) | Message::Pong(_) | Message::Frame(_) => {
            return Err(());
        }
    };
    let envelope = InboundEnvelope {
        message,
        _permit: permit,
    };
    tokio::select! {
        result = inbound.send(envelope) => result.map_err(|_| ()),
        result = shutdown.changed() => {
            result.map_err(|_| ())?;
            Ok(())
        }
    }
}

async fn run_writer<Socket>(
    mut writer: SplitSink<WebSocketStream<Socket>, Message>,
    mut outbound: mpsc::Receiver<OutboundCommand>,
    mut control: mpsc::Receiver<WriterControl>,
    mut shutdown: watch::Receiver<ShutdownSignal>,
    shutdown_sender: watch::Sender<ShutdownSignal>,
    state: watch::Sender<DriverState>,
) where
    Socket: AsyncRead + AsyncWrite + Unpin,
{
    loop {
        tokio::select! {
            biased;
            result = shutdown.changed() => {
                let signal = *shutdown.borrow();
                if result.is_err() || signal != ShutdownSignal::Running {
                    if signal == ShutdownSignal::Abort {
                        return;
                    }
                    let close_result = close_writer(&mut writer, signal, &mut shutdown).await;
                    if let Err(error) = close_result {
                        publish_writer_failure(&state, &error);
                        signal_abort(&shutdown_sender);
                    } else if let Some(outcome) = peer_close_outcome(&state) {
                        publish_terminal(&state, &outcome);
                    }
                    return;
                }
            }
            event = next_writer_event(&mut control, &mut outbound) => {
                match event {
                    WriterEvent::Control(Some(WriterControl::Flush)) => {
                        if writer.flush().await.is_err() {
                            publish_writer_failure(
                                &state,
                                &DriverError::Transport("WebSocket control flush failed".to_owned()),
                            );
                            signal_abort(&shutdown_sender);
                            return;
                        }
                    }
                    WriterEvent::Control(None) => {
                        publish_writer_failure(
                            &state,
                            &DriverError::Transport(
                                "connection reader stopped unexpectedly".to_owned(),
                            ),
                        );
                        signal_abort(&shutdown_sender);
                        return;
                    }
                    WriterEvent::Outbound(command) => {
                        let Some(command) = command else {
                            return;
                        };
                        match writer.send(command.message).await {
                            Ok(()) => {
                                let _ = command.response.send(Ok(()));
                            }
                            Err(error) => {
                                let error = DriverError::Transport(format!(
                                    "WebSocket send failed: {error}",
                                ));
                                let terminal = publish_writer_failure(&state, &error);
                                signal_abort(&shutdown_sender);
                                let _ = command.response.send(Err(terminal));
                                return;
                            }
                        }
                    }
                }
            }
        }
    }
}

async fn next_writer_event(
    control: &mut mpsc::Receiver<WriterControl>,
    outbound: &mut mpsc::Receiver<OutboundCommand>,
) -> WriterEvent {
    tokio::select! {
        command = control.recv() => WriterEvent::Control(command),
        command = outbound.recv() => WriterEvent::Outbound(command),
    }
}

async fn close_writer<Socket>(
    writer: &mut SplitSink<WebSocketStream<Socket>, Message>,
    signal: ShutdownSignal,
    shutdown: &mut watch::Receiver<ShutdownSignal>,
) -> Result<(), DriverError>
where
    Socket: AsyncRead + AsyncWrite + Unpin,
{
    if signal == ShutdownSignal::LocalClose {
        writer
            .send(Message::Close(Some(CloseFrame {
                code: CloseCode::Normal,
                reason: "".into(),
            })))
            .await
            .map_err(|error| DriverError::Transport(format!("WebSocket close failed: {error}")))?;
        while *shutdown.borrow() == ShutdownSignal::LocalClose {
            shutdown.changed().await.map_err(|_| {
                DriverError::Transport(
                    "connection driver stopped during the close handshake".to_owned(),
                )
            })?;
        }
        if *shutdown.borrow() == ShutdownSignal::Abort {
            return Ok(());
        }
    }
    writer
        .flush()
        .await
        .map_err(|error| DriverError::Transport(format!("WebSocket close failed: {error}")))
}

fn peer_close_outcome(state: &watch::Sender<DriverState>) -> Option<DriverError> {
    let current = state.borrow().clone();
    match current {
        DriverState::PeerClosed(outcome) => Some(outcome),
        DriverState::Closed(_) | DriverState::Closing | DriverState::Open => None,
    }
}

fn publish_writer_failure(state: &watch::Sender<DriverState>, error: &DriverError) -> DriverError {
    let mut terminal = error.clone();
    let _ = state.send_if_modified(|current| {
        terminal = match current {
            DriverState::Closed(outcome) | DriverState::PeerClosed(outcome) => outcome.clone(),
            DriverState::Closing | DriverState::Open => error.clone(),
        };
        if current.is_terminal() {
            return false;
        }
        *current = DriverState::Closed(terminal.clone());
        true
    });
    terminal
}

fn classify_close(frame: Option<CloseFrame>, initiated_by_local: bool) -> DriverError {
    let Some(frame) = frame else {
        return DriverError::ClosedOk(CloseInfo {
            code: None,
            initiated_by_local,
            reason: String::new(),
        });
    };
    let clean = matches!(frame.code, CloseCode::Normal | CloseCode::Away);
    let info = CloseInfo {
        code: Some(frame.code.into()),
        initiated_by_local,
        reason: frame.reason.to_string(),
    };
    if clean {
        DriverError::ClosedOk(info)
    } else {
        DriverError::ClosedError(info)
    }
}

fn publish_terminal(state: &watch::Sender<DriverState>, terminal: &DriverError) {
    let _ = state.send_if_modified(|current| {
        if current.is_terminal() {
            false
        } else {
            *current = DriverState::Closed(terminal.clone());
            true
        }
    });
}

fn publish_peer_closed(state: &watch::Sender<DriverState>, outcome: &DriverError) {
    let _ = state.send_if_modified(|current| {
        if matches!(current, DriverState::Closed(_) | DriverState::PeerClosed(_)) {
            false
        } else {
            *current = DriverState::PeerClosed(outcome.clone());
            true
        }
    });
}

fn publish_close_timeout(state: &watch::Sender<DriverState>) {
    let _ = state.send_if_modified(|current| {
        if current.is_terminal() {
            return false;
        }
        let terminal = match current {
            DriverState::PeerClosed(outcome) => outcome.clone(),
            DriverState::Closing | DriverState::Open => {
                DriverError::Transport("WebSocket close timed out".to_owned())
            }
            DriverState::Closed(_) => unreachable!(),
        };
        *current = DriverState::Closed(terminal);
        true
    });
}

fn signal_abort(shutdown: &watch::Sender<ShutdownSignal>) {
    let _ = shutdown.send_if_modified(|current| {
        if *current == ShutdownSignal::Abort {
            false
        } else {
            *current = ShutdownSignal::Abort;
            true
        }
    });
}

fn signal_local_close(shutdown: &watch::Sender<ShutdownSignal>) {
    let _ = shutdown.send_if_modified(|current| {
        if *current == ShutdownSignal::Running {
            *current = ShutdownSignal::LocalClose;
            true
        } else {
            false
        }
    });
}

fn signal_peer_close(shutdown: &watch::Sender<ShutdownSignal>) {
    let _ = shutdown.send_if_modified(|current| {
        if matches!(
            current,
            ShutdownSignal::Running | ShutdownSignal::LocalClose
        ) {
            *current = ShutdownSignal::PeerClose;
            true
        } else {
            false
        }
    });
}

async fn wait_for_terminal(state: &watch::Sender<DriverState>) {
    wait_for_terminal_state(state.subscribe()).await;
}

async fn wait_for_terminal_state(mut receiver: watch::Receiver<DriverState>) {
    while !receiver.borrow().is_terminal() {
        if receiver.changed().await.is_err() {
            return;
        }
    }
}

async fn wait_for_driver_tasks(mut remaining: watch::Receiver<usize>) -> Result<(), DriverError> {
    while *remaining.borrow() != 0 {
        if remaining.changed().await.is_err() {
            if *remaining.borrow() == 0 {
                return Ok(());
            }
            return Err(DriverError::Transport(
                "connection tasks stopped without completion confirmation".to_owned(),
            ));
        }
    }
    Ok(())
}

async fn close_timeout_expired(
    state: watch::Receiver<DriverState>,
    timeout: std::time::Duration,
) -> bool {
    tokio::select! {
        () = tokio::time::sleep(timeout) => true,
        () = wait_for_terminal_state(state) => false,
    }
}

fn close_result(error: DriverError) -> Result<(), DriverError> {
    match error {
        DriverError::ClosedOk(_) => Ok(()),
        other => Err(other),
    }
}

#[cfg(test)]
mod tests {
    use std::{sync::Arc, time::Duration};

    use tokio::{
        sync::{Semaphore, mpsc, oneshot, watch},
        time::timeout,
    };
    use tokio_tungstenite::tungstenite::Message;

    use super::{
        DriverState, OutboundCommand, ShutdownSignal, WriterControl, WriterEvent, classify_close,
        close_timeout_expired, next_writer_event, publish_close_timeout, publish_terminal,
        publish_writer_failure, signal_abort, signal_local_close, signal_peer_close,
    };
    use crate::error::{CloseInfo, DriverError};

    #[tokio::test]
    async fn successful_close_stops_watchdog_promptly() {
        let (state, receiver) = watch::channel(DriverState::Open);
        let watchdog = tokio::spawn(close_timeout_expired(receiver, Duration::from_mins(1)));
        publish_terminal(&state, &DriverError::ClosedOk(CloseInfo::local_normal()));

        let expired = timeout(Duration::from_millis(100), watchdog)
            .await
            .expect("close watchdog kept sleeping after terminal state")
            .unwrap();
        assert!(!expired);
    }

    #[tokio::test]
    async fn writer_event_selection_does_not_starve_outbound_work() {
        let (control_sender, mut control) = mpsc::channel(1);
        let (outbound_sender, mut outbound) = mpsc::channel(1);
        control_sender.send(WriterControl::Flush).await.unwrap();
        let (response, _response_receiver) = oneshot::channel();
        let permit = Arc::new(Semaphore::new(1)).acquire_owned().await.unwrap();
        outbound_sender
            .send(OutboundCommand {
                message: Message::Text("application".into()),
                response,
                _permit: permit,
            })
            .await
            .unwrap();

        for _ in 0..64 {
            match next_writer_event(&mut control, &mut outbound).await {
                WriterEvent::Control(Some(WriterControl::Flush)) => {
                    control_sender.send(WriterControl::Flush).await.unwrap();
                }
                WriterEvent::Outbound(Some(command)) => {
                    assert_eq!(command.message, Message::Text("application".into()));
                    return;
                }
                WriterEvent::Control(None) | WriterEvent::Outbound(None) => {
                    panic!("writer event channel closed during fairness probe");
                }
            }
        }

        panic!("outbound work was starved by a continuously ready control queue");
    }

    #[test]
    fn close_without_status_code_is_clean_but_preserves_missing_code() {
        let outcome = classify_close(None, false);

        assert!(matches!(
            outcome,
            DriverError::ClosedOk(CloseInfo {
                code: None,
                initiated_by_local: false,
                ..
            }),
        ));
    }

    #[test]
    fn abort_shutdown_is_absorbing() {
        let (shutdown, receiver) = watch::channel(ShutdownSignal::Running);

        signal_abort(&shutdown);
        signal_local_close(&shutdown);
        signal_peer_close(&shutdown);

        assert_eq!(*receiver.borrow(), ShutdownSignal::Abort);
    }

    #[test]
    fn peer_close_advances_local_close() {
        let (shutdown, receiver) = watch::channel(ShutdownSignal::Running);

        signal_local_close(&shutdown);
        signal_peer_close(&shutdown);

        assert_eq!(*receiver.borrow(), ShutdownSignal::PeerClose);
    }

    #[test]
    fn close_timeout_preserves_an_observed_peer_outcome() {
        let outcome = DriverError::ClosedOk(CloseInfo {
            code: Some(1000),
            initiated_by_local: false,
            reason: "done".to_owned(),
        });
        let (state, receiver) = watch::channel(DriverState::PeerClosed(outcome.clone()));

        publish_close_timeout(&state);

        assert_eq!(*receiver.borrow(), DriverState::Closed(outcome));
    }

    #[test]
    fn writer_close_error_preserves_an_observed_peer_outcome() {
        let outcome = DriverError::ClosedOk(CloseInfo {
            code: Some(1000),
            initiated_by_local: false,
            reason: "done".to_owned(),
        });
        let (state, receiver) = watch::channel(DriverState::PeerClosed(outcome.clone()));

        let selected = publish_writer_failure(
            &state,
            &DriverError::Transport("close reply failed".to_owned()),
        );

        assert_eq!(*receiver.borrow(), DriverState::Closed(outcome.clone()));
        assert_eq!(selected, outcome);
    }
}
