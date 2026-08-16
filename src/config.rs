use std::time::Duration;

use tokio::sync::Semaphore;
use tokio_tungstenite::tungstenite::protocol::WebSocketConfig;

use crate::error::DriverError;

pub const DEFAULT_CLOSE_TIMEOUT_SECONDS: f64 = 10.0;
pub const DEFAULT_MAX_BUFFERED_BYTES: usize = 1_048_576;
pub const DEFAULT_MAX_MESSAGE_SIZE: usize = 1_048_576;
pub const DEFAULT_MAX_QUEUE: usize = 16;
pub const MAX_CONTROL_PAYLOAD_BYTES: usize = 125;
pub const MAX_FRAME_HEADER_BYTES: usize = 14;
pub const MAX_QUEUE_CAPACITY: usize = Semaphore::MAX_PERMITS;
pub const READ_BUFFER_BYTES: usize = 16_384;
pub const WRITE_BUFFER_BYTES: usize = 0;

#[derive(Clone, Copy, Debug)]
pub struct ConnectionConfig {
    pub close_timeout: Duration,
    pub max_buffered_bytes: usize,
    pub max_message_size: usize,
    pub max_queue: usize,
}

impl ConnectionConfig {
    pub fn new(
        max_queue: usize,
        max_message_size: usize,
        max_buffered_bytes: usize,
        close_timeout_seconds: f64,
    ) -> Result<Self, DriverError> {
        if max_queue == 0 {
            return Err(DriverError::ResourceLimit(
                "max_queue must be greater than zero".to_owned(),
            ));
        }
        if max_queue > MAX_QUEUE_CAPACITY {
            return Err(DriverError::ResourceLimit(format!(
                "max_queue must not exceed {MAX_QUEUE_CAPACITY}",
            )));
        }
        if max_message_size == 0 {
            return Err(DriverError::ResourceLimit(
                "max_message_size must be greater than zero".to_owned(),
            ));
        }
        if max_buffered_bytes < max_message_size {
            return Err(DriverError::ResourceLimit(
                "max_buffered_bytes must be at least max_message_size".to_owned(),
            ));
        }
        if max_buffered_bytes > u32::MAX as usize {
            return Err(DriverError::ResourceLimit(format!(
                "max_buffered_bytes must not exceed {}",
                u32::MAX,
            )));
        }
        let close_timeout = Duration::try_from_secs_f64(close_timeout_seconds).map_err(|_| {
            DriverError::ResourceLimit(
                "close_timeout must be a finite number greater than zero".to_owned(),
            )
        })?;
        if close_timeout.is_zero() {
            return Err(DriverError::ResourceLimit(
                "close_timeout must be a finite number greater than zero".to_owned(),
            ));
        }

        Ok(Self {
            close_timeout,
            max_buffered_bytes,
            max_message_size,
            max_queue,
        })
    }

    pub fn websocket_config(self) -> WebSocketConfig {
        let max_write_buffer_size =
            self.max_buffered_bytes.max(MAX_CONTROL_PAYLOAD_BYTES) + MAX_FRAME_HEADER_BYTES;
        WebSocketConfig::default()
            .read_buffer_size(READ_BUFFER_BYTES)
            .write_buffer_size(WRITE_BUFFER_BYTES)
            .max_write_buffer_size(max_write_buffer_size)
            .max_message_size(Some(self.max_message_size))
            .max_frame_size(Some(self.max_message_size))
    }
}

impl Default for ConnectionConfig {
    fn default() -> Self {
        Self {
            close_timeout: Duration::from_secs_f64(DEFAULT_CLOSE_TIMEOUT_SECONDS),
            max_buffered_bytes: DEFAULT_MAX_BUFFERED_BYTES,
            max_message_size: DEFAULT_MAX_MESSAGE_SIZE,
            max_queue: DEFAULT_MAX_QUEUE,
        }
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn defaults_are_finite_and_cross_field_safe() {
        let config = ConnectionConfig::default();
        let websocket = config.websocket_config();

        assert_eq!(config.max_queue, DEFAULT_MAX_QUEUE);
        assert_eq!(config.max_buffered_bytes, DEFAULT_MAX_BUFFERED_BYTES);
        assert_eq!(websocket.read_buffer_size, READ_BUFFER_BYTES);
        assert_eq!(websocket.write_buffer_size, WRITE_BUFFER_BYTES);
        assert_eq!(
            websocket.max_write_buffer_size,
            DEFAULT_MAX_BUFFERED_BYTES + MAX_FRAME_HEADER_BYTES,
        );
        assert_eq!(websocket.max_message_size, Some(DEFAULT_MAX_MESSAGE_SIZE));
        assert_eq!(websocket.max_frame_size, Some(DEFAULT_MAX_MESSAGE_SIZE));
    }

    #[test]
    fn rejects_a_byte_budget_smaller_than_one_message() {
        let result = ConnectionConfig::new(DEFAULT_MAX_QUEUE, 2, 1, DEFAULT_CLOSE_TIMEOUT_SECONDS);

        assert!(matches!(result, Err(DriverError::ResourceLimit(_))));
    }

    #[test]
    fn rejects_queue_capacity_above_tokio_limit() {
        let result =
            ConnectionConfig::new(MAX_QUEUE_CAPACITY + 1, 1, 1, DEFAULT_CLOSE_TIMEOUT_SECONDS);

        assert!(matches!(result, Err(DriverError::ResourceLimit(_))));
    }
}
