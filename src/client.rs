use std::sync::Arc;

use tokio_tungstenite::{connect_async_with_config, tungstenite::client::IntoClientRequest};

use crate::{config::ConnectionConfig, driver::ConnectionDriver, error::DriverError};

pub async fn connect(
    uri: String,
    config: ConnectionConfig,
) -> Result<Arc<ConnectionDriver>, DriverError> {
    let request = uri
        .into_client_request()
        .map_err(|error| DriverError::InvalidUri(format!("invalid WebSocket URI: {error}")))?;
    if request.uri().scheme_str() != Some("ws") {
        return Err(DriverError::InvalidUri(
            "only plain ws:// URIs are supported in this release slice".to_owned(),
        ));
    }

    let (websocket, _) = connect_async_with_config(request, Some(config.websocket_config()), false)
        .await
        .map_err(|error| {
            DriverError::ConnectionFailed(format!("WebSocket connection failed: {error}"))
        })?;
    Ok(ConnectionDriver::start(websocket, config))
}
