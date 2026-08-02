use std::sync::Arc;

use tokio_tungstenite::{
    connect_async_with_config,
    tungstenite::{client::IntoClientRequest, http::Uri},
};

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
    if has_invalid_explicit_port(request.uri()) {
        return Err(DriverError::InvalidUri(
            "WebSocket URI has an invalid explicit port".to_owned(),
        ));
    }

    let (websocket, _) = connect_async_with_config(request, Some(config.websocket_config()), false)
        .await
        .map_err(|error| {
            DriverError::ConnectionFailed(format!("WebSocket connection failed: {error}"))
        })?;
    Ok(ConnectionDriver::start(websocket, config))
}

fn has_invalid_explicit_port(uri: &Uri) -> bool {
    uri.authority().is_some_and(|authority| {
        let host_and_port = authority
            .as_str()
            .rsplit('@')
            .next()
            .expect("authority always has one host segment");
        host_and_port.len() > authority.host().len() && authority.port_u16().is_none()
    })
}

#[cfg(test)]
mod tests {
    use tokio_tungstenite::tungstenite::client::IntoClientRequest;

    use super::has_invalid_explicit_port;

    #[test]
    fn explicit_port_validation_preserves_valid_authorities() {
        for value in [
            "ws://example.test/path",
            "ws://example.test:0/path",
            "ws://example.test:65535/path",
            "ws://[::1]/path",
            "ws://[::1]:80/path",
        ] {
            let request = value.into_client_request().unwrap();
            assert!(!has_invalid_explicit_port(request.uri()), "{value}");
        }
    }

    #[test]
    fn explicit_port_validation_rejects_unusable_authorities() {
        for value in [
            "ws://example.test:/path",
            "ws://example.test:abc/path",
            "ws://example.test:65536/path",
            "ws://[::1]:99999/path",
        ] {
            let request = value.into_client_request().unwrap();
            assert!(has_invalid_explicit_port(request.uri()), "{value}");
        }
    }
}
