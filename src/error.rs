use std::fmt;

#[derive(Clone, Debug, Eq, PartialEq)]
pub struct CloseInfo {
    pub code: Option<u16>,
    pub initiated_by_local: bool,
    pub reason: String,
}

impl CloseInfo {
    pub fn local_normal() -> Self {
        Self {
            code: Some(1000),
            initiated_by_local: true,
            reason: String::new(),
        }
    }

    pub fn describe(&self) -> String {
        let code = self.code.map_or_else(
            || "without a status code".to_owned(),
            |value| value.to_string(),
        );
        let side = if self.initiated_by_local {
            "local endpoint"
        } else {
            "remote endpoint"
        };
        format!("connection closed by {side} with code {code}")
    }
}

#[derive(Clone, Debug, Eq, PartialEq)]
pub enum DriverError {
    ClosedError(CloseInfo),
    ClosedOk(CloseInfo),
    Concurrency(String),
    ConnectionFailed(String),
    InvalidUri(String),
    ResourceLimit(String),
    Transport(String),
}

impl fmt::Display for DriverError {
    fn fmt(&self, formatter: &mut fmt::Formatter<'_>) -> fmt::Result {
        match self {
            Self::ClosedError(info) | Self::ClosedOk(info) => formatter.write_str(&info.describe()),
            Self::Concurrency(message)
            | Self::ConnectionFailed(message)
            | Self::InvalidUri(message)
            | Self::ResourceLimit(message)
            | Self::Transport(message) => formatter.write_str(message),
        }
    }
}

impl std::error::Error for DriverError {}

#[cfg(test)]
mod tests {
    use super::{CloseInfo, DriverError};

    #[test]
    fn close_diagnostic_excludes_the_untrusted_reason() {
        let info = CloseInfo {
            code: Some(1011),
            initiated_by_local: false,
            reason: "failed\nforged-log-entry".to_owned(),
        };
        let diagnostic = DriverError::ClosedError(info.clone()).to_string();

        assert_eq!(info.reason, "failed\nforged-log-entry");
        assert_eq!(
            diagnostic,
            "connection closed by remote endpoint with code 1011"
        );
    }
}
