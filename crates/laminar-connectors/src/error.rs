//! Connector SDK error types.
//!
//! Provides a unified error hierarchy for all connector operations:
//! - `ConnectorError`: Top-level error for source/sink connector operations
//! - `SerdeError`: Serialization/deserialization errors

use thiserror::Error;

/// Errors that can occur during connector operations.
#[derive(Debug, Error)]
pub enum ConnectorError {
    /// Failed to connect to the external system.
    #[error("connection failed: {0}")]
    ConnectionFailed(String),

    /// Authentication or authorization error.
    #[error("authentication failed: {0}")]
    AuthenticationFailed(String),

    /// Invalid connector configuration.
    #[error("configuration error: {0}")]
    ConfigurationError(String),

    /// Required configuration key is missing.
    #[error("missing required config: {0}")]
    MissingConfig(String),

    /// Error reading data from a source.
    #[error("read error: {0}")]
    ReadError(String),

    /// Error writing data to a sink.
    #[error("write error: {0}")]
    WriteError(String),

    /// Serialization or deserialization error.
    #[error("serde error: {0}")]
    Serde(#[from] SerdeError),

    /// Checkpoint or offset commit error.
    #[error("checkpoint error: {0}")]
    CheckpointError(String),

    /// Transaction error (begin/commit/rollback).
    #[error("transaction error: {0}")]
    TransactionError(String),

    /// The connector is not in the expected state.
    #[error("invalid state: expected {expected}, got {actual}")]
    InvalidState {
        /// The expected state.
        expected: String,
        /// The actual state.
        actual: String,
    },

    /// Schema mismatch between expected and actual data.
    #[error("schema mismatch: {0}")]
    SchemaMismatch(String),

    /// Operation timed out.
    #[error("timeout after {0}ms")]
    Timeout(u64),

    /// The connector has been closed.
    #[error("connector closed")]
    Closed,

    /// An internal error that doesn't fit other categories.
    #[error("internal error: {0}")]
    Internal(String),

    /// An I/O error from the underlying system.
    #[error("I/O error: {0}")]
    Io(#[from] std::io::Error),
}

/// Errors that occur during record serialization or deserialization.
#[derive(Debug, Error)]
pub enum SerdeError {
    /// JSON parsing or encoding error.
    #[error("JSON error: {0}")]
    Json(String),

    /// CSV parsing or encoding error.
    #[error("CSV error: {0}")]
    Csv(String),

    /// The data format is not supported.
    #[error("unsupported format: {0}")]
    UnsupportedFormat(String),

    /// A required field is missing from the input.
    #[error("missing field: {0}")]
    MissingField(String),

    /// A field value could not be converted to the target Arrow type.
    #[error("type conversion error: field '{field}', expected {expected}: {message}")]
    TypeConversion {
        /// The field name.
        field: String,
        /// The expected Arrow data type.
        expected: String,
        /// Details about the conversion failure.
        message: String,
    },

    /// The input data is malformed.
    #[error("malformed input: {0}")]
    MalformedInput(String),
}

impl From<serde_json::Error> for SerdeError {
    fn from(e: serde_json::Error) -> Self {
        SerdeError::Json(e.to_string())
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn test_connector_error_display() {
        let err = ConnectorError::ConnectionFailed("host unreachable".into());
        assert_eq!(err.to_string(), "connection failed: host unreachable");
    }

    #[test]
    fn test_serde_error_from_json() {
        let json_err: Result<serde_json::Value, _> = serde_json::from_str("{bad json");
        let serde_err: SerdeError = json_err.unwrap_err().into();
        assert!(matches!(serde_err, SerdeError::Json(_)));
    }

    #[test]
    fn test_serde_error_into_connector_error() {
        let serde_err = SerdeError::MissingField("timestamp".into());
        let conn_err: ConnectorError = serde_err.into();
        assert!(matches!(conn_err, ConnectorError::Serde(_)));
        assert!(conn_err.to_string().contains("timestamp"));
    }

    #[test]
    fn test_invalid_state_error() {
        let err = ConnectorError::InvalidState {
            expected: "Running".into(),
            actual: "Closed".into(),
        };
        assert!(err.to_string().contains("Running"));
        assert!(err.to_string().contains("Closed"));
    }
}
