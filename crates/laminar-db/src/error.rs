//! Error types for the `LaminarDB` facade.

/// Errors from database operations.
#[derive(Debug, thiserror::Error)]
pub enum DbError {
    /// SQL parse error
    #[error("SQL error: {0}")]
    Sql(#[from] laminar_sql::Error),

    /// Core engine error
    #[error("Engine error: {0}")]
    Engine(#[from] laminar_core::Error),

    /// Streaming API error
    #[error("Streaming error: {0}")]
    Streaming(#[from] laminar_core::streaming::StreamingError),

    /// `DataFusion` error
    #[error("DataFusion error: {0}")]
    DataFusion(#[from] datafusion_common::DataFusionError),

    /// Source not found
    #[error("Source '{0}' not found")]
    SourceNotFound(String),

    /// Sink not found
    #[error("Sink '{0}' not found")]
    SinkNotFound(String),

    /// Query not found
    #[error("Query '{0}' not found")]
    QueryNotFound(String),

    /// Source already exists
    #[error("Source '{0}' already exists")]
    SourceAlreadyExists(String),

    /// Sink already exists
    #[error("Sink '{0}' already exists")]
    SinkAlreadyExists(String),

    /// Schema mismatch between Rust type and SQL definition
    #[error("Schema mismatch: {0}")]
    SchemaMismatch(String),

    /// Invalid SQL statement for the operation
    #[error("Invalid operation: {0}")]
    InvalidOperation(String),

    /// SQL parse error (from streaming parser)
    #[error("SQL parse error: {0}")]
    SqlParse(#[from] laminar_sql::parser::ParseError),

    /// Database is shut down
    #[error("Database is shut down")]
    Shutdown,
}
