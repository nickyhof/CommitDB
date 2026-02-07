/// Error type for CommitDB client operations.
#[derive(Debug, thiserror::Error)]
pub enum Error {
    /// TCP or I/O error.
    #[error("connection error: {0}")]
    Connection(#[from] std::io::Error),

    /// JSON serialization/deserialization error.
    #[error("protocol error: {0}")]
    Protocol(#[from] serde_json::Error),

    /// TLS handshake or configuration error.
    #[error("TLS error: {0}")]
    Tls(#[from] native_tls::Error),

    /// TLS handshake error (wraps the generic HandshakeError).
    #[error("TLS handshake failed")]
    TlsHandshake(String),

    /// Server returned an error message.
    #[error("server error: {0}")]
    Server(String),

    /// Authentication failed.
    #[error("authentication failed: {0}")]
    Auth(String),

    /// Client is not connected.
    #[error("not connected — call connect() first")]
    NotConnected,
}

/// Convenience type alias.
pub type Result<T> = std::result::Result<T, Error>;
