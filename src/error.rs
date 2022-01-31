use std::io;

/// Error type for kvs.
#[derive(thiserror::Error, Debug)]
pub enum KvsError {
    /// IO error.
    #[error("{0}")]
    IO(io::Error),
    /// Deserialization error from json.
    #[error("0")]
    Deserialization(serde_json::Error),
    /// The key being read (from index, indicating inconsistency between index and log)
    /// or removed does not exist.
    #[error("Key not found")]
    KeyNotFound,
}

/// Result type for kvs.
pub type Result<T> = std::result::Result<T, KvsError>;

impl From<io::Error> for KvsError {
    fn from(err: io::Error) -> Self {
        KvsError::IO(err)
    }
}

impl From<serde_json::Error> for KvsError {
    fn from(err: serde_json::Error) -> Self {
        KvsError::Deserialization(err)
    }
}
