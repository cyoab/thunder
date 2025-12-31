//! Summary: Error types for the thunder database engine.
//! Copyright (c) Yoab. All rights reserved.

use std::fmt;
use std::io;

/// Result type alias for thunder operations.
pub type Result<T> = std::result::Result<T, Error>;

/// Error types for thunder database operations.
#[derive(Debug)]
#[non_exhaustive]
pub enum Error {
    /// I/O error from filesystem operations.
    Io(io::Error),
    /// Database file is corrupted or invalid.
    Corrupted(String),
    /// Invalid page encountered.
    InvalidPage(String),
    /// Transaction is no longer valid.
    TxClosed,
    /// Key not found in the database.
    KeyNotFound,
    /// Database is already open.
    DatabaseAlreadyOpen,
    /// Database file could not be opened.
    DatabaseOpen(String),
}

impl fmt::Display for Error {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        match self {
            Error::Io(err) => write!(f, "I/O error: {err}"),
            Error::Corrupted(msg) => write!(f, "database corrupted: {msg}"),
            Error::InvalidPage(msg) => write!(f, "invalid page: {msg}"),
            Error::TxClosed => write!(f, "transaction is closed"),
            Error::KeyNotFound => write!(f, "key not found"),
            Error::DatabaseAlreadyOpen => write!(f, "database is already open"),
            Error::DatabaseOpen(msg) => write!(f, "failed to open database: {msg}"),
        }
    }
}

impl std::error::Error for Error {
    fn source(&self) -> Option<&(dyn std::error::Error + 'static)> {
        match self {
            Error::Io(err) => Some(err),
            _ => None,
        }
    }
}

impl From<io::Error> for Error {
    fn from(err: io::Error) -> Self {
        Error::Io(err)
    }
}
