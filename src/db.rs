//! Summary: Database open/close and core management logic.
//! Copyright (c) Yoab. All rights reserved.

use std::path::Path;

use crate::error::Result;
use crate::tx::{ReadTx, WriteTx};

/// The main database handle.
///
/// A `Database` represents an open connection to a thunder database file.
/// It provides methods to begin read and write transactions.
///
/// # Concurrency
///
/// - Multiple read transactions can be active concurrently.
/// - Only one write transaction can be active at a time.
pub struct Database {
    #[allow(dead_code)]
    path: std::path::PathBuf,
}

impl Database {
    /// Opens a database at the given path.
    ///
    /// If the file does not exist, a new database will be created.
    ///
    /// # Errors
    ///
    /// Returns an error if the file cannot be opened or created,
    /// or if the database file is corrupted.
    pub fn open<P: AsRef<Path>>(_path: P) -> Result<Self> {
        todo!("Database::open not yet implemented")
    }

    /// Begins a new read-only transaction.
    ///
    /// Read transactions provide a consistent snapshot view of the database.
    /// Multiple read transactions can be active concurrently.
    pub fn read_tx(&self) -> ReadTx<'_> {
        todo!("Database::read_tx not yet implemented")
    }

    /// Begins a new read-write transaction.
    ///
    /// Only one write transaction can be active at a time.
    /// The transaction must be committed to persist changes.
    pub fn write_tx(&mut self) -> WriteTx<'_> {
        todo!("Database::write_tx not yet implemented")
    }
}
