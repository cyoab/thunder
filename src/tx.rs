//! Summary: Read and write transaction types.
//! Copyright (c) Yoab. All rights reserved.

use crate::db::Database;
use crate::error::Result;

/// A read-only transaction.
///
/// Provides a consistent snapshot view of the database at the time
/// the transaction was started. Read transactions never block other
/// read transactions.
///
/// # Lifetime
///
/// The transaction holds a reference to the database and must not
/// outlive it.
pub struct ReadTx<'db> {
    #[allow(dead_code)]
    db: &'db Database,
}

impl<'db> ReadTx<'db> {
    /// Creates a new read transaction.
    pub(crate) fn new(db: &'db Database) -> Self {
        Self { db }
    }

    /// Retrieves the value associated with the given key.
    ///
    /// Returns `None` if the key does not exist.
    pub fn get(&self, _key: &[u8]) -> Option<Vec<u8>> {
        todo!("ReadTx::get not yet implemented")
    }
}

/// A read-write transaction.
///
/// Provides exclusive write access to the database. Changes are not
/// visible to other transactions until `commit()` is called.
///
/// # Lifetime
///
/// The transaction holds a mutable reference to the database and must
/// not outlive it. Only one write transaction can exist at a time.
pub struct WriteTx<'db> {
    #[allow(dead_code)]
    db: &'db mut Database,
}

impl<'db> WriteTx<'db> {
    /// Creates a new write transaction.
    pub(crate) fn new(db: &'db mut Database) -> Self {
        Self { db }
    }

    /// Inserts or updates a key-value pair.
    ///
    /// If the key already exists, its value will be overwritten.
    pub fn put(&mut self, _key: &[u8], _value: &[u8]) {
        todo!("WriteTx::put not yet implemented")
    }

    /// Deletes a key from the database.
    ///
    /// Does nothing if the key does not exist.
    pub fn delete(&mut self, _key: &[u8]) {
        todo!("WriteTx::delete not yet implemented")
    }

    /// Commits the transaction, persisting all changes.
    ///
    /// # Errors
    ///
    /// Returns an error if the commit fails due to I/O errors
    /// or other issues.
    pub fn commit(self) -> Result<()> {
        todo!("WriteTx::commit not yet implemented")
    }
}
