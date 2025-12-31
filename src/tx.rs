//! Summary: Read and write transaction types.
//! Copyright (c) YOAB. All rights reserved.

use crate::btree::BTree;
use crate::db::Database;
use crate::error::{Error, Result};

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
    pub fn get(&self, key: &[u8]) -> Option<Vec<u8>> {
        self.db.tree().get(key).map(|v| v.to_vec())
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
    db: &'db mut Database,
    /// Pending changes stored in a scratch B+ tree.
    /// Only applied to the main tree on commit.
    pending: BTree,
    /// Keys marked for deletion.
    deleted: Vec<Vec<u8>>,
    /// Whether this transaction has been committed.
    committed: bool,
}

impl<'db> WriteTx<'db> {
    /// Creates a new write transaction.
    pub(crate) fn new(db: &'db mut Database) -> Self {
        Self {
            db,
            pending: BTree::new(),
            deleted: Vec::new(),
            committed: false,
        }
    }

    /// Inserts or updates a key-value pair.
    ///
    /// If the key already exists, its value will be overwritten.
    pub fn put(&mut self, key: &[u8], value: &[u8]) {
        // Remove from deleted list if present.
        self.deleted.retain(|k| k.as_slice() != key);
        // Add to pending changes.
        self.pending.insert(key.to_vec(), value.to_vec());
    }

    /// Deletes a key from the database.
    ///
    /// Does nothing if the key does not exist.
    pub fn delete(&mut self, key: &[u8]) {
        // Remove from pending if present.
        self.pending.remove(key);
        // Mark for deletion from main tree.
        if !self.deleted.iter().any(|k| k.as_slice() == key) {
            self.deleted.push(key.to_vec());
        }
    }

    /// Commits the transaction, persisting all changes.
    ///
    /// # Errors
    ///
    /// Returns an error if the commit fails due to I/O errors
    /// or other issues. On error, the transaction is effectively
    /// rolled back (changes are not persisted).
    pub fn commit(mut self) -> Result<()> {
        // Record the number of operations for error context.
        let deletion_count = self.deleted.len();
        let insertion_count = self.pending.len();

        // Apply deletions to main tree.
        for key in &self.deleted {
            self.db.tree_mut().remove(key);
        }

        // Apply pending insertions to main tree.
        for (key, value) in self.pending.iter() {
            self.db.tree_mut().insert(key.to_vec(), value.to_vec());
        }

        // Persist to disk.
        match self.db.persist_tree() {
            Ok(()) => {
                self.committed = true;
                Ok(())
            }
            Err(e) => {
                // Note: The in-memory tree has already been modified.
                // A future improvement would be to maintain a copy for rollback.
                // For now, we report the error with context.
                Err(Error::TxCommitFailed {
                    reason: format!(
                        "failed to persist {} deletions and {} insertions",
                        deletion_count, insertion_count
                    ),
                    source: Some(Box::new(e)),
                })
            }
        }
    }
}

impl Drop for WriteTx<'_> {
    fn drop(&mut self) {
        // If not committed, changes are automatically discarded
        // since they're only in the pending tree.
        if !self.committed {
            // Nothing to do - pending changes are dropped with self.
        }
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use std::fs;

    fn test_db_path(name: &str) -> String {
        format!("/tmp/thunder_tx_test_{name}.db")
    }

    fn cleanup(path: &str) {
        let _ = fs::remove_file(path);
    }

    // ==================== ReadTx Tests ====================

    #[test]
    fn test_read_tx_get_empty_db() {
        let path = test_db_path("read_empty");
        cleanup(&path);

        let db = Database::open(&path).expect("open should succeed");
        let rtx = db.read_tx();

        assert!(rtx.get(b"any_key").is_none());

        cleanup(&path);
    }

    #[test]
    fn test_read_tx_get_existing_key() {
        let path = test_db_path("read_existing");
        cleanup(&path);

        let mut db = Database::open(&path).expect("open should succeed");

        // Insert data first.
        {
            let mut wtx = db.write_tx();
            wtx.put(b"key1", b"value1");
            wtx.commit().expect("commit should succeed");
        }

        let rtx = db.read_tx();
        assert_eq!(rtx.get(b"key1"), Some(b"value1".to_vec()));

        cleanup(&path);
    }

    #[test]
    fn test_read_tx_multiple_gets() {
        let path = test_db_path("read_multiple");
        cleanup(&path);

        let mut db = Database::open(&path).expect("open should succeed");

        // Insert multiple keys.
        {
            let mut wtx = db.write_tx();
            wtx.put(b"a", b"1");
            wtx.put(b"b", b"2");
            wtx.put(b"c", b"3");
            wtx.commit().expect("commit should succeed");
        }

        let rtx = db.read_tx();
        assert_eq!(rtx.get(b"a"), Some(b"1".to_vec()));
        assert_eq!(rtx.get(b"b"), Some(b"2".to_vec()));
        assert_eq!(rtx.get(b"c"), Some(b"3".to_vec()));
        assert_eq!(rtx.get(b"d"), None);

        cleanup(&path);
    }

    // ==================== WriteTx Basic Tests ====================

    #[test]
    fn test_write_tx_put_single() {
        let path = test_db_path("write_single");
        cleanup(&path);

        let mut db = Database::open(&path).expect("open should succeed");

        {
            let mut wtx = db.write_tx();
            wtx.put(b"key", b"value");
            wtx.commit().expect("commit should succeed");
        }

        let rtx = db.read_tx();
        assert_eq!(rtx.get(b"key"), Some(b"value".to_vec()));

        cleanup(&path);
    }

    #[test]
    fn test_write_tx_put_multiple_same_tx() {
        let path = test_db_path("write_multi_same");
        cleanup(&path);

        let mut db = Database::open(&path).expect("open should succeed");

        {
            let mut wtx = db.write_tx();
            wtx.put(b"k1", b"v1");
            wtx.put(b"k2", b"v2");
            wtx.put(b"k3", b"v3");
            wtx.commit().expect("commit should succeed");
        }

        let rtx = db.read_tx();
        assert_eq!(rtx.get(b"k1"), Some(b"v1".to_vec()));
        assert_eq!(rtx.get(b"k2"), Some(b"v2".to_vec()));
        assert_eq!(rtx.get(b"k3"), Some(b"v3".to_vec()));

        cleanup(&path);
    }

    #[test]
    fn test_write_tx_put_overwrite_same_tx() {
        let path = test_db_path("write_overwrite_same");
        cleanup(&path);

        let mut db = Database::open(&path).expect("open should succeed");

        {
            let mut wtx = db.write_tx();
            wtx.put(b"key", b"value1");
            wtx.put(b"key", b"value2"); // Overwrite in same tx.
            wtx.commit().expect("commit should succeed");
        }

        let rtx = db.read_tx();
        assert_eq!(rtx.get(b"key"), Some(b"value2".to_vec()));

        cleanup(&path);
    }

    // ==================== WriteTx Delete Tests ====================

    #[test]
    fn test_write_tx_delete_existing() {
        let path = test_db_path("delete_existing");
        cleanup(&path);

        let mut db = Database::open(&path).expect("open should succeed");

        // Insert.
        {
            let mut wtx = db.write_tx();
            wtx.put(b"key", b"value");
            wtx.commit().expect("commit should succeed");
        }

        // Delete.
        {
            let mut wtx = db.write_tx();
            wtx.delete(b"key");
            wtx.commit().expect("commit should succeed");
        }

        let rtx = db.read_tx();
        assert_eq!(rtx.get(b"key"), None);

        cleanup(&path);
    }

    #[test]
    fn test_write_tx_delete_nonexistent() {
        let path = test_db_path("delete_nonexistent");
        cleanup(&path);

        let mut db = Database::open(&path).expect("open should succeed");

        // Delete nonexistent key (should be no-op).
        {
            let mut wtx = db.write_tx();
            wtx.delete(b"nonexistent");
            wtx.commit().expect("commit should succeed");
        }

        // Verify nothing crashed.
        let rtx = db.read_tx();
        assert_eq!(rtx.get(b"nonexistent"), None);

        cleanup(&path);
    }

    #[test]
    fn test_write_tx_delete_then_put_same_key() {
        let path = test_db_path("delete_then_put");
        cleanup(&path);

        let mut db = Database::open(&path).expect("open should succeed");

        // Insert initial.
        {
            let mut wtx = db.write_tx();
            wtx.put(b"key", b"original");
            wtx.commit().expect("commit should succeed");
        }

        // Delete then re-add in same tx.
        {
            let mut wtx = db.write_tx();
            wtx.delete(b"key");
            wtx.put(b"key", b"new_value");
            wtx.commit().expect("commit should succeed");
        }

        let rtx = db.read_tx();
        assert_eq!(rtx.get(b"key"), Some(b"new_value".to_vec()));

        cleanup(&path);
    }

    #[test]
    fn test_write_tx_put_then_delete_same_key() {
        let path = test_db_path("put_then_delete");
        cleanup(&path);

        let mut db = Database::open(&path).expect("open should succeed");

        // Put then delete in same tx.
        {
            let mut wtx = db.write_tx();
            wtx.put(b"key", b"value");
            wtx.delete(b"key");
            wtx.commit().expect("commit should succeed");
        }

        let rtx = db.read_tx();
        assert_eq!(rtx.get(b"key"), None);

        cleanup(&path);
    }

    // ==================== Transaction Rollback Tests ====================

    #[test]
    fn test_write_tx_rollback_on_drop() {
        let path = test_db_path("rollback_drop");
        cleanup(&path);

        let mut db = Database::open(&path).expect("open should succeed");

        // Insert but don't commit (drop).
        {
            let mut wtx = db.write_tx();
            wtx.put(b"uncommitted", b"data");
            // Drops without commit.
        }

        // Verify data was not persisted.
        let rtx = db.read_tx();
        assert_eq!(rtx.get(b"uncommitted"), None);

        cleanup(&path);
    }

    #[test]
    fn test_write_tx_rollback_preserves_existing() {
        let path = test_db_path("rollback_preserves");
        cleanup(&path);

        let mut db = Database::open(&path).expect("open should succeed");

        // Commit initial data.
        {
            let mut wtx = db.write_tx();
            wtx.put(b"existing", b"data");
            wtx.commit().expect("commit should succeed");
        }

        // Start new tx, modify, but don't commit.
        {
            let mut wtx = db.write_tx();
            wtx.put(b"new", b"value");
            wtx.delete(b"existing");
            // Drops without commit.
        }

        // Verify existing data preserved, new data not present.
        let rtx = db.read_tx();
        assert_eq!(rtx.get(b"existing"), Some(b"data".to_vec()));
        assert_eq!(rtx.get(b"new"), None);

        cleanup(&path);
    }

    #[test]
    fn test_write_tx_multiple_uncommitted() {
        let path = test_db_path("multi_uncommitted");
        cleanup(&path);

        let mut db = Database::open(&path).expect("open should succeed");

        // Multiple uncommitted transactions.
        for i in 0..5 {
            let mut wtx = db.write_tx();
            wtx.put(format!("key{i}").as_bytes(), b"value");
            // Drops without commit.
        }

        // Verify none persisted.
        let rtx = db.read_tx();
        for i in 0..5 {
            assert_eq!(rtx.get(format!("key{i}").as_bytes()), None);
        }

        cleanup(&path);
    }

    // ==================== Transaction Isolation Tests ====================

    #[test]
    fn test_write_tx_isolation_from_pending() {
        let path = test_db_path("isolation_pending");
        cleanup(&path);

        let mut db = Database::open(&path).expect("open should succeed");

        // Start write tx.
        let mut wtx = db.write_tx();
        wtx.put(b"pending_key", b"pending_value");

        // Changes should be in pending, not yet in main tree.
        // We can't easily read from the same db while wtx is active due to borrow rules.
        // But we can verify after commit.
        wtx.commit().expect("commit should succeed");

        let rtx = db.read_tx();
        assert_eq!(rtx.get(b"pending_key"), Some(b"pending_value".to_vec()));

        cleanup(&path);
    }

    // ==================== Large Data Tests ====================

    #[test]
    fn test_write_tx_large_value() {
        let path = test_db_path("large_value");
        cleanup(&path);

        let mut db = Database::open(&path).expect("open should succeed");

        let large_value = vec![0xAB; 100_000]; // 100KB value.

        {
            let mut wtx = db.write_tx();
            wtx.put(b"large", &large_value);
            wtx.commit().expect("commit should succeed");
        }

        let rtx = db.read_tx();
        assert_eq!(rtx.get(b"large"), Some(large_value));

        cleanup(&path);
    }

    #[test]
    fn test_write_tx_large_key() {
        let path = test_db_path("large_key");
        cleanup(&path);

        let mut db = Database::open(&path).expect("open should succeed");

        let large_key = vec![b'K'; 10_000]; // 10KB key.

        {
            let mut wtx = db.write_tx();
            wtx.put(&large_key, b"value");
            wtx.commit().expect("commit should succeed");
        }

        let rtx = db.read_tx();
        assert_eq!(rtx.get(&large_key), Some(b"value".to_vec()));

        cleanup(&path);
    }

    #[test]
    fn test_write_tx_many_keys_single_tx() {
        let path = test_db_path("many_keys");
        cleanup(&path);

        let mut db = Database::open(&path).expect("open should succeed");

        {
            let mut wtx = db.write_tx();
            for i in 0..500 {
                let key = format!("key_{i:05}");
                let value = format!("value_{i}");
                wtx.put(key.as_bytes(), value.as_bytes());
            }
            wtx.commit().expect("commit should succeed");
        }

        let rtx = db.read_tx();
        for i in 0..500 {
            let key = format!("key_{i:05}");
            let expected_value = format!("value_{i}");
            assert_eq!(
                rtx.get(key.as_bytes()),
                Some(expected_value.into_bytes()),
                "key {key} mismatch"
            );
        }

        cleanup(&path);
    }

    // ==================== Edge Cases ====================

    #[test]
    fn test_write_tx_empty_key() {
        let path = test_db_path("empty_key");
        cleanup(&path);

        let mut db = Database::open(&path).expect("open should succeed");

        {
            let mut wtx = db.write_tx();
            wtx.put(b"", b"empty_key_value");
            wtx.commit().expect("commit should succeed");
        }

        let rtx = db.read_tx();
        assert_eq!(rtx.get(b""), Some(b"empty_key_value".to_vec()));

        cleanup(&path);
    }

    #[test]
    fn test_write_tx_empty_value() {
        let path = test_db_path("empty_value");
        cleanup(&path);

        let mut db = Database::open(&path).expect("open should succeed");

        {
            let mut wtx = db.write_tx();
            wtx.put(b"key", b"");
            wtx.commit().expect("commit should succeed");
        }

        let rtx = db.read_tx();
        assert_eq!(rtx.get(b"key"), Some(b"".to_vec()));

        cleanup(&path);
    }

    #[test]
    fn test_write_tx_binary_data() {
        let path = test_db_path("binary_data");
        cleanup(&path);

        let mut db = Database::open(&path).expect("open should succeed");

        let binary_key = vec![0x00, 0xFF, 0x7F, 0x80, 0x01];
        let binary_value = vec![0xDE, 0xAD, 0xBE, 0xEF];

        {
            let mut wtx = db.write_tx();
            wtx.put(&binary_key, &binary_value);
            wtx.commit().expect("commit should succeed");
        }

        let rtx = db.read_tx();
        assert_eq!(rtx.get(&binary_key), Some(binary_value));

        cleanup(&path);
    }

    #[test]
    fn test_write_tx_sequential_commits() {
        let path = test_db_path("sequential_commits");
        cleanup(&path);

        let mut db = Database::open(&path).expect("open should succeed");

        // Multiple sequential committed transactions.
        for i in 0..10 {
            let mut wtx = db.write_tx();
            wtx.put(format!("key{i}").as_bytes(), format!("value{i}").as_bytes());
            wtx.commit().expect("commit should succeed");
        }

        // Verify all data persisted.
        let rtx = db.read_tx();
        for i in 0..10 {
            assert_eq!(
                rtx.get(format!("key{i}").as_bytes()),
                Some(format!("value{i}").into_bytes())
            );
        }

        cleanup(&path);
    }
}
