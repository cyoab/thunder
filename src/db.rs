//! Summary: Database open/close and core management logic.
//! Copyright (c) YOAB. All rights reserved.

use std::fs::{File, OpenOptions};
use std::io::{BufWriter, Read, Seek, SeekFrom, Write};
use std::path::Path;

#[cfg(unix)]
use std::os::unix::io::AsRawFd;

use crate::btree::BTree;
use crate::error::{Error, Result};
use crate::meta::Meta;
use crate::page::PAGE_SIZE;
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
    /// Path to the database file.
    path: std::path::PathBuf,
    /// The underlying file handle.
    file: File,
    /// Current meta page (the one with higher txid).
    meta: Meta,
    /// In-memory B+ tree storing all key-value pairs.
    tree: BTree,
    /// Current write offset in the data section (for append-only writes).
    data_end_offset: u64,
    /// Number of entries currently persisted.
    persisted_entry_count: u64,
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
    pub fn open<P: AsRef<Path>>(path: P) -> Result<Self> {
        let path = path.as_ref();
        let path_buf = path.to_path_buf();

        // Check if file exists to determine if we need to initialize.
        let file_exists = path.exists();

        let mut file = match OpenOptions::new()
            .read(true)
            .write(true)
            .create(true)
            .truncate(false)
            .open(path)
        {
            Ok(f) => f,
            Err(e) => {
                return Err(Error::FileOpen {
                    path: path_buf,
                    source: e,
                });
            }
        };

        let file_len = match file.metadata() {
            Ok(m) => m.len(),
            Err(e) => {
                return Err(Error::FileMetadata {
                    path: path_buf,
                    source: e,
                });
            }
        };

        let (meta, tree, data_end_offset, persisted_entry_count) = if file_exists && file_len > 0 {
            // Existing database: read and validate meta pages, load data.
            let meta = Self::load_meta(&mut file, &path_buf)?;
            let (tree, data_end, count) = Self::load_tree(&mut file, &meta)?;
            (meta, tree, data_end, count)
        } else {
            // New database: initialize with two meta pages.
            let meta = Self::init_db(&mut file, &path_buf)?;
            let data_offset = 2 * PAGE_SIZE as u64 + 8; // After meta pages + entry count
            (meta, BTree::new(), data_offset, 0)
        };

        Ok(Self {
            path: path_buf,
            file,
            meta,
            tree,
            data_end_offset,
            persisted_entry_count,
        })
    }

    /// Initializes a new database file with two meta pages.
    fn init_db(file: &mut File, path: &std::path::PathBuf) -> Result<Meta> {
        let meta = Meta::new();
        let meta_bytes = meta.to_bytes();

        // Seek to beginning of file.
        if let Err(e) = file.seek(SeekFrom::Start(0)) {
            return Err(Error::FileSeek {
                offset: 0,
                context: "initializing database, seeking to start",
                source: e,
            });
        }

        // Write meta page 0.
        if let Err(e) = file.write_all(&meta_bytes) {
            return Err(Error::FileWrite {
                offset: 0,
                len: PAGE_SIZE,
                context: "writing initial meta page 0",
                source: e,
            });
        }

        // Write meta page 1 (identical initially).
        if let Err(e) = file.write_all(&meta_bytes) {
            return Err(Error::FileWrite {
                offset: PAGE_SIZE as u64,
                len: PAGE_SIZE,
                context: "writing initial meta page 1",
                source: e,
            });
        }

        // Ensure data is persisted to disk.
        if let Err(e) = file.sync_all() {
            return Err(Error::FileSync {
                context: "syncing initial meta pages",
                source: e,
            });
        }

        // Log successful initialization (only in debug builds).
        #[cfg(debug_assertions)]
        {
            eprintln!(
                "[thunder] initialized new database at '{}'",
                path.display()
            );
        }
        let _ = path; // Suppress unused warning in release.

        Ok(meta)
    }

    /// Loads and validates meta pages from an existing database file.
    fn load_meta(file: &mut File, _path: &std::path::PathBuf) -> Result<Meta> {
        let mut buf = [0u8; PAGE_SIZE];

        // Seek to meta page 0.
        if let Err(e) = file.seek(SeekFrom::Start(0)) {
            return Err(Error::FileSeek {
                offset: 0,
                context: "seeking to meta page 0",
                source: e,
            });
        }

        // Read meta page 0.
        if let Err(e) = file.read_exact(&mut buf) {
            return Err(Error::FileRead {
                offset: 0,
                len: PAGE_SIZE,
                context: "reading meta page 0",
                source: e,
            });
        }
        let meta0 = Meta::from_bytes(&buf);

        // Seek to meta page 1.
        let meta1_offset = PAGE_SIZE as u64;
        if let Err(e) = file.seek(SeekFrom::Start(meta1_offset)) {
            return Err(Error::FileSeek {
                offset: meta1_offset,
                context: "seeking to meta page 1",
                source: e,
            });
        }

        // Read meta page 1.
        if let Err(e) = file.read_exact(&mut buf) {
            return Err(Error::FileRead {
                offset: meta1_offset,
                len: PAGE_SIZE,
                context: "reading meta page 1",
                source: e,
            });
        }
        let meta1 = Meta::from_bytes(&buf);

        // Select the valid meta page with the highest txid.
        match (meta0, meta1) {
            (Some(m0), Some(m1)) => {
                let m0_valid = m0.validate();
                let m1_valid = m1.validate();

                if !m0_valid && !m1_valid {
                    return Err(Error::BothMetaPagesInvalid);
                }

                if !m0_valid {
                    Ok(m1)
                } else if !m1_valid {
                    Ok(m0)
                } else if m1.txid > m0.txid {
                    Ok(m1)
                } else {
                    Ok(m0)
                }
            }
            (Some(m), None) => {
                if m.validate() {
                    Ok(m)
                } else {
                    Err(Error::InvalidMetaPage {
                        page_number: 0,
                        reason: "meta page 0 parsed but failed validation",
                    })
                }
            }
            (None, Some(m)) => {
                if m.validate() {
                    Ok(m)
                } else {
                    Err(Error::InvalidMetaPage {
                        page_number: 1,
                        reason: "meta page 1 parsed but failed validation",
                    })
                }
            }
            (None, None) => Err(Error::BothMetaPagesInvalid),
        }
    }

    /// Loads the B+ tree data from the database file.
    fn load_tree(file: &mut File, meta: &Meta) -> Result<(BTree, u64, u64)> {
        let mut tree = BTree::new();

        // Data starts after the two meta pages.
        let data_offset = 2 * PAGE_SIZE as u64;

        // Read the data page count from meta.
        if meta.root == 0 {
            // No data stored yet.
            return Ok((tree, data_offset + 8, 0));
        }

        // Seek to data section.
        if let Err(e) = file.seek(SeekFrom::Start(data_offset)) {
            return Err(Error::FileSeek {
                offset: data_offset,
                context: "seeking to data section",
                source: e,
            });
        }

        // Read number of entries.
        let mut count_buf = [0u8; 8];
        if file.read_exact(&mut count_buf).is_err() {
            // No data section yet - this is OK for empty databases.
            return Ok((tree, data_offset + 8, 0));
        }
        let entry_count = u64::from_le_bytes(count_buf);

        // Validate entry count is reasonable (prevent OOM).
        const MAX_ENTRIES: u64 = 100_000_000; // 100 million entries max.
        if entry_count > MAX_ENTRIES {
            return Err(Error::Corrupted {
                context: "loading data entries",
                details: format!(
                    "entry count {entry_count} exceeds maximum allowed {MAX_ENTRIES}"
                ),
            });
        }

        // Track current position for computing end offset.
        let mut current_offset = data_offset + 8;

        // Read each entry.
        for entry_idx in 0..entry_count {
            // Read key length.
            let mut len_buf = [0u8; 4];
            if let Err(e) = file.read_exact(&mut len_buf) {
                return Err(Error::EntryReadFailed {
                    entry_index: entry_idx,
                    field: "key length",
                    source: e,
                });
            }
            let key_len = u32::from_le_bytes(len_buf) as usize;
            current_offset += 4;

            // Validate key length.
            const MAX_KEY_LEN: usize = 64 * 1024; // 64KB max key.
            if key_len > MAX_KEY_LEN {
                return Err(Error::Corrupted {
                    context: "loading entry key",
                    details: format!(
                        "entry {entry_idx}: key length {key_len} exceeds maximum {MAX_KEY_LEN}"
                    ),
                });
            }

            // Read key.
            let mut key = vec![0u8; key_len];
            if let Err(e) = file.read_exact(&mut key) {
                return Err(Error::EntryReadFailed {
                    entry_index: entry_idx,
                    field: "key data",
                    source: e,
                });
            }
            current_offset += key_len as u64;

            // Read value length.
            if let Err(e) = file.read_exact(&mut len_buf) {
                return Err(Error::EntryReadFailed {
                    entry_index: entry_idx,
                    field: "value length",
                    source: e,
                });
            }
            let value_len = u32::from_le_bytes(len_buf) as usize;
            current_offset += 4;

            // Validate value length.
            const MAX_VALUE_LEN: usize = 512 * 1024 * 1024; // 512MB max value.
            if value_len > MAX_VALUE_LEN {
                return Err(Error::Corrupted {
                    context: "loading entry value",
                    details: format!(
                        "entry {entry_idx}: value length {value_len} exceeds maximum {MAX_VALUE_LEN}"
                    ),
                });
            }

            // Read value.
            let mut value = vec![0u8; value_len];
            if let Err(e) = file.read_exact(&mut value) {
                return Err(Error::EntryReadFailed {
                    entry_index: entry_idx,
                    field: "value data",
                    source: e,
                });
            }
            current_offset += value_len as u64;

            tree.insert(key, value);
        }

        Ok((tree, current_offset, entry_count))
    }

    /// Persists the B+ tree data to the database file.
    /// This performs a FULL rewrite of all data - use `persist_incremental` for better performance.
    pub(crate) fn persist_tree(&mut self) -> Result<()> {
        // Data starts after the two meta pages.
        let data_offset = 2 * PAGE_SIZE as u64;

        // Seek to data section.
        if let Err(e) = self.file.seek(SeekFrom::Start(data_offset)) {
            return Err(Error::FileSeek {
                offset: data_offset,
                context: "seeking to data section for persist",
                source: e,
            });
        }

        // Use a buffered writer for better performance.
        let mut writer = BufWriter::with_capacity(64 * 1024, &self.file);

        // Write number of entries.
        let entry_count = self.tree.len() as u64;
        if let Err(e) = writer.write_all(&entry_count.to_le_bytes()) {
            return Err(Error::FileWrite {
                offset: data_offset,
                len: 8,
                context: "writing entry count",
                source: e,
            });
        }

        // Track current offset for error reporting.
        let mut current_offset = data_offset + 8;

        // Write each entry.
        for (key, value) in self.tree.iter() {
            // Write key length.
            let key_len_bytes = (key.len() as u32).to_le_bytes();
            if let Err(e) = writer.write_all(&key_len_bytes) {
                return Err(Error::FileWrite {
                    offset: current_offset,
                    len: 4,
                    context: "writing key length",
                    source: e,
                });
            }
            current_offset += 4;

            // Write key.
            if let Err(e) = writer.write_all(key) {
                return Err(Error::FileWrite {
                    offset: current_offset,
                    len: key.len(),
                    context: "writing key data",
                    source: e,
                });
            }
            current_offset += key.len() as u64;

            // Write value length.
            let value_len_bytes = (value.len() as u32).to_le_bytes();
            if let Err(e) = writer.write_all(&value_len_bytes) {
                return Err(Error::FileWrite {
                    offset: current_offset,
                    len: 4,
                    context: "writing value length",
                    source: e,
                });
            }
            current_offset += 4;

            // Write value.
            if let Err(e) = writer.write_all(value) {
                return Err(Error::FileWrite {
                    offset: current_offset,
                    len: value.len(),
                    context: "writing value data",
                    source: e,
                });
            }
            current_offset += value.len() as u64;
        }

        // Flush the buffered writer.
        if let Err(e) = writer.flush() {
            return Err(Error::FileSync {
                context: "flushing buffered writer",
                source: e,
            });
        }
        drop(writer);

        // Update tracking info.
        self.data_end_offset = current_offset;
        self.persisted_entry_count = entry_count;

        // Update meta page.
        self.meta.txid += 1;
        self.meta.root = if self.tree.is_empty() { 0 } else { 1 };

        // Write to alternating meta page.
        let meta_page = if self.meta.txid.is_multiple_of(2) { 0 } else { 1 };
        let meta_offset = meta_page * PAGE_SIZE as u64;

        if let Err(e) = self.file.seek(SeekFrom::Start(meta_offset)) {
            return Err(Error::FileSeek {
                offset: meta_offset,
                context: "seeking to meta page for update",
                source: e,
            });
        }

        let meta_bytes = self.meta.to_bytes();
        if let Err(e) = self.file.write_all(&meta_bytes) {
            return Err(Error::FileWrite {
                offset: meta_offset,
                len: PAGE_SIZE,
                context: "writing updated meta page",
                source: e,
            });
        }

        // Use fdatasync instead of fsync for better performance (skips metadata sync).
        Self::fdatasync(&self.file)?;

        Ok(())
    }

    /// Persists incremental changes (new insertions only) to the database file.
    ///
    /// This is much faster than `persist_tree` for workloads with many small commits
    /// because it only appends new entries rather than rewriting all data.
    ///
    /// # Arguments
    ///
    /// * `new_entries` - Iterator of (key, value) pairs to append.
    /// * `has_deletions` - If true, falls back to full rewrite (deletions require compaction).
    pub(crate) fn persist_incremental<'a, I>(
        &mut self,
        new_entries: I,
        has_deletions: bool,
    ) -> Result<()>
    where
        I: Iterator<Item = (&'a [u8], &'a [u8])>,
    {
        // If there are deletions, we need to do a full rewrite.
        // In the future, we could implement lazy compaction.
        if has_deletions {
            return self.persist_tree();
        }

        // Collect new entries for writing.
        let entries: Vec<_> = new_entries.collect();
        if entries.is_empty() {
            // Nothing to write, but still need to sync meta.
            return self.sync_meta_only();
        }

        let new_entry_count = entries.len() as u64;
        let total_entry_count = self.persisted_entry_count + new_entry_count;

        // Seek to append position.
        if let Err(e) = self.file.seek(SeekFrom::Start(self.data_end_offset)) {
            return Err(Error::FileSeek {
                offset: self.data_end_offset,
                context: "seeking to append position",
                source: e,
            });
        }

        // Use a buffered writer for better performance.
        let mut writer = BufWriter::with_capacity(64 * 1024, &self.file);
        let mut current_offset = self.data_end_offset;

        // Append new entries.
        for (key, value) in &entries {
            // Write key length.
            let key_len_bytes = (key.len() as u32).to_le_bytes();
            if let Err(e) = writer.write_all(&key_len_bytes) {
                return Err(Error::FileWrite {
                    offset: current_offset,
                    len: 4,
                    context: "writing key length (incremental)",
                    source: e,
                });
            }
            current_offset += 4;

            // Write key.
            if let Err(e) = writer.write_all(key) {
                return Err(Error::FileWrite {
                    offset: current_offset,
                    len: key.len(),
                    context: "writing key data (incremental)",
                    source: e,
                });
            }
            current_offset += key.len() as u64;

            // Write value length.
            let value_len_bytes = (value.len() as u32).to_le_bytes();
            if let Err(e) = writer.write_all(&value_len_bytes) {
                return Err(Error::FileWrite {
                    offset: current_offset,
                    len: 4,
                    context: "writing value length (incremental)",
                    source: e,
                });
            }
            current_offset += 4;

            // Write value.
            if let Err(e) = writer.write_all(value) {
                return Err(Error::FileWrite {
                    offset: current_offset,
                    len: value.len(),
                    context: "writing value data (incremental)",
                    source: e,
                });
            }
            current_offset += value.len() as u64;
        }

        // Flush the buffered writer.
        if let Err(e) = writer.flush() {
            return Err(Error::FileSync {
                context: "flushing incremental writer",
                source: e,
            });
        }
        drop(writer);

        // Update entry count at the beginning of data section.
        let data_offset = 2 * PAGE_SIZE as u64;
        if let Err(e) = self.file.seek(SeekFrom::Start(data_offset)) {
            return Err(Error::FileSeek {
                offset: data_offset,
                context: "seeking to update entry count",
                source: e,
            });
        }
        if let Err(e) = self.file.write_all(&total_entry_count.to_le_bytes()) {
            return Err(Error::FileWrite {
                offset: data_offset,
                len: 8,
                context: "updating entry count",
                source: e,
            });
        }

        // Update tracking info.
        self.data_end_offset = current_offset;
        self.persisted_entry_count = total_entry_count;

        // Update and sync meta page.
        self.meta.txid += 1;
        self.meta.root = 1; // We have data

        let meta_page = if self.meta.txid.is_multiple_of(2) { 0 } else { 1 };
        let meta_offset = meta_page * PAGE_SIZE as u64;

        if let Err(e) = self.file.seek(SeekFrom::Start(meta_offset)) {
            return Err(Error::FileSeek {
                offset: meta_offset,
                context: "seeking to meta page for incremental update",
                source: e,
            });
        }

        let meta_bytes = self.meta.to_bytes();
        if let Err(e) = self.file.write_all(&meta_bytes) {
            return Err(Error::FileWrite {
                offset: meta_offset,
                len: PAGE_SIZE,
                context: "writing meta page (incremental)",
                source: e,
            });
        }

        // Use fdatasync for better performance.
        Self::fdatasync(&self.file)?;

        Ok(())
    }

    /// Syncs only the meta page (for commits with no data changes).
    fn sync_meta_only(&mut self) -> Result<()> {
        self.meta.txid += 1;

        let meta_page = if self.meta.txid.is_multiple_of(2) { 0 } else { 1 };
        let meta_offset = meta_page * PAGE_SIZE as u64;

        if let Err(e) = self.file.seek(SeekFrom::Start(meta_offset)) {
            return Err(Error::FileSeek {
                offset: meta_offset,
                context: "seeking to meta page for sync",
                source: e,
            });
        }

        let meta_bytes = self.meta.to_bytes();
        if let Err(e) = self.file.write_all(&meta_bytes) {
            return Err(Error::FileWrite {
                offset: meta_offset,
                len: PAGE_SIZE,
                context: "writing meta page (sync only)",
                source: e,
            });
        }

        Self::fdatasync(&self.file)?;
        Ok(())
    }

    /// Performs fdatasync on Unix systems, falling back to sync_all elsewhere.
    /// fdatasync is faster than fsync because it doesn't sync file metadata.
    #[inline]
    fn fdatasync(file: &File) -> Result<()> {
        #[cfg(unix)]
        {
            // SAFETY: fdatasync is a standard POSIX call, safe with a valid fd.
            let ret = unsafe { libc::fdatasync(file.as_raw_fd()) };
            if ret != 0 {
                return Err(Error::FileSync {
                    context: "fdatasync failed",
                    source: std::io::Error::last_os_error(),
                });
            }
            Ok(())
        }

        #[cfg(not(unix))]
        {
            file.sync_all().map_err(|e| Error::FileSync {
                context: "sync_all fallback",
                source: e,
            })
        }
    }

    /// Returns the path to the database file.
    #[allow(dead_code)]
    pub fn path(&self) -> &Path {
        &self.path
    }

    /// Returns a reference to the current meta page.
    #[allow(dead_code)]
    pub(crate) fn meta(&self) -> &Meta {
        &self.meta
    }

    /// Returns a mutable reference to the file handle.
    #[allow(dead_code)]
    pub(crate) fn file_mut(&mut self) -> &mut File {
        &mut self.file
    }

    /// Returns a reference to the B+ tree.
    pub(crate) fn tree(&self) -> &BTree {
        &self.tree
    }

    /// Returns a mutable reference to the B+ tree.
    pub(crate) fn tree_mut(&mut self) -> &mut BTree {
        &mut self.tree
    }

    /// Begins a new read-only transaction.
    ///
    /// Read transactions provide a consistent snapshot view of the database.
    /// Multiple read transactions can be active concurrently.
    pub fn read_tx(&self) -> ReadTx<'_> {
        ReadTx::new(self)
    }

    /// Begins a new read-write transaction.
    ///
    /// Only one write transaction can be active at a time.
    /// The transaction must be committed to persist changes.
    pub fn write_tx(&mut self) -> WriteTx<'_> {
        WriteTx::new(self)
    }
}

