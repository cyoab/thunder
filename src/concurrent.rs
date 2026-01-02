//! Summary: Concurrent write transaction support using rayon for parallel processing.
//! Copyright (c) YOAB. All rights reserved.
//!
//! This module provides utilities for parallel data preparation during write
//! transactions. The main bottleneck in write performance is data serialization
//! and checksum computation, which can be parallelized across multiple cores.
//!
//! # Design
//!
//! - Entry serialization is parallelized using rayon
//! - Checksum computation uses SIMD-accelerated crc32fast
//! - I/O operations remain sequential for correctness
//! - Thread pool is shared across all operations for efficiency
//!
//! # Performance Characteristics
//!
//! - Small batches (<100 entries): Overhead may exceed benefit
//! - Medium batches (100-10000): Good parallelization gains
//! - Large batches (>10000): Near-linear speedup on multi-core systems

use rayon::prelude::*;

/// Minimum number of entries to trigger parallel processing.
/// Below this threshold, sequential processing is faster due to thread overhead.
pub const PARALLEL_THRESHOLD: usize = 100;

/// Chunk size for parallel processing.
/// Balances thread overhead against work distribution.
pub const CHUNK_SIZE: usize = 256;

/// Represents a prepared entry ready for persistence.
///
/// Contains the serialized key, value, and metadata needed to write
/// the entry to disk. Creating these in parallel is the main optimization.
#[derive(Debug)]
pub struct PreparedEntry {
    /// Serialized key bytes with length prefix.
    pub key_data: Vec<u8>,
    /// Serialized value bytes with length prefix, or overflow marker + ref.
    pub value_data: Vec<u8>,
    /// Original key for bloom filter and tree updates.
    pub key: Vec<u8>,
    /// Original value for tree updates.
    pub value: Vec<u8>,
    /// Whether this entry uses overflow storage.
    pub is_overflow: bool,
}

impl PreparedEntry {
    /// Total serialized size of this entry.
    #[inline]
    pub fn serialized_size(&self) -> usize {
        self.key_data.len() + self.value_data.len()
    }
}

/// Prepares entries for persistence in parallel.
///
/// This function parallelizes the serialization of key-value pairs,
/// which includes:
/// - Length prefix encoding
/// - Data copying to output buffers
/// - Overflow threshold checking
///
/// # Arguments
///
/// * `entries` - Slice of (key, value) references to prepare.
/// * `overflow_threshold` - Values larger than this are marked for overflow.
///
/// # Returns
///
/// A vector of `PreparedEntry` objects ready for sequential I/O.
///
/// # Performance
///
/// For batches larger than `PARALLEL_THRESHOLD`, uses rayon's parallel
/// iterator to distribute work across available CPU cores.
pub fn prepare_entries_parallel<'a>(
    entries: &[(&'a [u8], &'a [u8])],
    overflow_threshold: usize,
) -> Vec<PreparedEntry> {
    if entries.len() < PARALLEL_THRESHOLD {
        // Sequential processing for small batches
        entries
            .iter()
            .map(|(key, value)| prepare_single_entry(key, value, overflow_threshold))
            .collect()
    } else {
        // Parallel processing for large batches
        entries
            .par_iter()
            .map(|(key, value)| prepare_single_entry(key, value, overflow_threshold))
            .collect()
    }
}

/// Prepares a single entry for persistence.
///
/// # Arguments
///
/// * `key` - The key bytes.
/// * `value` - The value bytes.
/// * `overflow_threshold` - Values larger than this are marked for overflow.
#[inline]
fn prepare_single_entry(key: &[u8], value: &[u8], overflow_threshold: usize) -> PreparedEntry {
    // Prepare key data: [len:4][key_bytes]
    let mut key_data = Vec::with_capacity(4 + key.len());
    key_data.extend_from_slice(&(key.len() as u32).to_le_bytes());
    key_data.extend_from_slice(key);

    // Prepare value data based on size
    let is_overflow = value.len() > overflow_threshold;
    let value_data = if is_overflow {
        // For overflow entries, we'll write the marker + placeholder ref
        // The actual ref will be filled in during the sequential phase
        let mut data = Vec::with_capacity(16);
        data.extend_from_slice(&0xFFFF_FFFFu32.to_le_bytes()); // Overflow marker
        data.extend_from_slice(&[0u8; 12]); // Placeholder for OverflowRef
        data
    } else {
        // Inline value: [len:4][value_bytes]
        let mut data = Vec::with_capacity(4 + value.len());
        data.extend_from_slice(&(value.len() as u32).to_le_bytes());
        data.extend_from_slice(value);
        data
    };

    PreparedEntry {
        key_data,
        value_data,
        key: key.to_vec(),
        value: value.to_vec(),
        is_overflow,
    }
}

/// Computes checksums for multiple data buffers in parallel.
///
/// Uses crc32fast with SIMD acceleration for maximum throughput.
///
/// # Arguments
///
/// * `buffers` - Slice of data buffers to checksum.
///
/// # Returns
///
/// Vector of CRC32 checksums in the same order as input.
pub fn compute_checksums_parallel(buffers: &[&[u8]]) -> Vec<u32> {
    if buffers.len() < PARALLEL_THRESHOLD {
        buffers.iter().map(|buf| crc32fast::hash(buf)).collect()
    } else {
        buffers.par_iter().map(|buf| crc32fast::hash(buf)).collect()
    }
}

/// Splits a slice into chunks for parallel processing.
///
/// Returns an iterator of chunk slices that can be processed in parallel.
///
/// # Arguments
///
/// * `data` - The slice to split.
/// * `chunk_size` - Target size for each chunk.
#[inline]
pub fn chunk_for_parallel<T>(data: &[T], chunk_size: usize) -> impl Iterator<Item = &[T]> {
    data.chunks(chunk_size.max(1))
}

/// Statistics about parallel write operations.
#[derive(Debug, Clone, Default)]
pub struct ParallelWriteStats {
    /// Number of entries processed.
    pub entry_count: usize,
    /// Number of entries that used overflow storage.
    pub overflow_count: usize,
    /// Total bytes serialized.
    pub total_bytes: usize,
    /// Whether parallel processing was used.
    pub used_parallel: bool,
}

impl ParallelWriteStats {
    /// Creates stats from prepared entries.
    pub fn from_prepared(entries: &[PreparedEntry], used_parallel: bool) -> Self {
        let overflow_count = entries.iter().filter(|e| e.is_overflow).count();
        let total_bytes = entries.iter().map(|e| e.serialized_size()).sum();

        Self {
            entry_count: entries.len(),
            overflow_count,
            total_bytes,
            used_parallel,
        }
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn test_prepare_single_entry_inline() {
        let key = b"test_key";
        let value = b"test_value";
        let entry = prepare_single_entry(key, value, 1024);

        assert!(!entry.is_overflow);
        assert_eq!(entry.key, key);
        assert_eq!(entry.value, value);
        assert_eq!(entry.key_data.len(), 4 + key.len());
        assert_eq!(entry.value_data.len(), 4 + value.len());
    }

    #[test]
    fn test_prepare_single_entry_overflow() {
        let key = b"test_key";
        let value = vec![0u8; 2048];
        let entry = prepare_single_entry(key, &value, 1024);

        assert!(entry.is_overflow);
        assert_eq!(entry.key, key);
        assert_eq!(entry.value, value);
        // Overflow marker + placeholder ref
        assert_eq!(entry.value_data.len(), 16);
    }

    #[test]
    fn test_prepare_entries_parallel_small() {
        // This test just verifies the function doesn't panic
        // Small batches use sequential processing
        let entries: Vec<_> = (0..10)
            .map(|i| {
                let key = format!("key{i}");
                let value = format!("value{i}");
                (key.into_bytes(), value.into_bytes())
            })
            .collect();

        let refs: Vec<_> = entries
            .iter()
            .map(|(k, v)| (k.as_slice(), v.as_slice()))
            .collect();

        let prepared = prepare_entries_parallel(&refs, 1024);
        assert_eq!(prepared.len(), 10);
    }

    #[test]
    fn test_prepare_entries_parallel_large() {
        let entries: Vec<_> = (0..500)
            .map(|i| {
                let key = format!("key{i:05}");
                let value = format!("value{i}");
                (key.into_bytes(), value.into_bytes())
            })
            .collect();

        let refs: Vec<_> = entries
            .iter()
            .map(|(k, v)| (k.as_slice(), v.as_slice()))
            .collect();

        let prepared = prepare_entries_parallel(&refs, 1024);
        assert_eq!(prepared.len(), 500);

        // Verify data integrity
        for (i, entry) in prepared.iter().enumerate() {
            let expected_key = format!("key{i:05}");
            let expected_value = format!("value{i}");
            assert_eq!(entry.key, expected_key.as_bytes());
            assert_eq!(entry.value, expected_value.as_bytes());
        }
    }

    #[test]
    fn test_compute_checksums_parallel() {
        let data: Vec<Vec<u8>> = (0..200)
            .map(|i| format!("data_{i}").into_bytes())
            .collect();

        let refs: Vec<&[u8]> = data.iter().map(|d| d.as_slice()).collect();

        let checksums = compute_checksums_parallel(&refs);
        assert_eq!(checksums.len(), 200);

        // Verify checksums are correct
        for (i, checksum) in checksums.iter().enumerate() {
            let expected = crc32fast::hash(&data[i]);
            assert_eq!(*checksum, expected);
        }
    }

    #[test]
    fn test_parallel_write_stats() {
        let entries: Vec<_> = (0..10)
            .map(|i| {
                let key = format!("key{i}");
                let value = if i % 2 == 0 {
                    vec![0u8; 2048] // Will be overflow
                } else {
                    format!("value{i}").into_bytes()
                };
                (key.into_bytes(), value)
            })
            .collect();

        let refs: Vec<_> = entries
            .iter()
            .map(|(k, v)| (k.as_slice(), v.as_slice()))
            .collect();

        let prepared = prepare_entries_parallel(&refs, 1024);
        let stats = ParallelWriteStats::from_prepared(&prepared, false);

        assert_eq!(stats.entry_count, 10);
        assert_eq!(stats.overflow_count, 5);
        assert!(!stats.used_parallel);
    }
}
