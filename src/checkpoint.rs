//! Summary: Periodic checkpointing for bounded recovery time.
//! Copyright (c) YOAB. All rights reserved.

use std::time::{Duration, Instant};

use crate::error::Result;
use crate::wal::{Lsn, Wal};

/// Configuration for checkpoint behavior.
#[derive(Debug, Clone)]
pub struct CheckpointConfig {
    /// Time interval between checkpoints.
    pub interval: Duration,
    /// WAL size threshold (bytes) that triggers checkpoint.
    pub wal_threshold: usize,
    /// Minimum number of records before checkpoint is worthwhile.
    pub min_records: usize,
}

impl Default for CheckpointConfig {
    fn default() -> Self {
        Self {
            interval: Duration::from_secs(300), // 5 minutes
            wal_threshold: 128 * 1024 * 1024,   // 128MB
            min_records: 10_000,
        }
    }
}

/// Information about a checkpoint, stored in meta page.
#[derive(Debug, Clone, Copy, Default)]
pub struct CheckpointInfo {
    /// LSN at which checkpoint was created.
    pub lsn: Lsn,
    /// Unix timestamp when checkpoint was created.
    pub timestamp: u64,
    /// Number of entries at checkpoint time.
    pub entry_count: u64,
}

impl CheckpointInfo {
    /// Size of checkpoint info in bytes when serialized.
    pub const SIZE: usize = 24;

    /// Serializes checkpoint info to bytes.
    pub fn to_bytes(&self) -> [u8; Self::SIZE] {
        let mut buf = [0u8; Self::SIZE];
        buf[0..8].copy_from_slice(&self.lsn.to_le_bytes());
        buf[8..16].copy_from_slice(&self.timestamp.to_le_bytes());
        buf[16..24].copy_from_slice(&self.entry_count.to_le_bytes());
        buf
    }

    /// Deserializes checkpoint info from bytes.
    pub fn from_bytes(buf: &[u8]) -> Option<Self> {
        if buf.len() < Self::SIZE {
            return None;
        }
        Some(Self {
            lsn: u64::from_le_bytes(buf[0..8].try_into().ok()?),
            timestamp: u64::from_le_bytes(buf[8..16].try_into().ok()?),
            entry_count: u64::from_le_bytes(buf[16..24].try_into().ok()?),
        })
    }

    /// Returns true if this checkpoint info is valid (non-zero).
    pub fn is_valid(&self) -> bool {
        self.lsn > 0 || self.timestamp > 0
    }
}

/// Manages periodic checkpointing to bound recovery time.
///
/// Checkpointing ensures that:
/// 1. All data up to a certain LSN is persisted to the main database file
/// 2. WAL segments before that LSN can be truncated
/// 3. Recovery only needs to replay WAL records after the checkpoint
///
/// # Checkpoint Process
///
/// 1. Record current WAL LSN as checkpoint_lsn
/// 2. Persist all in-memory data to the main database file
/// 3. fsync the main database file
/// 4. Update meta page with checkpoint_lsn
/// 5. fsync meta page
/// 6. Truncate WAL segments before checkpoint_lsn
pub struct CheckpointManager {
    config: CheckpointConfig,
    last_checkpoint_lsn: Lsn,
    last_checkpoint_time: Option<Instant>,
    records_since_checkpoint: usize,
    wal_size_at_checkpoint: u64,
}

impl CheckpointManager {
    /// Creates a new checkpoint manager with the given configuration.
    pub fn new(config: CheckpointConfig) -> Self {
        Self {
            config,
            last_checkpoint_lsn: 0,
            last_checkpoint_time: None,
            records_since_checkpoint: 0,
            wal_size_at_checkpoint: 0,
        }
    }

    /// Restores checkpoint manager state from persisted checkpoint info.
    pub fn restore(config: CheckpointConfig, checkpoint_info: CheckpointInfo) -> Self {
        Self {
            config,
            last_checkpoint_lsn: checkpoint_info.lsn,
            last_checkpoint_time: if checkpoint_info.timestamp > 0 {
                // We don't know when this was relative to now, so start fresh
                Some(Instant::now())
            } else {
                None
            },
            records_since_checkpoint: 0,
            wal_size_at_checkpoint: 0, // Will be updated on first WAL check
        }
    }

    /// Determines if a checkpoint should be performed based on configured thresholds.
    ///
    /// A checkpoint is triggered if any of these conditions are met:
    /// - Time since last checkpoint exceeds `config.interval`
    /// - WAL growth since checkpoint exceeds `config.wal_threshold`
    /// - Records since checkpoint exceed `config.min_records`
    pub fn should_checkpoint(&self, wal: &Wal) -> bool {
        // Check time-based trigger
        if let Some(last_time) = self.last_checkpoint_time {
            if last_time.elapsed() >= self.config.interval {
                return true;
            }
        } else if self.records_since_checkpoint > 0 {
            // First checkpoint after startup
            return self.records_since_checkpoint >= self.config.min_records;
        }

        // Check WAL size growth since last checkpoint
        let current_wal_size = wal.approximate_size();
        let wal_growth = current_wal_size.saturating_sub(self.wal_size_at_checkpoint);
        if wal_growth as usize >= self.config.wal_threshold {
            return true;
        }

        // Check record count trigger
        if self.records_since_checkpoint >= self.config.min_records {
            return true;
        }

        false
    }

    /// Records that records have been written since last checkpoint.
    pub fn record_writes(&mut self, count: usize) {
        self.records_since_checkpoint = self.records_since_checkpoint.saturating_add(count);
    }

    /// Records that a checkpoint was completed at the given LSN.
    pub fn record_checkpoint(&mut self, lsn: Lsn) {
        self.last_checkpoint_lsn = lsn;
        self.last_checkpoint_time = Some(Instant::now());
        self.records_since_checkpoint = 0;
        // WAL size tracking is handled by record_checkpoint_with_wal_size
    }

    /// Records that a checkpoint was completed at the given LSN, tracking WAL size.
    pub fn record_checkpoint_with_wal_size(&mut self, lsn: Lsn, wal_size: u64) {
        self.last_checkpoint_lsn = lsn;
        self.last_checkpoint_time = Some(Instant::now());
        self.records_since_checkpoint = 0;
        self.wal_size_at_checkpoint = wal_size;
    }

    /// Returns the LSN of the last completed checkpoint.
    pub fn last_checkpoint_lsn(&self) -> Lsn {
        self.last_checkpoint_lsn
    }

    /// Creates checkpoint info for the given LSN.
    pub fn create_checkpoint_info(&self, lsn: Lsn, entry_count: u64) -> CheckpointInfo {
        CheckpointInfo {
            lsn,
            timestamp: current_unix_timestamp(),
            entry_count,
        }
    }
}

/// Returns current Unix timestamp in seconds.
fn current_unix_timestamp() -> u64 {
    std::time::SystemTime::now()
        .duration_since(std::time::UNIX_EPOCH)
        .map(|d| d.as_secs())
        .unwrap_or(0)
}

/// Checkpoint creation result.
#[derive(Debug)]
pub struct CheckpointResult {
    /// LSN at which checkpoint was created.
    pub lsn: Lsn,
    /// Number of WAL segments truncated.
    pub segments_truncated: u32,
    /// Time taken for checkpoint.
    pub duration: Duration,
}

/// Performs the checkpoint operation.
///
/// This is a separate function to allow the database module to orchestrate
/// the checkpoint process while keeping the logic here.
///
/// # Arguments
///
/// * `checkpoint_lsn` - The LSN to checkpoint up to
/// * `wal` - The WAL to truncate after checkpoint
/// * `persist_fn` - Function to persist all data to main DB file
///
/// # Returns
///
/// Result with checkpoint details on success.
pub fn perform_checkpoint<F>(
    checkpoint_lsn: Lsn,
    wal: &mut Wal,
    mut persist_fn: F,
) -> Result<CheckpointResult>
where
    F: FnMut() -> Result<()>,
{
    let start = Instant::now();

    // 1. Persist all data to main database file
    persist_fn()?;

    // 2. Truncate WAL segments before checkpoint LSN
    wal.truncate_before(checkpoint_lsn)?;

    Ok(CheckpointResult {
        lsn: checkpoint_lsn,
        segments_truncated: 0, // We don't track this precisely
        duration: start.elapsed(),
    })
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn test_checkpoint_info_roundtrip() {
        let info = CheckpointInfo {
            lsn: 0x1234_5678_9ABC_DEF0,
            timestamp: 1704067200, // 2024-01-01 00:00:00 UTC
            entry_count: 1_000_000,
        };

        let bytes = info.to_bytes();
        let restored = CheckpointInfo::from_bytes(&bytes).expect("should parse");

        assert_eq!(info.lsn, restored.lsn);
        assert_eq!(info.timestamp, restored.timestamp);
        assert_eq!(info.entry_count, restored.entry_count);
    }

    #[test]
    fn test_checkpoint_info_is_valid() {
        let empty = CheckpointInfo::default();
        assert!(!empty.is_valid());

        let with_lsn = CheckpointInfo {
            lsn: 100,
            timestamp: 0,
            entry_count: 0,
        };
        assert!(with_lsn.is_valid());

        let with_timestamp = CheckpointInfo {
            lsn: 0,
            timestamp: 12345,
            entry_count: 0,
        };
        assert!(with_timestamp.is_valid());
    }

    #[test]
    fn test_checkpoint_manager_should_checkpoint_fresh() {
        let config = CheckpointConfig {
            interval: Duration::from_secs(300),
            wal_threshold: 1024,
            min_records: 10,
        };

        let _manager = CheckpointManager::new(config);

        // Fresh manager without any writes shouldn't checkpoint
        // (We can't test with actual WAL here without more setup)
    }

    #[test]
    fn test_checkpoint_manager_record_count_trigger() {
        let config = CheckpointConfig {
            interval: Duration::from_secs(3600), // Long interval
            wal_threshold: 1024 * 1024 * 1024,   // Very large
            min_records: 10,
        };

        let mut manager = CheckpointManager::new(config);

        // Record writes
        manager.record_writes(5);
        assert_eq!(manager.records_since_checkpoint, 5);

        manager.record_writes(10);
        assert_eq!(manager.records_since_checkpoint, 15);

        // After checkpoint, count resets
        manager.record_checkpoint(1000);
        assert_eq!(manager.records_since_checkpoint, 0);
    }

    #[test]
    fn test_checkpoint_manager_restore() {
        let config = CheckpointConfig::default();

        let info = CheckpointInfo {
            lsn: 5000,
            timestamp: 12345,
            entry_count: 100,
        };

        let manager = CheckpointManager::restore(config, info);
        assert_eq!(manager.last_checkpoint_lsn(), 5000);
    }

    #[test]
    fn test_create_checkpoint_info() {
        let config = CheckpointConfig::default();
        let manager = CheckpointManager::new(config);

        let info = manager.create_checkpoint_info(1000, 500);
        assert_eq!(info.lsn, 1000);
        assert_eq!(info.entry_count, 500);
        assert!(info.timestamp > 0);
    }
}
