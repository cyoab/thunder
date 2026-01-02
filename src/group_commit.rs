//! Summary: Batched transaction commit for high throughput.
//! Copyright (c) YOAB. All rights reserved.

use std::sync::atomic::{AtomicU64, Ordering};
use std::sync::{Arc, Condvar, Mutex};
use std::time::{Duration, Instant};

use crate::error::{Error, Result};
use crate::wal::{Lsn, Wal};

/// Configuration for group commit behavior.
#[derive(Debug, Clone)]
pub struct GroupCommitConfig {
    /// Maximum time to wait for additional commits before flushing.
    pub max_wait: Duration,
    /// Maximum number of commits per batch before flushing.
    pub max_batch_size: usize,
}

impl Default for GroupCommitConfig {
    fn default() -> Self {
        Self {
            max_wait: Duration::from_millis(10),
            max_batch_size: 100,
        }
    }
}

/// Pending commit waiting for group sync.
struct PendingCommit {
    /// LSN for this commit (reserved for future batching optimization).
    #[allow(dead_code)]
    lsn: Lsn,
    /// Completion signal.
    completed: Arc<(Mutex<Option<Result<()>>>, Condvar)>,
}

/// Internal state protected by mutex.
struct GroupCommitInner {
    /// Queue of pending commits.
    pending: Vec<PendingCommit>,
    /// Whether a leader is currently performing sync.
    leader_active: bool,
    /// Time when first pending commit was added.
    first_pending_time: Option<Instant>,
}

/// Manages batched transaction commits for improved throughput.
///
/// Multiple concurrent transactions queue their commits, and a "leader"
/// thread performs the actual WAL sync for the entire batch. This amortizes
/// the cost of fsync across many transactions.
///
/// # Algorithm
///
/// 1. Transaction calls `commit(lsn)` after writing to WAL
/// 2. If no leader is active, this thread becomes the leader
/// 3. Leader waits up to `max_wait` or until `max_batch_size` reached
/// 4. Leader performs WAL sync
/// 5. Leader notifies all pending commits of completion
/// 6. Non-leader threads wait on condvar until notified
///
/// # Thread Safety
///
/// This struct is designed for concurrent access from multiple threads.
pub struct GroupCommitManager {
    inner: Mutex<GroupCommitInner>,
    leader_condvar: Condvar,
    wal: Arc<Mutex<Wal>>,
    config: GroupCommitConfig,
    /// Statistics: number of batches performed.
    batch_count: AtomicU64,
    /// Statistics: total commits processed.
    commit_count: AtomicU64,
}

impl GroupCommitManager {
    /// Creates a new group commit manager.
    pub fn new(config: GroupCommitConfig, wal: Arc<Mutex<Wal>>) -> Self {
        Self {
            inner: Mutex::new(GroupCommitInner {
                pending: Vec::with_capacity(config.max_batch_size),
                leader_active: false,
                first_pending_time: None,
            }),
            leader_condvar: Condvar::new(),
            wal,
            config,
            batch_count: AtomicU64::new(0),
            commit_count: AtomicU64::new(0),
        }
    }

    /// Queues a commit and waits for group sync.
    ///
    /// This function blocks until the WAL has been synced to disk,
    /// guaranteeing durability of the commit.
    ///
    /// # Arguments
    ///
    /// * `lsn` - The LSN of the committed transaction
    ///
    /// # Returns
    ///
    /// `Ok(())` when the WAL sync completes successfully.
    pub fn commit(&self, lsn: Lsn) -> Result<()> {
        // Create completion signal
        let completion = Arc::new((Mutex::new(None), Condvar::new()));
        let pending = PendingCommit {
            lsn,
            completed: completion.clone(),
        };

        let should_lead: bool;
        {
            let mut inner = self.inner.lock().unwrap();

            if inner.first_pending_time.is_none() {
                inner.first_pending_time = Some(Instant::now());
            }

            inner.pending.push(pending);

            // Become leader if no leader is active
            should_lead = !inner.leader_active;
            if should_lead {
                inner.leader_active = true;
            }
        }

        if should_lead {
            self.run_leader();
        }

        // Wait for completion
        let (lock, condvar) = &*completion;
        let mut result = lock.lock().unwrap();
        while result.is_none() {
            result = condvar.wait(result).unwrap();
        }

        self.commit_count.fetch_add(1, Ordering::Relaxed);

        // Take the result (replacing with None)
        result.take().unwrap_or(Ok(()))
    }

    /// Leader thread logic: wait for batch, sync, notify.
    fn run_leader(&self) {
        // Wait for batch conditions
        let deadline = Instant::now() + self.config.max_wait;

        loop {
            let should_flush = {
                let inner = self.inner.lock().unwrap();

                inner.pending.len() >= self.config.max_batch_size
                    || Instant::now() >= deadline
                    || inner.pending.is_empty()
            };

            if should_flush {
                break;
            }

            // Sleep briefly to allow more commits to accumulate
            std::thread::sleep(Duration::from_micros(500));
        }

        // Collect pending commits and perform sync
        let pending_commits: Vec<PendingCommit>;
        {
            let mut inner = self.inner.lock().unwrap();
            pending_commits = std::mem::take(&mut inner.pending);
            inner.first_pending_time = None;
        }

        // Perform the actual sync
        let sync_result = {
            let mut wal = self.wal.lock().unwrap();
            wal.sync()
        };

        self.batch_count.fetch_add(1, Ordering::Relaxed);

        // Notify all pending commits
        for commit in pending_commits {
            let (lock, condvar) = &*commit.completed;
            let mut guard = lock.lock().unwrap();
            *guard = Some(sync_result.as_ref().map(|_| ()).map_err(|e| {
                Error::GroupCommitFailed {
                    reason: format!("WAL sync failed: {e}"),
                }
            }));
            condvar.notify_one();
        }

        // Release leader status
        {
            let mut inner = self.inner.lock().unwrap();
            inner.leader_active = false;

            // If more pending commits arrived, wake a new leader
            if !inner.pending.is_empty() {
                self.leader_condvar.notify_one();
            }
        }
    }

    /// Returns the number of batch syncs performed.
    pub fn batch_count(&self) -> u64 {
        self.batch_count.load(Ordering::Relaxed)
    }

    /// Returns the total number of commits processed.
    pub fn commit_count(&self) -> u64 {
        self.commit_count.load(Ordering::Relaxed)
    }

    /// Returns the average commits per batch.
    pub fn avg_batch_size(&self) -> f64 {
        let batches = self.batch_count.load(Ordering::Relaxed);
        let commits = self.commit_count.load(Ordering::Relaxed);
        if batches == 0 {
            0.0
        } else {
            commits as f64 / batches as f64
        }
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::wal::{SyncPolicy, WalConfig};
    use std::sync::atomic::AtomicU64;
    use std::thread;

    static TEST_COUNTER: AtomicU64 = AtomicU64::new(0);

    fn test_wal_dir(name: &str) -> std::path::PathBuf {
        let counter = TEST_COUNTER.fetch_add(1, Ordering::SeqCst);
        let pid = std::process::id();
        std::path::PathBuf::from(format!(
            "/tmp/thunder_gc_test_{name}_{pid}_{counter}"
        ))
    }

    fn cleanup(dir: &std::path::Path) {
        let _ = std::fs::remove_dir_all(dir);
    }

    #[test]
    fn test_single_commit() {
        let dir = test_wal_dir("single");
        cleanup(&dir);
        std::fs::create_dir_all(&dir).unwrap();

        let wal_config = WalConfig {
            segment_size: 64 * 1024 * 1024,
            sync_policy: SyncPolicy::None,
        };
        let wal = Arc::new(Mutex::new(Wal::open(&dir, wal_config).unwrap()));

        let gc_config = GroupCommitConfig {
            max_wait: Duration::from_millis(10),
            max_batch_size: 100,
        };
        let gc = GroupCommitManager::new(gc_config, wal.clone());

        // Append record and commit
        let lsn = {
            let mut w = wal.lock().unwrap();
            w.append(&crate::wal_record::WalRecord::Put {
                key: b"test".to_vec(),
                value: b"value".to_vec(),
            })
            .unwrap()
        };

        gc.commit(lsn).expect("commit should succeed");

        assert!(gc.batch_count() >= 1);
        assert!(gc.commit_count() >= 1);

        cleanup(&dir);
    }

    #[test]
    fn test_concurrent_commits() {
        let dir = test_wal_dir("concurrent");
        cleanup(&dir);
        std::fs::create_dir_all(&dir).unwrap();

        let wal_config = WalConfig {
            segment_size: 64 * 1024 * 1024,
            sync_policy: SyncPolicy::None,
        };
        let wal = Arc::new(Mutex::new(Wal::open(&dir, wal_config).unwrap()));

        let gc_config = GroupCommitConfig {
            max_wait: Duration::from_millis(50),
            max_batch_size: 10,
        };
        let gc = Arc::new(GroupCommitManager::new(gc_config, wal.clone()));

        let num_threads = 4;
        let commits_per_thread = 5;

        let handles: Vec<_> = (0..num_threads)
            .map(|tid| {
                let gc_clone = gc.clone();
                let wal_clone = wal.clone();

                thread::spawn(move || {
                    for i in 0..commits_per_thread {
                        let lsn = {
                            let mut w = wal_clone.lock().unwrap();
                            w.append(&crate::wal_record::WalRecord::Put {
                                key: format!("t{tid}_k{i}").into_bytes(),
                                value: b"v".to_vec(),
                            })
                            .unwrap()
                        };
                        gc_clone.commit(lsn).expect("commit");
                    }
                })
            })
            .collect();

        for h in handles {
            h.join().unwrap();
        }

        let total_commits = gc.commit_count();
        let total_batches = gc.batch_count();

        assert_eq!(
            total_commits,
            (num_threads * commits_per_thread) as u64,
            "all commits should complete"
        );
        assert!(
            total_batches < total_commits,
            "batching should reduce sync count"
        );

        cleanup(&dir);
    }

    #[test]
    fn test_max_batch_size_trigger() {
        let dir = test_wal_dir("max_batch");
        cleanup(&dir);
        std::fs::create_dir_all(&dir).unwrap();

        let wal_config = WalConfig {
            segment_size: 64 * 1024 * 1024,
            sync_policy: SyncPolicy::None,
        };
        let wal = Arc::new(Mutex::new(Wal::open(&dir, wal_config).unwrap()));

        let gc_config = GroupCommitConfig {
            max_wait: Duration::from_secs(60), // Very long wait
            max_batch_size: 5,                 // Small batch size
        };
        let gc = Arc::new(GroupCommitManager::new(gc_config, wal.clone()));

        // Spawn threads that will exceed batch size
        let handles: Vec<_> = (0..10)
            .map(|i| {
                let gc_clone = gc.clone();
                let wal_clone = wal.clone();

                thread::spawn(move || {
                    let lsn = {
                        let mut w = wal_clone.lock().unwrap();
                        w.append(&crate::wal_record::WalRecord::Put {
                            key: format!("key_{i}").into_bytes(),
                            value: b"value".to_vec(),
                        })
                        .unwrap()
                    };
                    gc_clone.commit(lsn).expect("commit");
                })
            })
            .collect();

        for h in handles {
            h.join().unwrap();
        }

        // Should have completed (batch size trigger, not timeout)
        assert_eq!(gc.commit_count(), 10);

        cleanup(&dir);
    }
}
