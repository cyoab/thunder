# Thunder Onboarding

## What is Thunder?

Thunder is a minimal, embedded, transactional key-value database engine in Rust, inspired by bbolt. It prioritizes correctness over performance and clarity over cleverness.

## File Structure

```
src/
├── lib.rs        // Public API re-exports and integration tests
├── db.rs         // Database open/close, persistence, mmap management
├── tx.rs         // ReadTx and WriteTx transactions
├── btree.rs      // In-memory B+ tree implementation
├── bucket.rs     // Bucket namespacing for key isolation
├── bloom.rs      // Bloom filter for fast negative lookups
├── overflow.rs   // Overflow page management for large values
├── coalescer.rs  // Write batching for efficient I/O
├── mmap.rs       // Memory-mapped file I/O
├── page.rs       // Page constants and types
├── meta.rs       // Meta page serialization/validation
├── freelist.rs   // Page allocation and free page tracking
├── error.rs      // Error types with detailed context
├── io_backend.rs // I/O backend abstraction (sync, io_uring, direct)
├── uring.rs      // io_uring backend (Linux 5.1+, feature-gated)
├── aligned.rs    // Aligned buffer allocation for O_DIRECT
├── parallel.rs   // Parallel write coordination
├── wal.rs        // Write-ahead logging for durability
├── wal_record.rs // WAL record types and serialization
├── group_commit.rs // Group commit for batched syncs
├── checkpoint.rs // Checkpoint management for bounded recovery
├── arena.rs      // Bump allocator for transaction-scoped memory
└── node_pool.rs  // Object pool for B+ tree node reuse
```

## Key Components

### Database (`db.rs`)

- `Database::open(path)` — Opens or creates database file with default options
- `Database::open_with_options(path, options)` — Opens with custom configuration
- `DatabaseOptions` — Configures page_size, overflow_threshold, write_buffer_size, WAL options
- `DatabaseOptions::nvme_optimized()` — Preset for NVMe SSDs (16KB pages)
- Initializes two meta pages on new database
- Loads existing data into in-memory B+ tree on open
- `persist_tree()` — Serializes tree to disk after meta pages
- `persist_incremental()` — Append-only persistence for insert workloads
- `mmap_slice(offset, len)` — Read from memory-mapped file region (Unix)
- `checkpoint()` — Create checkpoint to bound recovery time and truncate WAL
- Strict error handling with explicit context for all I/O operations
- **Write buffer size**: 256KB default for reduced syscall overhead
- **Mmap integration**: Automatically initialized on open and refreshed on commit
- **Bloom filter**: Integrated for fast negative key lookups
- **Overflow pages**: Large values (>2KB) stored in chained overflow pages
- **Page size mismatch detection**: Error on opening with wrong page size
- **WAL integration**: Optional write-ahead logging for crash recovery
- **WAL replay on open**: Automatically replays uncommitted WAL records

### Transactions (`tx.rs`)

- **`ReadTx`** — Immutable reference to database, reads from B+ tree
  - `get()` — Returns `Option<Vec<u8>>` (cloned value)
  - `get_ref()` — Returns `Option<&[u8]>` (zero-copy, no allocation)
  - `iter()`, `range()` — Key-value iteration
  - `bucket()`, `bucket_exists()`, `list_buckets()` — Bucket access
  - Uses bloom filter for fast rejection of non-existent keys
- **`WriteTx`** — Mutable reference, uses pending tree for uncommitted changes
  - Changes staged in separate `BTree` + deletion list
  - `commit()` applies changes, updates bloom filter, and persists to disk
  - Dropping without commit = automatic rollback
  - `create_bucket()`, `delete_bucket()` — Bucket management
  - `bucket_put()`, `bucket_get()`, `bucket_delete()` — Bucket operations

### B+ Tree (`btree.rs`)

- In-memory tree with 32-key node capacity (`LEAF_MAX_KEYS`, `BRANCH_MAX_KEYS`)
- `get()`, `insert()`, `remove()`, `iter()`, `range()`
- `Bound` enum — Range bounds (Unbounded, Included, Excluded)
- `BTreeIter` — Iterator over all key-value pairs in sorted order
- `BTreeRangeIter` — Iterator for range scans with start/end bounds
- Automatic node splitting on overflow (page splitting)
- Automatic rebalancing (borrow/merge) on underflow
- Keys ordered lexicographically
- Copy-on-write semantics via pending tree isolation

### Buckets (`bucket.rs`)

- Logical namespacing for key isolation (similar to bbolt/etcd)
- `BucketRef` — Read-only view of a bucket
- `BucketMut` — Mutable view for write transactions
- `BucketIter` — Iterator over bucket's key-value pairs
- `BucketRangeIter` — Range iterator scoped to a bucket
- `MAX_BUCKET_NAME_LEN = 255` — Maximum bucket name size
- Internal key format: `[prefix][name_len][name][user_key]`
- Bucket metadata prefix: `0x00`, data prefix: `0x01`

### Memory Mapping (`mmap.rs`)

- `Mmap::new(file, len)` — Creates read-only mapping via `libc::mmap`
- `page(id)` — Returns slice for specific page (4KB)
- `page_with_size(id, size)` — Returns slice for page with custom size
- `slice(offset, len)` — Returns arbitrary byte range from mapped region
- `as_slice()` — Returns full mapped region as slice
- Thread-safe (`Send + Sync`)
- Requires `libc` crate (only external dependency)
- Integrated into `Database` for efficient read access

### Overflow Pages (`overflow.rs`)

- `OverflowHeader` (24 bytes) — next_page, data_len, flags, CRC32 checksum
- `OverflowRef` (12 bytes) — start_page + total_len reference for entries
- `OverflowManager` — Allocates and reads overflow page chains
- `DEFAULT_OVERFLOW_THRESHOLD = 2048` — Values >2KB use overflow pages
- CRC32 checksums for data integrity verification
- Chain validation with MAX_CHAIN_LENGTH limit (1M pages)
- Supports both mmap and file-based reading

### Write Coalescer (`coalescer.rs`)

- `WriteCoalescer` — Batches pages and sequential data for efficient I/O
- `WriteBatch` — Contains sorted pages + sequential data buffer
- `ContiguousRange` — Identifies adjacent pages for combined writes
- Page deduplication (later writes to same page_id win)
- Configurable buffer size limit with flush detection
- `queue_page()` — Add overflow page to batch
- `queue_sequential()` — Add entry data to sequential buffer

### Bloom Filter (`bloom.rs`)

- `BloomFilter::new(expected_items, fp_rate)` — Creates filter with target false positive rate
- `insert(key)` — Adds key to filter
- `may_contain(key)` — Fast probabilistic membership check
  - Returns `false` = key definitely NOT present
  - Returns `true` = key MIGHT be present
- `to_bytes()` / `from_bytes()` — Serialization support
- Uses FNV-1a double hashing for hash family
- Default: 1% false positive rate, ~10 bits per key
- Automatically populated on database load and updated on commit

### I/O Backend (`io_backend.rs`)

- `IoBackend` trait — Abstraction for pluggable I/O strategies
- `WriteOp` / `ReadOp` — Batched I/O operation descriptors
- `ReadResult` — Result of a read operation with bytes_read
- `SyncBackend` — Standard synchronous I/O (fallback for all platforms)
- `write_batch()` — Submit multiple writes in one call
- `read_batch()` — Submit multiple reads in one call
- `sync()` — Ensure durability (fdatasync on Unix)
- `name()` — Backend identifier for logging
- `supports_parallel()` — Whether backend supports concurrent ops
- `optimal_batch_size()` — Recommended batch size for efficiency

### io_uring Backend (`uring.rs`) — Linux 5.1+ only

- `UringBackend` — High-performance async I/O via io_uring
- Feature-gated: `cargo build --features io_uring`
- `UringBackend::new(file, queue_depth)` — Create with custom queue depth
- `UringBackend::with_defaults(file)` — Create with 64-entry queue
- `UringBackend::is_supported()` — Check kernel support
- Kernel-side polling (SQPOLL) for reduced syscalls
- Batched submissions for maximum throughput
- Automatic queue overflow handling

### Aligned Buffers (`aligned.rs`)

- `AlignedBuffer` — Memory buffer with guaranteed alignment for O_DIRECT
- `AlignedBuffer::new(capacity, alignment)` — Create with specific alignment
- `AlignedBuffer::with_default_alignment(capacity)` — 4KB aligned buffer
- `extend_from_slice()` — Append data to buffer
- `pad_to_alignment()` — Zero-pad to alignment boundary
- `is_aligned(offset, len)` — Check if I/O params are aligned
- `AlignedBufferPool` — Pool for buffer reuse (reduces allocations)
- `DEFAULT_ALIGNMENT = 4096` — Standard filesystem block size

### Arena Allocator (`arena.rs`)

- `Arena` — Bump allocator for transaction-scoped memory
- `Arena::new(chunk_size)` — Create arena with specified chunk size
- `Arena::with_default_size()` — Create with 64KB default chunk size
- `Arena::with_capacity(initial_capacity)` — Pre-allocate initial chunk
- `alloc(size)` — Fast O(1) bump allocation, returns zeroed memory
- `copy_slice(data)` — Allocate and copy data in one operation
- `reset()` — Reset for reuse, **securely zeros all memory**
- `bytes_used()` — Total bytes currently allocated
- `capacity()` — Total capacity across all chunks
- **Security**: Memory zeroed on reset to prevent data leakage
- **Performance**: O(1) allocation vs heap allocation overhead
- `TypedArena<T>` — Type-safe arena for Copy types
- `DEFAULT_ARENA_SIZE = 64KB` — Default chunk size

### Node Pool (`node_pool.rs`)

- `NodePool` — Object pool for B+ tree leaf and branch nodes
- `NodePool::new(max_pooled)` — Create pool with size limit
- `NodePool::with_default_size()` — Create with 256-node limit
- `acquire_leaf()` — Get empty leaf node (pool hit or new allocation)
- `release_leaf(node)` — Return leaf to pool after clearing
- `acquire_branch()` — Get empty branch node
- `release_branch(node)` — Return branch to pool after clearing
- `stats()` — Get hit/miss statistics for monitoring
- `clear()` — Drop all pooled nodes
- **Security**: Nodes cleared before pooling to prevent data leakage
- **Performance**: O(1) acquire/release vs allocation overhead
- `PooledLeafNode` / `PooledBranchNode` — Poolable node types
- `PoolStats` — Hit rate tracking (leaf_hits, leaf_misses, etc.)
- `DEFAULT_MAX_POOLED = 256` — Default pool size

### Parallel Writes (`parallel.rs`)

- `ParallelConfig` — Configuration for parallel I/O
  - `num_workers` — Thread count (default: min(CPU cores, 8))
  - `ops_per_batch` — Max ops per worker (default: 32)
  - `use_thread_local_backend` — Per-worker file handles
- `ParallelConfig::nvme_optimized()` — Preset for NVMe SSDs
- `ParallelWriter` — Coordinates parallel write execution
- `partition_for_parallel(batch, n)` — Split batch into n partitions
- `PartitionStats` — Statistics about partition distribution
- Sequential data preserved in first partition only

### Write-Ahead Logging (`wal.rs`)

- `Wal::open(dir, config)` — Open or create WAL in directory
- `Wal::append(record)` — Append record to WAL, returns LSN
- `Wal::sync()` — Flush WAL to disk based on sync policy
- `Wal::replay(from_lsn, callback)` — Replay records for recovery
- `Wal::truncate_before(lsn)` — Remove segments before LSN (after checkpoint)
- `Wal::current_lsn()` — Get current log sequence number
- `Wal::approximate_size()` — Get total WAL size in bytes
- `WalConfig` — Configuration for segment_size and sync_policy
- **Segment files**: 64MB default, auto-rotation on overflow
- **LSN format**: `(segment_id << 32) | offset` for fast segment lookup

### WAL Records (`wal_record.rs`)

- `WalRecord` enum — Record types for WAL entries:
  - `Put { key, value }` — Key-value insertion
  - `Delete { key }` — Key deletion
  - `TxBegin { txid }` — Transaction start marker
  - `TxCommit { txid }` — Transaction commit marker
  - `TxAbort { txid }` — Transaction abort marker
  - `Checkpoint { lsn }` — Checkpoint marker
- `encode(record)` — Serialize to bytes with CRC32 checksum
- `decode(bytes)` — Deserialize with checksum validation
- **Header format**: length (4B) + type (1B) + CRC32 (4B) = 9 bytes
- **Corruption detection**: CRC32 checksum on record payload

### Group Commit (`group_commit.rs`)

- `GroupCommitManager::new(config)` — Create group commit coordinator
- `GroupCommitManager::commit(lsn)` — Request commit, may batch with others
- `GroupCommitConfig` — Configuration options:
  - `max_wait` — Maximum time to wait for batch (default: 10ms)
  - `max_batch_size` — Maximum commits per batch (default: 100)
- **Leader/follower pattern**: First committer becomes leader, others wait
- **Batched sync**: Single fsync for multiple transactions
- **Statistics**: `batch_count()`, `total_commits()` for monitoring

### Sync Policy (`wal.rs`)

- `SyncPolicy::Immediate` — fsync after every write (maximum durability)
- `SyncPolicy::Batched(Duration)` — fsync at intervals (balanced)
- `SyncPolicy::None` — No fsync, rely on OS (maximum performance, crash risk)
- Default: `Immediate` for safety, configurable per database

### Checkpoint Manager (`checkpoint.rs`)

- `CheckpointManager::new(config)` — Create checkpoint coordinator
- `CheckpointManager::should_checkpoint(wal)` — Check if checkpoint needed
- `CheckpointManager::record_checkpoint(lsn)` — Record checkpoint completion
- `CheckpointManager::record_checkpoint_with_wal_size(lsn, size)` — Record with WAL tracking
- `CheckpointManager::restore(config, info)` — Restore from persisted state
- `CheckpointConfig` — Checkpoint triggers:
  - `interval` — Time-based trigger (default: 5 minutes)
  - `wal_threshold` — WAL size trigger (default: 128MB)
  - `min_records` — Record count trigger (default: 10,000)
- `CheckpointInfo` — Persisted in meta page (24 bytes):
  - `lsn` — LSN at checkpoint
  - `timestamp` — Unix timestamp
  - `entry_count` — Entries at checkpoint time

### Meta Page (`meta.rs`)

- Stores: magic, version, page_size, txid, root, freelist, page_count, checksum, checkpoint info
- `to_bytes()` / `from_bytes()` — Serialization with FNV-1a checksum
- `validate()` — Checks magic, version, page size
- `validate_with_page_size()` — Validates against specific page size config
- `with_page_size()` — Creates meta with custom page size
- **Meta page switching** — Alternates between page 0/1 for crash recovery
- **Checkpoint fields** — checkpoint_lsn, checkpoint_timestamp, checkpoint_entry_count (bytes 64-88)

### FreeList (`freelist.rs`)

- Tracks freed pages available for reuse
- `free(page_id)` — Marks page as available
- `allocate()` — Returns freed page (lowest ID first)
- O(log n) operations via `BTreeSet`
- Automatic deduplication
- `to_bytes()` / `from_bytes()` — Compact serialization

### Page Constants (`page.rs`)

- `PAGE_SIZE = 4096` (4KB default)
- `PageSizeConfig` — Enum for supported sizes (4K, 8K, 16K, 64K)
- `MAGIC = 0x54484E44` ("THND")
- `VERSION = 2` (updated for large value support)
- `PageId = u64`
- `PageType` enum: Meta, Freelist, Branch, Leaf, Overflow

### Error Handling (`error.rs`)

- 18+ specific error variants with context
- `FileOpen`, `FileSeek`, `FileRead`, `FileWrite`, `FileSync` — I/O errors with path/offset
- `Corrupted`, `InvalidMetaPage`, `BothMetaPagesInvalid` — Data integrity errors
- `EntryReadFailed` — Per-entry load errors
- `TxClosed`, `TxCommitFailed` — Transaction errors
- `BucketNotFound`, `BucketAlreadyExists`, `InvalidBucketName` — Bucket errors
- `PageSizeMismatch` — Opening database with wrong page size
- `IoUringInit`, `IoUringSubmit`, `IoUringCompletion`, `IoUringQueueFull` — io_uring errors (Linux)
- `DirectIoAlignment` — O_DIRECT alignment requirement errors
- `WalCorrupted`, `WalRecordInvalid` — WAL integrity errors
- `CheckpointFailed`, `GroupCommitFailed` — Durability operation errors
- All errors preserve source for debugging

## Storage Format

```
┌─────────────────┐  Offset 0
│   Meta Page 0   │  4KB - txid even writes here
├─────────────────┤  Offset 4096
│   Meta Page 1   │  4KB - txid odd writes here
├─────────────────┤  Offset 8192
│   Data Section  │  entry_count (8B) + entries
│   (key-values)  │  Each: key_len(4B) + key + val_len(4B) + val
│                 │  Large values: key_len(4B) + key + MARKER(4B) + OverflowRef(12B)
├─────────────────┤  Page-aligned boundary
│ Overflow Pages  │  For values >2KB threshold
│   (chained)     │  Header(24B) + data per page
└─────────────────┘
```

### Overflow Page Format

```
┌──────────────────────────────────────┐
│ OverflowHeader (24 bytes)            │
│  - next_page: u64 (0 if last)        │
│  - data_len: u32                     │
│  - flags: u32 (reserved)             │
│  - checksum: u32 (CRC32)             │
│  - reserved: u32                     │
├──────────────────────────────────────┤
│ Data (page_size - 24 bytes)          │
└──────────────────────────────────────┘
```

## Design Decisions

1. **In-memory B+ tree** — Full tree loaded on open, persisted on commit
2. **Simple serialization** — Length-prefixed key-value pairs (not page-based yet)
3. **Meta page switching** — txid % 2 determines which page to write (crash recovery)
4. **Pending changes isolation** — WriteTx uses separate tree until commit (COW)
5. **Strict error handling** — No `?` operator, explicit match with context
6. **FreeList for space reclaim** — BTreeSet-based tracking for page reuse
7. **libc for mmap** — Only required dependency, for memory mapping
8. **Zero-copy reads** — `get_ref()` returns references to avoid allocation
9. **Bloom filter optimization** — Fast rejection of non-existent keys
10. **Incremental persistence** — Append-only writes for insert-heavy workloads
11. **Overflow pages** — Large values (>2KB) stored in chained pages with CRC32
12. **Variable page sizes** — 4K/8K/16K/64K configurable for workload optimization
13. **Write coalescing** — Batch writes to reduce I/O syscall overhead
14. **I/O backend abstraction** — Pluggable backends (sync, io_uring, direct)
15. **Aligned buffers** — Safe memory allocation for O_DIRECT requirements
16. **Parallel write coordination** — Partition batches for concurrent I/O
17. **Write-ahead logging** — Optional WAL for crash recovery with CRC32 checksums
18. **Group commit** — Batched syncs to amortize fsync cost across transactions
19. **Checkpoint management** — Bounded recovery time with configurable triggers
20. **LSN-based recovery** — Log sequence numbers for precise WAL replay

## Transaction Guarantees

- Uncommitted writes are invisible to readers (copy-on-write)
- Commit is atomic (meta page swap)
- Drop without commit = rollback (pending changes discarded)
- Single writer enforced by Rust's `&mut` borrow
- Crash recovery via dual meta pages

## Running Tests

```bash
cargo test                        # Run all tests
cargo test --all-features         # Run with io_uring support (Linux)
cargo test --test large_value     # Run large value optimization tests
cargo test --test io_stack        # Run I/O stack tests
cargo test --test wal_durability  # Run WAL durability tests
cargo clippy                      # Lint check
cargo clippy --all-features       # Lint with all features
```

## Performance Optimizations

Thunder includes several performance optimizations:

| Feature | Benefit |
|---------|---------|
| **256KB write buffer** | Reduces syscall overhead for batch writes |
| **Zero-copy `get_ref()`** | Avoids allocation for read operations |
| **Bloom filter** | 10x+ faster rejection of non-existent keys |
| **Mmap integration** | Foundation for efficient page-level reads |
| **Incremental persist** | Append-only writes for insert workloads |
| **Overflow pages** | Efficient storage for large values (>2KB) |
| **Variable page sizes** | 4K/8K/16K/64K tunable for workload |
| **Write coalescing** | Batches writes for reduced I/O operations |
| **io_uring backend** | 20-40% write throughput on NVMe (Linux) |
| **Aligned buffers** | O_DIRECT support for bypassing page cache |
| **Parallel writes** | Multi-queue NVMe saturation |
| **Write-ahead logging** | Crash recovery with minimal data loss |
| **Group commit** | Amortizes fsync cost across transactions |
| **Checkpointing** | Bounded recovery time, WAL truncation |
| **Arena allocator** | O(1) bump allocation for transaction scopes |
| **Node pool** | Object pooling for B+ tree nodes |

## Configuration Options

```rust
// Default configuration (WAL disabled)
let db = Database::open("my.db")?;

// Custom configuration with WAL enabled
let options = DatabaseOptions {
    page_size: PageSizeConfig::Size16K,      // 16KB pages
    overflow_threshold: 4096,                 // 4KB overflow threshold
    write_buffer_size: 512 * 1024,           // 512KB write buffer
    wal_enabled: true,                        // Enable WAL
    wal_dir: Some(PathBuf::from("my.db.wal")), // WAL directory
    wal_sync_policy: SyncPolicy::Immediate,   // fsync after each write
    wal_segment_size: 64 * 1024 * 1024,      // 64MB segments
    checkpoint_interval_secs: 300,            // 5 minute checkpoints
    checkpoint_wal_threshold: 128 * 1024 * 1024, // 128MB WAL threshold
    ..Default::default()
};
let db = Database::open_with_options("my.db", options)?;

// NVMe-optimized preset (16KB pages, 4KB threshold)
let db = Database::open_with_options("my.db", DatabaseOptions::nvme_optimized())?;

// High-throughput configuration (batched sync)
let options = DatabaseOptions {
    wal_enabled: true,
    wal_sync_policy: SyncPolicy::Batched(Duration::from_millis(10)),
    ..Default::default()
};
```

## Feature Flags

| Feature | Description | Platform |
|---------|-------------|----------|
| `io_uring` | Enable io_uring async I/O backend | Linux 5.1+ |

```bash
# Build with io_uring support
cargo build --features io_uring

# Run benchmarks with io_uring
cargo run --release --features io_uring --bin thunder_bench
```
