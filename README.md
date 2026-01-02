# ⚡ Thunder

**Thunder** is a fast, embedded, transactional key-value database engine written in Rust, inspired by [BBolt](https://github.com/etcd-io/bbolt).

What started as a hobby/learning project has evolved into a capable embedded database that **outperforms BBolt** in most benchmarks while remaining relatively simple (~3,000 lines of Rust).

> ⚠️ **Work in Progress**: Thunder is still a hobby project under active development. If you need a battle-tested, production-ready embedded database, consider [SQLite](https://sqlite.org/), [RocksDB](https://rocksdb.org/), or [BBolt](https://github.com/etcd-io/bbolt). Thunder is great for learning, experimentation, and non-critical use cases.

## Features

- **Embedded** — Runs in-process as a Rust library, no server required
- **Single-file storage** — Entire database in one file
- **ACID transactions** — Full durability with crash-safe commits
- **MVCC** — Multiple concurrent readers, single writer
- **Buckets** — Logical namespaces for organizing data
- **Nested buckets** — Hierarchical bucket organization (up to 16 levels deep)
- **Range queries** — Efficient iteration and range scans
- **Zero-copy reads** — `get_ref()` API returns references without allocation
- **Bloom filter** — Fast rejection of non-existent keys (8.5M ops/sec)
- **CRC32 checksums** — Data integrity verification with SIMD acceleration
- **Parallel writes** — Bulk operations use rayon for multi-core throughput (1M+ ops/sec)
- **Minimal dependencies** — Only `libc`, `crc32fast`, `nix`, and `rayon`

## Performance

Thunder achieves excellent performance, outperforming BBolt across most workloads:

| Benchmark | Thunder | BBolt | Result |
|-----------|---------|-------|--------|
| Sequential writes | 617K ops/sec | 315K ops/sec | **Thunder 2.0× faster** |
| Sequential reads | 2.4M ops/sec | 1.5M ops/sec | **Thunder 1.6× faster** |
| Iterator scan | 78.6M ops/sec | 27.1M ops/sec | **Thunder 2.9× faster** |
| Mixed workload | 6,034 ops/sec | 5,086 ops/sec | **Thunder 1.2× faster** |
| Transaction throughput | 1,396 tx/sec | 1,214 tx/sec | **Thunder 1.2× faster** |
| Large values (10KB) | 437 MB/sec | 115 MB/sec | **Thunder 3.8× faster** |
| Large values (100KB) | 534 MB/sec | 244 MB/sec | **Thunder 2.2× faster** |

See [bench.md](bench.md) for full benchmark details and methodology.

## Quick Start

```rust
use thunder::Database;

fn main() -> thunder::Result<()> {
    // Open or create a database
    let mut db = Database::open("my.db")?;

    // Write data
    {
        let mut tx = db.write_tx();
        tx.put(b"hello", b"world");
        tx.put(b"foo", b"bar");
        tx.commit()?;
    }

    // Read data
    {
        let tx = db.read_tx();
        assert_eq!(tx.get(b"hello"), Some(b"world".to_vec()));
    }

    Ok(())
}
```

## Buckets

Organize data into logical namespaces:

```rust
let mut tx = db.write_tx();

// Create buckets
tx.create_bucket(b"users")?;
tx.create_bucket(b"posts")?;

// Write to buckets
tx.bucket_put(b"users", b"alice", b"data")?;
tx.bucket_put(b"posts", b"post1", b"content")?;

tx.commit()?;
```

## Nested Buckets

Create hierarchical bucket structures for complex data organization:

```rust
let mut tx = db.write_tx();

// Create parent bucket
tx.create_bucket(b"config")?;

// Create nested buckets
tx.create_nested_bucket(b"config", b"network")?;
tx.create_nested_bucket(b"config", b"storage")?;

// Write to nested buckets
tx.nested_bucket_put(b"config", b"network", b"host", b"localhost")?;
tx.nested_bucket_put(b"config", b"network", b"port", b"8080")?;

// Create deeply nested buckets (up to 16 levels)
tx.create_nested_bucket_at_path(&[b"config", b"storage"], b"cache")?;
tx.nested_bucket_put_at_path(&[b"config", b"storage", b"cache"], b"size", b"1GB")?;

tx.commit()?;

// Read from nested buckets
let rtx = db.read_tx();
let network = rtx.nested_bucket(b"config", b"network")?;
assert_eq!(network.get(b"host"), Some(&b"localhost"[..]));

// List nested buckets
let children = rtx.list_nested_buckets(b"config")?;
assert!(children.contains(&b"network".to_vec()));
```

## Bulk Operations

For high-throughput writes, use the batch APIs which leverage parallel processing:

```rust
let mut tx = db.write_tx();

// Bulk insert (parallelized for batches >= 100 entries)
let entries: Vec<(Vec<u8>, Vec<u8>)> = (0..10_000)
    .map(|i| (format!("key_{i}").into_bytes(), format!("value_{i}").into_bytes()))
    .collect();

tx.batch_put(entries);
tx.commit()?;
```

**Bulk write throughput:**
| Batch Size | Throughput |
|------------|------------|
| 1,000 entries | ~720K ops/sec |
| 10,000 entries | ~910K ops/sec |
| 100,000 entries | ~1.08M ops/sec |

## Limitations

Thunder is a hobby project that has grown into something useful, but it has limitations compared to mature solutions:

- **No cursor API** — Only forward iteration is supported
- **No compaction** — Deleted data is not reclaimed until full rewrite
- **No encryption** — Data is stored in plaintext
- **No compression** — Values are stored as-is
- **Limited testing** — Not battle-tested in production environments

For production use cases requiring stability and robustness, please use established solutions like SQLite, RocksDB, or BBolt.

## Architecture

Thunder is implemented in ~3,500 lines of Rust:

```
src/
├── lib.rs        # Public API
├── db.rs         # Database open/close, persistence
├── tx.rs         # Read and write transactions
├── btree.rs      # In-memory B+ tree
├── bucket.rs     # Bucket management
├── bloom.rs      # Bloom filter for fast negative lookups
├── overflow.rs   # Large value handling
├── page.rs       # Page layout constants
├── meta.rs       # Meta page handling
├── mmap.rs       # Memory-mapped I/O
├── ivec.rs       # Inline vector optimization
├── concurrent.rs # Parallel write support (rayon)
└── error.rs      # Error types
```

### Key Optimizations

1. **In-memory B+ tree** — The entire tree lives in memory for fast reads
2. **Append-only writes** — New entries are appended, enabling fast commits
3. **fdatasync** — Uses `fdatasync()` instead of `fsync()` to reduce latency
4. **Zero-copy reads** — `get_ref()` returns references without allocation
5. **Bloom filter** — Fast rejection of non-existent keys
6. **Direct overflow format** — Large values use compact storage (12 bytes overhead)
7. **SIMD checksums** — CRC32 with hardware acceleration (~10 GB/s)
8. **pwrite** — Positioned writes avoid seek syscalls
9. **Parallel serialization** — Bulk writes use rayon for multi-core data preparation

## Building

```bash
cargo build --release
```

## Testing

```bash
cargo test
```

## Running Benchmarks

```bash
# Run Thunder benchmark
cargo build --release --example thunder_bench
./target/release/examples/thunder_bench

# Run BBolt benchmark for comparison
cd bench && go run bbolt_bench.go
```

## License

MIT
