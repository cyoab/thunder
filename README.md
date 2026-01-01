# ⚡ Thunder

**Thunder** is a minimal, embedded, transactional key-value database engine written in Rust, inspired by [BBolt](https://github.com/etcd-io/bbolt).

What started as a hobby/learning project has evolved into a fully functional embedded database that **outperforms BBolt** in most benchmarks while remaining simple and easy to understand.

## Features

- **Embedded** — Runs in-process as a Rust library, no server required
- **Single-file storage** — Entire database in one file
- **ACID transactions** — Full durability with crash-safe commits
- **MVCC** — Multiple concurrent readers, single writer
- **Buckets** — Logical namespaces for organizing data
- **Range queries** — Efficient iteration and range scans
- **Zero dependencies** — Only uses `libc` for mmap/fdatasync

## Performance

Thunder achieves excellent performance, outperforming BBolt in most benchmarks:

| Benchmark | Thunder | BBolt | Result |
|-----------|---------|-------|--------|
| Sequential writes | 723K ops/sec | 301K ops/sec | **Thunder 2.4x faster** |
| Sequential reads | 2.7M ops/sec | 1.3M ops/sec | **Thunder 2.0x faster** |
| Random reads | 1.3M ops/sec | 1.3M ops/sec | Tie |
| Iterator scan | 112M ops/sec | 68M ops/sec | **Thunder 1.6x faster** |
| Transaction throughput | 1,463 tx/sec | 1,113 tx/sec | **Thunder 1.3x faster** |

See [bench.md](bench.md) for full benchmark details.

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

## Limitations

Thunder is a learning project that has grown into something useful, but it's still limited compared to mature solutions like BBolt:

- **No nested buckets** — Buckets cannot contain other buckets
- **No cursor API** — Only forward iteration is supported
- **No compaction** — Deleted data is not reclaimed until full rewrite
- **Single-threaded writes** — No concurrent write transactions
- **No encryption** — Data is stored in plaintext
- **No compression** — Values are stored as-is

These are all features that could be added in the future, but the current implementation focuses on simplicity and correctness.

## Architecture

Thunder is implemented in ~2,500 lines of Rust:

```
src/
├── lib.rs      # Public API
├── db.rs       # Database open/close, persistence
├── tx.rs       # Read and write transactions
├── btree.rs    # In-memory B+ tree
├── bucket.rs   # Bucket management
├── page.rs     # Page layout constants
├── meta.rs     # Meta page handling
├── freelist.rs # Free page tracking
├── mmap.rs     # Memory-mapped I/O
└── error.rs    # Error types
```

### Design Decisions

1. **In-memory B+ tree** — The entire tree lives in memory for fast reads. This trades memory for speed.

2. **Append-only writes** — New entries are appended rather than rewriting the entire database, enabling fast commits.

3. **fdatasync** — Uses `fdatasync()` instead of `fsync()` to skip metadata sync, reducing commit latency.

4. **Copy-on-write semantics** — Write transactions work on a scratch tree, only applying changes on commit.

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
# Thunder benchmark
cargo run --release --bin thunder_bench

# BBolt benchmark (requires Go)
cd bench && go run bbolt_bench.go
```

## License

MIT
