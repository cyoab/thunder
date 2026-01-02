# Thunder vs BBolt Benchmark Results

## Raw Results

### Thunder (Rust)

| Benchmark | Time | Throughput |
|-----------|------|------------|
| Sequential writes (100K keys, 1 tx) | 138ms | 722,672 ops/sec |
| Sequential reads (100K keys) | 38ms | 2,659,045 ops/sec |
| Random reads (100K lookups) | 75ms | 1,341,810 ops/sec |
| Iterator scan (100K keys) | 0.9ms | 111,502,361 ops/sec |
| Mixed workload (10K ops, 70% read) | 1.8s | 5,600 ops/sec |
| Batch writes (1K tx, 100 ops/tx) | 684ms | 146,304 ops/sec (1,463 tx/sec) |
| Large values (1KB x 100) | 1.1ms | 90.4 MB/sec |
| Large values (10KB x 100) | 2.3ms | 417.6 MB/sec |
| Large values (100KB x 100) | 24.2ms | 403.6 MB/sec |
| Large values (1MB x 100) | 438ms | 228.3 MB/sec |

### BBolt (Go)

| Benchmark | Time | Throughput |
|-----------|------|------------|
| Sequential writes (100K keys, 1 tx) | 332ms | 300,907 ops/sec |
| Sequential reads (100K keys) | 77ms | 1,300,365 ops/sec |
| Random reads (100K lookups) | 77ms | 1,294,650 ops/sec |
| Iterator scan (100K keys) | 1.5ms | 68,466,233 ops/sec |
| Mixed workload (10K ops, 70% read) | 1.9s | 5,350 ops/sec |
| Batch writes (1K tx, 100 ops/tx) | 898ms | 111,338 ops/sec (1,113 tx/sec) |
| Large values (1KB x 100) | 1.3ms | 76.2 MB/sec |
| Large values (10KB x 100) | 3.0ms | 324.9 MB/sec |
| Large values (100KB x 100) | 15.5ms | 628.3 MB/sec |
| Large values (1MB x 100) | 379ms | 264.0 MB/sec |

## Performance Comparison

| Benchmark | Thunder | BBolt | Winner |
|-----------|---------|-------|--------|
| Sequential writes (1 tx) | 723K ops/sec | 301K ops/sec | **Thunder 2.4x** |
| Sequential reads | 2.7M ops/sec | 1.3M ops/sec | **Thunder 2.0x** |
| Random reads | 1.3M ops/sec | 1.3M ops/sec | Tie |
| Iterator scan | 112M ops/sec | 68M ops/sec | **Thunder 1.6x** |
| Mixed workload | 5,600 ops/sec | 5,350 ops/sec | **Thunder 1.05x** |
| Batch writes (tx/sec) | 1,463 tx/sec | 1,113 tx/sec | **Thunder 1.3x** |
| Large values (1KB) | 90 MB/sec | 76 MB/sec | **Thunder 1.2x** |
| Large values (10KB) | 418 MB/sec | 325 MB/sec | **Thunder 1.3x** |
| Large values (100KB) | 404 MB/sec | 628 MB/sec | **BBolt 1.6x** |
| Large values (1MB) | 228 MB/sec | 264 MB/sec | **BBolt 1.2x** |

## Analysis

### Where Thunder Excels

- **Write operations**: Thunder is 2.4x faster for single-transaction bulk writes and 1.3x faster for multi-transaction batch writes
- **Read operations**: Thunder is 2x faster for sequential reads thanks to efficient in-memory B+ tree traversal
- **Iterator performance**: Thunder's iterator is 1.6x faster due to optimized leaf node traversal
- **Transaction throughput**: Thunder achieves 1,463 tx/sec compared to BBolt's 1,113 tx/sec

### Where BBolt Excels

- **Large value writes (100KB+)**: BBolt is 1.2-1.6x faster for very large values, likely due to more sophisticated page management and copy-on-write optimizations

### Key Optimizations in Thunder

1. **Incremental/Append-Only Writes**: Instead of rewriting the entire database on each commit, Thunder appends only new entries for insert-only workloads
2. **fdatasync vs fsync**: Thunder uses `fdatasync()` which skips metadata sync, reducing commit latency
3. **Buffered I/O**: 64KB write buffers batch small writes together, reducing syscall overhead
4. **In-Memory B+ Tree**: Thunder's B+ tree operates entirely in memory with efficient cache locality

### Summary

Thunder achieves competitive or better performance than BBolt across most workloads while being implemented in ~2,500 lines of Rust. The implementation prioritizes simplicity and correctness while still delivering excellent performance for typical embedded database use cases.
