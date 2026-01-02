# Thunder Benchmark Results

## System Information

- **OS**: Linux (Ubuntu 24.04.3 LTS)
- **CPU**: AMD EPYC 7763 64-Core Processor
- **Memory**: 8 GB RAM
- **Date**: January 2, 2026
- **Thunder Version**: 0.3.0
- **BBolt Version**: 1.3.8

---

## Thunder vs BBolt Performance Comparison

### Raw Results

#### Thunder (Rust)

| Benchmark | Time | Throughput |
|-----------|------|------------|
| Sequential writes (100K keys, 1 tx) | 162ms | 617,000 ops/sec |
| Sequential reads (100K keys) | 41ms | 2,422,000 ops/sec |
| Random reads (100K lookups) | 107ms | 939,000 ops/sec |
| Iterator scan (100K keys) | 1.3ms | 78,600,000 ops/sec |
| Mixed workload (10K ops, 70% read) | 1.66s | 6,034 ops/sec |
| Batch writes (1K tx, 100 ops/tx) | 716ms | 139,600 ops/sec (1,396 tx/sec) |
| Large values (1KB × 100) | 2.8ms | 35.5 MB/sec |
| Large values (10KB × 100) | 2.3ms | 437.0 MB/sec |
| Large values (100KB × 100) | 19ms | 534.4 MB/sec |
| Large values (1MB × 100) | 465ms | 215.3 MB/sec |

#### BBolt (Go)

| Benchmark | Time | Throughput |
|-----------|------|------------|
| Sequential writes (100K keys, 1 tx) | 318ms | 315,000 ops/sec |
| Sequential reads (100K keys) | 70ms | 1,479,000 ops/sec |
| Random reads (100K lookups) | 105ms | 955,000 ops/sec |
| Iterator scan (100K keys) | 3.9ms | 27,100,000 ops/sec |
| Mixed workload (10K ops, 70% read) | 1.97s | 5,086 ops/sec |
| Batch writes (1K tx, 100 ops/tx) | 826ms | 121,100 ops/sec (1,214 tx/sec) |
| Large values (1KB × 100) | 3.1ms | 40.9 MB/sec |
| Large values (10KB × 100) | 12.1ms | 115.0 MB/sec |
| Large values (100KB × 100) | 43ms | 244.3 MB/sec |
| Large values (1MB × 100) | 486ms | 207.1 MB/sec |

### Performance Comparison Summary

| Benchmark | Thunder | BBolt | Winner |
|-----------|---------|-------|--------|
| Sequential writes (1 tx) | 617K ops/sec | 315K ops/sec | **Thunder 2.0×** |
| Sequential reads | 2.4M ops/sec | 1.5M ops/sec | **Thunder 1.6×** |
| Random reads | 939K ops/sec | 955K ops/sec | Tie |
| Iterator scan | 78.6M ops/sec | 27.1M ops/sec | **Thunder 2.9×** |
| Mixed workload | 6,034 ops/sec | 5,086 ops/sec | **Thunder 1.2×** |
| Batch writes (tx/sec) | 1,396 tx/sec | 1,214 tx/sec | **Thunder 1.2×** |
| Large values (1KB) | 35.5 MB/sec | 40.9 MB/sec | BBolt 1.2× |
| Large values (10KB) | 437.0 MB/sec | 115.0 MB/sec | **Thunder 3.8×** |
| Large values (100KB) | 534.4 MB/sec | 244.3 MB/sec | **Thunder 2.2×** |
| Large values (1MB) | 215.3 MB/sec | 207.1 MB/sec | **Thunder 1.04×** |

---

## Analysis

### Where Thunder Excels

1. **Bulk Writes**: Thunder is 2.0× faster for single-transaction bulk writes (617K vs 315K ops/sec)
2. **Read Operations**: Thunder is 1.6× faster for sequential reads (2.4M vs 1.5M ops/sec)
3. **Iterator Performance**: Thunder's iterator is 2.9× faster (78.6M vs 27.1M ops/sec) - major advantage
4. **Transaction Throughput**: Thunder achieves 1,396 tx/sec vs BBolt's 1,214 tx/sec (1.2× faster)
5. **Medium-Large Values**: Thunder excels at 10KB-100KB values
   - 10KB: Thunder is 3.8× faster (437 vs 115 MB/sec)
   - 100KB: Thunder is 2.2× faster (534 vs 244 MB/sec)
6. **Mixed Workloads**: Thunder is 1.2× faster (6,034 vs 5,086 ops/sec)
7. **Very Large Values (1MB)**: Thunder is now slightly faster (215 vs 207 MB/sec)

### Where BBolt Excels

1. **Small Value Writes (1KB)**: BBolt is 1.2× faster (40.9 vs 35.5 MB/sec)
2. **Random Reads**: Essentially tied performance (~950K ops/sec)

### Key Optimizations in Thunder

1. **Incremental/Append-Only Writes**: Thunder appends only new entries for insert-only workloads
2. **fdatasync vs fsync**: Thunder uses `fdatasync()` which skips metadata sync, reducing commit latency
3. **Buffered I/O**: 256KB write buffers batch small writes together
4. **Zero-Copy Reads**: `get_ref()` returns references without allocation
5. **Bloom Filter**: Fast rejection of non-existent keys
6. **In-Memory B+ Tree**: Optimized cache locality for tree traversal
7. **Direct Overflow Format**: Large values use a compact format `[magic:4][len:4][data:N][crc:4]` with only 12 bytes overhead instead of 24-byte page headers per page
8. **Byte-Offset Addressing**: Overflow data stored with exact byte offsets for efficient random access
9. **Large Value Optimization Mode**: `DatabaseOptions::large_value_optimized()` for 100KB+ values

### v0.3.0 Performance Improvements

1. **Iterator Performance**: 2.9× faster than BBolt (up from 1.2× in previous version)
2. **Large Value Handling**: Now faster than BBolt at all sizes 10KB and above
3. **Write Throughput**: Sequential writes improved to 2.0× faster than BBolt

### Areas for Future Optimization

1. **Small Value Writes**: Optimize 1KB value throughput
2. **Parallel Writes**: Multi-threaded write support for higher throughput
3. **io_uring Integration**: Linux-specific I/O optimization (optional feature)

---

## Benchmark Suite Usage

### Running Thunder Benchmark

```bash
# Build and run Thunder benchmark
cargo build --release --example thunder_bench
./target/release/examples/thunder_bench
```

### Running BBolt Benchmark

```bash
# Run BBolt benchmark for comparison
cd bench
go mod tidy
go run bbolt_bench.go
```

---

## Benchmark Files

```
examples/
└── thunder_bench.rs  # Thunder Rust benchmark

bench/
├── bbolt_bench.go    # BBolt Go benchmark
└── go.mod            # Go module for BBolt
```

### Benchmark Configuration

Both benchmarks use identical parameters for fair comparison:

| Parameter | Value |
|-----------|-------|
| Number of keys | 100,000 |
| Value size (small) | 100 bytes |
| Batch transactions | 1,000 |
| Operations per batch | 100 |
| Large value counts | 100 values each |
| Large value sizes | 1KB, 10KB, 100KB, 1MB |

### Benchmarks Performed

- **Sequential write**: Single transaction with 100K inserts
- **Sequential read**: Read all 100K keys in order
- **Random read**: Read 100K keys in pseudo-random order
- **Iterator scan**: Full table scan using cursor/iterator
- **Mixed workload**: 70% reads, 30% writes with individual transactions
- **Batch writes**: 1,000 transactions, 100 operations each
- **Large values**: Write 100 large values at various sizes

---

## Summary

Thunder v0.3.0 achieves superior or competitive performance compared to BBolt across all major workloads:

| Category | Thunder Performance |
|----------|---------------------|
| **Bulk writes** | 2.0× faster |
| **Sequential reads** | 1.6× faster |
| **Iterator scans** | 2.9× faster |
| **Mixed workloads** | 1.2× faster |
| **Transaction throughput** | 1.2× faster |
| **10KB values** | 3.8× faster |
| **100KB values** | 2.2× faster |
| **1MB values** | 1.04× faster |

Thunder is implemented in approximately 3,500 lines of Rust, prioritizing simplicity and correctness while delivering excellent performance for embedded database use cases. The v0.3.0 release significantly improved iterator performance and large value handling, making Thunder faster than BBolt across nearly all benchmarks.

### Notable Achievements

- **Iterator scan throughput**: 78.6 million ops/sec
- **Sequential read throughput**: 2.4 million ops/sec
- **10KB value write throughput**: 437 MB/sec
- **100KB value write throughput**: 534 MB/sec
