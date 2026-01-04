# Thunder Benchmark Results

## System Information

- **OS**: Linux (Ubuntu 24.04.3 LTS)
- **CPU**: AMD EPYC 7763 64-Core Processor
- **Memory**: 8 GB RAM
- **Date**: January 4, 2026
- **Thunder Version**: 0.3.0
- **Sled Version**: 0.34.7
- **RocksDB Version**: 0.22.0 (librocksdb 8.10.0)
- **BBolt Version**: 1.3.8

---

## Performance Comparison: Thunder vs Sled vs RocksDB vs BBolt

### Raw Results

#### Thunder (Rust)

| Benchmark | Time | Throughput |
|-----------|------|------------|
| Sequential writes (100K keys, 1 tx) | 169ms | 590,000 ops/sec |
| Sequential reads (100K keys) | 38ms | 2,605,000 ops/sec |
| Random reads (100K lookups) | 89ms | 1,121,000 ops/sec |
| Iterator scan (100K keys) | 1.3ms | 78,564,000 ops/sec |
| Mixed workload (10K ops, 70% read) | 1.85s | 5,400 ops/sec |
| Batch writes (1K tx, 100 ops/tx) | 886ms | 112,900 ops/sec (1,129 tx/sec) |
| Large values (1KB × 100) | 2.4ms | 39.9 MB/sec |
| Large values (10KB × 100) | 2.0ms | 483.6 MB/sec |
| Large values (100KB × 100) | 15ms | 642.4 MB/sec |
| Large values (1MB × 100) | 434ms | 230.3 MB/sec |

#### Sled (Rust)

| Benchmark | Time | Throughput |
|-----------|------|------------|
| Sequential writes (100K keys, 1 batch) | 695ms | 144,000 ops/sec |
| Sequential reads (100K keys) | 467ms | 214,000 ops/sec |
| Random reads (100K lookups) | 186ms | 539,000 ops/sec |
| Iterator scan (100K keys) | 104ms | 957,000 ops/sec |
| Mixed workload (10K ops, 70% read) | 546ms | 18,320 ops/sec |
| Batch writes (1K batches, 100 ops/batch) | 958ms | 104,400 ops/sec (1,044 batch/sec) |
| Large values (1KB × 100) | 1.2ms | 83.2 MB/sec |
| Large values (10KB × 100) | 3.6ms | 271.9 MB/sec |
| Large values (100KB × 100) | 22ms | 434.3 MB/sec |
| Large values (1MB × 100) | 239ms | 417.6 MB/sec |

#### RocksDB (C++)

| Benchmark | Time | Throughput |
|-----------|------|------------|
| Sequential writes (100K keys, 1 batch) | 89ms | 1,122,000 ops/sec |
| Sequential reads (100K keys) | 160ms | 624,000 ops/sec |
| Random reads (100K lookups) | 173ms | 577,000 ops/sec |
| Iterator scan (100K keys) | 24ms | 4,145,000 ops/sec |
| Mixed workload (10K ops, 70% read) | 1.51s | 6,619 ops/sec |
| Batch writes (1K batches, 100 ops/batch) | 601ms | 166,300 ops/sec (1,663 batch/sec) |
| Large values (1KB × 100) | 0.8ms | 119.7 MB/sec |
| Large values (10KB × 100) | 3.6ms | 275.0 MB/sec |
| Large values (100KB × 100) | 23ms | 415.9 MB/sec |
| Large values (1MB × 100) | 474ms | 211.1 MB/sec |

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

---

## Performance Comparison Summary

### Core Operations

| Benchmark | Thunder | Sled | RocksDB | BBolt | Best |
|-----------|---------|------|---------|-------|------|
| Sequential writes (1 tx) | 590K | 144K | 1,122K | 315K | **RocksDB** |
| Sequential reads | **2,605K** | 214K | 624K | 1,479K | **Thunder 1.8×** |
| Random reads | **1,121K** | 539K | 577K | 955K | **Thunder 1.2×** |
| Iterator scan | **78.6M** | 957K | 4.1M | 27.1M | **Thunder 2.9×** |
| Mixed workload | 5,400 | **18,320** | 6,619 | 5,086 | Sled 2.8× |
| Batch writes (tx/sec) | 1,129 | 1,044 | **1,663** | 1,214 | RocksDB |

### Large Value Throughput (MB/sec)

| Size | Thunder | Sled | RocksDB | BBolt | Best |
|------|---------|------|---------|-------|------|
| 1KB | 39.9 | 83.2 | **119.7** | 40.9 | RocksDB |
| 10KB | **483.6** | 271.9 | 275.0 | 115.0 | **Thunder 1.8×** |
| 100KB | **642.4** | 434.3 | 415.9 | 244.3 | **Thunder 1.5×** |
| 1MB | 230.3 | **417.6** | 211.1 | 207.1 | Sled 1.8× |

---

## Analysis

### Thunder Strengths

1. **Read Performance Leader**: Thunder dominates read workloads
   - Sequential reads: 2.6M ops/sec (1.8× faster than RocksDB, 4.2× faster than BBolt)
   - Random reads: 1.1M ops/sec (1.9× faster than RocksDB, 2.1× faster than Sled)

2. **Iterator Performance**: Thunder's iterator is exceptionally fast
   - 78.6M ops/sec (19× faster than RocksDB, 82× faster than Sled)
   - This makes Thunder ideal for range scans and full table iterations

3. **Medium-Large Values (10KB-100KB)**: Thunder excels at these sizes
   - 10KB: 483.6 MB/sec (1.8× faster than RocksDB and Sled)
   - 100KB: 642.4 MB/sec (1.5× faster than Sled, 1.5× faster than RocksDB)

4. **Simplicity**: ~3,500 lines of Rust vs RocksDB's massive codebase

### RocksDB Strengths

1. **Single-Transaction Bulk Writes**: 1.1M ops/sec (1.9× faster than Thunder)
2. **Small Value Writes (1KB)**: 119.7 MB/sec (3× faster than Thunder)
3. **Batch Transaction Throughput**: 1,663 tx/sec

### Sled Strengths

1. **Mixed Workloads**: 18,320 ops/sec (3.4× faster than Thunder)
   - Sled's lock-free architecture excels at concurrent read-heavy workloads
2. **Very Large Values (1MB)**: 417.6 MB/sec (1.8× faster than Thunder)
3. **Small Value Writes (1KB)**: 83.2 MB/sec (2.1× faster than Thunder)

### BBolt Strengths

1. **Mature & Battle-Tested**: Used in production by etcd, Consul, InfluxDB
2. **Memory Efficiency**: Memory-mapped with minimal overhead
3. **ACID Transactions**: Full serializable isolation

---

## Use Case Recommendations

| Use Case | Recommended Database | Reason |
|----------|---------------------|--------|
| **Read-heavy workloads** | Thunder | 2.6M reads/sec, 78M iterator ops/sec |
| **Range scans / Analytics** | Thunder | 82× faster iterator than Sled |
| **Document storage (10-100KB)** | Thunder | Best throughput at these sizes |
| **Write-heavy (small values)** | RocksDB | 1.1M writes/sec |
| **Mixed read/write** | Sled | 18K ops/sec mixed workload |
| **Large blob storage (1MB+)** | Sled | 417 MB/sec for 1MB values |
| **Embedded simplicity** | Thunder/BBolt | Small codebase, easy to understand |

---

## Key Optimizations in Thunder

1. **In-Memory B+ Tree**: Optimized cache locality for tree traversal
2. **Zero-Copy Reads**: `get_ref()` returns references without allocation
3. **fdatasync vs fsync**: Skips metadata sync, reducing commit latency
4. **Buffered I/O**: 256KB write buffers batch small writes together
5. **Bloom Filter**: Fast rejection of non-existent keys
6. **Direct Overflow Format**: Large values use compact `[magic:4][len:4][data:N][crc:4]` format
7. **Byte-Offset Addressing**: Exact byte offsets for efficient random access
8. **Large Value Optimization Mode**: `DatabaseOptions::large_value_optimized()` for 100KB+ values

---

## Benchmark Suite Usage

### Running All Benchmarks

```bash
# Build all benchmarks
cd bench
cargo build --release

# Run Thunder benchmark
./target/release/thunder_bench

# Run Sled benchmark
./target/release/sled_bench

# Run RocksDB benchmark
./target/release/rocksdb_bench

# Run BBolt benchmark (Go)
go run bbolt_bench.go
```

### Benchmark Configuration

All benchmarks use identical parameters for fair comparison:

| Parameter | Value |
|-----------|-------|
| Number of keys | 100,000 |
| Value size (small) | 100 bytes |
| Batch transactions | 1,000 |
| Operations per batch | 100 |
| Large value counts | 100 values each |
| Large value sizes | 1KB, 10KB, 100KB, 1MB |

---

## Benchmark Files

```
bench/
├── thunder_bench.rs  # Thunder benchmark
├── sled_bench.rs     # Sled benchmark  
├── rocksdb_bench.rs  # RocksDB benchmark
├── bbolt_bench.go    # BBolt benchmark
├── Cargo.toml        # Rust dependencies
└── go.mod            # Go module for BBolt
```

---

## Summary

Thunder v0.3.0 delivers **best-in-class read performance** while maintaining competitive write throughput:

| Category | Thunder vs Best Alternative |
|----------|---------------------------|
| **Sequential reads** | ✅ **1.8× faster** than any competitor |
| **Random reads** | ✅ **1.2× faster** than BBolt |
| **Iterator scans** | ✅ **2.9× faster** than BBolt, **19× faster** than RocksDB |
| **10KB values** | ✅ **1.8× faster** than RocksDB/Sled |
| **100KB values** | ✅ **1.5× faster** than Sled |
| **Bulk writes** | ❌ RocksDB 1.9× faster |
| **Mixed workloads** | ❌ Sled 3.4× faster |
| **1MB values** | ❌ Sled 1.8× faster |

### Peak Performance Achieved

| Metric | Thunder Performance |
|--------|---------------------|
| Iterator scan | **78.6 million ops/sec** |
| Sequential reads | **2.6 million ops/sec** |
| Random reads | **1.1 million ops/sec** |
| 100KB value writes | **642 MB/sec** |
| 10KB value writes | **484 MB/sec** |

Thunder is ideal for **read-heavy embedded databases**, **range scan workloads**, and **document storage** where values are typically 10-100KB in size.
