//! Summary: Benchmark suite for Thunder database (for comparison with sled and RocksDB).
//! Copyright (c) YOAB. All rights reserved.
//!
//! Run with: cargo run --release --manifest-path bench/Cargo.toml --bin thunder_bench

use std::fs;
use std::time::Instant;
use thunder::{Database, DatabaseOptions};

const NUM_KEYS: usize = 100_000;
const VALUE_SIZE: usize = 100;
const BATCH_SIZE: usize = 100;
const BATCH_TXS: usize = 1_000;

fn main() {
    println!("=== Thunder Benchmark Suite ===");
    println!("Keys: {NUM_KEYS}, Value size: {VALUE_SIZE} bytes\n");

    let db_path = "/tmp/thunder_benchmark.db";
    let _ = fs::remove_file(db_path);

    run_benchmarks(db_path);

    // Clean up
    let _ = fs::remove_file(db_path);
}

fn run_benchmarks(db_path: &str) {
    // Sequential writes (single transaction)
    bench_sequential_writes(db_path);

    // Sequential reads
    bench_sequential_reads(db_path);

    // Random reads
    bench_random_reads(db_path);

    // Iterator scan
    bench_iterator_scan(db_path);

    // Mixed workload
    bench_mixed_workload(db_path);

    // Batch writes (multiple transactions)
    bench_batch_writes(db_path);

    // Large value benchmarks
    bench_large_values(db_path);
}

fn bench_sequential_writes(db_path: &str) {
    let _ = fs::remove_file(db_path);

    let mut db = Database::open(db_path).expect("open should succeed");
    let value = vec![b'v'; VALUE_SIZE];

    let start = Instant::now();
    {
        let mut wtx = db.write_tx();
        for i in 0..NUM_KEYS {
            let key = format!("key_{i:08}");
            wtx.put(key.as_bytes(), &value);
        }
        wtx.commit().expect("commit should succeed");
    }
    let elapsed = start.elapsed();

    let ops_per_sec = NUM_KEYS as f64 / elapsed.as_secs_f64();
    println!(
        "Sequential writes ({}K keys, 1 tx): {:?} ({:.0} ops/sec)",
        NUM_KEYS / 1000,
        elapsed,
        ops_per_sec
    );
}

fn bench_sequential_reads(db_path: &str) {
    let db = Database::open(db_path).expect("open should succeed");

    // Warm up
    {
        let rtx = db.read_tx();
        let _ = rtx.get(b"key_00000000");
    }

    let start = Instant::now();
    {
        let rtx = db.read_tx();
        for i in 0..NUM_KEYS {
            let key = format!("key_{i:08}");
            let _ = rtx.get(key.as_bytes());
        }
    }
    let elapsed = start.elapsed();

    let ops_per_sec = NUM_KEYS as f64 / elapsed.as_secs_f64();
    println!(
        "Sequential reads ({}K keys): {:?} ({:.0} ops/sec)",
        NUM_KEYS / 1000,
        elapsed,
        ops_per_sec
    );
}

fn bench_random_reads(db_path: &str) {
    let db = Database::open(db_path).expect("open should succeed");

    // Generate random access pattern (deterministic)
    let indices: Vec<usize> = (0..NUM_KEYS)
        .map(|i| (i * 7919 + 104729) % NUM_KEYS)
        .collect();

    let start = Instant::now();
    {
        let rtx = db.read_tx();
        for &i in &indices {
            let key = format!("key_{i:08}");
            let _ = rtx.get(key.as_bytes());
        }
    }
    let elapsed = start.elapsed();

    let ops_per_sec = NUM_KEYS as f64 / elapsed.as_secs_f64();
    println!(
        "Random reads ({}K lookups): {:?} ({:.0} ops/sec)",
        NUM_KEYS / 1000,
        elapsed,
        ops_per_sec
    );
}

fn bench_iterator_scan(db_path: &str) {
    let db = Database::open(db_path).expect("open should succeed");

    let start = Instant::now();
    {
        let rtx = db.read_tx();
        let mut count = 0;
        for _ in rtx.iter() {
            count += 1;
        }
        assert_eq!(count, NUM_KEYS);
    }
    let elapsed = start.elapsed();

    let ops_per_sec = NUM_KEYS as f64 / elapsed.as_secs_f64();
    println!(
        "Iterator scan ({}K keys): {:?} ({:.0} ops/sec)",
        NUM_KEYS / 1000,
        elapsed,
        ops_per_sec
    );
}

fn bench_mixed_workload(db_path: &str) {
    let _ = fs::remove_file(db_path);

    let mut db = Database::open(db_path).expect("open should succeed");
    let value = vec![b'v'; VALUE_SIZE];

    // Pre-populate with 10K keys
    {
        let mut wtx = db.write_tx();
        for i in 0..10_000 {
            let key = format!("key_{i:08}");
            wtx.put(key.as_bytes(), &value);
        }
        wtx.commit().expect("commit should succeed");
    }

    // Mixed workload: 70% reads, 30% writes
    const MIXED_OPS: usize = 10_000;
    let indices: Vec<usize> = (0..MIXED_OPS)
        .map(|i| (i * 7919 + 104729) % 10_000)
        .collect();

    let start = Instant::now();
    for (op_idx, &i) in indices.iter().enumerate() {
        if op_idx % 10 < 7 {
            // 70% reads
            let rtx = db.read_tx();
            let key = format!("key_{i:08}");
            let _ = rtx.get(key.as_bytes());
        } else {
            // 30% writes
            let mut wtx = db.write_tx();
            let key = format!("mixed_{op_idx:08}");
            wtx.put(key.as_bytes(), &value);
            wtx.commit().expect("commit should succeed");
        }
    }
    let elapsed = start.elapsed();

    let ops_per_sec = MIXED_OPS as f64 / elapsed.as_secs_f64();
    println!(
        "Mixed workload ({}K ops, 70% read): {:?} ({:.0} ops/sec)",
        MIXED_OPS / 1000,
        elapsed,
        ops_per_sec
    );
}

fn bench_batch_writes(db_path: &str) {
    let _ = fs::remove_file(db_path);

    let mut db = Database::open(db_path).expect("open should succeed");
    let value = vec![b'v'; VALUE_SIZE];

    let start = Instant::now();
    for tx_idx in 0..BATCH_TXS {
        let mut wtx = db.write_tx();
        for op_idx in 0..BATCH_SIZE {
            let key = format!("batch_{tx_idx:06}_{op_idx:04}");
            wtx.put(key.as_bytes(), &value);
        }
        wtx.commit().expect("commit should succeed");
    }
    let elapsed = start.elapsed();

    let total_ops = BATCH_TXS * BATCH_SIZE;
    let ops_per_sec = total_ops as f64 / elapsed.as_secs_f64();
    let tx_per_sec = BATCH_TXS as f64 / elapsed.as_secs_f64();
    println!(
        "Batch writes ({}K tx, {} ops/tx): {:?} ({:.0} ops/sec, {:.0} tx/sec)",
        BATCH_TXS / 1000,
        BATCH_SIZE,
        elapsed,
        ops_per_sec,
        tx_per_sec
    );
}

fn bench_large_values(db_path: &str) {
    let sizes: &[(usize, &str)] = &[
        (1024, "1KB"),
        (10 * 1024, "10KB"),
        (100 * 1024, "100KB"),
        (1024 * 1024, "1MB"),
    ];

    for &(size, label) in sizes {
        let _ = fs::remove_file(db_path);

        // Use large value optimized options for larger sizes
        let options = if size >= 100 * 1024 {
            DatabaseOptions::large_value_optimized()
        } else {
            DatabaseOptions::default()
        };

        let mut db = Database::open_with_options(db_path, options).expect("open should succeed");
        let value = vec![b'x'; size];

        const NUM_LARGE: usize = 100;

        let start = Instant::now();
        {
            let mut wtx = db.write_tx();
            for i in 0..NUM_LARGE {
                let key = format!("large_{i:04}");
                wtx.put(key.as_bytes(), &value);
            }
            wtx.commit().expect("commit should succeed");
        }
        let elapsed = start.elapsed();

        let total_bytes = NUM_LARGE * size;
        let mb_per_sec = (total_bytes as f64 / (1024.0 * 1024.0)) / elapsed.as_secs_f64();
        println!(
            "Large values ({} × {}): {:?} ({:.1} MB/sec)",
            NUM_LARGE, label, elapsed, mb_per_sec
        );
    }
}
