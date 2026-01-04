//! Summary: Benchmark suite for sled database (comparison with Thunder).
//! Copyright (c) YOAB. All rights reserved.
//!
//! Run with: cargo run --release --manifest-path bench/Cargo.toml --bin sled_bench

use std::fs;
use std::time::Instant;

const NUM_KEYS: usize = 100_000;
const VALUE_SIZE: usize = 100;
const BATCH_SIZE: usize = 100;
const BATCH_TXS: usize = 1_000;

fn main() {
    println!("=== Sled Benchmark Suite ===");
    println!("Keys: {NUM_KEYS}, Value size: {VALUE_SIZE} bytes\n");

    let db_path = "/tmp/sled_benchmark";
    let _ = fs::remove_dir_all(db_path);

    run_benchmarks(db_path);

    // Clean up
    let _ = fs::remove_dir_all(db_path);
}

fn run_benchmarks(db_path: &str) {
    // Sequential writes (single batch)
    bench_sequential_writes(db_path);

    // Sequential reads
    bench_sequential_reads(db_path);

    // Random reads
    bench_random_reads(db_path);

    // Iterator scan
    bench_iterator_scan(db_path);

    // Mixed workload
    bench_mixed_workload(db_path);

    // Batch writes (multiple batches)
    bench_batch_writes(db_path);

    // Large value benchmarks
    bench_large_values(db_path);
}

fn bench_sequential_writes(db_path: &str) {
    let _ = fs::remove_dir_all(db_path);

    let db = sled::open(db_path).expect("open should succeed");
    let value = vec![b'v'; VALUE_SIZE];

    let start = Instant::now();
    {
        let mut batch = sled::Batch::default();
        for i in 0..NUM_KEYS {
            let key = format!("key_{i:08}");
            batch.insert(key.as_bytes(), value.as_slice());
        }
        db.apply_batch(batch).expect("batch should succeed");
        db.flush().expect("flush should succeed");
    }
    let elapsed = start.elapsed();

    let ops_per_sec = NUM_KEYS as f64 / elapsed.as_secs_f64();
    println!(
        "Sequential writes ({}K keys, 1 batch): {:?} ({:.0} ops/sec)",
        NUM_KEYS / 1000,
        elapsed,
        ops_per_sec
    );
}

fn bench_sequential_reads(db_path: &str) {
    let db = sled::open(db_path).expect("open should succeed");

    // Warm up
    let _ = db.get(b"key_00000000");

    let start = Instant::now();
    {
        for i in 0..NUM_KEYS {
            let key = format!("key_{i:08}");
            let _ = db.get(key.as_bytes());
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
    let db = sled::open(db_path).expect("open should succeed");

    // Generate random access pattern (deterministic)
    let indices: Vec<usize> = (0..NUM_KEYS)
        .map(|i| (i * 7919 + 104729) % NUM_KEYS)
        .collect();

    let start = Instant::now();
    {
        for &i in &indices {
            let key = format!("key_{i:08}");
            let _ = db.get(key.as_bytes());
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
    let db = sled::open(db_path).expect("open should succeed");

    let start = Instant::now();
    {
        let mut count = 0;
        for result in db.iter() {
            let _ = result.expect("iter should succeed");
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
    let _ = fs::remove_dir_all(db_path);

    let db = sled::open(db_path).expect("open should succeed");
    let value = vec![b'v'; VALUE_SIZE];

    // Pre-populate with 10K keys
    {
        let mut batch = sled::Batch::default();
        for i in 0..10_000 {
            let key = format!("key_{i:08}");
            batch.insert(key.as_bytes(), value.as_slice());
        }
        db.apply_batch(batch).expect("batch should succeed");
        db.flush().expect("flush should succeed");
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
            let key = format!("key_{i:08}");
            let _ = db.get(key.as_bytes());
        } else {
            // 30% writes (with flush for durability like Thunder)
            let key = format!("mixed_{op_idx:08}");
            db.insert(key.as_bytes(), value.as_slice())
                .expect("insert should succeed");
            db.flush().expect("flush should succeed");
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
    let _ = fs::remove_dir_all(db_path);

    let db = sled::open(db_path).expect("open should succeed");
    let value = vec![b'v'; VALUE_SIZE];

    let start = Instant::now();
    for tx_idx in 0..BATCH_TXS {
        let mut batch = sled::Batch::default();
        for op_idx in 0..BATCH_SIZE {
            let key = format!("batch_{tx_idx:06}_{op_idx:04}");
            batch.insert(key.as_bytes(), value.as_slice());
        }
        db.apply_batch(batch).expect("batch should succeed");
        db.flush().expect("flush should succeed");
    }
    let elapsed = start.elapsed();

    let total_ops = BATCH_TXS * BATCH_SIZE;
    let ops_per_sec = total_ops as f64 / elapsed.as_secs_f64();
    let tx_per_sec = BATCH_TXS as f64 / elapsed.as_secs_f64();
    println!(
        "Batch writes ({}K batches, {} ops/batch): {:?} ({:.0} ops/sec, {:.0} batch/sec)",
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
        let _ = fs::remove_dir_all(db_path);

        let db = sled::open(db_path).expect("open should succeed");
        let value = vec![b'x'; size];

        const NUM_LARGE: usize = 100;

        let start = Instant::now();
        {
            let mut batch = sled::Batch::default();
            for i in 0..NUM_LARGE {
                let key = format!("large_{i:04}");
                batch.insert(key.as_bytes(), value.as_slice());
            }
            db.apply_batch(batch).expect("batch should succeed");
            db.flush().expect("flush should succeed");
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
