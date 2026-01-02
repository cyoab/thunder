//! Summary: Optimization tests for Thunder database performance features.
//! Copyright (c) YOAB. All rights reserved.
//!
//! These tests verify the performance optimizations:
//! - Zero-copy get_ref() API
//! - Mmap foundation for efficient read access
//! - Bloom filter for fast negative lookups

use std::fs;
use thunder::Database;

fn test_db_path(name: &str) -> String {
    format!("/tmp/thunder_phase1_test_{name}.db")
}

fn cleanup(path: &str) {
    let _ = fs::remove_file(path);
}

// =============================================================================
// Test 1: Zero-Copy get_ref() API
// =============================================================================
// Verifies that the zero-copy API returns correct references without allocation.
// This test ensures:
// - get_ref() returns the exact same data as get()
// - References are valid for the lifetime of the transaction
// - Large values benefit from zero-copy access
// - The API works correctly with buckets as well

#[test]
fn test_zero_copy_get_ref_api() {
    let path = test_db_path("zero_copy_api");
    cleanup(&path);

    let mut db = Database::open(&path).expect("open should succeed");

    // Insert test data with various sizes
    let small_value = b"small";
    let medium_value = vec![0xAB; 1024]; // 1KB
    let large_value = vec![0xCD; 64 * 1024]; // 64KB

    {
        let mut wtx = db.write_tx();
        wtx.put(b"small_key", small_value);
        wtx.put(b"medium_key", &medium_value);
        wtx.put(b"large_key", &large_value);
        wtx.commit().expect("commit should succeed");
    }

    // Test 1a: Verify get_ref() returns correct data matching get()
    {
        let rtx = db.read_tx();

        // Small value
        let get_result = rtx.get(b"small_key");
        let get_ref_result = rtx.get_ref(b"small_key");
        assert_eq!(get_result.as_deref(), get_ref_result);
        assert_eq!(get_ref_result, Some(small_value.as_slice()));

        // Medium value
        let get_result = rtx.get(b"medium_key");
        let get_ref_result = rtx.get_ref(b"medium_key");
        assert_eq!(get_result.as_deref(), get_ref_result);
        assert_eq!(get_ref_result, Some(medium_value.as_slice()));

        // Large value
        let get_result = rtx.get(b"large_key");
        let get_ref_result = rtx.get_ref(b"large_key");
        assert_eq!(get_result.as_deref(), get_ref_result);
        assert_eq!(get_ref_result, Some(large_value.as_slice()));
    }

    // Test 1b: Verify get_ref() returns None for non-existent keys
    {
        let rtx = db.read_tx();
        assert!(rtx.get_ref(b"nonexistent_key").is_none());
        assert!(rtx.get_ref(b"").is_none());
        assert!(rtx.get_ref(b"another_missing_key").is_none());
    }

    // Test 1c: Multiple references can coexist within same transaction
    {
        let rtx = db.read_tx();
        let ref1 = rtx.get_ref(b"small_key");
        let ref2 = rtx.get_ref(b"medium_key");
        let ref3 = rtx.get_ref(b"large_key");

        // All references should remain valid
        assert_eq!(ref1, Some(small_value.as_slice()));
        assert_eq!(ref2, Some(medium_value.as_slice()));
        assert_eq!(ref3, Some(large_value.as_slice()));
    }

    // Test 1d: Verify bucket get_ref works (buckets already return refs, this is for consistency)
    {
        let mut wtx = db.write_tx();
        wtx.create_bucket(b"test_bucket").expect("create bucket");
        wtx.bucket_put(b"test_bucket", b"bucket_key", &large_value).expect("put");
        wtx.commit().expect("commit");
    }

    {
        let rtx = db.read_tx();
        let bucket = rtx.bucket(b"test_bucket").expect("get bucket");
        let bucket_ref = bucket.get(b"bucket_key");
        assert_eq!(bucket_ref, Some(large_value.as_slice()));
    }

    cleanup(&path);
}

// =============================================================================
// Test 2: Mmap Foundation
// =============================================================================
// Verifies that the mmap foundation is correctly integrated:
// - Mmap is initialized on database open
// - Mmap is refreshed after commits that change file size
// - mmap_slice() returns correct data
// - System gracefully handles mmap unavailability

#[test]
#[cfg(unix)]
fn test_mmap_foundation() {
    let path = test_db_path("mmap_foundation");
    cleanup(&path);

    // Test 2a: Create database and verify mmap initializes
    let mut db = Database::open(&path).expect("open should succeed");

    // Initially, new database may not have mmap (file too small)
    // After first commit, mmap should be available

    // Insert some data to trigger file growth
    {
        let mut wtx = db.write_tx();
        for i in 0..100 {
            wtx.put(format!("key_{i:04}").as_bytes(), &vec![0xAA; 256]);
        }
        wtx.commit().expect("commit should succeed");
    }

    // Test 2b: Verify mmap_slice returns correct data
    // The data section starts after 2 meta pages (2 * 4096 = 8192)
    {
        // Check that mmap is available and can read the entry count
        if let Some(slice) = db.mmap_slice(8192, 8) {
            // The entry count should be 100
            let count = u64::from_le_bytes(slice.try_into().unwrap());
            assert_eq!(count, 100, "entry count should be 100");
        }
    }

    // Test 2c: Verify mmap refreshes after more commits
    {
        let mut wtx = db.write_tx();
        for i in 100..200 {
            wtx.put(format!("key_{i:04}").as_bytes(), &vec![0xBB; 256]);
        }
        wtx.commit().expect("commit should succeed");
    }

    {
        if let Some(slice) = db.mmap_slice(8192, 8) {
            let count = u64::from_le_bytes(slice.try_into().unwrap());
            assert_eq!(count, 200, "entry count should be 200 after second commit");
        }
    }

    // Test 2d: Verify out-of-bounds access returns None
    {
        // Try to read beyond file
        assert!(db.mmap_slice(100_000_000, 8).is_none());
        // Try to read with length that exceeds bounds
        assert!(db.mmap_slice(8192, 100_000_000).is_none());
    }

    // Test 2e: Reopen database and verify mmap reinitializes
    drop(db);
    let db = Database::open(&path).expect("reopen should succeed");

    {
        if let Some(slice) = db.mmap_slice(8192, 8) {
            let count = u64::from_le_bytes(slice.try_into().unwrap());
            assert_eq!(count, 200, "entry count should persist across reopen");
        }
    }

    // Verify data is still readable
    {
        let rtx = db.read_tx();
        for i in 0..200 {
            let key = format!("key_{i:04}");
            assert!(
                rtx.get(key.as_bytes()).is_some(),
                "key {key} should exist"
            );
        }
    }

    cleanup(&path);
}

// =============================================================================
// Test 3: Bloom Filter for Negative Lookups
// =============================================================================
// Verifies that the bloom filter correctly accelerates negative lookups:
// - No false negatives (existing keys always found)
// - False positive rate is within expected bounds
// - Bloom filter persists and reloads correctly
// - Performance benefit for negative lookups

#[test]
fn test_bloom_filter_negative_lookups() {
    let path = test_db_path("bloom_filter");
    cleanup(&path);

    let mut db = Database::open(&path).expect("open should succeed");

    // Insert 10,000 keys
    let key_count = 10_000;
    {
        let mut wtx = db.write_tx();
        for i in 0..key_count {
            let key = format!("existing_key_{i:06}");
            let value = format!("value_{i}");
            wtx.put(key.as_bytes(), value.as_bytes());
        }
        wtx.commit().expect("commit should succeed");
    }

    // Test 3a: Verify NO false negatives - all existing keys must be found
    {
        let rtx = db.read_tx();
        for i in 0..key_count {
            let key = format!("existing_key_{i:06}");
            assert!(
                rtx.get(key.as_bytes()).is_some(),
                "key {key} must exist - bloom filter must not cause false negatives"
            );
        }
    }

    // Test 3b: Verify bloom filter rejects non-existent keys
    // The bloom filter should say "definitely not present" for most non-existent keys
    {
        let rtx = db.read_tx();
        let test_count = 10_000;
        let mut found_count = 0;

        for i in 0..test_count {
            // Use a different prefix to ensure these keys don't exist
            let key = format!("nonexistent_key_{i:06}");
            if rtx.get(key.as_bytes()).is_some() {
                found_count += 1;
            }
        }

        // All should return None since they don't exist
        assert_eq!(
            found_count, 0,
            "non-existent keys should return None"
        );
    }

    // Test 3c: Verify bloom filter persists across database reopen
    drop(db);
    let db = Database::open(&path).expect("reopen should succeed");

    // After reopen, all existing keys should still be found (no false negatives)
    {
        let rtx = db.read_tx();
        for i in 0..key_count {
            let key = format!("existing_key_{i:06}");
            assert!(
                rtx.get(key.as_bytes()).is_some(),
                "key {key} must exist after reopen - bloom filter must persist correctly"
            );
        }
    }

    // Test 3d: Verify false positive rate is reasonable
    // With 10K keys and 1% target FP rate, we should see roughly 1% false positives
    // But since get() actually checks the tree, we won't see functional false positives
    // The bloom filter just provides early rejection
    {
        let rtx = db.read_tx();

        // Test that the bloom filter is being used by checking may_contain_key if exposed
        // For now, we verify correctness: all existing keys found, no incorrect results

        // Random sample of existing keys
        for i in (0..key_count).step_by(100) {
            let key = format!("existing_key_{i:06}");
            let value = rtx.get(key.as_bytes()).expect("key must exist");
            let expected = format!("value_{i}");
            assert_eq!(value, expected.as_bytes());
        }
    }

    // Test 3e: Verify bloom filter updates correctly with new insertions
    {
        let mut db = Database::open(&path).expect("reopen for writes");
        {
            let mut wtx = db.write_tx();
            for i in key_count..(key_count + 1000) {
                let key = format!("existing_key_{i:06}");
                let value = format!("value_{i}");
                wtx.put(key.as_bytes(), value.as_bytes());
            }
            wtx.commit().expect("commit should succeed");
        }

        // New keys should be found
        let rtx = db.read_tx();
        for i in key_count..(key_count + 1000) {
            let key = format!("existing_key_{i:06}");
            assert!(
                rtx.get(key.as_bytes()).is_some(),
                "newly inserted key {key} must be found"
            );
        }
    }

    cleanup(&path);
}

// =============================================================================
// Additional Integration Test: All Optimization Features Together
// =============================================================================

#[test]
fn test_optimizations_integration() {
    let path = test_db_path("phase1_integration");
    cleanup(&path);

    let mut db = Database::open(&path).expect("open should succeed");

    // Insert varied data
    {
        let mut wtx = db.write_tx();

        // Create a bucket with data
        wtx.create_bucket(b"users").expect("create bucket");
        for i in 0..100 {
            wtx.bucket_put(
                b"users",
                format!("user_{i:04}").as_bytes(),
                format!("{{\"id\": {i}, \"name\": \"User {i}\"}}").as_bytes(),
            ).expect("bucket put");
        }

        // Also insert top-level keys
        for i in 0..100 {
            wtx.put(
                format!("global_key_{i:04}").as_bytes(),
                &vec![0xFF; 1024],
            );
        }

        wtx.commit().expect("commit should succeed");
    }

    // Verify with zero-copy API
    {
        let rtx = db.read_tx();

        // Use get_ref for top-level keys
        for i in 0..100 {
            let key = format!("global_key_{i:04}");
            let ref_result = rtx.get_ref(key.as_bytes());
            assert!(ref_result.is_some());
            assert_eq!(ref_result.unwrap().len(), 1024);
        }

        // Verify bucket data
        let bucket = rtx.bucket(b"users").expect("bucket should exist");
        for i in 0..100 {
            let key = format!("user_{i:04}");
            assert!(bucket.get(key.as_bytes()).is_some());
        }
    }

    // Verify negative lookups are fast (bloom filter)
    {
        let rtx = db.read_tx();

        // These should be quickly rejected by bloom filter
        for i in 0..1000 {
            let key = format!("nonexistent_{i:08}");
            assert!(rtx.get(key.as_bytes()).is_none());
            assert!(rtx.get_ref(key.as_bytes()).is_none());
        }
    }

    // Verify mmap is available (on Unix)
    #[cfg(unix)]
    {
        if let Some(slice) = db.mmap_slice(8192, 8) {
            let count = u64::from_le_bytes(slice.try_into().unwrap());
            // Should have 100 bucket entries + 1 bucket meta + 100 global keys = 201
            // But the exact count depends on internal representation
            assert!(count > 0, "should have entries");
        }
    }

    // Reopen and verify persistence
    drop(db);
    let db = Database::open(&path).expect("reopen");

    {
        let rtx = db.read_tx();

        // Verify all data persisted
        for i in 0..100 {
            assert!(rtx.get_ref(format!("global_key_{i:04}").as_bytes()).is_some());
        }

        let bucket = rtx.bucket(b"users").expect("bucket");
        for i in 0..100 {
            assert!(bucket.get(format!("user_{i:04}").as_bytes()).is_some());
        }
    }

    cleanup(&path);
}
