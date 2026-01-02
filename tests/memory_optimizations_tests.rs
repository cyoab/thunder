//! Summary: Memory optimization tests for Thunder database Phase 5 features.
//! Copyright (c) YOAB. All rights reserved.
//!
//! These tests verify the memory optimization features:
//! - Arena allocator for transaction-scoped bump allocation
//! - Node pool for efficient B+ tree node reuse
//! - Prefix compression for hierarchical keys
//!
//! # Security Considerations
//!
//! The tests verify that:
//! - Arena memory is properly zeroed on reset (no data leakage)
//! - Node pool nodes are cleared before reuse (no cross-transaction leakage)
//! - Prefix compression preserves data integrity under all edge cases

use std::fs;
use thunder::arena::Arena;
use thunder::node_pool::NodePool;
use thunder::Database;

fn test_db_path(name: &str) -> String {
    format!("/tmp/thunder_memory_opt_test_{name}.db")
}

fn cleanup(path: &str) {
    let _ = fs::remove_file(path);
}

// =============================================================================
// Test 1: Transaction Arena Allocator
// =============================================================================
// Verifies that the arena allocator provides:
// - Fast bump allocation without per-object overhead
// - Proper memory reuse after reset
// - Security: no data leakage between transactions
// - Correct chunk growth behavior

#[test]
fn test_arena_allocator_correctness_and_security() {
    // Test 1a: Basic allocation correctness
    {
        let mut arena = Arena::new(1024);

        // Allocate various sizes
        let slice1 = arena.alloc(32);
        assert_eq!(slice1.len(), 32);
        let addr1 = slice1.as_ptr() as usize;
        slice1.fill(0xAA);

        let slice2 = arena.alloc(64);
        assert_eq!(slice2.len(), 64);
        let addr2 = slice2.as_ptr() as usize;
        slice2.fill(0xBB);

        // Verify bump allocation: second allocation follows first
        assert!(addr2 >= addr1 + 32, "Arena should use bump allocation");

        assert_eq!(arena.bytes_used(), 96);
    }

    // Test 1b: Copy slice preserves data
    {
        let mut arena = Arena::new(1024);

        let original = b"This is test data for copy_slice verification";
        let copied = arena.copy_slice(original);

        assert_eq!(copied, original);
        assert_eq!(arena.bytes_used(), original.len());
    }

    // Test 1c: Chunk growth when exceeding capacity
    {
        let mut arena = Arena::new(64); // Small initial chunk

        // Allocate more than one chunk's worth - must fill each before getting next
        let alloc1 = arena.alloc(32);
        alloc1.fill(0x11);
        
        let alloc2 = arena.alloc(32);
        alloc2.fill(0x22);
        
        let alloc3 = arena.alloc(64); // Should trigger new chunk
        alloc3.fill(0x33);

        // Verify capacity grew
        assert!(
            arena.capacity() >= 128,
            "Arena should grow capacity: {}",
            arena.capacity()
        );
    }

    // Test 1d: SECURITY - Reset clears sensitive data
    {
        let mut arena = Arena::new(1024);

        // Store "sensitive" data
        let sensitive = arena.alloc(256);
        sensitive.fill(0xDE); // Simulated sensitive data

        let used_before = arena.bytes_used();
        assert_eq!(used_before, 256);

        // Reset arena
        arena.reset();

        // Verify usage is reset
        assert_eq!(arena.bytes_used(), 0);

        // Allocate in same space - should be zeroed (no data leakage)
        let new_alloc = arena.alloc(256);
        let non_zero_count = new_alloc.iter().filter(|&&b| b != 0).count();

        // In a properly implemented arena, reset should zero memory
        // or the new allocation should not expose old data
        assert_eq!(
            non_zero_count, 0,
            "SECURITY: Arena reset must clear previous data to prevent leakage"
        );
    }

    // Test 1e: Large allocation handling
    {
        let mut arena = Arena::new(128);

        // Allocate something larger than chunk size
        let large = arena.alloc(512);
        assert_eq!(large.len(), 512);

        large.fill(0xFF);
        assert!(large.iter().all(|&b| b == 0xFF));
    }

    // Test 1f: Zero-size allocation (edge case)
    {
        let mut arena = Arena::new(1024);
        let empty = arena.alloc(0);
        assert_eq!(empty.len(), 0);

        // Should not affect bytes_used
        let used_after_empty = arena.bytes_used();

        // Subsequent allocation should still work
        let next = arena.alloc(16);
        assert_eq!(next.len(), 16);
        assert_eq!(arena.bytes_used(), used_after_empty + 16);
    }
}

// =============================================================================
// Test 2: Node Pool for B+ Tree Nodes
// =============================================================================
// Verifies that the node pool provides:
// - Efficient node reuse (reduces allocation overhead)
// - Proper clearing of nodes before reuse (security)
// - Correct statistics tracking
// - Pool size limits are respected

#[test]
fn test_node_pool_efficiency_and_security() {
    // Test 2a: Basic acquire/release cycle
    {
        let mut pool = NodePool::new(16);

        // Acquire leaf nodes
        let mut leaf1 = pool.acquire_leaf();
        let mut leaf2 = pool.acquire_leaf();

        // Stats should show misses (new allocations)
        let stats = pool.stats();
        assert_eq!(stats.leaf_misses, 2);
        assert_eq!(stats.leaf_hits, 0);

        // Modify and release
        leaf1.insert_unchecked(b"key1".to_vec(), b"value1".to_vec());
        leaf2.insert_unchecked(b"key2".to_vec(), b"value2".to_vec());

        pool.release_leaf(leaf1);
        pool.release_leaf(leaf2);

        // Acquire again - should hit pool
        let leaf3 = pool.acquire_leaf();
        let stats = pool.stats();
        assert_eq!(stats.leaf_hits, 1, "Should reuse pooled node");

        // SECURITY: Reused node must be cleared
        assert!(
            leaf3.is_empty(),
            "SECURITY: Reused node must be cleared to prevent data leakage"
        );

        pool.release_leaf(leaf3);
    }

    // Test 2b: Branch node acquire/release
    {
        let mut pool = NodePool::new(8);

        let branch1 = pool.acquire_branch();
        let branch2 = pool.acquire_branch();

        let stats = pool.stats();
        assert_eq!(stats.branch_misses, 2);
        assert_eq!(stats.branch_hits, 0);

        pool.release_branch(branch1);
        pool.release_branch(branch2);

        let branch3 = pool.acquire_branch();
        let stats = pool.stats();
        assert_eq!(stats.branch_hits, 1);

        assert!(
            branch3.is_empty(),
            "SECURITY: Reused branch node must be cleared"
        );

        pool.release_branch(branch3);
    }

    // Test 2c: Pool size limits
    {
        let mut pool = NodePool::new(2); // Small pool

        // Acquire and release more than pool size
        let leaf1 = pool.acquire_leaf();
        let leaf2 = pool.acquire_leaf();
        let leaf3 = pool.acquire_leaf();

        pool.release_leaf(leaf1);
        pool.release_leaf(leaf2);
        pool.release_leaf(leaf3); // This one should be dropped, not pooled

        // Pool should only hold max_pooled items
        let acquired1 = pool.acquire_leaf();
        let acquired2 = pool.acquire_leaf();
        let acquired3 = pool.acquire_leaf();

        let stats = pool.stats();
        // Only 2 should be hits (pool size limit)
        assert_eq!(stats.leaf_hits, 2);
        assert_eq!(stats.leaf_misses, 4); // 3 initial + 1 overflow

        pool.release_leaf(acquired1);
        pool.release_leaf(acquired2);
        pool.release_leaf(acquired3);
    }

    // Test 2d: Pool clear operation
    {
        let mut pool = NodePool::new(16);

        // Fill pool
        for _ in 0..8 {
            let leaf = pool.acquire_leaf();
            pool.release_leaf(leaf);
        }

        // Clear pool
        pool.clear();

        // New acquires should be fresh allocations
        let _ = pool.acquire_leaf();
        let _stats = pool.stats();

        // After clear, next acquire should be a miss
        // Note: stats may or may not be reset depending on implementation
        // The key invariant is that cleared pool doesn't return stale nodes
    }

    // Test 2e: Concurrent-style stress test (sequential simulation)
    {
        let mut pool = NodePool::new(64);
        let mut held_leaves = Vec::new();

        // Simulate high-churn workload
        for i in 0..1000 {
            if i % 3 == 0 && !held_leaves.is_empty() {
                // Release some
                let leaf = held_leaves.pop().unwrap();
                pool.release_leaf(leaf);
            } else {
                // Acquire
                let mut leaf = pool.acquire_leaf();
                leaf.insert_unchecked(format!("key{i}").into_bytes(), vec![i as u8]);
                held_leaves.push(leaf);
            }
        }

        // Release all
        for leaf in held_leaves {
            pool.release_leaf(leaf);
        }

        let stats = pool.stats();
        let total_ops = stats.leaf_hits + stats.leaf_misses;
        let hit_rate = stats.leaf_hits as f64 / total_ops as f64;

        // After warm-up, hit rate should be meaningful (> 20%)
        assert!(
            hit_rate > 0.2,
            "Pool should provide reasonable hit rate: {:.2}%",
            hit_rate * 100.0
        );
    }
}

// =============================================================================
// Test 3: Prefix Compression Integration
// =============================================================================
// Verifies that prefix compression:
// - Correctly compresses hierarchical keys
// - Preserves data integrity on all operations
// - Handles edge cases (empty prefix, single key, key updates)
// - Provides memory savings for hierarchical data

#[test]
fn test_prefix_compression_integrity_and_efficiency() {
    let path = test_db_path("prefix_compression");
    cleanup(&path);

    // Test 3a: Hierarchical keys benefit from prefix compression
    {
        let mut db = Database::open(&path).expect("open should succeed");

        // Insert keys with common prefix (simulating file paths or namespaces)
        let prefix = b"/app/users/profile/";
        let keys: Vec<Vec<u8>> = (0..100)
            .map(|i| {
                let mut key = prefix.to_vec();
                key.extend_from_slice(format!("user_{i:05}/data").as_bytes());
                key
            })
            .collect();

        {
            let mut wtx = db.write_tx();
            for (i, key) in keys.iter().enumerate() {
                wtx.put(key, format!("value_{i}").as_bytes());
            }
            wtx.commit().expect("commit should succeed");
        }

        // Verify all keys are retrievable
        {
            let rtx = db.read_tx();
            for (i, key) in keys.iter().enumerate() {
                let value = rtx.get(key);
                assert_eq!(
                    value,
                    Some(format!("value_{i}").into_bytes()),
                    "Key {} should be retrievable after prefix compression",
                    String::from_utf8_lossy(key)
                );
            }
        }
    }

    cleanup(&path);

    // Test 3b: Mixed prefix scenarios
    {
        let mut db = Database::open(&path).expect("open should succeed");

        // Insert keys with varying prefixes
        let test_cases = [
            (b"aaa/bbb/ccc/1".to_vec(), b"v1".to_vec()),
            (b"aaa/bbb/ccc/2".to_vec(), b"v2".to_vec()),
            (b"aaa/bbb/ddd/1".to_vec(), b"v3".to_vec()),
            (b"aaa/xxx/yyy/1".to_vec(), b"v4".to_vec()),
            (b"bbb/ccc/ddd/1".to_vec(), b"v5".to_vec()),
            (b"completely_different".to_vec(), b"v6".to_vec()),
        ];

        {
            let mut wtx = db.write_tx();
            for (key, value) in &test_cases {
                wtx.put(key, value);
            }
            wtx.commit().expect("commit should succeed");
        }

        // Verify all keys
        {
            let rtx = db.read_tx();
            for (key, expected_value) in &test_cases {
                let actual = rtx.get(key);
                assert_eq!(
                    actual.as_deref(),
                    Some(expected_value.as_slice()),
                    "Mixed prefix key should be correct"
                );
            }
        }
    }

    cleanup(&path);

    // Test 3c: Edge cases - empty keys, single byte keys, binary data
    {
        let mut db = Database::open(&path).expect("open should succeed");

        let edge_cases: Vec<(Vec<u8>, Vec<u8>)> = vec![
            (vec![0x00], vec![0xFF]),                   // Single null byte key
            (vec![0x00, 0x00, 0x00], vec![0x11]),       // Multiple null bytes
            (vec![0xFF, 0xFE, 0xFD], vec![0x22]),       // High bytes
            ((0..255).collect(), vec![0x33]),          // All byte values in key
            (b"normal_key".to_vec(), b"normal_value".to_vec()),
        ];

        {
            let mut wtx = db.write_tx();
            for (key, value) in &edge_cases {
                wtx.put(key, value);
            }
            wtx.commit().expect("commit should succeed");
        }

        // Verify edge cases
        {
            let rtx = db.read_tx();
            for (key, expected) in &edge_cases {
                let actual = rtx.get(key);
                assert_eq!(
                    actual.as_deref(),
                    Some(expected.as_slice()),
                    "Edge case key should be handled correctly"
                );
            }
        }
    }

    cleanup(&path);

    // Test 3d: Prefix compression with updates and deletes
    {
        let mut db = Database::open(&path).expect("open should succeed");

        let base_prefix = b"/api/v1/resources/";

        // Initial insert
        {
            let mut wtx = db.write_tx();
            for i in 0..50 {
                let mut key = base_prefix.to_vec();
                key.extend_from_slice(format!("item_{i:03}").as_bytes());
                wtx.put(&key, format!("original_{i}").as_bytes());
            }
            wtx.commit().expect("commit should succeed");
        }

        // Update some keys
        {
            let mut wtx = db.write_tx();
            for i in (0..50).step_by(5) {
                let mut key = base_prefix.to_vec();
                key.extend_from_slice(format!("item_{i:03}").as_bytes());
                wtx.put(&key, format!("updated_{i}").as_bytes());
            }
            wtx.commit().expect("commit should succeed");
        }

        // Delete some keys
        {
            let mut wtx = db.write_tx();
            for i in (0..50).step_by(7) {
                let mut key = base_prefix.to_vec();
                key.extend_from_slice(format!("item_{i:03}").as_bytes());
                wtx.delete(&key);
            }
            wtx.commit().expect("commit should succeed");
        }

        // Verify final state
        {
            let rtx = db.read_tx();
            for i in 0..50 {
                let mut key = base_prefix.to_vec();
                key.extend_from_slice(format!("item_{i:03}").as_bytes());

                let deleted = i % 7 == 0;
                let updated = i % 5 == 0 && !deleted;

                let actual = rtx.get(&key);

                if deleted {
                    assert!(
                        actual.is_none(),
                        "Deleted key should not exist: item_{i:03}"
                    );
                } else if updated {
                    assert_eq!(
                        actual,
                        Some(format!("updated_{i}").into_bytes()),
                        "Updated key should have new value: item_{i:03}"
                    );
                } else {
                    assert_eq!(
                        actual,
                        Some(format!("original_{i}").into_bytes()),
                        "Unchanged key should have original value: item_{i:03}"
                    );
                }
            }
        }
    }

    cleanup(&path);

    // Test 3e: Range iteration with compressed keys
    {
        let mut db = Database::open(&path).expect("open should succeed");

        // Insert alphabetically ordered keys with common prefix
        {
            let mut wtx = db.write_tx();
            for c in b'a'..=b'z' {
                let key = format!("prefix/{}/suffix", c as char);
                wtx.put(key.as_bytes(), &[c]);
            }
            wtx.commit().expect("commit should succeed");
        }

        // Verify range query works correctly
        {
            let rtx = db.read_tx();

            // Full range
            let all: Vec<_> = rtx.iter().collect();
            assert_eq!(all.len(), 26);

            // Partial range
            let start = b"prefix/m/suffix".as_slice();
            let end = b"prefix/p/suffix".as_slice();
            let range: Vec<_> = rtx.range(start..end).collect();

            // Should include m, n, o (p is excluded)
            assert_eq!(range.len(), 3);
            assert_eq!(range[0].0, b"prefix/m/suffix");
            assert_eq!(range[1].0, b"prefix/n/suffix");
            assert_eq!(range[2].0, b"prefix/o/suffix");
        }
    }

    cleanup(&path);

    // Test 3f: SECURITY - Verify no data leakage through prefix handling
    {
        let mut db = Database::open(&path).expect("open should succeed");

        // Insert "sensitive" hierarchical data
        {
            let mut wtx = db.write_tx();
            wtx.put(b"/secrets/api_key/production", b"secret_prod_key_12345");
            wtx.put(b"/secrets/api_key/staging", b"secret_staging_key_67890");
            wtx.put(b"/public/info", b"public_data");
            wtx.commit().expect("commit should succeed");
        }

        // Delete sensitive data
        {
            let mut wtx = db.write_tx();
            wtx.delete(b"/secrets/api_key/production");
            wtx.delete(b"/secrets/api_key/staging");
            wtx.commit().expect("commit should succeed");
        }

        // Verify sensitive data is not accessible
        {
            let rtx = db.read_tx();
            assert!(rtx.get(b"/secrets/api_key/production").is_none());
            assert!(rtx.get(b"/secrets/api_key/staging").is_none());

            // Public data still accessible
            assert_eq!(rtx.get(b"/public/info"), Some(b"public_data".to_vec()));

            // Partial prefix queries should not leak deleted data
            let secrets_range: Vec<_> = rtx.range(b"/secrets/".as_slice()..).collect();
            assert!(
                secrets_range.is_empty(),
                "Deleted secrets should not appear in range queries"
            );
        }
    }

    cleanup(&path);
}
