//! Summary: Tests for Phase 3 I/O Stack Modernization.
//! Copyright (c) YOAB. All rights reserved.
//!
//! This module contains high-quality tests for:
//! - I/O backend abstraction and operations
//! - Aligned buffer allocation and safety
//! - Parallel write partitioning and execution
//!
//! These tests verify correctness, efficiency, and security of the I/O stack.

use std::fs::{self, File, OpenOptions};
use std::io::{Seek, SeekFrom, Write};

// Test utilities
fn test_file_path(name: &str) -> String {
    format!("/tmp/thunder_io_stack_test_{name}.db")
}

fn cleanup(path: &str) {
    let _ = fs::remove_file(path);
}

fn create_test_file(path: &str, size: usize) -> File {
    cleanup(path);
    let mut file = OpenOptions::new()
        .read(true)
        .write(true)
        .create(true)
        .truncate(true)
        .open(path)
        .expect("failed to create test file");
    
    // Pre-allocate file with zeros
    file.write_all(&vec![0u8; size]).expect("failed to write zeros");
    file.seek(SeekFrom::Start(0)).expect("failed to seek");
    file
}

// ==================== I/O Backend Abstraction Tests ====================

mod io_backend_tests {
    use super::*;
    use thunder::io_backend::{IoBackend, ReadOp, SyncBackend, WriteOp};

    /// Tests that WriteOp and ReadOp correctly represent I/O operations
    /// and that the SyncBackend performs sequential I/O correctly.
    #[test]
    fn test_sync_backend_write_and_read_correctness() {
        let path = test_file_path("sync_backend_basic");
        let file = create_test_file(&path, 64 * 1024);
        let mut backend = SyncBackend::new(file);

        // Test data with known patterns to verify no corruption
        let patterns: Vec<(u64, Vec<u8>)> = vec![
            (0, vec![0xAAu8; 4096]),      // First page
            (4096, vec![0xBBu8; 4096]),   // Second page
            (8192, vec![0xCCu8; 4096]),   // Third page
            (16384, vec![0xDDu8; 8192]),  // Double-size write
        ];

        // Write operations
        let write_ops: Vec<WriteOp> = patterns
            .iter()
            .map(|(offset, data)| WriteOp {
                offset: *offset,
                data: data.clone(),
            })
            .collect();

        backend.write_batch(write_ops).expect("write_batch should succeed");
        backend.sync().expect("sync should succeed");

        // Read back and verify each pattern
        let read_ops: Vec<ReadOp> = patterns
            .iter()
            .map(|(offset, data)| ReadOp {
                offset: *offset,
                len: data.len(),
            })
            .collect();

        let results = backend.read_batch(read_ops).expect("read_batch should succeed");

        assert_eq!(results.len(), patterns.len(), "should return correct number of results");

        for (i, ((_offset, expected), result)) in patterns.iter().zip(results.iter()).enumerate() {
            assert_eq!(
                result.bytes_read, expected.len(),
                "read {i}: bytes_read mismatch"
            );
            assert_eq!(
                &result.data, expected,
                "read {i}: data corruption detected"
            );
        }

        cleanup(&path);
    }

    /// Tests that overlapping writes are handled correctly (last write wins)
    /// and that the backend properly sequences operations.
    #[test]
    fn test_sync_backend_overlapping_writes() {
        let path = test_file_path("sync_backend_overlap");
        let file = create_test_file(&path, 16 * 1024);
        let mut backend = SyncBackend::new(file);

        // Write initial data
        backend
            .write_batch(vec![WriteOp {
                offset: 0,
                data: vec![0x11u8; 8192],
            }])
            .expect("initial write");

        // Overlapping write (partial overwrite)
        backend
            .write_batch(vec![WriteOp {
                offset: 2048,
                data: vec![0x22u8; 4096],
            }])
            .expect("overlapping write");

        backend.sync().expect("sync");

        // Verify the result: [0x11 * 2048][0x22 * 4096][0x11 * 2048]
        let results = backend
            .read_batch(vec![
                ReadOp { offset: 0, len: 2048 },
                ReadOp { offset: 2048, len: 4096 },
                ReadOp { offset: 6144, len: 2048 },
            ])
            .expect("read");

        assert!(results[0].data.iter().all(|&b| b == 0x11), "first segment corrupted");
        assert!(results[1].data.iter().all(|&b| b == 0x22), "middle segment corrupted");
        assert!(results[2].data.iter().all(|&b| b == 0x11), "last segment corrupted");

        cleanup(&path);
    }

    /// Tests that the backend properly handles edge cases:
    /// - Empty write batch
    /// - Single-byte operations
    /// - Maximum reasonable batch size
    #[test]
    fn test_sync_backend_edge_cases() {
        let path = test_file_path("sync_backend_edge");
        let file = create_test_file(&path, 1024 * 1024);
        let mut backend = SyncBackend::new(file);

        // Empty batch should succeed without error
        backend
            .write_batch(Vec::new())
            .expect("empty write batch should succeed");

        // Single byte operations
        backend
            .write_batch(vec![WriteOp {
                offset: 100,
                data: vec![0xFF],
            }])
            .expect("single byte write");

        let results = backend
            .read_batch(vec![ReadOp { offset: 100, len: 1 }])
            .expect("single byte read");
        assert_eq!(results[0].data, vec![0xFF]);

        // Large batch (256 operations)
        let large_batch: Vec<WriteOp> = (0..256)
            .map(|i| WriteOp {
                offset: (i * 4096) as u64,
                data: vec![(i & 0xFF) as u8; 1024],
            })
            .collect();

        backend.write_batch(large_batch).expect("large batch write");
        backend.sync().expect("sync after large batch");

        // Verify a sample of the large batch
        let sample_reads: Vec<ReadOp> = vec![0, 50, 100, 200, 255]
            .into_iter()
            .map(|i| ReadOp {
                offset: (i * 4096) as u64,
                len: 1024,
            })
            .collect();

        let sample_results = backend.read_batch(sample_reads).expect("sample reads");
        for (idx, result) in [0usize, 50, 100, 200, 255].iter().zip(sample_results.iter()) {
            let expected_byte = (*idx & 0xFF) as u8;
            assert!(
                result.data.iter().all(|&b| b == expected_byte),
                "large batch verification failed at index {idx}"
            );
        }

        cleanup(&path);
    }

    /// Tests backend metadata methods return sensible values.
    #[test]
    fn test_sync_backend_metadata() {
        let path = test_file_path("sync_backend_meta");
        let file = create_test_file(&path, 4096);
        let backend = SyncBackend::new(file);

        assert_eq!(backend.name(), "sync");
        assert!(!backend.supports_parallel());
        assert_eq!(backend.optimal_batch_size(), 1);

        cleanup(&path);
    }
}

// ==================== Aligned Buffer Tests ====================

mod aligned_buffer_tests {
    use thunder::aligned::{AlignedBuffer, AlignedBufferPool, DEFAULT_ALIGNMENT};

    /// Tests that AlignedBuffer correctly allocates memory with proper alignment.
    /// This is critical for O_DIRECT I/O which requires aligned addresses.
    #[test]
    fn test_aligned_buffer_alignment_guarantee() {
        // Test common alignment values used in direct I/O
        for alignment in [512, 1024, 2048, 4096, 8192, 16384] {
            let buf = AlignedBuffer::new(1024, alignment);
            
            let ptr = buf.as_ptr() as usize;
            assert_eq!(
                ptr % alignment, 0,
                "buffer at {ptr:#x} not aligned to {alignment}"
            );
            assert_eq!(buf.alignment(), alignment);
            
            // Capacity should be rounded up to alignment
            assert!(buf.capacity() >= 1024);
            assert_eq!(buf.capacity() % alignment, 0);
        }
    }

    /// Tests that AlignedBuffer operations don't cause memory corruption.
    /// Uses sentinel patterns to detect buffer overflows.
    #[test]
    fn test_aligned_buffer_memory_safety() {
        let mut buf = AlignedBuffer::new(4096, 4096);

        // Write sentinel pattern
        let sentinel: Vec<u8> = (0..4096).map(|i| (i % 256) as u8).collect();
        buf.extend_from_slice(&sentinel);

        // Verify data integrity
        assert_eq!(buf.len(), 4096);
        for (i, &byte) in buf.as_slice().iter().enumerate() {
            assert_eq!(byte, (i % 256) as u8, "data corruption at byte {i}");
        }

        // Test clear doesn't corrupt
        buf.clear();
        assert_eq!(buf.len(), 0);
        assert!(buf.is_empty());

        // Re-use buffer
        buf.extend_from_slice(&[0xDE, 0xAD, 0xBE, 0xEF]);
        assert_eq!(buf.as_slice(), &[0xDE, 0xAD, 0xBE, 0xEF]);
    }

    /// Tests alignment padding for direct I/O requirements.
    #[test]
    fn test_aligned_buffer_padding() {
        let mut buf = AlignedBuffer::new(8192, 4096);

        // Write non-aligned data
        buf.extend_from_slice(&[0xAA; 1000]);
        assert_eq!(buf.len(), 1000);

        // Pad to alignment boundary
        buf.pad_to_alignment();
        assert_eq!(buf.len(), 4096, "padding should round up to alignment");
        assert_eq!(buf.len() % 4096, 0);

        // First 1000 bytes should be original data
        assert!(buf.as_slice()[..1000].iter().all(|&b| b == 0xAA));
        // Remaining should be zeros (padding)
        assert!(buf.as_slice()[1000..].iter().all(|&b| b == 0));
    }

    /// Tests that AlignedBuffer clone creates an independent copy.
    #[test]
    fn test_aligned_buffer_clone_independence() {
        let mut original = AlignedBuffer::new(4096, 4096);
        original.extend_from_slice(&[0x12; 100]);

        let mut clone = original.clone();

        // Modify clone
        clone.clear();
        clone.extend_from_slice(&[0x34; 50]);

        // Original should be unaffected
        assert_eq!(original.len(), 100);
        assert!(original.as_slice().iter().all(|&b| b == 0x12));

        // Clone should have new data
        assert_eq!(clone.len(), 50);
        assert!(clone.as_slice().iter().all(|&b| b == 0x34));
    }

    /// Tests the buffer pool for efficient reuse without leaks.
    #[test]
    fn test_aligned_buffer_pool() {
        let mut pool = AlignedBufferPool::new(4096, DEFAULT_ALIGNMENT, 4);

        // Acquire buffers
        let mut buffers: Vec<AlignedBuffer> = (0..4).map(|_| pool.acquire()).collect();

        // All buffers should be properly aligned
        for buf in &buffers {
            assert_eq!(buf.as_ptr() as usize % DEFAULT_ALIGNMENT, 0);
            assert!(buf.capacity() >= 4096);
        }

        // Use and release
        for (i, buf) in buffers.iter_mut().enumerate() {
            buf.extend_from_slice(&[i as u8; 100]);
        }

        // Return to pool
        for buf in buffers {
            pool.release(buf);
        }

        // Acquire again - should get cleared buffers
        let reused = pool.acquire();
        assert_eq!(reused.len(), 0, "released buffer should be cleared");
        assert!(reused.capacity() >= 4096);
    }

    /// Tests that default alignment is appropriate for most filesystems.
    #[test]
    fn test_default_alignment() {
        assert_eq!(DEFAULT_ALIGNMENT, 4096, "default should be 4KB for most filesystems");
        
        let buf = AlignedBuffer::with_default_alignment(1024);
        assert_eq!(buf.alignment(), 4096);
    }

    #[test]
    #[should_panic(expected = "alignment must be power of 2")]
    fn test_non_power_of_two_alignment_panics() {
        let _ = AlignedBuffer::new(1024, 1000); // 1000 is not power of 2
    }
}

// ==================== Parallel Write Tests ====================

mod parallel_write_tests {
    use thunder::coalescer::{WriteBatch, WriteCoalescer};
    use thunder::parallel::{partition_for_parallel, ParallelConfig, ParallelWriter};

    /// Tests that partition_for_parallel correctly distributes pages
    /// while maintaining data integrity.
    #[test]
    fn test_partition_correctness() {
        // Create a batch with 100 pages
        let mut coalescer = WriteCoalescer::new(4096, 16 * 1024 * 1024);
        
        for i in 0..100u64 {
            let data = vec![(i & 0xFF) as u8; 4096];
            coalescer.queue_page(i, data);
        }
        coalescer.queue_sequential(&[0xEE; 1024]);

        let batch = coalescer.into_write_batch();
        let original_seq_data = batch.sequential_data.clone();
        let original_pages: std::collections::HashMap<_, _> = 
            batch.pages.iter().map(|(id, data)| (*id, data.clone())).collect();

        // Partition into 4 parts
        let partitions = partition_for_parallel(batch, 4);

        assert_eq!(partitions.len(), 4, "should create 4 partitions");

        // Verify all pages are accounted for exactly once
        let mut seen_pages = std::collections::HashSet::new();
        let mut total_pages = 0;

        for partition in &partitions {
            for (page_id, data) in &partition.pages {
                assert!(
                    seen_pages.insert(*page_id),
                    "page {page_id} appears in multiple partitions"
                );
                
                // Verify data integrity
                let original = original_pages.get(page_id).expect("page should exist");
                assert_eq!(data, original, "page {page_id} data corrupted");
                total_pages += 1;
            }
        }

        assert_eq!(total_pages, 100, "all pages should be distributed");
        assert_eq!(seen_pages.len(), 100);

        // Sequential data should only be in first partition
        assert_eq!(partitions[0].sequential_data, original_seq_data);
        for partition in &partitions[1..] {
            assert!(partition.sequential_data.is_empty());
        }
    }

    /// Tests partitioning with edge cases.
    #[test]
    fn test_partition_edge_cases() {
        // Empty batch
        let empty_batch = WriteBatch::empty();
        let partitions = partition_for_parallel(empty_batch, 4);
        assert_eq!(partitions.len(), 1, "empty batch should return single partition");

        // Single page batch
        let mut coalescer = WriteCoalescer::new(4096, 1024 * 1024);
        coalescer.queue_page(0, vec![0u8; 4096]);
        let single_batch = coalescer.into_write_batch();
        let partitions = partition_for_parallel(single_batch, 4);
        assert_eq!(partitions.len(), 1, "single page should not be split");

        // num_partitions = 1
        let mut coalescer = WriteCoalescer::new(4096, 1024 * 1024);
        for i in 0..10 {
            coalescer.queue_page(i, vec![0u8; 4096]);
        }
        let batch = coalescer.into_write_batch();
        let partitions = partition_for_parallel(batch, 1);
        assert_eq!(partitions.len(), 1);
        assert_eq!(partitions[0].pages.len(), 10);

        // More partitions than pages
        let mut coalescer = WriteCoalescer::new(4096, 1024 * 1024);
        for i in 0..3 {
            coalescer.queue_page(i, vec![0u8; 4096]);
        }
        let batch = coalescer.into_write_batch();
        let partitions = partition_for_parallel(batch, 10);
        // Should create at most as many non-empty partitions as pages
        let non_empty: Vec<_> = partitions.iter().filter(|p| !p.pages.is_empty()).collect();
        assert!(non_empty.len() <= 3);
    }

    /// Tests that ParallelConfig provides sensible defaults
    /// and validates configuration bounds.
    #[test]
    fn test_parallel_config_defaults() {
        let config = ParallelConfig::default();

        // Should use reasonable number of workers (not more than CPU cores)
        assert!(config.num_workers > 0);
        assert!(config.num_workers <= 64, "unreasonably high worker count");

        // Batch size should be reasonable
        assert!(config.ops_per_batch > 0);
        assert!(config.ops_per_batch <= 1024);
    }

    /// Tests ParallelWriter creation and basic operation.
    #[test]
    fn test_parallel_writer_basic() {
        let config = ParallelConfig {
            num_workers: 2,
            ops_per_batch: 16,
            use_thread_local_backend: false,
        };

        let _writer = ParallelWriter::new(config);
        // Writer creation should succeed
    }

    /// Tests that partitioning maintains deterministic order for reproducibility.
    #[test]
    fn test_partition_determinism() {
        let create_batch = || {
            let mut coalescer = WriteCoalescer::new(4096, 16 * 1024 * 1024);
            for i in 0..50u64 {
                coalescer.queue_page(i, vec![(i & 0xFF) as u8; 4096]);
            }
            coalescer.into_write_batch()
        };

        let partitions1 = partition_for_parallel(create_batch(), 4);
        let partitions2 = partition_for_parallel(create_batch(), 4);

        // Same input should produce same output
        assert_eq!(partitions1.len(), partitions2.len());
        for (p1, p2) in partitions1.iter().zip(partitions2.iter()) {
            assert_eq!(p1.pages.len(), p2.pages.len());
            for ((id1, data1), (id2, data2)) in p1.pages.iter().zip(p2.pages.iter()) {
                assert_eq!(id1, id2);
                assert_eq!(data1, data2);
            }
        }
    }
}

// ==================== Integration Tests ====================

mod integration_tests {
    use super::*;
    use thunder::io_backend::{IoBackend, SyncBackend, WriteOp};
    use thunder::aligned::AlignedBuffer;
    use thunder::coalescer::WriteCoalescer;

    /// End-to-end test: create a batch, partition it, write with backend,
    /// verify all data is correctly persisted.
    #[test]
    fn test_full_io_stack_integration() {
        let path = test_file_path("full_integration");
        let file = create_test_file(&path, 10 * 1024 * 1024);
        let mut backend = SyncBackend::new(file);
        let page_size = 4096;

        // Create a realistic workload
        let mut coalescer = WriteCoalescer::new(page_size, 16 * 1024 * 1024);

        // Add sequential data (entry data)
        let entry_data = vec![0xEE; 8192];
        coalescer.queue_sequential(&entry_data);

        // Add scattered pages (simulating overflow and tree pages)
        let page_data: Vec<(u64, Vec<u8>)> = vec![
            (100, vec![0x01; page_size]),
            (101, vec![0x02; page_size]),
            (200, vec![0x03; page_size]),  // Gap
            (500, vec![0x04; page_size]),  // Another gap
            (501, vec![0x05; page_size]),
            (502, vec![0x06; page_size]),
        ];

        for (page_id, data) in &page_data {
            coalescer.queue_page(*page_id, data.clone());
        }

        let batch = coalescer.into_write_batch();

        // Write using backend
        let data_offset = 2 * page_size as u64; // After meta pages

        // Write sequential data
        if !batch.sequential_data.is_empty() {
            backend
                .write_batch(vec![WriteOp {
                    offset: data_offset,
                    data: batch.sequential_data.clone(),
                }])
                .expect("sequential write");
        }

        // Write pages
        let page_ops: Vec<WriteOp> = batch
            .pages
            .iter()
            .map(|(page_id, data)| WriteOp {
                offset: *page_id * page_size as u64,
                data: data.clone(),
            })
            .collect();

        backend.write_batch(page_ops).expect("page writes");
        backend.sync().expect("final sync");

        // Verify sequential data
        let results = backend
            .read_batch(vec![thunder::io_backend::ReadOp {
                offset: data_offset,
                len: entry_data.len(),
            }])
            .expect("read sequential");
        assert_eq!(results[0].data, entry_data);

        // Verify each page
        for (page_id, expected_data) in &page_data {
            let results = backend
                .read_batch(vec![thunder::io_backend::ReadOp {
                    offset: *page_id * page_size as u64,
                    len: page_size,
                }])
                .expect("read page");
            assert_eq!(&results[0].data, expected_data, "page {page_id} mismatch");
        }

        cleanup(&path);
    }

    /// Tests that the I/O stack handles concurrent access patterns safely
    /// (simulating what parallel writes would do).
    #[test]
    fn test_io_stack_concurrent_pattern_safety() {
        let path = test_file_path("concurrent_pattern");
        let file = create_test_file(&path, 4 * 1024 * 1024);
        let mut backend = SyncBackend::new(file);
        let page_size = 4096;

        // Simulate what parallel writers would do: write to non-overlapping regions
        let regions: Vec<(u64, u8)> = vec![
            (0, 0xAA),
            (16, 0xBB),
            (32, 0xCC),
            (48, 0xDD),
        ];

        // Write all regions
        let ops: Vec<WriteOp> = regions
            .iter()
            .map(|(page_start, fill)| WriteOp {
                offset: *page_start * page_size as u64,
                data: vec![*fill; page_size * 4], // 4 pages each
            })
            .collect();

        backend.write_batch(ops).expect("concurrent-style writes");
        backend.sync().expect("sync");

        // Verify each region is intact
        for (page_start, expected_fill) in &regions {
            let results = backend
                .read_batch(vec![thunder::io_backend::ReadOp {
                    offset: *page_start * page_size as u64,
                    len: page_size * 4,
                }])
                .expect("read region");

            assert!(
                results[0].data.iter().all(|&b| b == *expected_fill),
                "region at page {page_start} corrupted"
            );
        }

        cleanup(&path);
    }

    /// Tests aligned buffer with actual I/O to ensure it works for direct I/O scenarios.
    #[test]
    fn test_aligned_buffer_io_compatibility() {
        let path = test_file_path("aligned_io");
        let file = create_test_file(&path, 64 * 1024);
        let mut backend = SyncBackend::new(file);

        // Create aligned buffer with test data
        let mut aligned_buf = AlignedBuffer::new(8192, 4096);
        aligned_buf.extend_from_slice(&vec![0x42; 4096]);
        aligned_buf.extend_from_slice(&vec![0x43; 4096]);

        // Write using the aligned buffer's data
        backend
            .write_batch(vec![WriteOp {
                offset: 0,
                data: aligned_buf.as_slice().to_vec(),
            }])
            .expect("aligned write");
        backend.sync().expect("sync");

        // Read back
        let results = backend
            .read_batch(vec![thunder::io_backend::ReadOp { offset: 0, len: 8192 }])
            .expect("read back");

        assert_eq!(results[0].data.len(), 8192);
        assert!(results[0].data[..4096].iter().all(|&b| b == 0x42));
        assert!(results[0].data[4096..].iter().all(|&b| b == 0x43));

        cleanup(&path);
    }
}
