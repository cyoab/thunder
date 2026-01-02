//! Summary: Overflow page management for large values.
//! Copyright (c) YOAB. All rights reserved.
//!
//! This module implements overflow page chains for values exceeding
//! the overflow threshold. Large values are stored in linked chains
//! of overflow pages, keeping leaf nodes compact.

use crate::mmap::Mmap;
use crate::page::PageId;

/// Default threshold for storing values in overflow pages.
/// Values larger than this are stored in overflow chains.
pub const DEFAULT_OVERFLOW_THRESHOLD: usize = 2048; // 2KB

/// Overflow page header size in bytes.
pub const OVERFLOW_HEADER_SIZE: usize = 24;

/// Overflow page header structure.
///
/// Layout (24 bytes total):
/// ```text
/// [0]       page_type (u8) - PageType::Overflow = 5
/// [1..8]    reserved for alignment
/// [8..16]   next_page (u64) - next overflow page ID (0 = end of chain)
/// [16..20]  data_len (u32) - length of data in this page
/// [20..24]  checksum (u32) - CRC32 of data for integrity verification
/// ```
#[derive(Debug, Clone, Copy)]
#[repr(C)]
pub struct OverflowHeader {
    /// Page type marker (PageType::Overflow = 5).
    pub page_type: u8,
    /// Reserved for alignment.
    pub _reserved: [u8; 7],
    /// Next overflow page ID (0 = end of chain).
    pub next_page: PageId,
    /// Length of data in this page.
    pub data_len: u32,
    /// CRC32 checksum of data for integrity verification.
    pub checksum: u32,
}

impl OverflowHeader {
    /// Size of the overflow header in bytes.
    pub const SIZE: usize = OVERFLOW_HEADER_SIZE;

    /// Creates a new overflow header.
    pub fn new(next_page: PageId, data_len: u32, checksum: u32) -> Self {
        Self {
            page_type: 5, // PageType::Overflow
            _reserved: [0; 7],
            next_page,
            data_len,
            checksum,
        }
    }

    /// Serializes the header to bytes.
    pub fn to_bytes(&self) -> [u8; Self::SIZE] {
        let mut buf = [0u8; Self::SIZE];
        buf[0] = self.page_type;
        buf[8..16].copy_from_slice(&self.next_page.to_le_bytes());
        buf[16..20].copy_from_slice(&self.data_len.to_le_bytes());
        buf[20..24].copy_from_slice(&self.checksum.to_le_bytes());
        buf
    }

    /// Deserializes a header from bytes.
    ///
    /// Returns `None` if the buffer is too small or page type is invalid.
    pub fn from_bytes(buf: &[u8]) -> Option<Self> {
        if buf.len() < Self::SIZE {
            return None;
        }

        let page_type = buf[0];
        if page_type != 5 {
            return None; // Not an overflow page
        }

        Some(Self {
            page_type,
            _reserved: [0; 7],
            next_page: u64::from_le_bytes(buf[8..16].try_into().ok()?),
            data_len: u32::from_le_bytes(buf[16..20].try_into().ok()?),
            checksum: u32::from_le_bytes(buf[20..24].try_into().ok()?),
        })
    }
}

/// Reference to a value stored in overflow pages.
///
/// This is stored in the leaf node entry instead of the actual value data.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub struct OverflowRef {
    /// First page of the overflow chain.
    pub start_page: PageId,
    /// Total length of the value in bytes.
    pub total_len: u32,
}

impl OverflowRef {
    /// Marker value indicating an overflow reference (0xFFFFFFFF).
    /// This value is used in place of value_len to indicate overflow storage.
    pub const MARKER: u32 = 0xFFFF_FFFF;

    /// Serialized size of an overflow reference (after the marker).
    /// Format: [marker:4][start_page:8][total_len:4] = 16 bytes total
    /// The SIZE here is just the page_id + total_len = 12 bytes (marker written separately)
    pub const SIZE: usize = 12;

    /// Creates a new overflow reference.
    pub fn new(start_page: PageId, total_len: u32) -> Self {
        Self {
            start_page,
            total_len,
        }
    }

    /// Serializes the overflow reference to bytes (without marker).
    pub fn to_bytes(&self) -> [u8; Self::SIZE] {
        let mut buf = [0u8; Self::SIZE];
        buf[0..8].copy_from_slice(&self.start_page.to_le_bytes());
        buf[8..12].copy_from_slice(&self.total_len.to_le_bytes());
        buf
    }

    /// Deserializes an overflow reference from bytes.
    pub fn from_bytes(buf: &[u8]) -> Option<Self> {
        if buf.len() < Self::SIZE {
            return None;
        }

        Some(Self {
            start_page: u64::from_le_bytes(buf[0..8].try_into().ok()?),
            total_len: u32::from_le_bytes(buf[8..12].try_into().ok()?),
        })
    }
}

/// Manages overflow page allocation and retrieval.
///
/// Handles the creation of overflow page chains for large values
/// and provides methods to read values back from overflow chains.
pub struct OverflowManager {
    /// Next available page ID for overflow allocation.
    next_page_id: PageId,
    /// Free overflow pages available for reuse.
    free_pages: Vec<PageId>,
    /// Page size for this database.
    page_size: usize,
    /// Usable data per overflow page (page_size - header).
    overflow_data_size: usize,
}

impl OverflowManager {
    /// Creates a new overflow manager.
    ///
    /// # Arguments
    ///
    /// * `page_size` - The page size for this database.
    /// * `next_page_id` - The next available page ID.
    pub fn new(page_size: usize, next_page_id: PageId) -> Self {
        Self {
            next_page_id,
            free_pages: Vec::new(),
            page_size,
            overflow_data_size: page_size - OVERFLOW_HEADER_SIZE,
        }
    }

    /// Returns the overflow data size for this manager.
    #[inline]
    pub fn overflow_data_size(&self) -> usize {
        self.overflow_data_size
    }

    /// Returns the next page ID that will be allocated.
    #[inline]
    pub fn next_page_id(&self) -> PageId {
        self.next_page_id
    }

    /// Sets the next page ID (used when loading from disk).
    #[inline]
    pub fn set_next_page_id(&mut self, page_id: PageId) {
        self.next_page_id = page_id;
    }

    /// Allocates overflow pages for a large value.
    ///
    /// Returns the `OverflowRef` and the pages to write.
    ///
    /// # Arguments
    ///
    /// * `value` - The value data to store in overflow pages.
    ///
    /// # Returns
    ///
    /// A tuple of (OverflowRef, Vec<(PageId, page_data)>).
    pub fn allocate_overflow(&mut self, value: &[u8]) -> (OverflowRef, Vec<(PageId, Vec<u8>)>) {
        if value.is_empty() {
            // Edge case: empty value shouldn't use overflow, but handle gracefully
            return (
                OverflowRef::new(0, 0),
                Vec::new(),
            );
        }

        let mut pages = Vec::new();
        let mut remaining = value;
        let first_page = self.alloc_page();
        let mut current_page_id = first_page;

        while !remaining.is_empty() {
            let chunk_len = remaining.len().min(self.overflow_data_size);
            let chunk = &remaining[..chunk_len];
            remaining = &remaining[chunk_len..];

            // Determine next page (0 if this is the last chunk)
            let next_page = if remaining.is_empty() {
                0
            } else {
                self.alloc_page()
            };

            // Calculate checksum for data integrity
            let checksum = Self::compute_checksum(chunk);

            let header = OverflowHeader::new(next_page, chunk_len as u32, checksum);

            let mut page_data = vec![0u8; self.page_size];
            page_data[..OVERFLOW_HEADER_SIZE].copy_from_slice(&header.to_bytes());
            page_data[OVERFLOW_HEADER_SIZE..OVERFLOW_HEADER_SIZE + chunk_len]
                .copy_from_slice(chunk);

            pages.push((current_page_id, page_data));
            current_page_id = next_page;
        }

        let overflow_ref = OverflowRef::new(first_page, value.len() as u32);
        (overflow_ref, pages)
    }

    /// Reads a value from overflow pages using mmap.
    ///
    /// # Arguments
    ///
    /// * `overflow_ref` - Reference to the overflow chain.
    /// * `mmap` - Memory-mapped file for reading pages.
    ///
    /// # Returns
    ///
    /// The reconstructed value, or `None` if reading fails.
    #[cfg(unix)]
    pub fn read_overflow(&self, overflow_ref: OverflowRef, mmap: &Mmap) -> Option<Vec<u8>> {
        if overflow_ref.start_page == 0 {
            return Some(Vec::new());
        }

        let mut result = Vec::with_capacity(overflow_ref.total_len as usize);
        let mut current_page = overflow_ref.start_page;
        let mut pages_read = 0;

        // Safety limit to prevent infinite loops on corrupted chains
        const MAX_CHAIN_LENGTH: usize = 1_000_000;

        while current_page != 0 && pages_read < MAX_CHAIN_LENGTH {
            let page_data = mmap.page_with_size(current_page, self.page_size)?;
            let header = OverflowHeader::from_bytes(page_data)?;

            // Validate data length
            let data_end = OVERFLOW_HEADER_SIZE + header.data_len as usize;
            if data_end > page_data.len() {
                return None; // Corrupted
            }

            let chunk = &page_data[OVERFLOW_HEADER_SIZE..data_end];

            // Verify checksum
            let computed_checksum = Self::compute_checksum(chunk);
            if computed_checksum != header.checksum {
                return None; // Data corruption detected
            }

            result.extend_from_slice(chunk);
            current_page = header.next_page;
            pages_read += 1;
        }

        // Verify we got the expected amount of data
        if result.len() != overflow_ref.total_len as usize {
            return None;
        }

        Some(result)
    }

    /// Reads a value from overflow pages by reading from file directly.
    ///
    /// Used when mmap is not available or for non-Unix platforms.
    #[allow(dead_code)]
    pub fn read_overflow_from_file(
        &self,
        overflow_ref: OverflowRef,
        file: &mut std::fs::File,
    ) -> Option<Vec<u8>> {
        use std::io::{Read, Seek, SeekFrom};

        if overflow_ref.start_page == 0 {
            return Some(Vec::new());
        }

        let mut result = Vec::with_capacity(overflow_ref.total_len as usize);
        let mut current_page = overflow_ref.start_page;
        let mut pages_read = 0;

        const MAX_CHAIN_LENGTH: usize = 1_000_000;

        while current_page != 0 && pages_read < MAX_CHAIN_LENGTH {
            let offset = current_page * self.page_size as u64;
            file.seek(SeekFrom::Start(offset)).ok()?;

            let mut page_data = vec![0u8; self.page_size];
            file.read_exact(&mut page_data).ok()?;

            let header = OverflowHeader::from_bytes(&page_data)?;

            let data_end = OVERFLOW_HEADER_SIZE + header.data_len as usize;
            if data_end > page_data.len() {
                return None;
            }

            let chunk = &page_data[OVERFLOW_HEADER_SIZE..data_end];

            // Verify checksum
            let computed_checksum = Self::compute_checksum(chunk);
            if computed_checksum != header.checksum {
                return None;
            }

            result.extend_from_slice(chunk);
            current_page = header.next_page;
            pages_read += 1;
        }

        if result.len() != overflow_ref.total_len as usize {
            return None;
        }

        Some(result)
    }

    /// Frees an overflow chain for reuse.
    ///
    /// Traverses the chain and adds all pages to the free list.
    #[cfg(unix)]
    pub fn free_overflow(&mut self, overflow_ref: OverflowRef, mmap: &Mmap) {
        let mut current_page = overflow_ref.start_page;
        let mut pages_freed = 0;

        const MAX_CHAIN_LENGTH: usize = 1_000_000;

        while current_page != 0 && pages_freed < MAX_CHAIN_LENGTH {
            if let Some(page_data) = mmap.page_with_size(current_page, self.page_size)
                && let Some(header) = OverflowHeader::from_bytes(page_data)
            {
                self.free_pages.push(current_page);
                current_page = header.next_page;
                pages_freed += 1;
                continue;
            }
            break;
        }
    }

    /// Allocates a page ID, either from the free list or by incrementing.
    fn alloc_page(&mut self) -> PageId {
        self.free_pages.pop().unwrap_or_else(|| {
            let id = self.next_page_id;
            self.next_page_id += 1;
            id
        })
    }

    /// Computes a CRC32 checksum for data integrity verification.
    fn compute_checksum(data: &[u8]) -> u32 {
        // Simple CRC32-like checksum using polynomial division
        let mut crc: u32 = 0xFFFF_FFFF;
        for &byte in data {
            crc ^= u32::from(byte);
            for _ in 0..8 {
                crc = if crc & 1 != 0 {
                    (crc >> 1) ^ 0xEDB8_8320
                } else {
                    crc >> 1
                };
            }
        }
        !crc
    }

    /// Returns whether a value should use overflow storage.
    #[inline]
    pub fn should_overflow(value_len: usize, threshold: usize) -> bool {
        value_len > threshold
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn test_overflow_header_roundtrip() {
        let header = OverflowHeader::new(42, 1024, 0xDEADBEEF);
        let bytes = header.to_bytes();
        let recovered = OverflowHeader::from_bytes(&bytes).expect("should parse");

        assert_eq!(recovered.page_type, 5);
        assert_eq!(recovered.next_page, 42);
        assert_eq!(recovered.data_len, 1024);
        assert_eq!(recovered.checksum, 0xDEADBEEF);
    }

    #[test]
    fn test_overflow_ref_roundtrip() {
        let oref = OverflowRef::new(100, 50000);
        let bytes = oref.to_bytes();
        let recovered = OverflowRef::from_bytes(&bytes).expect("should parse");

        assert_eq!(recovered.start_page, 100);
        assert_eq!(recovered.total_len, 50000);
    }

    #[test]
    fn test_overflow_manager_allocation() {
        let mut mgr = OverflowManager::new(4096, 10);

        // Allocate space for a 10KB value (3 pages needed)
        let value = vec![0xABu8; 10 * 1024];
        let (oref, pages) = mgr.allocate_overflow(&value);

        assert_eq!(oref.total_len, 10 * 1024);
        assert_eq!(oref.start_page, 10);
        assert_eq!(pages.len(), 3); // 10KB needs 3 pages with 4KB page size

        // Verify chain linkage
        let header0 = OverflowHeader::from_bytes(&pages[0].1).unwrap();
        let header1 = OverflowHeader::from_bytes(&pages[1].1).unwrap();
        let header2 = OverflowHeader::from_bytes(&pages[2].1).unwrap();

        assert_eq!(header0.next_page, pages[1].0);
        assert_eq!(header1.next_page, pages[2].0);
        assert_eq!(header2.next_page, 0); // End of chain
    }

    #[test]
    fn test_checksum_computation() {
        let data = b"Hello, World!";
        let checksum1 = OverflowManager::compute_checksum(data);
        let checksum2 = OverflowManager::compute_checksum(data);

        assert_eq!(checksum1, checksum2);

        // Different data should have different checksum
        let other_data = b"Hello, World?";
        let checksum3 = OverflowManager::compute_checksum(other_data);
        assert_ne!(checksum1, checksum3);
    }

    #[test]
    fn test_should_overflow() {
        assert!(!OverflowManager::should_overflow(1024, 2048));
        assert!(!OverflowManager::should_overflow(2048, 2048));
        assert!(OverflowManager::should_overflow(2049, 2048));
        assert!(OverflowManager::should_overflow(10000, 2048));
    }
}
