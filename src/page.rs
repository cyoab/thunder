//! Summary: Page layout, encoding, and helper utilities.
//! Copyright (c) YOAB. All rights reserved.

/// Default page size in bytes (4KB).
pub const PAGE_SIZE: usize = 4096;

/// Magic number to identify thunder database files.
pub const MAGIC: u32 = 0x54_48_4E_44; // "THND" in ASCII

/// Current database file format version.
pub const VERSION: u32 = 2; // Bumped for Phase 2 features

/// Page identifier type.
pub type PageId = u64;

/// Supported page size configurations.
///
/// Different page sizes optimize for different workloads:
/// - 4KB: Traditional, good for small values and random access
/// - 8KB: Balanced performance
/// - 16KB: Recommended for NVMe, better for larger values
/// - 64KB: High-throughput workloads with large sequential writes
#[derive(Debug, Clone, Copy, PartialEq, Eq, Default)]
#[repr(u32)]
pub enum PageSizeConfig {
    /// 4KB pages (default, traditional).
    #[default]
    Size4K = 4096,
    /// 8KB pages.
    Size8K = 8192,
    /// 16KB pages (recommended for NVMe).
    Size16K = 16384,
    /// 64KB pages (high-throughput workloads).
    Size64K = 65536,
}

impl PageSizeConfig {
    /// Returns the page size in bytes.
    #[inline]
    pub fn as_usize(self) -> usize {
        self as usize
    }

    /// Creates a `PageSizeConfig` from a u32 value.
    ///
    /// Returns `None` if the value is not a supported page size.
    pub fn from_u32(value: u32) -> Option<Self> {
        match value {
            4096 => Some(Self::Size4K),
            8192 => Some(Self::Size8K),
            16384 => Some(Self::Size16K),
            65536 => Some(Self::Size64K),
            _ => None,
        }
    }

    /// Returns true if this is a valid, supported page size.
    #[inline]
    pub fn is_valid(value: u32) -> bool {
        Self::from_u32(value).is_some()
    }
}

/// Page types used in the database file.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
#[repr(u8)]
pub enum PageType {
    /// Meta page containing database metadata.
    Meta = 1,
    /// Freelist page tracking free pages.
    Freelist = 2,
    /// Branch page (internal B+ tree node).
    Branch = 3,
    /// Leaf page (B+ tree leaf with key-value pairs).
    Leaf = 4,
    /// Overflow page for large values.
    Overflow = 5,
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn test_page_constants() {
        assert_eq!(PAGE_SIZE, 4096);
        assert!(PAGE_SIZE.is_power_of_two());
        assert_eq!(MAGIC, 0x54_48_4E_44);
        assert_eq!(&MAGIC.to_be_bytes(), b"THND");
        assert_eq!(VERSION, 2);
    }

    #[test]
    fn test_page_types() {
        assert_eq!(PageType::Meta as u8, 1);
        assert_eq!(PageType::Freelist as u8, 2);
        assert_eq!(PageType::Branch as u8, 3);
        assert_eq!(PageType::Leaf as u8, 4);
        assert_eq!(PageType::Overflow as u8, 5);
        assert_eq!(PageType::Meta, PageType::Meta);
        assert_ne!(PageType::Meta, PageType::Leaf);
    }

    #[test]
    fn test_page_size_config() {
        assert_eq!(PageSizeConfig::Size4K.as_usize(), 4096);
        assert_eq!(PageSizeConfig::Size8K.as_usize(), 8192);
        assert_eq!(PageSizeConfig::Size16K.as_usize(), 16384);
        assert_eq!(PageSizeConfig::Size64K.as_usize(), 65536);

        assert_eq!(PageSizeConfig::from_u32(4096), Some(PageSizeConfig::Size4K));
        assert_eq!(PageSizeConfig::from_u32(8192), Some(PageSizeConfig::Size8K));
        assert_eq!(PageSizeConfig::from_u32(16384), Some(PageSizeConfig::Size16K));
        assert_eq!(PageSizeConfig::from_u32(65536), Some(PageSizeConfig::Size64K));
        assert_eq!(PageSizeConfig::from_u32(1024), None);
        assert_eq!(PageSizeConfig::from_u32(0), None);

        assert!(PageSizeConfig::is_valid(4096));
        assert!(!PageSizeConfig::is_valid(1000));

        assert_eq!(PageSizeConfig::default(), PageSizeConfig::Size4K);
    }
}
