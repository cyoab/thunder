//! Summary: Page layout, encoding, and helper utilities.
//! Copyright (c) YOAB. All rights reserved.

/// Default page size in bytes (4KB).
pub const PAGE_SIZE: usize = 4096;

/// Magic number to identify thunder database files.
pub const MAGIC: u32 = 0x54_48_4E_44; // "THND" in ASCII

/// Current database file format version.
pub const VERSION: u32 = 1;

/// Page identifier type.
pub type PageId = u64;

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
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn test_page_size_is_4kb() {
        assert_eq!(PAGE_SIZE, 4096);
    }

    #[test]
    fn test_page_size_is_power_of_two() {
        assert!(PAGE_SIZE.is_power_of_two());
    }

    #[test]
    fn test_magic_number_value() {
        // "THND" in little-endian.
        assert_eq!(MAGIC, 0x54_48_4E_44);
    }

    #[test]
    fn test_magic_number_as_bytes() {
        let bytes = MAGIC.to_be_bytes();
        assert_eq!(&bytes, b"THND");
    }

    #[test]
    fn test_version_is_one() {
        assert_eq!(VERSION, 1);
    }

    #[test]
    fn test_page_type_values() {
        assert_eq!(PageType::Meta as u8, 1);
        assert_eq!(PageType::Freelist as u8, 2);
        assert_eq!(PageType::Branch as u8, 3);
        assert_eq!(PageType::Leaf as u8, 4);
    }

    #[test]
    fn test_page_type_debug() {
        let meta = PageType::Meta;
        assert_eq!(format!("{meta:?}"), "Meta");
    }

    #[test]
    fn test_page_type_equality() {
        assert_eq!(PageType::Meta, PageType::Meta);
        assert_ne!(PageType::Meta, PageType::Leaf);
    }

    #[test]
    fn test_page_type_clone() {
        let original = PageType::Branch;
        let cloned = original;
        assert_eq!(original, cloned);
    }

    #[test]
    fn test_page_id_type() {
        let page_id: PageId = 12345;
        assert_eq!(page_id, 12345u64);
    }

    #[test]
    fn test_page_id_max() {
        let max_page_id: PageId = u64::MAX;
        assert_eq!(max_page_id, u64::MAX);
    }
}
