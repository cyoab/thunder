//! Summary: Bloom filter for probabilistic set membership.
//! Copyright (c) YOAB. All rights reserved.
//!
//! Provides a space-efficient probabilistic data structure for fast
//! negative lookups. When the filter says a key is not present, it
//! is definitely not present. When it says a key might be present,
//! there is a small probability of a false positive.

/// A space-efficient probabilistic data structure for set membership.
///
/// # Performance Characteristics
///
/// - `insert()`: O(k) where k is the number of hash functions
/// - `may_contain()`: O(k) where k is the number of hash functions
/// - Memory: approximately 10 bits per key at 1% false positive rate
///
/// # False Positive Rate
///
/// The false positive rate depends on the number of bits per key and
/// the number of hash functions. This implementation uses optimal
/// parameters for the configured false positive rate.
#[derive(Debug, Clone)]
pub struct BloomFilter {
    /// Bit array stored as u64 words for efficient access.
    bits: Vec<u64>,
    /// Number of hash functions to use.
    num_hashes: u8,
    /// Total number of bits in the filter.
    num_bits: usize,
    /// Number of items inserted (for statistics).
    item_count: usize,
}

impl BloomFilter {
    /// Creates a new Bloom filter sized for the expected number of items.
    ///
    /// # Arguments
    ///
    /// * `expected_items` - Expected number of items to insert.
    /// * `fp_rate` - Desired false positive rate (e.g., 0.01 for 1%).
    ///
    /// # Panics
    ///
    /// Panics if `expected_items` is 0 or `fp_rate` is not in (0, 1).
    ///
    /// # Example
    ///
    /// ```ignore
    /// let filter = BloomFilter::new(10000, 0.01);
    /// ```
    pub fn new(expected_items: usize, fp_rate: f64) -> Self {
        assert!(expected_items > 0, "expected_items must be positive");
        assert!(
            fp_rate > 0.0 && fp_rate < 1.0,
            "fp_rate must be between 0 and 1"
        );

        // Calculate optimal number of bits: m = -n * ln(p) / (ln(2)^2)
        let ln2_squared = std::f64::consts::LN_2 * std::f64::consts::LN_2;
        let num_bits = (-(expected_items as f64) * fp_rate.ln() / ln2_squared).ceil() as usize;

        // Ensure minimum size and round up to u64 boundary
        let num_bits = num_bits.max(64);
        let num_words = num_bits.div_ceil(64);
        let num_bits = num_words * 64;

        // Calculate optimal number of hash functions: k = (m/n) * ln(2)
        let num_hashes = ((num_bits as f64 / expected_items as f64) * std::f64::consts::LN_2)
            .ceil()
            .clamp(1.0, 16.0) as u8;

        Self {
            bits: vec![0u64; num_words],
            num_hashes,
            num_bits,
            item_count: 0,
        }
    }

    /// Creates a Bloom filter with default parameters for the given capacity.
    ///
    /// Uses a 1% false positive rate.
    pub fn with_capacity(expected_items: usize) -> Self {
        Self::new(expected_items, 0.01)
    }

    /// Adds a key to the filter.
    ///
    /// After this call, `may_contain(key)` will always return `true`.
    #[inline]
    pub fn insert(&mut self, key: &[u8]) {
        let (h1, h2) = self.hash_pair(key);

        for i in 0..self.num_hashes as u64 {
            let idx = self.get_bit_index(h1, h2, i);
            let word_idx = idx / 64;
            let bit_idx = idx % 64;
            self.bits[word_idx] |= 1 << bit_idx;
        }

        self.item_count += 1;
    }

    /// Checks if a key might be in the set.
    ///
    /// # Returns
    ///
    /// * `false` - The key is definitely NOT present.
    /// * `true` - The key MIGHT be present (with false positive probability).
    ///
    /// # Performance
    ///
    /// This method is very fast (O(k) where k is ~7 for 1% FP rate)
    /// and is suitable for hot paths.
    #[inline]
    pub fn may_contain(&self, key: &[u8]) -> bool {
        let (h1, h2) = self.hash_pair(key);

        for i in 0..self.num_hashes as u64 {
            let idx = self.get_bit_index(h1, h2, i);
            let word_idx = idx / 64;
            let bit_idx = idx % 64;

            if self.bits[word_idx] & (1 << bit_idx) == 0 {
                return false;
            }
        }

        true
    }

    /// Returns the number of items that have been inserted.
    #[inline]
    pub fn item_count(&self) -> usize {
        self.item_count
    }

    /// Returns the size of the filter in bits.
    #[inline]
    pub fn size_bits(&self) -> usize {
        self.num_bits
    }

    /// Returns the size of the filter in bytes.
    #[inline]
    pub fn size_bytes(&self) -> usize {
        self.bits.len() * 8
    }

    /// Returns the number of hash functions used.
    #[inline]
    pub fn num_hashes(&self) -> u8 {
        self.num_hashes
    }

    /// Clears the filter, removing all items.
    pub fn clear(&mut self) {
        for word in &mut self.bits {
            *word = 0;
        }
        self.item_count = 0;
    }

    /// Serializes the filter to bytes.
    ///
    /// Format:
    /// - 4 bytes: num_bits (u32)
    /// - 1 byte: num_hashes
    /// - 8 bytes: item_count (u64)
    /// - remaining: bit array
    pub fn to_bytes(&self) -> Vec<u8> {
        let mut bytes = Vec::with_capacity(13 + self.bits.len() * 8);

        // Header
        bytes.extend_from_slice(&(self.num_bits as u32).to_le_bytes());
        bytes.push(self.num_hashes);
        bytes.extend_from_slice(&(self.item_count as u64).to_le_bytes());

        // Bit array
        for word in &self.bits {
            bytes.extend_from_slice(&word.to_le_bytes());
        }

        bytes
    }

    /// Deserializes a filter from bytes.
    ///
    /// # Returns
    ///
    /// `None` if the data is corrupted or too short.
    pub fn from_bytes(data: &[u8]) -> Option<Self> {
        // Minimum size: 4 + 1 + 8 = 13 bytes header
        if data.len() < 13 {
            return None;
        }

        let num_bits = u32::from_le_bytes(data[0..4].try_into().ok()?) as usize;
        let num_hashes = data[4];
        let item_count = u64::from_le_bytes(data[5..13].try_into().ok()?) as usize;

        // Validate
        if num_bits == 0 || num_hashes == 0 {
            return None;
        }

        let num_words = num_bits.div_ceil(64);
        let expected_len = 13 + num_words * 8;

        if data.len() < expected_len {
            return None;
        }

        // Parse bit array
        let mut bits = Vec::with_capacity(num_words);
        for i in 0..num_words {
            let offset = 13 + i * 8;
            let word = u64::from_le_bytes(data[offset..offset + 8].try_into().ok()?);
            bits.push(word);
        }

        Some(Self {
            bits,
            num_hashes,
            num_bits,
            item_count,
        })
    }

    /// Computes two independent hash values for double hashing.
    ///
    /// Uses FNV-1a for both hashes with different seeds for independence.
    #[inline]
    fn hash_pair(&self, key: &[u8]) -> (u64, u64) {
        // FNV-1a hash with seed 0
        let h1 = fnv1a_hash(key, 0);
        // FNV-1a hash with seed 1 (different starting state)
        let h2 = fnv1a_hash(key, 0x517cc1b727220a95);

        (h1, h2)
    }

    /// Computes the bit index for hash function i using double hashing.
    ///
    /// Uses the formula: h(i) = (h1 + i * h2) mod m
    #[inline]
    fn get_bit_index(&self, h1: u64, h2: u64, i: u64) -> usize {
        (h1.wrapping_add(i.wrapping_mul(h2)) % self.num_bits as u64) as usize
    }
}

/// FNV-1a hash function.
///
/// A fast, non-cryptographic hash function suitable for hash tables
/// and bloom filters.
#[inline]
fn fnv1a_hash(data: &[u8], seed: u64) -> u64 {
    const FNV_OFFSET_BASIS: u64 = 0xcbf29ce484222325;
    const FNV_PRIME: u64 = 0x100000001b3;

    let mut hash = FNV_OFFSET_BASIS ^ seed;

    for &byte in data {
        hash ^= byte as u64;
        hash = hash.wrapping_mul(FNV_PRIME);
    }

    hash
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn test_bloom_filter_basic() {
        let mut filter = BloomFilter::new(1000, 0.01);

        // Insert keys
        filter.insert(b"hello");
        filter.insert(b"world");
        filter.insert(b"test");

        // Should definitely contain inserted keys
        assert!(filter.may_contain(b"hello"));
        assert!(filter.may_contain(b"world"));
        assert!(filter.may_contain(b"test"));

        // Likely doesn't contain (might have false positives)
        // But with only 3 items in a 1000-item filter, false positives are rare
        assert!(!filter.may_contain(b"not_inserted"));
        assert!(!filter.may_contain(b"another_key"));
    }

    #[test]
    fn test_bloom_filter_no_false_negatives() {
        let mut filter = BloomFilter::new(10000, 0.01);

        // Insert 10000 unique keys
        for i in 0..10000u32 {
            let key = format!("key_{i:06}");
            filter.insert(key.as_bytes());
        }

        // ALL inserted keys must be found (no false negatives)
        for i in 0..10000u32 {
            let key = format!("key_{i:06}");
            assert!(
                filter.may_contain(key.as_bytes()),
                "key {key} must be found"
            );
        }
    }

    #[test]
    fn test_bloom_filter_false_positive_rate() {
        let num_items = 10000;
        let target_fp_rate = 0.01;
        let mut filter = BloomFilter::new(num_items, target_fp_rate);

        // Insert items
        for i in 0..num_items {
            let key = format!("existing_{i:06}");
            filter.insert(key.as_bytes());
        }

        // Test false positive rate with non-existent keys
        let test_count = 100000;
        let mut false_positives = 0;

        for i in 0..test_count {
            let key = format!("nonexistent_{i:08}");
            if filter.may_contain(key.as_bytes()) {
                false_positives += 1;
            }
        }

        let actual_fp_rate = false_positives as f64 / test_count as f64;

        // Allow 3x target rate (bloom filters can vary)
        assert!(
            actual_fp_rate < target_fp_rate * 3.0,
            "false positive rate {actual_fp_rate:.4} is too high (target: {target_fp_rate})"
        );
    }

    #[test]
    fn test_bloom_filter_serialization() {
        let mut filter = BloomFilter::new(1000, 0.01);

        for i in 0..100 {
            filter.insert(format!("key_{i}").as_bytes());
        }

        // Serialize
        let bytes = filter.to_bytes();

        // Deserialize
        let restored = BloomFilter::from_bytes(&bytes).expect("deserialization should succeed");

        // Verify restored filter works the same
        for i in 0..100 {
            assert!(restored.may_contain(format!("key_{i}").as_bytes()));
        }

        assert_eq!(filter.num_bits, restored.num_bits);
        assert_eq!(filter.num_hashes, restored.num_hashes);
        assert_eq!(filter.item_count, restored.item_count);
    }

    #[test]
    fn test_bloom_filter_clear() {
        let mut filter = BloomFilter::new(100, 0.01);

        filter.insert(b"key1");
        filter.insert(b"key2");
        assert!(filter.may_contain(b"key1"));

        filter.clear();

        assert!(!filter.may_contain(b"key1"));
        assert!(!filter.may_contain(b"key2"));
        assert_eq!(filter.item_count(), 0);
    }

    #[test]
    fn test_bloom_filter_deserialization_invalid() {
        // Too short
        assert!(BloomFilter::from_bytes(&[0, 0, 0]).is_none());

        // Invalid num_bits (0)
        assert!(BloomFilter::from_bytes(&[0, 0, 0, 0, 1, 0, 0, 0, 0, 0, 0, 0, 0]).is_none());

        // Invalid num_hashes (0)
        assert!(BloomFilter::from_bytes(&[64, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0]).is_none());
    }
}
