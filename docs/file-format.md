# ThunderDB File Format Specification

**Version:** 1.0  
**Format Version:** 3  
**Status:** Internal  
**Copyright (c) YOAB. All rights reserved.**

---

## 1. Overview

ThunderDB uses a page-based file format with dual meta pages for crash recovery. The database file is organized into fixed-size pages, with the first two pages reserved for metadata. This document specifies the binary format, versioning scheme, and integrity guarantees.

## 2. Magic Numbers

### 2.1 Database File Magic

| Field | Value | Description |
|-------|-------|-------------|
| Magic | `0x54484E44` | ASCII "THND" (big-endian representation) |
| Bytes | `54 48 4E 44` | Stored as little-endian u32: `0x544E4854` on disk |

The magic number identifies a valid ThunderDB database file. It is stored in the first 4 bytes of each meta page.

### 2.2 WAL Segment Magic

| Field | Value | Description |
|-------|-------|-------------|
| Magic | `0x574C4F47` | ASCII "WLOG" |
| Bytes | `57 4C 4F 47` | Write-ahead log segment identifier |

WAL segment files use a separate magic number to distinguish them from database files.

## 3. Format Versioning

### 3.1 Version Scheme

ThunderDB uses a single integer version number stored in the meta page. The current version is **3**.

| Version | Description |
|---------|-------------|
| 1 | Initial release, 4KB pages |
| 2 | Added overflow page support |
| 3 | 32KB HPC page size, checkpoint fields |

### 3.2 Compatibility Rules

- **Forward Compatibility:** A database with `version <= current` can be opened.
- **Backward Compatibility:** Older versions cannot open newer format databases.
- **Validation:** `meta.version <= VERSION` must hold for a valid database.

```
file_version <= library_version  →  OK (can open)
file_version >  library_version  →  ERROR (incompatible)
```

### 3.3 WAL Version

WAL segments have an independent version number, currently **1**.

| Version | Description |
|---------|-------------|
| 1 | Initial WAL format with CRC32 records |

## 4. Page Size

### 4.1 Supported Page Sizes

ThunderDB supports configurable page sizes. The page size is stored in the meta page and must match the configured value when opening an existing database.

| Config | Size | Use Case |
|--------|------|----------|
| `Size4K` | 4,096 bytes | Legacy, small values |
| `Size8K` | 8,192 bytes | Balanced workloads |
| `Size16K` | 16,384 bytes | NVMe optimized |
| `Size32K` | 32,768 bytes | **Default**, HPC standard |
| `Size64K` | 65,536 bytes | High-throughput, large values |

### 4.2 Page Size Constraints

- Must be a power of two
- Must be one of the supported values (4K, 8K, 16K, 32K, 64K)
- Cannot be changed after database creation
- Meta pages always occupy the first two page slots

### 4.3 Page Size Selection

```rust
// Default page size
pub const PAGE_SIZE: usize = 32768;  // 32KB

// Valid page sizes (must be power of 2)
const VALID_SIZES: [u32; 5] = [4096, 8192, 16384, 32768, 65536];
```

## 5. File Layout

### 5.1 Database File Structure

```
┌─────────────────────────────────────────────────────────┐
│  Page 0: Meta Page A (primary)                         │
├─────────────────────────────────────────────────────────┤
│  Page 1: Meta Page B (backup)                          │
├─────────────────────────────────────────────────────────┤
│  Page 2+: Data Pages (B+ tree, freelist, overflow)     │
│    - Branch pages (internal nodes)                     │
│    - Leaf pages (key-value data)                       │
│    - Overflow pages (large values)                     │
│    - Freelist pages                                    │
└─────────────────────────────────────────────────────────┘
```

### 5.2 Meta Page Layout (88 bytes used)

```
Offset  Size  Field                  Description
──────  ────  ─────                  ───────────
0       4     magic                  Magic number (0x54484E44)
4       4     version                Format version (currently 3)
8       4     page_size              Page size in bytes
12      4     (reserved)             Padding for alignment
16      8     txid                   Transaction ID (monotonic)
24      8     root                   Root page ID of B+ tree
32      8     freelist               Freelist page ID
40      8     page_count             Total pages in file
48      8     (reserved)             Future use
56      8     checksum               FNV-1a checksum
64      8     checkpoint_lsn         WAL LSN at last checkpoint
72      8     checkpoint_timestamp   Unix timestamp of checkpoint
80      8     checkpoint_entry_count Entry count at checkpoint
88+     -     (padding to page_size) Zero-filled
```

### 5.3 Page Types

| Type | Value | Description |
|------|-------|-------------|
| Meta | 1 | Database metadata |
| Freelist | 2 | Free page tracking |
| Branch | 3 | B+ tree internal node |
| Leaf | 4 | B+ tree leaf node |
| Overflow | 5 | Large value storage |

## 6. Checksum Rules

### 6.1 Meta Page Checksum

Meta pages use FNV-1a (64-bit) for integrity verification.

**Algorithm:** FNV-1a 64-bit
```
FNV_OFFSET = 0xcbf29ce484222325
FNV_PRIME  = 0x0100000001b3

hash = FNV_OFFSET
for byte in data:
    hash ^= byte
    hash *= FNV_PRIME
return hash
```

**Coverage:** 
- Bytes 0-55 (before checksum field)
- Bytes 64-87 (checkpoint fields)
- Excludes bytes 56-63 (checksum field itself)

### 6.2 WAL Record Checksum

WAL records use CRC32 (IEEE polynomial) via `crc32fast` with SIMD acceleration.

**Algorithm:** CRC32-IEEE
**Coverage:** Record type byte + payload bytes
**Performance:** ~10 GB/s with SIMD

```
Record Layout:
[length: u32][type: u8][crc32: u32][payload...]

Checksum covers: type byte + payload
```

### 6.3 Overflow Page Checksum

Overflow pages use CRC32 for data integrity.

**Algorithm:** CRC32-IEEE  
**Coverage:** Data portion of overflow page  
**Optional:** Can be disabled with `no_checksum` feature for maximum performance

### 6.4 Checksum Bypass

The `no_checksum` feature flag disables data checksums for overflow pages when maximum performance is required and the filesystem is trusted for integrity.

```rust
#[cfg(not(feature = "no_checksum"))]
fn verify_checksum(data: &[u8], expected: u32) -> bool {
    crc32fast::hash(data) == expected
}

#[cfg(feature = "no_checksum")]
fn verify_checksum(_data: &[u8], _expected: u32) -> bool {
    true  // Trust filesystem
}
```

## 7. Endianness

### 7.1 Byte Order

**All multi-byte integers are stored in little-endian format.**

This applies to:
- Magic numbers (stored as LE u32)
- Version numbers
- Page IDs (u64)
- Checksums
- Length fields
- Transaction IDs

### 7.2 Rationale

Little-endian is chosen because:
1. x86/x86-64 and ARM (in LE mode) are the primary targets
2. Avoids byte-swapping overhead on common platforms
3. Consistent with Rust's `to_le_bytes()` / `from_le_bytes()`

### 7.3 Encoding Examples

```rust
// Writing a u64 page ID
let page_id: u64 = 0x0000000000000042;
let bytes = page_id.to_le_bytes();
// bytes = [0x42, 0x00, 0x00, 0x00, 0x00, 0x00, 0x00, 0x00]

// Reading a u32 magic number
let bytes = [0x44, 0x4E, 0x48, 0x54];  // "THND" reversed
let magic = u32::from_le_bytes(bytes);
// magic = 0x54484E44
```

## 8. WAL Segment Format

### 8.1 Segment Header (64 bytes)

```
Offset  Size  Field       Description
──────  ────  ─────       ───────────
0       4     magic       WAL magic (0x574C4F47)
4       4     version     WAL format version (1)
8       8     segment_id  Segment sequence number
16      8     first_lsn   First LSN in segment
24      40    (reserved)  Future use
```

### 8.2 Record Format

```
Offset  Size  Field       Description
──────  ────  ─────       ───────────
0       4     length      Total record size (including header)
4       1     type        Record type (1-6)
5       4     crc32       Checksum of type + payload
9       var   payload     Record-specific data
```

### 8.3 Record Types

| Type | Value | Payload |
|------|-------|---------|
| Put | 1 | key_len (u32) + key + value_len (u32) + value |
| Delete | 2 | key_len (u32) + key |
| TxBegin | 3 | txid (u64) |
| TxCommit | 4 | txid (u64) |
| TxAbort | 5 | txid (u64) |
| Checkpoint | 6 | lsn (u64) |

## 9. Overflow Page Format

### 9.1 Header (24 bytes)

```
Offset  Size  Field       Description
──────  ────  ─────       ───────────
0       1     page_type   Always 5 (Overflow)
1       7     (reserved)  Alignment padding
8       8     next_page   Next overflow page (0 = end)
16      4     data_len    Bytes of data in this page
20      4     checksum    CRC32 of data
```

### 9.2 Data Layout

```
[Header: 24 bytes][Data: up to (page_size - 24) bytes]
```

Large values exceeding `DEFAULT_OVERFLOW_THRESHOLD` (16KB) are split across multiple overflow pages linked by `next_page`.

## 10. Crash Recovery

### 10.1 Dual Meta Pages

Two meta pages provide atomic metadata updates:
1. The meta page with the higher `txid` is current
2. Updates write to the non-current page first
3. A successful fsync makes the update visible

### 10.2 Recovery Process

1. Read both meta pages
2. Validate magic, version, checksum
3. Select page with higher valid `txid`
4. Replay WAL from `checkpoint_lsn` if present
5. Verify B+ tree root is accessible

## 11. Limits

| Limit | Value | Notes |
|-------|-------|-------|
| Max key size | 64 KB | WAL record limit |
| Max value size | 64 MB | WAL record limit |
| Max inline value | 16 KB | Default overflow threshold |
| Max file size | ~8 EB | 64-bit page count × 64KB pages |
| Max pages | 2^64 | PageId is u64 |
| Max txid | 2^64 | Transaction ID is u64 |

---

## Appendix A: Constants Reference

```rust
// File identification
pub const MAGIC: u32 = 0x54_48_4E_44;      // "THND"
pub const VERSION: u32 = 3;

// Page sizes
pub const PAGE_SIZE: usize = 32768;         // Default 32KB

// WAL
const WAL_MAGIC: u32 = 0x574C_4F47;         // "WLOG"
const WAL_VERSION: u32 = 1;
const WAL_SEGMENT_SIZE: u64 = 64 * 1024 * 1024;  // 64MB

// Overflow
pub const DEFAULT_OVERFLOW_THRESHOLD: usize = 16 * 1024;  // 16KB
pub const OVERFLOW_HEADER_SIZE: usize = 24;

// Checksums
const FNV_OFFSET: u64 = 0xcbf29ce484222325;
const FNV_PRIME: u64 = 0x0100000001b3;
```

## Appendix B: Validation Pseudocode

```rust
fn validate_database(path: &Path) -> Result<()> {
    let file = open(path)?;
    
    // Read both meta pages
    let meta0 = read_meta(file, 0)?;
    let meta1 = read_meta(file, 1)?;
    
    // Validate magic
    assert!(meta0.magic == MAGIC || meta1.magic == MAGIC);
    
    // Validate version
    assert!(meta0.version <= VERSION || meta1.version <= VERSION);
    
    // Validate checksums
    let valid0 = verify_checksum(&meta0);
    let valid1 = verify_checksum(&meta1);
    assert!(valid0 || valid1);
    
    // Select current meta (highest valid txid)
    let current = match (valid0, valid1) {
        (true, true) => if meta0.txid > meta1.txid { meta0 } else { meta1 },
        (true, false) => meta0,
        (false, true) => meta1,
        (false, false) => return Err("No valid meta page"),
    };
    
    // Validate page size
    assert!(PageSizeConfig::is_valid(current.page_size));
    
    Ok(())
}
```

---

*This specification is for internal use. The format may change in future versions with appropriate version number increments.*
