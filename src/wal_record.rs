//! Summary: WAL record types and serialization with CRC32 checksums.
//! Copyright (c) YOAB. All rights reserved.

use crate::error::{Error, Result};

/// Record header layout:
/// - length: u32 (total record size including header)
/// - record_type: u8
/// - crc32: u32
pub const RECORD_HEADER_SIZE: usize = 9;

/// Maximum allowed record payload size (64MB).
/// Prevents DoS via oversized records.
const MAX_RECORD_PAYLOAD: usize = 64 * 1024 * 1024;

/// Maximum key size (64KB).
const MAX_KEY_SIZE: usize = 64 * 1024;

/// WAL record types.
#[repr(u8)]
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum RecordType {
    Put = 1,
    Delete = 2,
    TxBegin = 3,
    TxCommit = 4,
    TxAbort = 5,
    Checkpoint = 6,
}

impl RecordType {
    /// Converts a byte to RecordType.
    pub fn from_u8(v: u8) -> Option<Self> {
        match v {
            1 => Some(RecordType::Put),
            2 => Some(RecordType::Delete),
            3 => Some(RecordType::TxBegin),
            4 => Some(RecordType::TxCommit),
            5 => Some(RecordType::TxAbort),
            6 => Some(RecordType::Checkpoint),
            _ => None,
        }
    }
}

/// WAL record representing a single database operation.
///
/// All records are self-contained and include enough information
/// for replay during crash recovery.
#[derive(Debug, Clone, PartialEq, Eq)]
pub enum WalRecord {
    /// Insert or update a key-value pair.
    Put { key: Vec<u8>, value: Vec<u8> },
    /// Delete a key.
    Delete { key: Vec<u8> },
    /// Begin a transaction.
    TxBegin { txid: u64 },
    /// Commit a transaction (makes changes durable).
    TxCommit { txid: u64 },
    /// Abort a transaction (discard changes).
    TxAbort { txid: u64 },
    /// Checkpoint marker with LSN.
    Checkpoint { lsn: u64 },
}

impl WalRecord {
    /// Returns the record type byte for this record.
    pub fn record_type(&self) -> u8 {
        match self {
            WalRecord::Put { .. } => RecordType::Put as u8,
            WalRecord::Delete { .. } => RecordType::Delete as u8,
            WalRecord::TxBegin { .. } => RecordType::TxBegin as u8,
            WalRecord::TxCommit { .. } => RecordType::TxCommit as u8,
            WalRecord::TxAbort { .. } => RecordType::TxAbort as u8,
            WalRecord::Checkpoint { .. } => RecordType::Checkpoint as u8,
        }
    }

    /// Encodes the record to bytes with CRC32 checksum.
    ///
    /// Format:
    /// ```text
    /// [len: u32][type: u8][crc32: u32][payload...]
    /// ```
    ///
    /// The CRC32 covers the type byte and payload.
    pub fn encode(&self) -> Vec<u8> {
        let payload = self.encode_payload();
        let record_type = self.record_type();

        // Calculate total length
        let total_len = RECORD_HEADER_SIZE + payload.len();

        // Build the data to checksum (type + payload)
        let mut checksum_data = Vec::with_capacity(1 + payload.len());
        checksum_data.push(record_type);
        checksum_data.extend_from_slice(&payload);

        let crc = crc32_checksum(&checksum_data);

        // Build final record
        let mut result = Vec::with_capacity(total_len);
        result.extend_from_slice(&(total_len as u32).to_le_bytes());
        result.push(record_type);
        result.extend_from_slice(&crc.to_le_bytes());
        result.extend_from_slice(&payload);

        result
    }

    /// Decodes a record from bytes, validating CRC32.
    ///
    /// Returns the decoded record and number of bytes consumed.
    ///
    /// # Errors
    ///
    /// Returns error if:
    /// - Buffer too small for header
    /// - Invalid record type
    /// - CRC32 mismatch (corruption detected)
    /// - Payload validation fails
    pub fn decode(data: &[u8]) -> Result<(Self, usize)> {
        // Check minimum size
        if data.len() < RECORD_HEADER_SIZE {
            return Err(Error::WalRecordInvalid {
                lsn: 0,
                reason: format!(
                    "buffer too small: {} bytes, need at least {}",
                    data.len(),
                    RECORD_HEADER_SIZE
                ),
            });
        }

        // Parse header
        let total_len = u32::from_le_bytes(data[0..4].try_into().unwrap()) as usize;
        let record_type = data[4];
        let stored_crc = u32::from_le_bytes(data[5..9].try_into().unwrap());

        // Validate total length
        if total_len < RECORD_HEADER_SIZE {
            return Err(Error::WalRecordInvalid {
                lsn: 0,
                reason: format!("invalid record length: {total_len}"),
            });
        }

        if total_len > data.len() {
            return Err(Error::WalRecordInvalid {
                lsn: 0,
                reason: format!(
                    "record length {} exceeds buffer size {}",
                    total_len,
                    data.len()
                ),
            });
        }

        let payload_len = total_len - RECORD_HEADER_SIZE;
        if payload_len > MAX_RECORD_PAYLOAD {
            return Err(Error::WalRecordInvalid {
                lsn: 0,
                reason: format!(
                    "payload size {} exceeds maximum {}",
                    payload_len, MAX_RECORD_PAYLOAD
                ),
            });
        }

        // Validate record type
        let rtype = RecordType::from_u8(record_type).ok_or_else(|| Error::WalRecordInvalid {
            lsn: 0,
            reason: format!("invalid record type: {record_type}"),
        })?;

        // Extract payload
        let payload = &data[RECORD_HEADER_SIZE..total_len];

        // Verify CRC32
        let mut checksum_data = Vec::with_capacity(1 + payload.len());
        checksum_data.push(record_type);
        checksum_data.extend_from_slice(payload);
        let computed_crc = crc32_checksum(&checksum_data);

        if computed_crc != stored_crc {
            return Err(Error::WalRecordInvalid {
                lsn: 0,
                reason: format!(
                    "CRC mismatch: stored {stored_crc:#x}, computed {computed_crc:#x}"
                ),
            });
        }

        // Decode payload based on type
        let record = Self::decode_payload(rtype, payload)?;

        Ok((record, total_len))
    }

    /// Encodes the payload (without header).
    fn encode_payload(&self) -> Vec<u8> {
        match self {
            WalRecord::Put { key, value } => {
                let mut buf = Vec::with_capacity(8 + key.len() + value.len());
                buf.extend_from_slice(&(key.len() as u32).to_le_bytes());
                buf.extend_from_slice(key);
                buf.extend_from_slice(&(value.len() as u32).to_le_bytes());
                buf.extend_from_slice(value);
                buf
            }
            WalRecord::Delete { key } => {
                let mut buf = Vec::with_capacity(4 + key.len());
                buf.extend_from_slice(&(key.len() as u32).to_le_bytes());
                buf.extend_from_slice(key);
                buf
            }
            WalRecord::TxBegin { txid } | WalRecord::TxCommit { txid } | WalRecord::TxAbort { txid } => {
                txid.to_le_bytes().to_vec()
            }
            WalRecord::Checkpoint { lsn } => lsn.to_le_bytes().to_vec(),
        }
    }

    /// Decodes payload into a WalRecord.
    fn decode_payload(rtype: RecordType, payload: &[u8]) -> Result<Self> {
        match rtype {
            RecordType::Put => {
                if payload.len() < 8 {
                    return Err(Error::WalRecordInvalid {
                        lsn: 0,
                        reason: "Put payload too small".to_string(),
                    });
                }

                let key_len = u32::from_le_bytes(payload[0..4].try_into().unwrap()) as usize;
                if key_len > MAX_KEY_SIZE {
                    return Err(Error::WalRecordInvalid {
                        lsn: 0,
                        reason: format!("key size {} exceeds maximum {}", key_len, MAX_KEY_SIZE),
                    });
                }

                if payload.len() < 4 + key_len + 4 {
                    return Err(Error::WalRecordInvalid {
                        lsn: 0,
                        reason: "Put payload truncated".to_string(),
                    });
                }

                let key = payload[4..4 + key_len].to_vec();
                let value_offset = 4 + key_len;
                let value_len =
                    u32::from_le_bytes(payload[value_offset..value_offset + 4].try_into().unwrap())
                        as usize;

                if payload.len() < value_offset + 4 + value_len {
                    return Err(Error::WalRecordInvalid {
                        lsn: 0,
                        reason: "Put value truncated".to_string(),
                    });
                }

                let value = payload[value_offset + 4..value_offset + 4 + value_len].to_vec();
                Ok(WalRecord::Put { key, value })
            }
            RecordType::Delete => {
                if payload.len() < 4 {
                    return Err(Error::WalRecordInvalid {
                        lsn: 0,
                        reason: "Delete payload too small".to_string(),
                    });
                }

                let key_len = u32::from_le_bytes(payload[0..4].try_into().unwrap()) as usize;
                if key_len > MAX_KEY_SIZE {
                    return Err(Error::WalRecordInvalid {
                        lsn: 0,
                        reason: format!("key size {} exceeds maximum {}", key_len, MAX_KEY_SIZE),
                    });
                }

                if payload.len() < 4 + key_len {
                    return Err(Error::WalRecordInvalid {
                        lsn: 0,
                        reason: "Delete key truncated".to_string(),
                    });
                }

                let key = payload[4..4 + key_len].to_vec();
                Ok(WalRecord::Delete { key })
            }
            RecordType::TxBegin => {
                if payload.len() < 8 {
                    return Err(Error::WalRecordInvalid {
                        lsn: 0,
                        reason: "TxBegin payload too small".to_string(),
                    });
                }
                let txid = u64::from_le_bytes(payload[0..8].try_into().unwrap());
                Ok(WalRecord::TxBegin { txid })
            }
            RecordType::TxCommit => {
                if payload.len() < 8 {
                    return Err(Error::WalRecordInvalid {
                        lsn: 0,
                        reason: "TxCommit payload too small".to_string(),
                    });
                }
                let txid = u64::from_le_bytes(payload[0..8].try_into().unwrap());
                Ok(WalRecord::TxCommit { txid })
            }
            RecordType::TxAbort => {
                if payload.len() < 8 {
                    return Err(Error::WalRecordInvalid {
                        lsn: 0,
                        reason: "TxAbort payload too small".to_string(),
                    });
                }
                let txid = u64::from_le_bytes(payload[0..8].try_into().unwrap());
                Ok(WalRecord::TxAbort { txid })
            }
            RecordType::Checkpoint => {
                if payload.len() < 8 {
                    return Err(Error::WalRecordInvalid {
                        lsn: 0,
                        reason: "Checkpoint payload too small".to_string(),
                    });
                }
                let lsn = u64::from_le_bytes(payload[0..8].try_into().unwrap());
                Ok(WalRecord::Checkpoint { lsn })
            }
        }
    }
}

/// Computes CRC32 checksum using the standard polynomial (IEEE 802.3).
///
/// This is a simple table-based implementation for correctness.
/// For production, consider using `crc32fast` crate.
fn crc32_checksum(data: &[u8]) -> u32 {
    const CRC32_TABLE: [u32; 256] = generate_crc32_table();

    let mut crc = 0xFFFF_FFFFu32;
    for &byte in data {
        let idx = ((crc ^ u32::from(byte)) & 0xFF) as usize;
        crc = CRC32_TABLE[idx] ^ (crc >> 8);
    }
    !crc
}

/// Generates CRC32 lookup table at compile time.
const fn generate_crc32_table() -> [u32; 256] {
    const POLYNOMIAL: u32 = 0xEDB8_8320; // Reversed polynomial
    let mut table = [0u32; 256];
    let mut i = 0;
    while i < 256 {
        let mut crc = i as u32;
        let mut j = 0;
        while j < 8 {
            if crc & 1 != 0 {
                crc = (crc >> 1) ^ POLYNOMIAL;
            } else {
                crc >>= 1;
            }
            j += 1;
        }
        table[i] = crc;
        i += 1;
    }
    table
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn test_record_type_roundtrip() {
        for t in [
            RecordType::Put,
            RecordType::Delete,
            RecordType::TxBegin,
            RecordType::TxCommit,
            RecordType::TxAbort,
            RecordType::Checkpoint,
        ] {
            let byte = t as u8;
            let restored = RecordType::from_u8(byte).expect("should restore");
            assert_eq!(t, restored);
        }
    }

    #[test]
    fn test_invalid_record_type() {
        assert!(RecordType::from_u8(0).is_none());
        assert!(RecordType::from_u8(7).is_none());
        assert!(RecordType::from_u8(255).is_none());
    }

    #[test]
    fn test_put_record_roundtrip() {
        let record = WalRecord::Put {
            key: b"test_key".to_vec(),
            value: b"test_value".to_vec(),
        };

        let encoded = record.encode();
        let (decoded, consumed) = WalRecord::decode(&encoded).expect("decode");

        assert_eq!(consumed, encoded.len());
        assert_eq!(record, decoded);
    }

    #[test]
    fn test_delete_record_roundtrip() {
        let record = WalRecord::Delete {
            key: b"deleted_key".to_vec(),
        };

        let encoded = record.encode();
        let (decoded, _) = WalRecord::decode(&encoded).expect("decode");
        assert_eq!(record, decoded);
    }

    #[test]
    fn test_tx_records_roundtrip() {
        for record in [
            WalRecord::TxBegin { txid: 12345 },
            WalRecord::TxCommit { txid: 67890 },
            WalRecord::TxAbort { txid: 11111 },
        ] {
            let encoded = record.encode();
            let (decoded, _) = WalRecord::decode(&encoded).expect("decode");
            assert_eq!(record, decoded);
        }
    }

    #[test]
    fn test_checkpoint_record_roundtrip() {
        let record = WalRecord::Checkpoint {
            lsn: 0x1234_5678_9ABC_DEF0,
        };

        let encoded = record.encode();
        let (decoded, _) = WalRecord::decode(&encoded).expect("decode");
        assert_eq!(record, decoded);
    }

    #[test]
    fn test_crc_corruption_detected() {
        let record = WalRecord::Put {
            key: b"key".to_vec(),
            value: b"value".to_vec(),
        };

        let mut encoded = record.encode();

        // Corrupt a byte in the payload
        let corrupt_idx = RECORD_HEADER_SIZE + 2;
        if corrupt_idx < encoded.len() {
            encoded[corrupt_idx] ^= 0xFF;
        }

        let result = WalRecord::decode(&encoded);
        assert!(result.is_err(), "corrupted record should fail");
    }

    #[test]
    fn test_truncated_buffer_rejected() {
        let record = WalRecord::Put {
            key: b"key".to_vec(),
            value: b"value".to_vec(),
        };

        let encoded = record.encode();

        // Test various truncation points
        for len in 0..RECORD_HEADER_SIZE {
            let result = WalRecord::decode(&encoded[..len]);
            assert!(result.is_err(), "truncated at {len} should fail");
        }
    }

    #[test]
    fn test_large_record() {
        let record = WalRecord::Put {
            key: vec![0xAA; 1024],
            value: vec![0xBB; 65536],
        };

        let encoded = record.encode();
        let (decoded, _) = WalRecord::decode(&encoded).expect("decode large record");

        if let (WalRecord::Put { key: k1, value: v1 }, WalRecord::Put { key: k2, value: v2 }) =
            (&record, &decoded)
        {
            assert_eq!(k1, k2);
            assert_eq!(v1, v2);
        } else {
            panic!("type mismatch");
        }
    }

    #[test]
    fn test_empty_key_value() {
        let record = WalRecord::Put {
            key: vec![],
            value: vec![],
        };

        let encoded = record.encode();
        let (decoded, _) = WalRecord::decode(&encoded).expect("decode empty");
        assert_eq!(record, decoded);
    }

    #[test]
    fn test_crc32_consistency() {
        // Known test vector
        let data = b"123456789";
        let crc = crc32_checksum(data);
        // Standard CRC32 (IEEE) of "123456789" is 0xCBF43926
        assert_eq!(crc, 0xCBF4_3926);
    }
}
