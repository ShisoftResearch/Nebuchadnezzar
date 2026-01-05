use crc_fast::{CrcAlgorithm, Digest};
use lz4_flex::block::{compress_prepend_size, decompress_size_prepended};
use std::io;

/// Magic number to identify compressed backup files
/// ASCII: "NEB\x02" (Nebuchadnezzar compressed format with CRC32 checksum)
pub const COMPRESSION_MAGIC: [u8; 4] = [0x4E, 0x45, 0x42, 0x02];

/// Header size: magic (4) + crc32 (4)
const HEADER_SIZE: usize = 8;

/// Calculate CRC32C checksum of data using hardware-accelerated CRC32C (iSCSI)
#[inline]
fn crc32_checksum(data: &[u8]) -> u32 {
    let mut digest = Digest::new(CrcAlgorithm::Crc32Iscsi);
    digest.update(data);
    digest.finalize() as u32
}

/// Compress data using LZ4 block format with CRC32 checksum
///
/// Format: [MAGIC(4)] [CRC32(4)] [compressed_data_with_prepended_size]
///
/// The CRC32 checksum is calculated on the UNCOMPRESSED data to detect
/// any corruption that may occur during storage, compression, or decompression.
///
/// The lz4_flex compress_prepend_size function uses the unsafe performance profile
/// when default-features is disabled, which is optimal for non-streaming use cases.
pub fn compress(data: &[u8]) -> io::Result<Vec<u8>> {
    // Calculate CRC32 of uncompressed data
    let checksum = crc32_checksum(data);

    // Compress with prepended size (includes uncompressed size in the output)
    let compressed = compress_prepend_size(data);

    // Build final output: magic + crc32 + compressed data
    let mut output = Vec::with_capacity(HEADER_SIZE + compressed.len());
    output.extend_from_slice(&COMPRESSION_MAGIC);
    output.extend_from_slice(&checksum.to_le_bytes());
    output.extend_from_slice(&compressed);

    debug!(
        "Compressed {} bytes -> {} bytes (ratio: {:.2}%, crc32: 0x{:08X})",
        data.len(),
        output.len(),
        (output.len() as f64 / data.len() as f64) * 100.0,
        checksum
    );

    Ok(output)
}

/// Decompress data or return original if not compressed
///
/// This function auto-detects compression by checking for the magic number.
/// If the data is not compressed, it returns the original data.
///
/// It verifies the CRC32 checksum and PANICS if it doesn't match,
/// as this indicates data corruption that could lead to silent data loss.
pub fn decompress_if_compressed(data: &[u8]) -> io::Result<Vec<u8>> {
    // Check if data has compression magic
    if data.len() < HEADER_SIZE {
        // Too small to be compressed, return as-is
        return Ok(data.to_vec());
    }

    let magic = &data[..4];

    // Check for compressed format
    if magic == &COMPRESSION_MAGIC {
        // Extract stored checksum
        let stored_checksum = u32::from_le_bytes([data[4], data[5], data[6], data[7]]);

        // Extract compressed data (skip magic + checksum)
        let compressed_data = &data[HEADER_SIZE..];

        // Decompress using size-prepended format
        let decompressed = match decompress_size_prepended(compressed_data) {
            Ok(d) => d,
            Err(e) => {
                error!("LZ4 decompression failed: {:?}", e);
                return Err(io::Error::new(
                    io::ErrorKind::InvalidData,
                    format!("Failed to decompress data: {:?}", e),
                ));
            }
        };

        // Verify CRC32 checksum
        let computed_checksum = crc32_checksum(&decompressed);
        if computed_checksum != stored_checksum {
            // PANIC on checksum mismatch - this is unrecoverable data corruption
            panic!(
                "BACKUP DATA CORRUPTION DETECTED!\n\
                 CRC32 checksum mismatch during decompression:\n\
                   Stored checksum:   0x{:08X}\n\
                   Computed checksum: 0x{:08X}\n\
                   Decompressed size: {} bytes\n\
                 This indicates the backup file has been corrupted.\n\
                 Recovery cannot proceed safely - manual intervention required.",
                stored_checksum,
                computed_checksum,
                decompressed.len()
            );
        }

        debug!(
            "Decompressed {} bytes -> {} bytes (crc32 verified: 0x{:08X})",
            data.len(),
            decompressed.len(),
            computed_checksum
        );

        Ok(decompressed)
    } else {
        // Not compressed, return as-is
        debug!("Data not compressed (no magic header), returning as-is");
        Ok(data.to_vec())
    }
}

/// Check if data appears to be compressed (has valid magic header)
pub fn is_compressed(data: &[u8]) -> bool {
    if data.len() < 4 {
        return false;
    }
    &data[..4] == &COMPRESSION_MAGIC
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn test_crc32c() {
        // Test vector for CRC32C (iSCSI): "123456789" should produce 0xE3069283
        let test_data = b"123456789";
        let checksum = crc32_checksum(test_data);
        assert_eq!(checksum, 0xE3069283, "CRC32C test vector failed");
    }

    #[test]
    fn test_compress_decompress() {
        let original = b"Hello, World! This is a test string that should compress well.";

        let compressed = compress(original).unwrap();
        
        // Verify format
        assert_eq!(&compressed[..4], &COMPRESSION_MAGIC);
        assert!(compressed.len() > HEADER_SIZE);

        let decompressed = decompress_if_compressed(&compressed).unwrap();
        assert_eq!(original.as_slice(), decompressed.as_slice());
    }

    #[test]
    fn test_uncompressed_passthrough() {
        let original = b"Some uncompressed data without magic";

        let result = decompress_if_compressed(original).unwrap();
        assert_eq!(original.as_slice(), result.as_slice());
    }

    #[test]
    #[should_panic(expected = "BACKUP DATA CORRUPTION DETECTED")]
    fn test_checksum_mismatch_panics() {
        let original = b"Test data for checksum verification";
        let mut compressed = compress(original).unwrap();
        
        // Corrupt the checksum
        compressed[4] ^= 0xFF;

        // This should panic
        let _ = decompress_if_compressed(&compressed);
    }

    #[test]
    fn test_compression_ratio() {
        // Create highly compressible data
        let original = vec![0u8; 8 * 1024 * 1024]; // 8MB of zeros

        let compressed = compress(&original).unwrap();
        let ratio = (compressed.len() as f64 / original.len() as f64) * 100.0;

        println!("Compression ratio for 8MB zeros: {:.2}%", ratio);
        // Zeros should compress extremely well
        assert!(
            ratio < 1.0,
            "Expected <1% compression ratio, got {:.2}%",
            ratio
        );

        // Verify decompression
        let decompressed = decompress_if_compressed(&compressed).unwrap();
        assert_eq!(original.len(), decompressed.len());
        assert_eq!(original, decompressed);
    }

    #[test]
    fn test_segment_like_data() {
        // Simulate segment data with mixed content
        let mut segment_data = Vec::with_capacity(8 * 1024 * 1024);

        // Add some structured data (like entry headers)
        for i in 0u32..1000 {
            segment_data.extend_from_slice(&i.to_le_bytes());
            segment_data.extend_from_slice(&[0u8; 100]); // padding
        }

        // Add some random data
        for i in 0..1000 {
            segment_data.push((i % 256) as u8);
        }

        let original_size = segment_data.len();
        let compressed = compress(&segment_data).unwrap();
        let ratio = (compressed.len() as f64 / original_size as f64) * 100.0;

        println!("Compression ratio for segment-like data: {:.2}%", ratio);

        // Verify decompression with checksum
        let decompressed = decompress_if_compressed(&compressed).unwrap();
        assert_eq!(segment_data, decompressed);
    }

    #[test]
    fn test_format_detection() {
        let original = b"Test data";
        
        // Compressed format
        let compressed = compress(original).unwrap();
        assert!(is_compressed(&compressed));
        
        // Uncompressed
        assert!(!is_compressed(original));
    }
}
