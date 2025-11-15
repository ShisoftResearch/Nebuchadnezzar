use std::io;
use lz4_flex::block::{compress_prepend_size, decompress_size_prepended};

/// Magic number to identify compressed backup files
/// ASCII: "NEB\x01" (Nebuchadnezzar version 1 compression format)
const COMPRESSION_MAGIC: [u8; 4] = [0x4E, 0x45, 0x42, 0x01];

/// Compress data using LZ4 block format with unsafe performance profile
/// 
/// Format: [MAGIC(4)] [compressed_data_with_prepended_size]
/// 
/// The lz4_flex compress_prepend_size function uses the unsafe performance profile
/// when default-features is disabled, which is optimal for non-streaming use cases.
pub fn compress(data: &[u8]) -> io::Result<Vec<u8>> {
    // Compress with prepended size (includes uncompressed size in the output)
    let compressed = compress_prepend_size(data);
    
    // Build final output: magic + compressed data
    let mut output = Vec::with_capacity(COMPRESSION_MAGIC.len() + compressed.len());
    output.extend_from_slice(&COMPRESSION_MAGIC);
    output.extend_from_slice(&compressed);
    
    debug!(
        "Compressed {} bytes -> {} bytes (ratio: {:.2}%)",
        data.len(),
        output.len(),
        (output.len() as f64 / data.len() as f64) * 100.0
    );
    
    Ok(output)
}

/// Decompress data or return original if not compressed
/// 
/// This function auto-detects compression by checking for the magic number.
/// If the data is not compressed (legacy files), it returns the original data.
pub fn decompress_if_compressed(data: &[u8]) -> io::Result<Vec<u8>> {
    // Check if data has compression magic
    if data.len() < COMPRESSION_MAGIC.len() {
        // Too small to be compressed, return as-is
        return Ok(data.to_vec());
    }
    
    if &data[..COMPRESSION_MAGIC.len()] != &COMPRESSION_MAGIC {
        // Not compressed, return as-is (backward compatibility)
        debug!("Data not compressed (no magic header), returning as-is");
        return Ok(data.to_vec());
    }
    
    // Extract compressed data (skip magic header)
    let compressed_data = &data[COMPRESSION_MAGIC.len()..];
    
    // Decompress using size-prepended format
    match decompress_size_prepended(compressed_data) {
        Ok(decompressed) => {
            debug!(
                "Decompressed {} bytes -> {} bytes",
                data.len(),
                decompressed.len()
            );
            Ok(decompressed)
        }
        Err(e) => {
            error!("LZ4 decompression failed: {:?}", e);
            Err(io::Error::new(
                io::ErrorKind::InvalidData,
                format!("Failed to decompress data: {:?}", e),
            ))
        }
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn test_compress_decompress() {
        let original = b"Hello, World! This is a test string that should compress well.";
        
        let compressed = compress(original).unwrap();
        assert!(compressed.len() > COMPRESSION_MAGIC.len());
        
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
    fn test_compression_ratio() {
        // Create highly compressible data
        let original = vec![0u8; 8 * 1024 * 1024]; // 8MB of zeros
        
        let compressed = compress(&original).unwrap();
        let ratio = (compressed.len() as f64 / original.len() as f64) * 100.0;
        
        println!("Compression ratio for 8MB zeros: {:.2}%", ratio);
        // Zeros should compress extremely well
        assert!(ratio < 1.0, "Expected <1% compression ratio, got {:.2}%", ratio);
        
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
        
        // Verify decompression
        let decompressed = decompress_if_compressed(&compressed).unwrap();
        assert_eq!(segment_data, decompressed);
    }
}

