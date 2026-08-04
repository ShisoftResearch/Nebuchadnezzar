use crc_fast::{CrcAlgorithm, Digest};
use lz4_flex::block::{compress_prepend_size, decompress_size_prepended};
use std::io;

/// Magic number to identify compressed backup files
/// ASCII: "NEB\x02" (Nebuchadnezzar compressed format with CRC32 checksum)
pub const COMPRESSION_MAGIC: [u8; 4] = [0x4E, 0x45, 0x42, 0x02];

/// Header size: magic (4) + crc32 (4)
const HEADER_SIZE: usize = 8;

/// ASCII "NEB\x03": block-indexed compressed format.
///
/// `NEB\x02` compresses a segment as one opaque blob, so the only entry point
/// is byte zero -- reading a single cell means decompressing all 8 MiB. That is
/// what forces a cold read to promote the whole segment, and with a working set
/// several times larger than the hot tier it is the dominant cost: a 1.7TB
/// import ran ~1374 promotions/s, materialising 8 MiB apiece to serve reads of
/// roughly a kilobyte.
///
/// This format compresses fixed spans of the *uncompressed* segment
/// independently and stores their file offsets, so the block holding a given
/// segment offset is `offset / block_size` -- arithmetic, no search -- and only
/// that block need be read and decompressed.
pub const BLOCK_COMPRESSION_MAGIC: [u8; 4] = [0x4E, 0x45, 0x42, 0x03];

/// Default uncompressed span covered by one block.
///
/// The trade is read amplification against compression ratio: LZ4 needs a
/// window to find matches, so smaller blocks compress worse, while larger ones
/// decompress more bytes than a single-cell read needs. 64 KiB is a starting
/// point, not a settled answer -- `NEB_BACKUP_BLOCK_SIZE` overrides it so the
/// curve can be measured on real segments.
pub const DEFAULT_BLOCK_SIZE: usize = 64 * 1024;

/// magic(4) + crc32(4) + block_size(4) + block_count(4)
const BLOCK_HEADER_SIZE: usize = 16;
/// Per-entry index: file_offset(u32) + compressed_len(u32)
const BLOCK_INDEX_ENTRY_SIZE: usize = 8;

/// Uncompressed span per block, overridable for measurement.
pub fn block_size() -> usize {
    std::env::var("NEB_BACKUP_BLOCK_SIZE")
        .ok()
        .and_then(|v| v.parse::<usize>().ok())
        .filter(|v| *v >= 4096 && v.is_power_of_two())
        .unwrap_or(DEFAULT_BLOCK_SIZE)
}

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

    // Block-indexed backups written by a newer server must still be readable in
    // full, so recovery and whole-segment promotion keep working unchanged.
    if data[..4] == BLOCK_COMPRESSION_MAGIC {
        return decompress_all_blocks(data);
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

/// Compress `data` as independently-decompressable blocks with an offset index.
///
/// Layout:
///   magic(4) | crc32(4) | block_size(4) | block_count(4)
///   index:  [file_offset(u32), compressed_len(u32)] * block_count
///   blocks: lz4(block 0), lz4(block 1), ...
///
/// The crc32 covers the whole uncompressed input, so a full read still verifies
/// end to end. Blocks carry their own length via `compress_prepend_size`, so a
/// single block can be decompressed without consulting anything but its index
/// entry.
pub fn compress_blocks(data: &[u8]) -> io::Result<Vec<u8>> {
    let bs = block_size();
    let block_count = data.len().div_ceil(bs);
    let checksum = crc32_checksum(data);

    let index_bytes = block_count * BLOCK_INDEX_ENTRY_SIZE;
    let mut blocks: Vec<Vec<u8>> = Vec::with_capacity(block_count);
    let mut index: Vec<u8> = Vec::with_capacity(index_bytes);

    // Offsets are absolute within the file so a reader can pread directly.
    let mut cursor = (BLOCK_HEADER_SIZE + index_bytes) as u32;
    for chunk in data.chunks(bs) {
        let compressed = compress_prepend_size(chunk);
        index.extend_from_slice(&cursor.to_le_bytes());
        index.extend_from_slice(&(compressed.len() as u32).to_le_bytes());
        cursor = cursor.saturating_add(compressed.len() as u32);
        blocks.push(compressed);
    }

    let total: usize = blocks.iter().map(|b| b.len()).sum();
    let mut out = Vec::with_capacity(BLOCK_HEADER_SIZE + index_bytes + total);
    out.extend_from_slice(&BLOCK_COMPRESSION_MAGIC);
    out.extend_from_slice(&checksum.to_le_bytes());
    out.extend_from_slice(&(bs as u32).to_le_bytes());
    out.extend_from_slice(&(block_count as u32).to_le_bytes());
    out.extend_from_slice(&index);
    for b in &blocks {
        out.extend_from_slice(b);
    }

    debug!(
        "Block-compressed {} bytes -> {} bytes in {} blocks of {} ({:.2}%)",
        data.len(),
        out.len(),
        block_count,
        bs,
        (out.len() as f64 / data.len().max(1) as f64) * 100.0
    );
    Ok(out)
}

/// Parsed header of a block-compressed buffer.
#[derive(Debug, Clone, Copy)]
pub struct BlockLayout {
    pub block_size: usize,
    pub block_count: usize,
    pub checksum: u32,
}

impl BlockLayout {
    /// Index entry for `block_idx`, as (file_offset, compressed_len).
    pub fn entry(&self, data: &[u8], block_idx: usize) -> io::Result<(usize, usize)> {
        if block_idx >= self.block_count {
            return Err(io::Error::new(
                io::ErrorKind::InvalidInput,
                format!("block {} out of range ({})", block_idx, self.block_count),
            ));
        }
        let at = BLOCK_HEADER_SIZE + block_idx * BLOCK_INDEX_ENTRY_SIZE;
        if data.len() < at + BLOCK_INDEX_ENTRY_SIZE {
            return Err(io::Error::new(
                io::ErrorKind::UnexpectedEof,
                "truncated block index",
            ));
        }
        let off = u32::from_le_bytes([data[at], data[at + 1], data[at + 2], data[at + 3]]) as usize;
        let len =
            u32::from_le_bytes([data[at + 4], data[at + 5], data[at + 6], data[at + 7]]) as usize;
        Ok((off, len))
    }

    /// Block holding uncompressed `offset`, and the offset within that block.
    #[inline]
    pub fn locate(&self, offset: usize) -> (usize, usize) {
        (offset / self.block_size, offset % self.block_size)
    }
}

/// Read the block layout, if `data` is in the block-indexed format.
pub fn block_layout(data: &[u8]) -> Option<BlockLayout> {
    if data.len() < BLOCK_HEADER_SIZE || data[..4] != BLOCK_COMPRESSION_MAGIC {
        return None;
    }
    Some(BlockLayout {
        checksum: u32::from_le_bytes([data[4], data[5], data[6], data[7]]),
        block_size: u32::from_le_bytes([data[8], data[9], data[10], data[11]]) as usize,
        block_count: u32::from_le_bytes([data[12], data[13], data[14], data[15]]) as usize,
    })
}

/// Decompress one block. This is the point of the format: serving a cold read
/// touches `block_size` bytes rather than the whole segment.
pub fn decompress_block(data: &[u8], block_idx: usize) -> io::Result<Vec<u8>> {
    let layout = block_layout(data).ok_or_else(|| {
        io::Error::new(
            io::ErrorKind::InvalidData,
            "not a block-compressed buffer",
        )
    })?;
    let (off, len) = layout.entry(data, block_idx)?;
    if data.len() < off + len {
        return Err(io::Error::new(
            io::ErrorKind::UnexpectedEof,
            format!("block {} extends past end of buffer", block_idx),
        ));
    }
    decompress_size_prepended(&data[off..off + len]).map_err(|e| {
        io::Error::new(
            io::ErrorKind::InvalidData,
            format!("failed to decompress block {}: {:?}", block_idx, e),
        )
    })
}

/// Decompress every block, verifying the whole-input checksum.
///
/// Used when a segment really is promoted in full; the per-block path is for
/// serving individual reads.
pub fn decompress_all_blocks(data: &[u8]) -> io::Result<Vec<u8>> {
    let layout = block_layout(data).ok_or_else(|| {
        io::Error::new(
            io::ErrorKind::InvalidData,
            "not a block-compressed buffer",
        )
    })?;
    let mut out = Vec::with_capacity(layout.block_count * layout.block_size);
    for i in 0..layout.block_count {
        out.extend_from_slice(&decompress_block(data, i)?);
    }
    let computed = crc32_checksum(&out);
    if computed != layout.checksum {
        panic!(
            "BACKUP DATA CORRUPTION DETECTED!\n\
             CRC32 mismatch on block-compressed backup:\n\
               Stored:   0x{:08X}\n\
               Computed: 0x{:08X}\n\
               Size:     {} bytes in {} blocks\n\
             Recovery cannot proceed safely.",
            layout.checksum,
            computed,
            out.len(),
            layout.block_count
        );
    }
    Ok(out)
}

/// Check if data appears to be compressed (has valid magic header)
pub fn is_compressed(data: &[u8]) -> bool {
    if data.len() < 4 {
        return false;
    }
    &data[..4] == &COMPRESSION_MAGIC || data[..4] == BLOCK_COMPRESSION_MAGIC
}

pub fn compress_field(data: &[u8]) -> io::Result<Vec<u8>> {
    let compressed = compress_prepend_size(data);

    debug!(
        "Field compress: {} bytes -> {} bytes (ratio: {:.2}%)",
        data.len(),
        compressed.len(),
        (compressed.len() as f64 / data.len() as f64) * 100.0
    );

    Ok(compressed)
}

pub fn decompress_field(compressed_data: &[u8]) -> io::Result<Vec<u8>> {
    match decompress_size_prepended(compressed_data) {
        Ok(decompressed) => {
            debug!(
                "Field decompress: {} bytes -> {} bytes",
                compressed_data.len(),
                decompressed.len()
            );
            Ok(decompressed)
        }
        Err(e) => {
            error!("Field LZ4 decompression failed: {:?}", e);
            Err(io::Error::new(
                io::ErrorKind::InvalidData,
                format!("Failed to decompress field data: {:?}", e),
            ))
        }
    }
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

    /// Segment-ish payload: repetitive structure with per-record variation, so
    /// compression has something to find but the blocks are not identical.
    fn segment_like(len: usize) -> Vec<u8> {
        let mut v = Vec::with_capacity(len);
        let mut i = 0u64;
        while v.len() < len {
            v.extend_from_slice(&i.to_le_bytes());
            v.extend_from_slice(b"{\"type\":\"item\",\"id\":\"Q");
            v.extend_from_slice(i.to_string().as_bytes());
            v.extend_from_slice(b"\",\"labels\":{\"en\":{\"language\":\"en\",\"value\":\"");
            v.extend_from_slice(format!("entity number {}", i).as_bytes());
            v.extend_from_slice(b"\"}}}");
            i += 1;
        }
        v.truncate(len);
        v
    }

    #[test]
    fn block_roundtrip_matches_input() {
        let data = segment_like(1 << 20);
        let packed = compress_blocks(&data).unwrap();
        assert!(is_compressed(&packed));
        assert_eq!(decompress_all_blocks(&packed).unwrap(), data);
        // The generic reader must handle it too, so recovery is unaffected.
        assert_eq!(decompress_if_compressed(&packed).unwrap(), data);
    }

    #[test]
    fn a_single_block_decompresses_without_touching_the_rest() {
        // The whole point of the format: serving one read must not materialise
        // the entire segment.
        let data = segment_like(1 << 20);
        let packed = compress_blocks(&data).unwrap();
        let layout = block_layout(&packed).expect("block layout");

        for idx in [0usize, 1, layout.block_count - 1] {
            let got = decompress_block(&packed, idx).unwrap();
            let start = idx * layout.block_size;
            let end = (start + layout.block_size).min(data.len());
            assert_eq!(got, &data[start..end], "block {idx} content");
        }
    }

    #[test]
    fn locate_maps_an_offset_to_its_block() {
        let data = segment_like(1 << 20);
        let packed = compress_blocks(&data).unwrap();
        let layout = block_layout(&packed).unwrap();

        let offset = layout.block_size * 3 + 17;
        let (idx, within) = layout.locate(offset);
        assert_eq!((idx, within), (3, 17));

        // And the bytes at that offset really are there.
        let block = decompress_block(&packed, idx).unwrap();
        assert_eq!(block[within], data[offset]);
    }

    #[test]
    fn block_size_is_configurable_for_measurement() {
        // Ratio versus read amplification is a curve to be measured on real
        // segments, so the knob has to be reachable without a rebuild.
        let data = segment_like(512 * 1024);
        let packed = compress_blocks(&data).unwrap();
        let layout = block_layout(&packed).unwrap();
        assert_eq!(layout.block_size, block_size());
        assert_eq!(layout.block_count, data.len().div_ceil(block_size()));
    }

    #[test]
    fn legacy_whole_blob_backups_still_read() {
        // Existing NEB\x02 backups on disk must not become unreadable.
        let data = segment_like(64 * 1024);
        let legacy = compress(&data).unwrap();
        assert_eq!(decompress_if_compressed(&legacy).unwrap(), data);
        assert!(block_layout(&legacy).is_none());
    }

    #[test]
    fn out_of_range_block_is_an_error_not_a_panic() {
        let packed = compress_blocks(&segment_like(64 * 1024)).unwrap();
        let layout = block_layout(&packed).unwrap();
        assert!(decompress_block(&packed, layout.block_count).is_err());
    }

    #[test]
    #[should_panic(expected = "BACKUP DATA CORRUPTION DETECTED")]
    fn corrupted_block_payload_is_caught_by_the_checksum() {
        let data = segment_like(256 * 1024);
        let mut packed = compress_blocks(&data).unwrap();
        let layout = block_layout(&packed).unwrap();
        let (off, _) = layout.entry(&packed, 1).unwrap();
        // Flip a byte inside a block's payload, past its length prefix.
        packed[off + 8] ^= 0xFF;
        let _ = decompress_all_blocks(&packed);
    }

    /// Prints the ratio-versus-amplification curve so block size can be chosen
    /// from data instead of taste. Ignored by default: it is a measurement, not
    /// an assertion. Run with:
    ///   cargo test --lib block_size_curve -- --ignored --nocapture
    #[test]
    #[ignore]
    fn block_size_curve() {
        let data = segment_like(8 * 1024 * 1024);
        let whole = compress(&data).unwrap().len();
        println!(
            "\nwhole-segment (NEB\\x02): {:.2}% of {} MiB\n",
            whole as f64 / data.len() as f64 * 100.0,
            data.len() >> 20
        );
        println!(
            "{:>10}  {:>10}  {:>8}  {:>12}  {:>10}",
            "block", "packed", "ratio", "vs whole", "read amp"
        );
        for bs in [4096usize, 8192, 16384, 32768, 65536, 131072, 262144, 524288] {
            std::env::set_var("NEB_BACKUP_BLOCK_SIZE", bs.to_string());
            let packed = compress_blocks(&data).unwrap();
            let ratio = packed.len() as f64 / data.len() as f64 * 100.0;
            // Bytes decompressed to serve one ~1 KiB cell read.
            let amp = bs as f64 / 1024.0;
            println!(
                "{:>10}  {:>10}  {:>7.2}%  {:>11.2}x  {:>9.0}x",
                bs,
                packed.len(),
                ratio,
                packed.len() as f64 / whole as f64,
                amp
            );
        }
        std::env::remove_var("NEB_BACKUP_BLOCK_SIZE");
    }

    #[test]
    fn test_field_decompress_corruption_returns_error() {
        let original = b"Field payload that should compress";
        let mut compressed = compress_field(original).unwrap();
        compressed.pop();
        assert!(decompress_field(&compressed).is_err());
    }
}
