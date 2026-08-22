//! Framing for write-ahead log records.
//!
//! The WAL used to be a raw byte-for-byte copy of the segment's append
//! region: its only structure was that file offset equalled segment offset,
//! by construction. That made a torn record indistinguishable from a good
//! one. A record whose 8-byte entry header reached disk but whose body did
//! not -- the ordinary shape of a power cut under delayed allocation, where
//! the allocated-but-unwritten tail reads back as zeros -- replayed at
//! recovery as a perfectly well-formed cell with a zeroed header.
//!
//! Every record now carries its own frame: where it belongs in the segment,
//! how long it is, and a CRC32C over both. Recovery verifies each frame and
//! stops at the first one that does not check out, so a torn tail truncates
//! instead of being ingested. The segment offset is written down rather
//! than implied, which also makes the positional invariant checkable: a WAL
//! reopened in append mode against a rewound segment (the desync that
//! silently broke every post-restart write to a recovered head) now
//! declares the contradiction instead of hiding it.
//!
//! Layout:
//!
//! ```text
//! file header (16 bytes)
//!   magic       [u8; 4]  b"NEBW"
//!   version     u8       FORMAT_VERSION
//!   reserved    [u8; 3]
//!   seq_id      u64      the segment incarnation this log belongs to
//!
//! record (20-byte header + payload), repeated
//!   magic       u32      RECORD_MAGIC
//!   seg_offset  u64      offset of the payload within the segment
//!   len         u32      payload length
//!   crc         u32      CRC32C over seg_offset, len and payload
//!   payload     [u8; len]
//! ```
//!
//! Legacy (unframed) logs are still read: a file whose first byte is a
//! valid `EntryType` discriminant (0, 1 or 2) cannot be a framed log, since
//! the magic's first byte is `b'N'`. That check is exact, not heuristic, so
//! the two formats can never be confused for one another.

use crc_fast::{CrcAlgorithm, Digest};
use std::io;

pub const FILE_MAGIC: [u8; 4] = *b"NEBW";
pub const FORMAT_VERSION: u8 = 1;
pub const FILE_HEADER_SIZE: usize = 16;

pub const RECORD_MAGIC: u32 = 0x5741_4C52; // "WALR"
pub const RECORD_HEADER_SIZE: usize = 20;

#[inline]
fn crc32c(parts: &[&[u8]]) -> u32 {
    let mut digest = Digest::new(CrcAlgorithm::Crc32Iscsi);
    for part in parts {
        digest.update(part);
    }
    digest.finalize() as u32
}

/// True when the buffer opens with a framed-log file header.
pub fn is_framed(data: &[u8]) -> bool {
    data.len() >= FILE_HEADER_SIZE && data[..4] == FILE_MAGIC
}

pub fn wal_file_header(seq_id: u64) -> [u8; FILE_HEADER_SIZE] {
    let mut header = [0u8; FILE_HEADER_SIZE];
    header[..4].copy_from_slice(&FILE_MAGIC);
    header[4] = FORMAT_VERSION;
    header[8..16].copy_from_slice(&seq_id.to_le_bytes());
    header
}

/// Frame one record. The payload is copied in, so the caller's buffer (a
/// live segment region) need not outlive the call.
pub fn frame_record(seg_offset: u64, payload: &[u8]) -> Vec<u8> {
    let len = payload.len() as u32;
    let offset_bytes = seg_offset.to_le_bytes();
    let len_bytes = len.to_le_bytes();
    let crc = crc32c(&[&offset_bytes, &len_bytes, payload]);

    let mut framed = Vec::with_capacity(RECORD_HEADER_SIZE + payload.len());
    framed.extend_from_slice(&RECORD_MAGIC.to_le_bytes());
    framed.extend_from_slice(&offset_bytes);
    framed.extend_from_slice(&len_bytes);
    framed.extend_from_slice(&crc.to_le_bytes());
    framed.extend_from_slice(payload);
    framed
}

/// Why a scan stopped before the end of the file.
#[derive(Debug, Clone, PartialEq, Eq)]
pub enum TailStop {
    /// The file ended exactly on a record boundary: nothing was torn.
    Clean,
    /// A partial or corrupt record was found; everything before it is good.
    Torn { at_file_offset: usize, reason: String },
}

pub struct ScanOutcome {
    /// Segment image rebuilt from the verified records.
    pub image: Vec<u8>,
    /// Bytes of `image` covered by verified records: the segment's live
    /// extent, which unframed logs could never declare.
    pub used_len: usize,
    pub records: usize,
    pub stop: TailStop,
    /// Seq id recorded in the file header.
    pub seq_id: u64,
}

/// Rebuild a segment image from a framed log, verifying every record.
///
/// Records are placed at the offsets they name rather than at their file
/// position, which is what lets a torn record be dropped without shifting
/// everything after it. Scanning stops at the first bad frame: past that
/// point nothing can be trusted to be a record boundary, and guessing is
/// how a torn tail becomes a plausible-looking cell.
pub fn scan_framed(data: &[u8], segment_size: usize) -> io::Result<ScanOutcome> {
    if !is_framed(data) {
        return Err(io::Error::new(
            io::ErrorKind::InvalidData,
            "not a framed WAL file",
        ));
    }
    let version = data[4];
    if version != FORMAT_VERSION {
        return Err(io::Error::new(
            io::ErrorKind::InvalidData,
            format!("unsupported WAL format version {version}"),
        ));
    }
    let seq_id = u64::from_le_bytes(data[8..16].try_into().unwrap());

    let mut image = vec![0u8; segment_size];
    let mut used_len = 0usize;
    let mut records = 0usize;
    let mut cursor = FILE_HEADER_SIZE;
    let stop = loop {
        if cursor == data.len() {
            break TailStop::Clean;
        }
        if cursor + RECORD_HEADER_SIZE > data.len() {
            break TailStop::Torn {
                at_file_offset: cursor,
                reason: format!(
                    "partial record header ({} of {RECORD_HEADER_SIZE} bytes)",
                    data.len() - cursor
                ),
            };
        }
        let magic = u32::from_le_bytes(data[cursor..cursor + 4].try_into().unwrap());
        if magic != RECORD_MAGIC {
            break TailStop::Torn {
                at_file_offset: cursor,
                reason: format!("bad record magic 0x{magic:08x}"),
            };
        }
        let offset_bytes: [u8; 8] = data[cursor + 4..cursor + 12].try_into().unwrap();
        let len_bytes: [u8; 4] = data[cursor + 12..cursor + 16].try_into().unwrap();
        let stored_crc = u32::from_le_bytes(data[cursor + 16..cursor + 20].try_into().unwrap());
        let seg_offset = u64::from_le_bytes(offset_bytes);
        let len = u32::from_le_bytes(len_bytes) as usize;

        let payload_start = cursor + RECORD_HEADER_SIZE;
        if len == 0 || payload_start + len > data.len() {
            break TailStop::Torn {
                at_file_offset: cursor,
                reason: format!(
                    "record claims {len} payload bytes, {} remain",
                    data.len().saturating_sub(payload_start)
                ),
            };
        }
        let payload = &data[payload_start..payload_start + len];
        let crc = crc32c(&[&offset_bytes, &len_bytes, payload]);
        if crc != stored_crc {
            break TailStop::Torn {
                at_file_offset: cursor,
                reason: format!("CRC mismatch (stored 0x{stored_crc:08x}, computed 0x{crc:08x})"),
            };
        }
        // A record naming a span outside the segment is corruption that
        // happens to check out -- refuse it rather than growing the image
        // to fit whatever the bytes claim.
        let end = seg_offset as usize + len;
        if end > segment_size {
            break TailStop::Torn {
                at_file_offset: cursor,
                reason: format!(
                    "record spans [{seg_offset}, {end}) beyond the {segment_size}-byte segment"
                ),
            };
        }
        image[seg_offset as usize..end].copy_from_slice(payload);
        used_len = used_len.max(end);
        records += 1;
        cursor = payload_start + len;
    };

    Ok(ScanOutcome {
        image,
        used_len,
        records,
        stop,
        seq_id,
    })
}

#[cfg(test)]
mod tests {
    use super::*;

    const SEG: usize = 4096;

    fn log_with(records: &[(u64, &[u8])]) -> Vec<u8> {
        let mut file = wal_file_header(7).to_vec();
        for (offset, payload) in records {
            file.extend_from_slice(&frame_record(*offset, payload));
        }
        file
    }

    #[test]
    fn a_clean_log_rebuilds_every_record_in_place() {
        let file = log_with(&[(0, b"first"), (64, b"second"), (128, b"third")]);
        let outcome = scan_framed(&file, SEG).unwrap();
        assert_eq!(outcome.stop, TailStop::Clean);
        assert_eq!(outcome.records, 3);
        assert_eq!(outcome.seq_id, 7);
        assert_eq!(&outcome.image[..5], b"first");
        assert_eq!(&outcome.image[64..70], b"second");
        assert_eq!(&outcome.image[128..133], b"third");
        assert_eq!(outcome.used_len, 133);
    }

    /// The shape this format exists for: the header of the last record
    /// reached disk, its body did not. Unframed, the zeros read back as a
    /// well-formed entry.
    #[test]
    fn a_torn_body_truncates_and_keeps_everything_before_it() {
        let mut file = log_with(&[(0, b"durable"), (64, b"torn-away")]);
        file.truncate(file.len() - 4);
        let outcome = scan_framed(&file, SEG).unwrap();
        assert_eq!(outcome.records, 1);
        assert_eq!(&outcome.image[..7], b"durable");
        assert_eq!(outcome.used_len, 7);
        assert!(outcome.image[64..73].iter().all(|byte| *byte == 0));
        match outcome.stop {
            TailStop::Torn { at_file_offset, .. } => {
                assert_eq!(at_file_offset, FILE_HEADER_SIZE + RECORD_HEADER_SIZE + 7)
            }
            other => panic!("expected a torn tail, got {other:?}"),
        }
    }

    #[test]
    fn a_flipped_payload_byte_is_refused_not_replayed() {
        let mut file = log_with(&[(0, b"good"), (64, b"corrupted")]);
        let last = file.len() - 1;
        file[last] ^= 0xFF;
        let outcome = scan_framed(&file, SEG).unwrap();
        assert_eq!(outcome.records, 1);
        assert!(matches!(outcome.stop, TailStop::Torn { .. }));
        assert!(outcome.image[64..73].iter().all(|byte| *byte == 0));
    }

    /// Zeros are the exact byte pattern a delayed-allocation tail reads
    /// back as, and the one an unframed log cannot tell from padding.
    #[test]
    fn a_zero_filled_tail_stops_the_scan() {
        let mut file = log_with(&[(0, b"durable")]);
        file.extend_from_slice(&[0u8; 64]);
        let outcome = scan_framed(&file, SEG).unwrap();
        assert_eq!(outcome.records, 1);
        assert!(matches!(outcome.stop, TailStop::Torn { .. }));
    }

    #[test]
    fn a_record_spanning_past_the_segment_is_refused() {
        let file = log_with(&[(SEG as u64 - 2, b"overrun")]);
        let outcome = scan_framed(&file, SEG).unwrap();
        assert_eq!(outcome.records, 0);
        assert!(matches!(outcome.stop, TailStop::Torn { .. }));
    }

    /// Legacy logs open with an EntryType discriminant (0, 1 or 2); the
    /// framed magic opens with b'N'. Neither can be mistaken for the other.
    #[test]
    fn legacy_logs_are_never_mistaken_for_framed_ones() {
        for entry_type in 0u32..=2 {
            let mut legacy = entry_type.to_le_bytes().to_vec();
            legacy.extend_from_slice(&64u32.to_le_bytes());
            legacy.extend_from_slice(&[0xAB; 64]);
            assert!(!is_framed(&legacy), "entry type {entry_type} looked framed");
        }
        assert!(is_framed(&log_with(&[(0, b"x")])));
    }

    #[test]
    fn an_empty_log_is_clean_and_declares_nothing_live() {
        let outcome = scan_framed(&wal_file_header(3), SEG).unwrap();
        assert_eq!(outcome.stop, TailStop::Clean);
        assert_eq!(outcome.records, 0);
        assert_eq!(outcome.used_len, 0);
    }
}
