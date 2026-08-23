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
    /// Abandoned-reservation gaps that were filled with PADDING entries.
    pub gaps: usize,
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
    // Extent of every verified record, for the gap fill after the loop.
    let mut covered: Vec<(usize, usize)> = Vec::new();
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
        let start = seg_offset as usize;
        image[start..end].copy_from_slice(payload);
        covered.push((start, end));
        used_len = used_len.max(end);
        records += 1;
        cursor = payload_start + len;
    };

    // Fill the gaps abandoned reservations left, AFTER every record is
    // placed -- and only then, because records arrive in JOURNAL order, not
    // offset order. Stamping gaps in-loop broke on exactly that: a slow
    // writer's low-offset record, journaled after a fast writer's
    // high-offset one, landed inside an already-stamped gap and overwrote
    // the padding header, leaving the rest of the gap as bare zeros that
    // stopped the forward walk and lost every entry after it.
    //
    // Working from the union of verified extents is order-independent: a
    // gap is a maximal uncovered span below `used_len`, whose bounds come
    // entirely from records that passed their CRC. Nothing inside the gap
    // is read, sized or trusted. Alignment makes every gap stampable:
    // entries are 8-byte aligned, so gaps are multiples of 8 and at least
    // ENTRY_HEAD_SIZE wide.
    let mut gaps = 0usize;
    covered.sort_unstable();
    let mut fill_from = 0usize;
    for &(start, end) in &covered {
        if start > fill_from {
            let span = start - fill_from;
            if crate::ram::entry::stamp_padding(&mut image, fill_from, span) {
                gaps += 1;
            } else {
                warn!(
                    "WAL rebuild found a {span}-byte gap at offset {fill_from} that cannot \
                     hold a padding header; a scan of this image will stop there"
                );
            }
        }
        fill_from = fill_from.max(end);
    }
    if gaps > 0 {
        info!(
            "WAL rebuild filled {} gap(s) left by abandoned reservations; the image is \
             contiguous, so the entries after them survive and the archive of this segment \
             carries no hole",
            gaps
        );
    }

    Ok(ScanOutcome {
        image,
        used_len,
        records,
        gaps,
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

    /// The abandoned-reservation case, which is fatal if left alone.
    ///
    /// `try_acquire` moves the append cursor before the entry bytes are
    /// written, so a crash in that window leaves zeros mid-image. A forward
    /// scan stops at zeros, and stopping in the MIDDLE loses every entry
    /// other writers durably appended after the gap -- unbounded loss from
    /// a one-entry window.
    #[test]
    fn a_gap_from_an_abandoned_reservation_is_filled_so_later_records_survive() {
        // Records at 0 and 256: the writer that reserved [8, 256) never got
        // to write, and the writer after it did.
        let file = log_with(&[(0, b"before the hole"), (256, b"after the hole")]);
        let outcome = scan_framed(&file, SEG).unwrap();

        assert_eq!(outcome.stop, TailStop::Clean);
        assert_eq!(outcome.records, 2);
        assert_eq!(outcome.gaps, 1, "the hole should have been filled");
        assert_eq!(&outcome.image[..15], b"before the hole");
        assert_eq!(&outcome.image[256..270], b"after the hole");

        // The point of the padding: a sequential walk must REACH the second
        // record instead of stopping at the zeros.
        let mut cursor = 15usize;
        // Round up to the padding header the filler placed.
        let mut hops = 0;
        let mut reached_second = false;
        while cursor + crate::ram::entry::ENTRY_HEAD_SIZE <= 256 && hops < 8 {
            let word = u32::from_le_bytes(outcome.image[cursor..cursor + 4].try_into().unwrap());
            let len =
                u32::from_le_bytes(outcome.image[cursor + 4..cursor + 8].try_into().unwrap());
            match crate::ram::entry::unpack_type_word(word) {
                crate::ram::entry::TypeWord::Checked(entry_type, _)
                | crate::ram::entry::TypeWord::Unchecked(entry_type) => {
                    assert_eq!(
                        entry_type,
                        crate::ram::entry::EntryType::PADDING,
                        "the gap should be spanned by padding, not garbage"
                    );
                    cursor += crate::ram::entry::ENTRY_HEAD_SIZE + len as usize;
                    if cursor == 256 {
                        reached_second = true;
                        break;
                    }
                }
                crate::ram::entry::TypeWord::Invalid => break,
            }
            hops += 1;
        }
        assert!(
            reached_second,
            "a scan must walk the padding and land exactly on the next record"
        );
    }

    /// The preemption scenario, with the journal order it actually produces.
    ///
    /// Records reach the WAL in JOURNAL order, not offset order: writer C
    /// reserves a low span, gets scheduled out, and journals AFTER writer B
    /// journaled a higher span. In-loop gap stamping breaks on this: when
    /// B's record arrives, the gap [C..B) is stamped as one padding entry
    /// headed at C's offset -- and when C's record then lands inside it, it
    /// overwrites that header, leaving the REST of the gap as bare zeros.
    /// A forward walk stops there and B is lost, exactly the loss the fill
    /// existed to prevent.
    #[test]
    fn out_of_order_journaling_still_leaves_a_walkable_image() {
        let mut file = wal_file_header(9).to_vec();
        file.extend_from_slice(&frame_record(0, b"aaaaaaaa")); // [0,16)
        // A reserved [16,208) and died un-journaled.
        file.extend_from_slice(&frame_record(208, b"bbbbbbbb")); // B, journaled early
        file.extend_from_slice(&frame_record(8, b"cccccccc")); // C, journaled LATE
        let outcome = scan_framed(&file, SEG).unwrap();

        assert_eq!(outcome.stop, TailStop::Clean);
        assert_eq!(outcome.records, 3);
        assert_eq!(&outcome.image[208..216], b"bbbbbbbb");

        // The payloads are raw test bytes, not entries; the walkability that
        // matters is the GAP's. Walk from the end of the covered prefix
        // ([0,16)) and require the padding to carry us exactly to B at 208.
        let mut cursor = 16usize;
        let mut reached = false;
        for _ in 0..16 {
            if cursor == 208 {
                reached = true;
                break;
            }
            if cursor > 208 {
                break;
            }
            let word =
                u32::from_le_bytes(outcome.image[cursor..cursor + 4].try_into().unwrap());
            let len =
                u32::from_le_bytes(outcome.image[cursor + 4..cursor + 8].try_into().unwrap());
            match crate::ram::entry::unpack_type_word(word) {
                crate::ram::entry::TypeWord::Checked(t, _)
                | crate::ram::entry::TypeWord::Unchecked(t)
                    if t == crate::ram::entry::EntryType::PADDING =>
                {
                    cursor += crate::ram::entry::ENTRY_HEAD_SIZE + len as usize;
                }
                _ => break, // zeros or garbage: the walk is dead
            }
        }
        assert!(
            reached,
            "padding must carry a forward walk exactly to the record at 208; it stopped \
             inside the gap instead"
        );
    }

    /// Padding must verify like any other entry, so nothing downstream has
    /// to make an exception for it.
    #[test]
    fn filled_padding_carries_a_valid_checksum() {
        let file = log_with(&[(0, b"first"), (128, b"second")]);
        let outcome = scan_framed(&file, SEG).unwrap();
        assert_eq!(outcome.gaps, 1);
        let pad_at = 5usize;
        let word = u32::from_le_bytes(outcome.image[pad_at..pad_at + 4].try_into().unwrap());
        match crate::ram::entry::unpack_type_word(word) {
            crate::ram::entry::TypeWord::Checked(entry_type, _) => {
                assert_eq!(entry_type, crate::ram::entry::EntryType::PADDING)
            }
            other => panic!("padding should be checksummed, got a different form: {:?}",
                matches!(other, crate::ram::entry::TypeWord::Invalid)),
        }
    }

    /// A log without gaps must not grow phantom padding.
    #[test]
    fn contiguous_records_produce_no_padding() {
        let mut file = wal_file_header(11).to_vec();
        // Two records that abut exactly.
        file.extend_from_slice(&frame_record(0, b"aaaaaaaa"));
        file.extend_from_slice(&frame_record(8, b"bbbbbbbb"));
        let outcome = scan_framed(&file, SEG).unwrap();
        assert_eq!(outcome.records, 2);
        assert_eq!(outcome.gaps, 0, "abutting records leave nothing to fill");
        assert_eq!(outcome.used_len, 16);
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
