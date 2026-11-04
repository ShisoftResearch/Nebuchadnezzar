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
//! file header (32 bytes)
//!   magic       [u8; 4]  b"NEBW"
//!   version     u8       FORMAT_VERSION
//!   reserved    [u8; 3]
//!   seq_id      u64      the segment incarnation this log belongs to
//!   chunk_id    u64      the chunk this log belongs to
//!   seg_id      u64      the segment this log belongs to
//!
//! record (20-byte header + payload), repeated
//!   magic       u32      RECORD_MAGIC
//!   seg_offset  u32      offset of the payload within the segment
//!   synced_len  u32      file bytes covered by a completed sync at this write
//!   len         u32      payload length
//!   crc         u32      CRC32C over seg_offset, synced_len, len and payload
//!   payload     [u8; len]
//! ```
//!
//! `synced_len` shares the 8 bytes that `seg_offset` alone needed: a segment
//! offset fits in 23 bits, so a record pays nothing for it.
//!
//! Two proofs separate a torn tail from damage at rest, and neither costs a
//! write. `docs/tla/WalDamage.tla` checks them.
//!
//! * A crash can tear only what no sync covered, so a bad record below the
//!   `synced_len` of any valid later record was durable.
//! * A tear leaves an absent sector, which reads as zeros in a fresh file, or
//!   the end of the file. A whole record with a bad CRC has neither, so its
//!   bytes changed after they were written. This rests on two facts of the
//!   disk: a sector is written whole, and a new block reads as zeros. ext4
//!   with `data=writeback` breaks the second fact, so a crash on it can stop
//!   a healthy start. XFS and ext4 `data=ordered` keep it.
//!
//! A zeroed sector in the final sync window has neither proof. It is the same
//! file that a crash leaves, so it still reads as a torn tail.
//!

use crc_fast::{CrcAlgorithm, Digest};
use std::io;

pub const FILE_MAGIC: [u8; 4] = *b"NEBW";
/// A file written with another layout must be REFUSED rather than read with
/// shifted offsets. There is no earlier format to stay compatible with, which
/// is what makes bumping free.
pub const FORMAT_VERSION: u8 = 4;
pub const FILE_HEADER_SIZE: usize = 32;

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

pub fn wal_file_header(chunk_id: usize, seg_id: u64, seq_id: u64) -> [u8; FILE_HEADER_SIZE] {
    let mut header = [0u8; FILE_HEADER_SIZE];
    header[..4].copy_from_slice(&FILE_MAGIC);
    header[4] = FORMAT_VERSION;
    header[8..16].copy_from_slice(&seq_id.to_le_bytes());
    header[16..24].copy_from_slice(&(chunk_id as u64).to_le_bytes());
    header[24..32].copy_from_slice(&seg_id.to_le_bytes());
    header
}

/// Frame one record. The payload is copied in, so the caller's buffer (a
/// live segment region) need not outlive the call.
///
/// `synced_len` is the file length that a completed sync covers now. It may
/// understate that length, which only loses a proof of damage. It must never
/// overstate it, or a crash reads as damage.
pub fn frame_record(seg_offset: u64, payload: &[u8], synced_len: u64) -> Vec<u8> {
    let len = payload.len() as u32;
    let offset_bytes = (seg_offset as u32).to_le_bytes();
    let len_bytes = len.to_le_bytes();
    let synced_bytes = (synced_len.min(u32::MAX as u64) as u32).to_le_bytes();
    let crc = crc32c(&[&offset_bytes, &synced_bytes, &len_bytes, payload]);

    let mut framed = Vec::with_capacity(RECORD_HEADER_SIZE + payload.len());
    framed.extend_from_slice(&RECORD_MAGIC.to_le_bytes());
    framed.extend_from_slice(&offset_bytes);
    framed.extend_from_slice(&synced_bytes);
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
    /// A bad record lies below the length that a later valid record says a
    /// sync covered. A crash cannot do this: the file changed at rest.
    Damaged {
        at_file_offset: usize,
        synced_len: u64,
        reason: String,
    },
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
    /// Chunk id recorded in the file header.
    pub chunk_id: u64,
    /// Segment id recorded in the file header.
    pub seg_id: u64,
    /// Image ranges that a damaged stretch of the file may have held. Only
    /// a [`TailStop::Damaged`] scan has them, and they stay unfilled.
    pub unreadable: Vec<std::ops::Range<usize>>,
}

/// A record that verifies, read from the bytes at `cursor`.
struct Frame<'a> {
    seg_offset: u64,
    synced_len: u64,
    payload: &'a [u8],
}

/// Read the record at `cursor`. An `Err` says why it is not a good record.
fn read_frame(data: &[u8], cursor: usize) -> Result<Frame<'_>, String> {
    if cursor + RECORD_HEADER_SIZE > data.len() {
        return Err(format!(
            "partial record header ({} of {RECORD_HEADER_SIZE} bytes)",
            data.len() - cursor
        ));
    }
    let magic = u32::from_le_bytes(data[cursor..cursor + 4].try_into().unwrap());
    if magic != RECORD_MAGIC {
        return Err(format!("bad record magic 0x{magic:08x}"));
    }
    let offset_bytes: [u8; 4] = data[cursor + 4..cursor + 8].try_into().unwrap();
    let synced_bytes: [u8; 4] = data[cursor + 8..cursor + 12].try_into().unwrap();
    let len_bytes: [u8; 4] = data[cursor + 12..cursor + 16].try_into().unwrap();
    let stored_crc = u32::from_le_bytes(data[cursor + 16..cursor + 20].try_into().unwrap());
    let len = u32::from_le_bytes(len_bytes) as usize;
    let payload_start = cursor + RECORD_HEADER_SIZE;
    if len == 0 || payload_start + len > data.len() {
        return Err(format!(
            "record claims {len} payload bytes, {} remain",
            data.len().saturating_sub(payload_start)
        ));
    }
    let payload = &data[payload_start..payload_start + len];
    let crc = crc32c(&[&offset_bytes, &synced_bytes, &len_bytes, payload]);
    if crc != stored_crc {
        return Err(format!(
            "CRC mismatch (stored 0x{stored_crc:08x}, computed 0x{crc:08x})"
        ));
    }
    Ok(Frame {
        seg_offset: u32::from_le_bytes(offset_bytes) as u64,
        synced_len: u32::from_le_bytes(synced_bytes) as u64,
        payload,
    })
}

/// Bytes a disk writes as one unit.
const SECTOR_BYTES: usize = 512;

/// Whether a sector's share of `range` is all zeros, as an absent sector is.
fn holds_absent_sector(data: &[u8], range: std::ops::Range<usize>) -> bool {
    let mut at = range.start;
    while at < range.end {
        let next = ((at / SECTOR_BYTES + 1) * SECTOR_BYTES).min(range.end);
        if data[at..next].iter().all(|byte| *byte == 0) {
            return true;
        }
        at = next;
    }
    false
}

/// Whether the bad record at `at` can be what a crash leaves.
///
/// When in doubt this says yes: that loses a proof of damage, and it never
/// stops a store that a crash alone left.
fn can_be_a_tear(data: &[u8], at: usize) -> bool {
    let header_end = at + RECORD_HEADER_SIZE;
    if header_end > data.len() {
        return true;
    }
    let magic = u32::from_le_bytes(data[at..at + 4].try_into().unwrap());
    if magic != RECORD_MAGIC {
        return holds_absent_sector(data, at..header_end);
    }
    let len = u32::from_le_bytes(data[at + 12..at + 16].try_into().unwrap()) as usize;
    let record_end = header_end.saturating_add(len);
    len == 0 || record_end > data.len() || holds_absent_sector(data, at..record_end)
}

/// Search for valid records after a bad one at `from`.
///
/// Record boundaries are lost at a bad record, so this tries every byte. The
/// magic and a CRC32C over the record must both match, so random bytes do not
/// pass. Each record is returned with its file offset.
fn later_frames(data: &[u8], from: usize) -> Vec<(usize, Frame<'_>)> {
    let mut frames = Vec::new();
    let mut cursor = from + 1;
    while cursor + RECORD_HEADER_SIZE <= data.len() {
        match read_frame(data, cursor) {
            Ok(frame) => {
                let next = cursor + RECORD_HEADER_SIZE + frame.payload.len();
                frames.push((cursor, frame));
                cursor = next;
            }
            Err(_) => cursor += 1,
        }
    }
    frames
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
    let chunk_id = u64::from_le_bytes(data[16..24].try_into().unwrap());
    let seg_id = u64::from_le_bytes(data[24..32].try_into().unwrap());

    let mut image = vec![0u8; segment_size];
    let mut used_len = 0usize;
    let mut records = 0usize;
    // Extent of every verified record, for the gap fill after the loop.
    let mut covered: Vec<(usize, usize)> = Vec::new();
    let mut place = |seg_offset: u64, payload: &[u8]| -> Result<(), String> {
        // A record naming a span outside the segment is corruption that
        // happens to check out -- refuse it rather than growing the image
        // to fit whatever the bytes claim.
        let start = seg_offset as usize;
        let end = start.saturating_add(payload.len());
        if end > segment_size {
            return Err(format!(
                "record spans [{seg_offset}, {end}) beyond the {segment_size}-byte segment"
            ));
        }
        image[start..end].copy_from_slice(payload);
        covered.push((start, end));
        used_len = used_len.max(end);
        records += 1;
        Ok(())
    };
    let mut cursor = FILE_HEADER_SIZE;
    let mut stop = loop {
        if cursor == data.len() {
            break TailStop::Clean;
        }
        let frame = match read_frame(data, cursor) {
            Ok(frame) => frame,
            Err(reason) => {
                break TailStop::Torn {
                    at_file_offset: cursor,
                    reason,
                }
            }
        };
        if let Err(reason) = place(frame.seg_offset, frame.payload) {
            break TailStop::Torn {
                at_file_offset: cursor,
                reason,
            };
        }
        cursor += RECORD_HEADER_SIZE + frame.payload.len();
    };

    // A bad record is a torn tail unless a later record proves it was synced.
    let mut damaged = false;
    if let TailStop::Torn {
        at_file_offset,
        reason,
    } = &stop
    {
        let at_file_offset = *at_file_offset;
        let frames = later_frames(data, at_file_offset);
        let proof = frames
            .iter()
            .map(|(_, frame)| frame.synced_len)
            .max()
            .filter(|len| *len > at_file_offset as u64);
        if proof.is_some() || !can_be_a_tear(data, at_file_offset) {
            stop = TailStop::Damaged {
                at_file_offset,
                synced_len: proof.unwrap_or(0),
                reason: reason.clone(),
            };
            damaged = true;
            // Later records are verified, and the repair may keep them.
            for (_, frame) in &frames {
                let _ = place(frame.seg_offset, frame.payload);
            }
        }
    }
    // The bad record held the last bytes of the image if no later one covers
    // them. Its own offset cannot be trusted, so one entry header past the end
    // stands for it: without this the repair finds no gap and reports no loss.
    let lost_tail = if damaged {
        let tail = used_len..used_len + crate::ram::entry::ENTRY_HEAD_SIZE;
        used_len = tail.end;
        Some(tail)
    } else {
        None
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
    //
    // After damage a gap can hide lost records, so it stays unfilled and is
    // reported: padding here would turn lost data into free space.
    let mut gaps = 0usize;
    let mut unreadable = Vec::new();
    covered.sort_unstable();
    let mut fill_from = 0usize;
    for &(start, end) in &covered {
        if start > fill_from {
            let span = start - fill_from;
            if damaged {
                unreadable.push(fill_from..start);
            } else if crate::ram::entry::stamp_padding(&mut image, fill_from, span) {
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

    unreadable.extend(lost_tail);
    Ok(ScanOutcome {
        image,
        used_len,
        records,
        gaps,
        stop,
        seq_id,
        chunk_id,
        seg_id,
        unreadable,
    })
}

#[cfg(test)]
mod tests {
    use super::*;

    const SEG: usize = 4096;

    /// Records of 16 payload bytes each, with the `synced_len` given for each.
    fn log_with_watermarks(synced: &[u64]) -> (Vec<u8>, Vec<usize>) {
        let mut file = wal_file_header(0, 0, 7).to_vec();
        let mut offsets = Vec::new();
        for (index, synced_len) in synced.iter().enumerate() {
            offsets.push(file.len());
            let at = index as u64 * 16;
            file.extend_from_slice(&frame_record(at, &[index as u8 + 1; 16], *synced_len));
        }
        (file, offsets)
    }

    #[test]
    fn a_bad_record_below_a_later_watermark_is_damage() {
        let (mut file, offsets) = log_with_watermarks(&[0, 0, 0, 0]);
        let synced = offsets[2] as u64;
        let (mut proved, _) = log_with_watermarks(&[0, 0, synced, synced]);
        // Zeros carry no shape proof, so only the watermark can decide.
        let bad = offsets[1]..offsets[2];
        proved[bad.clone()].fill(0);
        file[bad].fill(0);

        let outcome = scan_framed(&proved, SEG).unwrap();
        assert!(
            matches!(outcome.stop, TailStop::Damaged { at_file_offset, .. } if at_file_offset == offsets[1]),
            "{:?}",
            outcome.stop
        );
        assert_eq!(outcome.records, 3, "the verified records after it stay");
        assert_eq!(outcome.unreadable[0], 16..32, "the hole is not padded");

        // The same bad record with no proof is a torn tail, as after a crash.
        let outcome = scan_framed(&file, SEG).unwrap();
        assert!(matches!(outcome.stop, TailStop::Torn { .. }), "{:?}", outcome.stop);
        assert_eq!(outcome.records, 1);
    }

    #[test]
    fn a_watermark_at_the_bad_record_proves_nothing() {
        let offsets = log_with_watermarks(&[0, 0, 0]).1;
        let (mut file, _) = log_with_watermarks(&[0, 0, offsets[1] as u64]);
        file[offsets[1]..offsets[2]].fill(0);
        let outcome = scan_framed(&file, SEG).unwrap();
        assert!(matches!(outcome.stop, TailStop::Torn { .. }), "{:?}", outcome.stop);
    }

    #[test]
    fn a_later_record_is_found_past_a_zeroed_header() {
        let offsets = log_with_watermarks(&[0, 0, 0]).1;
        let (mut file, _) = log_with_watermarks(&[0, 0, offsets[2] as u64]);
        file[offsets[1]..offsets[1] + RECORD_HEADER_SIZE].fill(0);
        let outcome = scan_framed(&file, SEG).unwrap();
        assert!(matches!(outcome.stop, TailStop::Damaged { .. }), "{:?}", outcome.stop);
    }

    #[test]
    fn the_watermark_adds_no_bytes_to_a_record() {
        assert_eq!(RECORD_HEADER_SIZE, 20);
        assert_eq!(frame_record(0, b"payload!", 9).len(), RECORD_HEADER_SIZE + 8);
    }

    fn log_with(records: &[(u64, &[u8])]) -> Vec<u8> {
        let mut file = wal_file_header(0, 0, 7).to_vec();
        for (offset, payload) in records {
            file.extend_from_slice(&frame_record(*offset, payload, 0));
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
                | crate::ram::entry::TypeWord::Bare(entry_type) => {
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
        let mut file = wal_file_header(0, 0, 9).to_vec();
        file.extend_from_slice(&frame_record(0, b"aaaaaaaa", 0)); // [0,16)
        // A reserved [16,208) and died un-journaled.
        file.extend_from_slice(&frame_record(208, b"bbbbbbbb", 0)); // B, journaled early
        file.extend_from_slice(&frame_record(8, b"cccccccc", 0)); // C, journaled LATE
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
                | crate::ram::entry::TypeWord::Bare(t)
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
        let mut file = wal_file_header(0, 0, 11).to_vec();
        // Two records that abut exactly.
        file.extend_from_slice(&frame_record(0, b"aaaaaaaa", 0));
        file.extend_from_slice(&frame_record(8, b"bbbbbbbb", 0));
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
        // The whole record is there, so no crash left it.
        assert!(matches!(outcome.stop, TailStop::Damaged { .. }));
        assert!(outcome.image[64..73].iter().all(|byte| *byte == 0));
    }

    /// The repair sees only the image, so a lost last record must show there.
    #[test]
    fn a_damaged_last_record_is_reported_as_unreadable() {
        let mut file = log_with(&[(0, b"good"), (64, b"corrupted")]);
        let last = file.len() - 1;
        file[last] ^= 0xFF;
        let outcome = scan_framed(&file, SEG).unwrap();
        assert_eq!(outcome.unreadable.last(), Some(&(4..12)));
        assert_eq!(outcome.used_len, 12);
    }

    /// A torn tail stays torn when the record is cut, or when one sector of
    /// it is absent.
    #[test]
    fn the_shape_of_a_tear_is_not_damage() {
        let mut file = log_with(&[(0, b"good")]);
        let at = file.len();
        file.extend_from_slice(&frame_record(64, &[0x7Au8; 2048], 0));
        let whole = file.clone();

        file.truncate(at + RECORD_HEADER_SIZE + 100);
        assert!(matches!(scan_framed(&file, 8192).unwrap().stop, TailStop::Torn { .. }));

        let mut file = whole.clone();
        let sector = (at / SECTOR_BYTES + 2) * SECTOR_BYTES;
        file[sector..sector + SECTOR_BYTES].fill(0);
        assert!(matches!(scan_framed(&file, 8192).unwrap().stop, TailStop::Torn { .. }));

        let mut file = whole;
        file[sector + 7] ^= 0x01;
        assert!(matches!(scan_framed(&file, 8192).unwrap().stop, TailStop::Damaged { .. }));
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
        // Its CRC passes, so this is no tear.
        assert!(matches!(outcome.stop, TailStop::Damaged { .. }));
    }

    /// Legacy logs open with an EntryType discriminant (0, 1 or 2); the
    /// framed magic opens with b'N'. Neither can be mistaken for the other.
    #[test]
    fn unframed_buffers_are_not_framed_logs() {
        for entry_type in 0u32..=2 {
            let mut unframed = entry_type.to_le_bytes().to_vec();
            unframed.extend_from_slice(&64u32.to_le_bytes());
            unframed.extend_from_slice(&[0xAB; 64]);
            assert!(!is_framed(&unframed), "entry type {entry_type} looked framed");
        }
        assert!(is_framed(&log_with(&[(0, b"x")])));
    }

    #[test]
    fn an_empty_log_is_clean_and_declares_nothing_live() {
        let outcome = scan_framed(&wal_file_header(0, 0, 3), SEG).unwrap();
        assert_eq!(outcome.stop, TailStop::Clean);
        assert_eq!(outcome.records, 0);
        assert_eq!(outcome.used_len, 0);
    }
}
