use std::panic;
use std::{io::Cursor, mem};

use crate::ram::cell::CellHeader;
use crate::ram::tombstone::Tombstone;
use byteorder::{ReadBytesExt, WriteBytesExt};

use super::mem_cursor::{release_cursor, Endian};

#[derive(Copy, Clone, Debug, PartialEq, Eq, Default)]
#[repr(u32)]
pub enum EntryType {
    #[default]
    UNDECIDED = 0,
    CELL = 1,
    TOMBSTONE = 2,
}

impl EntryType {
    pub fn from_bits(bits: u32) -> Option<Self> {
        match bits {
            0 => Some(Self::UNDECIDED),
            1 => Some(Self::CELL),
            2 => Some(Self::TOMBSTONE),
            _ => None,
        }
    }

    pub fn bits(self) -> u32 {
        self as u32
    }
}

pub const ENTRY_HEAD_SIZE: usize = mem::size_of::<u64>();

/// The entry header's type word carries a checksum of the entry's content in
/// its upper bits.
///
/// There is no room in `CellHeader` -- it is exactly 24 bytes with no
/// padding -- and widening it would cost 8 bytes per cell after alignment,
/// which is tens of gigabytes at billion-cell scale. The type word, on the
/// other hand, is almost entirely unused: it holds 0, 1 or 2, so 30 of its
/// 32 bits are spare. Eight bits are kept for the type (leaving room for
/// 253 more) and the upper 24 carry the checksum, so the entry gains
/// integrity at zero bytes per cell.
///
/// This is defence in depth, not the primary line: the WAL and the backup
/// carry their own CRCs over what reaches disk. What this catches is
/// everything BETWEEN those checks -- an entry relocated by the cleaner's
/// raw memcpy (which is journaled nowhere), a cold segment whose pages read
/// back wrong, a stray write into resident memory -- none of which any
/// file-level checksum can see.
const ENTRY_TYPE_BITS: u32 = 8;
const ENTRY_TYPE_MASK: u32 = (1 << ENTRY_TYPE_BITS) - 1;
const ENTRY_CHECKSUM_MASK: u32 = !ENTRY_TYPE_MASK;

/// Checksum of `content_len` bytes at `content_pos`, folded to 24 bits and
/// never zero: zero means "this entry predates checksums" (see
/// [`unpack_type_word`]), so a computed zero is mapped to one rather than
/// being mistaken for an unchecked entry.
fn content_checksum(content_pos: usize, content_len: u32) -> u32 {
    let bytes = unsafe { std::slice::from_raw_parts(content_pos as *const u8, content_len as usize) };
    let mut digest = crc_fast::Digest::new(crc_fast::CrcAlgorithm::Crc32Iscsi);
    digest.update(bytes);
    let folded = (digest.finalize() as u32) >> ENTRY_TYPE_BITS;
    if folded == 0 {
        1
    } else {
        folded
    }
}

/// What a type word says about its entry.
pub enum TypeWord {
    /// Written before checksums existed: the word is the bare type, which is
    /// exactly as discriminating as it always was (only 0, 1 and 2 are
    /// valid out of 2^32).
    Unchecked(EntryType),
    /// Type plus the checksum the writer recorded.
    Checked(EntryType, u32),
    /// Not a valid entry header.
    Invalid,
}

/// Split a type word into its type and checksum.
///
/// A bare 0, 1 or 2 is a legacy entry and stays unchecked -- treating its
/// zero checksum field as a real checksum would reject every entry ever
/// written by an older build. Anything else must carry a valid type in its
/// low bits AND a non-zero checksum, so the pair is exactly as unlikely to
/// arise from garbage as the old 32-bit check was.
pub fn unpack_type_word(word: u32) -> TypeWord {
    if let Some(entry_type) = EntryType::from_bits(word) {
        return TypeWord::Unchecked(entry_type);
    }
    let checksum = (word & ENTRY_CHECKSUM_MASK) >> ENTRY_TYPE_BITS;
    match EntryType::from_bits(word & ENTRY_TYPE_MASK) {
        Some(entry_type) if checksum != 0 => TypeWord::Checked(entry_type, checksum),
        _ => TypeWord::Invalid,
    }
}

fn pack_type_word(entry_type: EntryType, checksum: u32) -> u32 {
    (checksum << ENTRY_TYPE_BITS) | entry_type.bits()
}

/// Verify the entry at `pos`, if it carries a checksum.
///
/// `Some(false)` means the content does not match what the writer recorded;
/// `Some(true)` that it does; `None` that the entry predates checksums and
/// nothing can be said either way.
pub fn verify_entry_at(pos: usize) -> Option<bool> {
    if pos % 8 != 0 {
        return Some(false);
    }
    let (word, content_length) = unsafe {
        let head = std::slice::from_raw_parts(pos as *const u8, ENTRY_HEAD_SIZE);
        (
            u32::from_le_bytes(head[..4].try_into().unwrap()),
            u32::from_le_bytes(head[4..8].try_into().unwrap()),
        )
    };
    match unpack_type_word(word) {
        TypeWord::Unchecked(_) => None,
        TypeWord::Invalid => Some(false),
        TypeWord::Checked(_, checksum) => {
            Some(checksum == content_checksum(pos + ENTRY_HEAD_SIZE, content_length))
        }
    }
}

#[derive(Copy, Clone, Debug, Default)]
pub struct EntryHeader {
    pub entry_type: EntryType,
    pub content_length: u32,
}

#[derive(Clone)]
pub struct EntryMeta {
    pub body_pos: usize,
    pub entry_pos: usize,
    pub entry_size: usize,
    pub entry_header: EntryHeader,
}

#[derive(Debug)]
pub enum EntryContent {
    Cell(CellHeader),
    Tombstone(Tombstone),
    Undecided,
}

pub struct Entry {
    pub meta: EntryMeta,
    pub content: EntryContent,
}

impl Entry {
    pub fn encode_to<W>(mut pos: usize, entry_type: EntryType, content_len: u32, write_content: W)
    where
        W: Fn(usize),
    {
        let head_pos = pos;
        let mut cursor = Cursor::new(unsafe { Box::from_raw(pos as *mut [u8; 8] as *mut [u8]) });
        cursor.write_u32::<Endian>(entry_type.bits()).unwrap();
        cursor.write_u32::<Endian>(content_len).unwrap();
        pos += ENTRY_HEAD_SIZE;
        write_content(pos);
        release_cursor(cursor);
        // The checksum can only be taken once the content is there, so the
        // type word is stamped a second time. Until that store lands the
        // entry reads as an ordinary unchecked one, which is what a reader
        // racing this write should see: the allocator has not published the
        // entry yet, and an entry that is torn HERE is torn in memory, where
        // no checksum was ever going to survive it either.
        let checksum = content_checksum(pos, content_len);
        let word = pack_type_word(entry_type, checksum);
        unsafe {
            std::ptr::copy_nonoverlapping(
                word.to_le_bytes().as_ptr(),
                head_pos as *mut u8,
                mem::size_of::<u32>(),
            );
        }
    }

    pub fn content_pos(pos: usize) -> usize {
        pos + ENTRY_HEAD_SIZE
    }

    /// Decode an entry header, or `None` when the bytes at `pos` are not a
    /// valid entry.
    ///
    /// For callers that read raw addresses WITHOUT holding cell locks or
    /// segment references -- the statistics scan is one, by design ("slightly
    /// stale data is acceptable") -- garbage is an expected outcome, not
    /// corruption: the address may point into a segment that went cold (its
    /// pages read back zeroed), was combined away, or is mid-write. Such
    /// callers must skip, not panic; `decode_from` panicking inside a rayon
    /// worker took down a whole server when a recovered store's statistics
    /// refresh walked into a cold segment.
    pub fn try_decode_from<R, RR>(pos: usize, content_read: R) -> Option<(EntryHeader, RR)>
    where
        R: Fn(usize, EntryHeader) -> RR,
    {
        if pos % 8 != 0 {
            return None;
        }
        let mut cursor = Cursor::new(unsafe { Box::from_raw(pos as *mut [u8; 8] as *mut [u8]) });
        let entry_type_bits = cursor.read_u32::<Endian>().unwrap();
        let content_length = cursor.read_u32::<Endian>().unwrap();
        release_cursor(cursor);
        let entry_type = match unpack_type_word(entry_type_bits) {
            TypeWord::Unchecked(entry_type) | TypeWord::Checked(entry_type, _) => entry_type,
            TypeWord::Invalid => return None,
        };
        let entry = EntryHeader {
            entry_type,
            content_length,
        };
        Some((entry, content_read(Self::content_pos(pos), entry)))
    }

    // Returns the entry header reader returns
    pub fn decode_from<R, RR>(mut pos: usize, content_read: R) -> (EntryHeader, RR)
    where
        R: Fn(usize, EntryHeader) -> RR,
    {
        // Validate address alignment before unsafe operations
        #[cfg(debug_assertions)]
        if pos % 8 != 0 {
            panic!(
                "Cannot decode entry header: address 0x{:016x} is misaligned (offset: {}). \
                This indicates memory corruption in the cell address storage. \
                Valid entry addresses must be 8-byte aligned.",
                pos,
                pos % 8
            );
        }

        let mut cursor = Cursor::new(unsafe { Box::from_raw(pos as *mut [u8; 8] as *mut [u8]) });
        let entry_type_bits = cursor.read_u32::<Endian>().unwrap();
        let content_length = cursor.read_u32::<Endian>().unwrap();
        if let TypeWord::Unchecked(entry_type) | TypeWord::Checked(entry_type, _) =
            unpack_type_word(entry_type_bits)
        {
            let entry = EntryHeader {
                entry_type,
                content_length,
            };
            pos = Self::content_pos(pos);
            release_cursor(cursor);
            (entry, content_read(pos, entry))
        } else {
            let segment_info = crate::ram::chunk::chunk_and_segment_from_addr(pos)
                .map(|(chunk_id, segment_id)| format!("chunk={}, segment={}", chunk_id, segment_id))
                .unwrap_or_else(|| "unknown (address not in allocated range)".to_string());

            panic!(
                "Cannot decode entry header: invalid entry_type_bits={} (0x{:08x}) at address 0x{:016x} ({}). \
                Valid types are: UNDECIDED(0), CELL(1), TOMBSTONE(2). \
                This likely indicates memory corruption or reading from an invalid address.",
                entry_type_bits, entry_type_bits, pos, segment_info
            );
        }
    }
}

impl EntryContent {
    pub fn as_cell_header(&self) -> &CellHeader {
        if let EntryContent::Cell(ref header) = self {
            return header;
        } else {
            panic!("entry not header");
        }
    }

    pub fn as_tombstone(&self) -> &Tombstone {
        if let EntryContent::Tombstone(ref tombstone) = self {
            return tombstone;
        } else {
            panic!("entry not tombstone");
        }
    }
}

#[cfg(test)]
mod checksum_tests {
    use super::*;

    /// Backwards compatibility is the whole reason the type word is split
    /// the way it is: every entry ever written by an older build carries a
    /// bare 0, 1 or 2 and must keep decoding.
    #[test]
    fn a_legacy_type_word_still_decodes_and_claims_no_checksum() {
        for (bits, expected) in [
            (0u32, EntryType::UNDECIDED),
            (1, EntryType::CELL),
            (2, EntryType::TOMBSTONE),
        ] {
            match unpack_type_word(bits) {
                TypeWord::Unchecked(entry_type) => assert_eq!(entry_type, expected),
                _ => panic!("legacy word {bits} must decode as unchecked"),
            }
        }
    }

    #[test]
    fn a_packed_word_round_trips_type_and_checksum() {
        for checksum in [1u32, 42, 0x00FF_FFFF] {
            let word = pack_type_word(EntryType::CELL, checksum);
            match unpack_type_word(word) {
                TypeWord::Checked(entry_type, found) => {
                    assert_eq!(entry_type, EntryType::CELL);
                    assert_eq!(found, checksum);
                }
                _ => panic!("packed word {word:#x} should carry a checksum"),
            }
        }
    }

    /// The header-level discrimination against garbage must not weaken: a
    /// random word is still overwhelmingly likely to be rejected, because a
    /// valid type in the low bits is only half the requirement.
    #[test]
    fn garbage_words_are_still_rejected_at_the_old_rate() {
        let mut accepted = 0u64;
        let mut state = 0x2545_F491_4F6C_DD1Du64;
        const TRIALS: u64 = 200_000;
        for _ in 0..TRIALS {
            // xorshift: deterministic, no rand dependency
            state ^= state << 13;
            state ^= state >> 7;
            state ^= state << 17;
            let word = state as u32;
            // A word is only "accepted" here if it parses at all; the
            // checksum then has to match the content as well, which this
            // level cannot see.
            if !matches!(unpack_type_word(word), TypeWord::Invalid) {
                accepted += 1;
            }
        }
        // 3 valid types out of 256 low-bit patterns; the checksum must then
        // match 24 more bits, which no garbage word will do by luck.
        let rate = accepted as f64 / TRIALS as f64;
        assert!(
            rate < 0.02,
            "garbage acceptance rate {rate} is too high ({accepted}/{TRIALS})"
        );
    }

    /// What no file-level checksum can catch: bytes changing in RESIDENT
    /// memory after the write. The WAL's CRC covers what reached the log,
    /// the backup's covers what reached disk; neither is recomputed from
    /// memory again. This is the layer that notices.
    #[test]
    fn a_scribble_in_resident_memory_is_caught() {
        let mut buffer = vec![0u8; 128];
        let pos = buffer.as_mut_ptr() as usize;
        assert_eq!(pos % 8, 0, "test buffer must be 8-byte aligned");
        let content = b"the bytes a reader would otherwise trust";
        Entry::encode_to(pos, EntryType::CELL, content.len() as u32, |content_pos| unsafe {
            std::ptr::copy_nonoverlapping(content.as_ptr(), content_pos as *mut u8, content.len());
        });

        assert_eq!(verify_entry_at(pos), Some(true), "a fresh entry must verify");

        buffer[ENTRY_HEAD_SIZE + 5] ^= 0xFF;
        assert_eq!(
            verify_entry_at(pos),
            Some(false),
            "a flipped content byte must fail verification"
        );
    }

    #[test]
    fn an_entry_written_before_checksums_verifies_as_unknown() {
        let mut buffer = vec![0u8; 64];
        let pos = buffer.as_mut_ptr() as usize;
        // Exactly what the pre-checksum writer produced: a bare type word.
        buffer[..4].copy_from_slice(&EntryType::CELL.bits().to_le_bytes());
        buffer[4..8].copy_from_slice(&16u32.to_le_bytes());
        assert_eq!(
            verify_entry_at(pos),
            None,
            "a legacy entry carries no checksum, so nothing can be said"
        );
    }
}
