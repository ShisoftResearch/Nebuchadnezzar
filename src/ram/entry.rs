use std::panic;
use std::{io::Cursor, mem};

use crate::ram::cell::CellHeader;
use crate::ram::tombstone::Tombstone;
use byteorder::ReadBytesExt;

use super::mem_cursor::{release_cursor, Endian};

#[derive(Copy, Clone, Debug, PartialEq, Eq, Default)]
#[repr(u32)]
pub enum EntryType {
    #[default]
    UNDECIDED = 0,
    CELL = 1,
    TOMBSTONE = 2,
    /// Occupies space that holds no data, so a scan can step over it.
    ///
    /// Written by WAL recovery over the gap an abandoned reservation left:
    /// `try_acquire` moves the append cursor before the entry bytes are
    /// written, so a crash in that window leaves zeros in the middle of the
    /// image. UNDECIDED cannot express that -- it means "nothing was ever
    /// written here", which is why a scan stops at it, and stopping in the
    /// MIDDLE discards every entry that other writers durably appended
    /// after the gap.
    PADDING = 3,
    /// Opens a transaction's bracket. Everything after it in this segment
    /// belongs to that transaction until its COMMIT, and a scan that reaches
    /// the end of the segment without one discards the lot.
    ///
    /// A marker is needed rather than inferring the bracket from the COMMIT
    /// alone, because ordinary entries can precede it in the same segment
    /// and those apply unconditionally.
    BEGIN = 4,
    /// Closes a transaction's bracket and IS the commit point: a bracket is
    /// committed if and only if this entry is present. Carries the manifest,
    /// which names every chunk and seq id the transaction wrote, so a
    /// multi-chunk transaction can be judged from any one of its parts.
    COMMIT = 5,
    /// Links a full chain segment to the next one, at a FIXED position: the
    /// last 24 bytes of the segment. Recovery reads it without scanning, so
    /// chain membership costs one read and an aborted chain is discarded
    /// without ever being walked.
    TXN_CONT = 6,
}

impl EntryType {
    pub fn from_bits(bits: u32) -> Option<Self> {
        match bits {
            0 => Some(Self::UNDECIDED),
            1 => Some(Self::CELL),
            2 => Some(Self::TOMBSTONE),
            3 => Some(Self::PADDING),
            4 => Some(Self::BEGIN),
            5 => Some(Self::COMMIT),
            6 => Some(Self::TXN_CONT),
            _ => None,
        }
    }

    /// Whether a bare (checksum-free) type word is legal for this kind.
    ///
    /// Only the two kinds with no content to vouch for. Everything else --
    /// cells, tombstones, and every bracket marker -- carries data, so a
    /// bare word for one of them is a header that lost its checksum.
    pub fn may_be_bare(self) -> bool {
        matches!(self, Self::PADDING | Self::UNDECIDED)
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
/// never zero: a zero checksum field makes the whole word bare, which only
/// PADDING and UNDECIDED may be (see [`unpack_type_word`]), so a computed
/// zero is mapped to one rather than turning a cell into a bare word.
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

/// Checksum of a slice, for callers holding a buffer rather than a mapped
/// address (WAL recovery builds an image before it is installed).
fn content_checksum_slice(bytes: &[u8]) -> u32 {
    let mut digest = crc_fast::Digest::new(crc_fast::CrcAlgorithm::Crc32Iscsi);
    digest.update(bytes);
    let folded = (digest.finalize() as u32) >> ENTRY_TYPE_BITS;
    if folded == 0 {
        1
    } else {
        folded
    }
}

/// Write a PADDING entry covering `span` bytes at `at` within `buf`.
///
/// `span` must be at least `ENTRY_HEAD_SIZE`; entries are 8-byte aligned so
/// any gap between two of them already is. Returns false if it will not fit,
/// which leaves the gap as it was -- the caller decides how loud to be.
pub fn stamp_padding(buf: &mut [u8], at: usize, span: usize) -> bool {
    if span < ENTRY_HEAD_SIZE || at + span > buf.len() {
        return false;
    }
    let content_len = (span - ENTRY_HEAD_SIZE) as u32;
    // Checksummed like any other entry, so nothing downstream has to make an
    // exception for it.
    let checksum = content_checksum_slice(&buf[at + ENTRY_HEAD_SIZE..at + span]);
    let word = pack_type_word(EntryType::PADDING, checksum);
    buf[at..at + 4].copy_from_slice(&word.to_le_bytes());
    buf[at + 4..at + ENTRY_HEAD_SIZE].copy_from_slice(&content_len.to_le_bytes());
    true
}

/// Stamp a CHECKSUMMED padding header over a finished span, in place.
///
/// The reservation variant below is deliberately bare, because at
/// reservation time the content is not written yet and there is nothing to
/// checksum. When a span's bytes are final -- an image that was appended
/// and then abandoned -- the padding can and should carry a checksum like
/// any other entry, so the entry verifies on its own rather than merely
/// escaping verification.
///
/// The header is published as ONE aligned 8-byte store, so a reader sees
/// the old entry or the padding, never a mixture.
pub fn stamp_checked_padding(addr: usize, span: u32) {
    if (span as usize) < ENTRY_HEAD_SIZE || addr % 8 != 0 {
        return;
    }
    let content_len = span - ENTRY_HEAD_SIZE as u32;
    let checksum = content_checksum(addr + ENTRY_HEAD_SIZE, content_len);
    let word = pack_type_word(EntryType::PADDING, checksum);
    let mut header = [0u8; ENTRY_HEAD_SIZE];
    header[..4].copy_from_slice(&word.to_le_bytes());
    header[4..].copy_from_slice(&content_len.to_le_bytes());
    unsafe {
        (*(addr as *const std::sync::atomic::AtomicU64)).store(
            u64::from_le_bytes(header),
            std::sync::atomic::Ordering::Release,
        );
    }
}

/// Stamp a bare PADDING header over a just-reserved span, in place.
///
/// Called immediately after the append cursor is advanced and before the
/// entry itself is written, so a crash in that window leaves a VALID entry
/// that a scan can step over instead of a run of zeros that stops it dead.
/// The real entry header overwrites this one moments later, so nothing
/// survives when the write completes.
///
/// Deliberately unchecked (a bare type word): computing a checksum here
/// would mean reading the whole reserved span on every acquire, on the
/// hottest path in the store, to protect bytes that carry no data. A scan
/// only needs the type and the length to step over it.
pub fn stamp_reservation_padding(addr: usize, span: u32) {
    if (span as usize) < ENTRY_HEAD_SIZE || addr % 8 != 0 {
        return;
    }
    let content_len = span - ENTRY_HEAD_SIZE as u32;
    let word = EntryType::PADDING.bits();
    unsafe {
        std::ptr::copy_nonoverlapping(
            word.to_le_bytes().as_ptr(),
            addr as *mut u8,
            mem::size_of::<u32>(),
        );
        std::ptr::copy_nonoverlapping(
            content_len.to_le_bytes().as_ptr(),
            (addr + mem::size_of::<u32>()) as *mut u8,
            mem::size_of::<u32>(),
        );
    }
}

/// What a type word says about its entry.
pub enum TypeWord {
    /// A bare type word carrying no checksum. Valid ONLY for the two entry
    /// kinds that have no content to vouch for: PADDING, whose bytes are
    /// meaningless by definition and whose header declares only a span, and
    /// UNDECIDED, a reservation that was never filled.
    ///
    /// A bare CELL or TOMBSTONE word is corruption. There is no format to
    /// stay compatible with, so every entry that carries data carries a
    /// checksum -- and accepting a bare one meant that zeroing a header's
    /// checksum bits promoted damaged data to trusted data.
    Bare(EntryType),
    /// Type plus the checksum the writer recorded.
    Checked(EntryType, u32),
    /// Not a valid entry header.
    Invalid,
}

/// Split a type word into its type and checksum.
///
/// A bare word (the whole u32 is 0, 1, 2 or 3) carries no checksum, which
/// only PADDING and UNDECIDED are entitled to: neither has content to
/// vouch for. A bare CELL or TOMBSTONE is damage wearing a valid type.
/// Everything else must carry a valid type in its low bits AND a non-zero
/// checksum.
pub fn unpack_type_word(word: u32) -> TypeWord {
    if let Some(entry_type) = EntryType::from_bits(word) {
        return if entry_type.may_be_bare() {
            TypeWord::Bare(entry_type)
        } else {
            TypeWord::Invalid
        };
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

/// Verify the entry at `pos`, if it has content to verify.
///
/// `Some(false)` means the content does not match what the writer recorded;
/// `Some(true)` that it does; `None` that the entry carries no content to
/// vouch for -- padding, or a reservation never filled. Every entry that
/// holds data answers `Some`.
/// `bound` is one past the last byte the caller may read. It is not optional
/// and it is not a nicety.
///
/// This function is asked to judge bytes that MAY BE GARBAGE -- that is the
/// entire reason it exists -- and the length it uses to checksum comes out of
/// those same bytes. Without a bound, a word that happens to decode as a
/// checked entry with a length of 3 GB makes the CRC read 3 GB past the
/// buffer: not a wrong answer, a SIGSEGV, with recovery's own thread taking
/// the process down. Caught on two machines and two branches, always as
/// `read_chain_link` -> here -> `content_checksum(content_len ~3e9)`.
///
/// A length that does not fit is not a checksum failure to be investigated;
/// it means these bytes are not an entry, which is `Some(false)`.
pub fn verify_entry_at(pos: usize, bound: usize) -> Option<bool> {
    if pos % 8 != 0 || pos.checked_add(ENTRY_HEAD_SIZE).is_none_or(|end| end > bound) {
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
        // Nothing to check: padding bytes are meaningless and an
        // unfilled reservation has no content at all.
        TypeWord::Bare(_) => None,
        TypeWord::Invalid => Some(false),
        TypeWord::Checked(_, checksum) => {
            let content_pos = pos + ENTRY_HEAD_SIZE;
            if content_pos
                .checked_add(content_length as usize)
                .is_none_or(|end| end > bound)
            {
                return Some(false);
            }
            Some(checksum == content_checksum(content_pos, content_length))
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
    pub fn encode_to<W>(pos: usize, entry_type: EntryType, content_len: u32, write_content: W)
    where
        W: Fn(usize),
    {
        // PUBLISH LAST, in one store. The old order -- bare type word, then
        // length, then content, then the checksummed word -- opened a
        // window where the span read as a bare CELL over content that was
        // not written yet. A panic inside `write_content` froze that state, and
        // the unwind still journals the span (`PendingEntry::drop`), so
        // recovery would install garbage it had every reason to trust.
        //
        // Now nothing in the header changes until the content is complete
        // and checksummed; then the whole 8-byte header -- word and length
        // together -- is published as a single aligned store. Entries are
        // 8-byte aligned, so the store cannot tear. Until it lands, the
        // span reads as whatever the allocator stamped over it (PADDING),
        // which every walker steps over and recovery discards: an entry is
        // either absent or whole, never half.
        write_content(Self::content_pos(pos));
        let checksum = content_checksum(Self::content_pos(pos), content_len);
        let word = pack_type_word(entry_type, checksum);
        let mut header = [0u8; ENTRY_HEAD_SIZE];
        header[..4].copy_from_slice(&word.to_le_bytes());
        header[4..].copy_from_slice(&content_len.to_le_bytes());
        debug_assert!(pos % 8 == 0, "entry headers are 8-byte aligned");
        unsafe {
            (*(pos as *const std::sync::atomic::AtomicU64))
                .store(u64::from_le_bytes(header), std::sync::atomic::Ordering::Release);
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
            TypeWord::Bare(entry_type) | TypeWord::Checked(entry_type, _) => entry_type,
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
        if let TypeWord::Bare(entry_type) | TypeWord::Checked(entry_type, _) =
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

    /// Nothing about the header may change until the content is complete.
    ///
    /// The old order published a bare CELL word before the content existed,
    /// and bare words carry no checksum, so nothing questioned them: a panic
    /// mid-content froze a trusted-looking entry over garbage, and the
    /// unwind still journaled it. The closure here runs exactly where that
    /// panic would, and the header must still read as the allocator's
    /// padding at that instant.
    #[test]
    fn the_header_is_published_only_after_the_content_is_written() {
        let mut buffer = vec![0u8; 128];
        let base = buffer.as_mut_ptr() as usize;
        stamp_reservation_padding(base, 64);

        let content = b"published all at once or not at all";
        Entry::encode_to(base, EntryType::CELL, content.len() as u32, |content_pos| {
            // Mid-write: the reservation's padding must still be standing.
            let word = unsafe { *(base as *const u32) };
            match unpack_type_word(word) {
                TypeWord::Bare(entry_type) => assert_eq!(
                    entry_type,
                    EntryType::PADDING,
                    "the header changed before the content was complete"
                ),
                other => panic!(
                    "the header changed before the content was complete (invalid: {})",
                    matches!(other, TypeWord::Invalid)
                ),
            }
            unsafe {
                std::ptr::copy_nonoverlapping(content.as_ptr(), content_pos as *mut u8, content.len());
            }
        });

        let (header, _) = Entry::decode_from(base, |_, header| header);
        assert_eq!(header.entry_type, EntryType::CELL);
        assert_eq!(verify_entry_at(base, base + buffer.len()), Some(true));
    }

    /// A reservation that is never filled must still be walkable.
    ///
    /// This is the archive case: segment memory is what gets snapshotted
    /// into a backup, and a backup carries no WAL records, so a hole baked
    /// into one has an extent nothing can ever recover. Stamping at
    /// reservation time means the snapshot contains a valid padding entry
    /// instead of zeros.
    #[test]
    fn an_unfilled_reservation_leaves_a_walkable_entry() {
        let mut buffer = vec![0u8; 256];
        let base = buffer.as_mut_ptr() as usize;
        assert_eq!(base % 8, 0, "test buffer must be 8-byte aligned");

        // Reserve 64 bytes and crash: nothing else is written.
        stamp_reservation_padding(base, 64);

        let (header, _) = Entry::decode_from(base, |_, header| header);
        assert_eq!(header.entry_type, EntryType::PADDING);
        assert_eq!(
            ENTRY_HEAD_SIZE + header.content_length as usize,
            64,
            "the padding must span exactly the reservation, so a scan lands on what follows"
        );
    }

    /// And when the write DOES complete, no trace of the padding remains.
    #[test]
    fn a_completed_write_overwrites_its_reservation_padding() {
        let mut buffer = vec![0u8; 256];
        let base = buffer.as_mut_ptr() as usize;
        stamp_reservation_padding(base, 64);

        let content = b"the real entry";
        Entry::encode_to(base, EntryType::CELL, content.len() as u32, |content_pos| unsafe {
            std::ptr::copy_nonoverlapping(content.as_ptr(), content_pos as *mut u8, content.len());
        });

        let (header, _) = Entry::decode_from(base, |_, header| header);
        assert_eq!(header.entry_type, EntryType::CELL);
        assert_eq!(header.content_length as usize, content.len());
        assert_eq!(verify_entry_at(base, base + buffer.len()), Some(true));
    }

    /// Only the entry kinds with nothing to vouch for may go unchecked.
    ///
    /// A bare word is how a reservation stamp and an unfilled reservation
    /// describe themselves, and it must stay readable. A bare CELL or
    /// TOMBSTONE is a different thing entirely: a data entry whose checksum
    /// bits were lost. Accepting it -- which the old backwards-compatible
    /// rule did -- promoted damaged data to trusted data, so damage that
    /// zeroed a header read back as a perfectly good cell.
    #[test]
    fn only_contentless_entries_may_go_unchecked() {
        for (bits, expected) in [(0u32, EntryType::UNDECIDED), (3, EntryType::PADDING)] {
            match unpack_type_word(bits) {
                TypeWord::Bare(entry_type) => assert_eq!(entry_type, expected),
                _ => panic!("bare word {bits} must decode as Bare({expected:?})"),
            }
        }
        for bits in [1u32, 2] {
            assert!(
                matches!(unpack_type_word(bits), TypeWord::Invalid),
                "a bare data-entry word ({bits}) is a header that lost its checksum, not an entry"
            );
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
        // A word parses if its low byte names a type, so the floor is set by
        // how many types exist -- derived here rather than written down, so
        // adding one cannot silently loosen the bound it was checked against.
        // Parsing is not accepting: the checksum must then match 24 more
        // bits, which no garbage word does by luck.
        let valid_types = (0..256u32).filter(|bits| EntryType::from_bits(*bits).is_some()).count();
        let floor = valid_types as f64 / 256.0;
        let rate = accepted as f64 / TRIALS as f64;
        assert!(
            rate < floor * 1.5,
            "garbage acceptance rate {rate} is too high for {valid_types} entry types \
             (floor {floor}, {accepted}/{TRIALS})"
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

        assert_eq!(
            verify_entry_at(pos, pos + buffer.len()),
            Some(true),
            "a fresh entry must verify"
        );

        buffer[ENTRY_HEAD_SIZE + 5] ^= 0xFF;
        assert_eq!(
            verify_entry_at(pos, pos + buffer.len()),
            Some(false),
            "a flipped content byte must fail verification"
        );
    }

    /// A length that runs off the end is NOT a checksum failure to
    /// investigate -- it means these bytes are not an entry.
    ///
    /// This is the crash, reproduced. Recovery's `read_chain_link` looks at a
    /// fixed offset near a segment's tail and asks whether an entry vouches
    /// for itself there. The bytes at that offset are often not an entry at
    /// all, and a word that happens to decode as `Checked` carries a length
    /// that is garbage too. Before the bound, that length went straight into
    /// a CRC: ~3 GB read past a 224-byte buffer, SIGSEGV on a recovery
    /// thread, no panic, process gone. Caught on two machines and two
    /// branches with the same three frames.
    ///
    /// Deliberately places the entry at the very END of the buffer so any
    /// nonzero content length is out of bounds -- ASAN or a guard page is not
    /// needed for this to be a real read past the end.
    #[test]
    fn a_content_length_past_the_bound_is_refused_rather_than_read() {
        let mut buffer = vec![0u8; 64];
        let base = buffer.as_mut_ptr() as usize;
        assert_eq!(base % 8, 0, "test buffer must be 8-byte aligned");
        let pos = base + 64 - ENTRY_HEAD_SIZE;
        let bound = base + buffer.len();

        // A well-formed CHECKED word -- exactly what garbage can look like --
        // with a length no buffer could hold.
        let word = pack_type_word(EntryType::CELL, 0x5AA5);
        buffer[64 - ENTRY_HEAD_SIZE..64 - ENTRY_HEAD_SIZE + 4]
            .copy_from_slice(&word.to_le_bytes());
        buffer[64 - ENTRY_HEAD_SIZE + 4..64].copy_from_slice(&3_000_000_000u32.to_le_bytes());

        assert_eq!(
            verify_entry_at(pos, bound),
            Some(false),
            "a length that does not fit inside the bound must be refused, not checksummed"
        );

        // And the ordinary in-bounds refusals still behave: one byte too far
        // is still too far.
        let mut small = vec![0u8; 32];
        let small_base = small.as_mut_ptr() as usize;
        let word = pack_type_word(EntryType::CELL, 0x1234);
        small[..4].copy_from_slice(&word.to_le_bytes());
        small[4..8].copy_from_slice(&(32u32 - ENTRY_HEAD_SIZE as u32 + 1).to_le_bytes());
        assert_eq!(
            verify_entry_at(small_base, small_base + small.len()),
            Some(false),
            "one byte past the bound is past the bound"
        );

        // A header that does not even fit is refused before it is read.
        assert_eq!(
            verify_entry_at(small_base + 28, small_base + small.len()),
            Some(false),
            "a header straddling the bound must be refused without reading it"
        );
    }

    #[test]
    fn an_entry_written_before_checksums_verifies_as_unknown() {
        let mut buffer = vec![0u8; 64];
        let pos = buffer.as_mut_ptr() as usize;
        // A bare CELL word: a data entry whose checksum bits are gone. It is
        // not an entry at all any more, and must not verify.
        buffer[..4].copy_from_slice(&EntryType::CELL.bits().to_le_bytes());
        buffer[4..8].copy_from_slice(&16u32.to_le_bytes());
        assert_eq!(
            verify_entry_at(pos, pos + buffer.len()),
            Some(false),
            "a bare CELL word is damage, not an unchecked entry"
        );
    }
}
