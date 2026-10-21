//! Transaction bracket records: BEGIN, COMMIT and TXN_CONT.
//!
//! One writer owns a segment while a transaction is writing into it, so a
//! transaction's entries are contiguous and need no per-entry stamps to be
//! attributed. What recovery needs instead is where the run starts, where it
//! ends, and -- when it outgrew one segment -- which segments it continued
//! into. That is exactly these three records and nothing more.
//!
//! Encoded by hand rather than through the schema machinery: recovery reads
//! these before any schema exists, and a bracket that could not be decoded
//! without one would be unreadable exactly when it matters.

use crate::server::transactions::TxnId;
use std::mem::size_of;

/// A transaction id on the wire: the HLC's two halves, little-endian.
pub const TXN_ID_SIZE: usize = 2 * size_of::<u64>();

/// `TXN_CONT` content: the short transaction id and the previous segment's
/// seq id.
pub const TXN_CONT_CONTENT_SIZE: usize = 2 * size_of::<u64>();

/// The whole `TXN_CONT` entry, header included. It sits at the very END of a
/// full chain segment, so the fit check reserves exactly this much whenever
/// more entries remain to be written.
pub const TXN_CONT_ENTRY_SIZE: usize = crate::ram::entry::ENTRY_HEAD_SIZE + TXN_CONT_CONTENT_SIZE;

/// The smallest COMMIT entry, header included: an empty manifest, padded to
/// alignment. Every bracket part keeps this much free behind its entries, so
/// that it can always be closed by a COMMIT of its own -- see
/// `Chunk::close_transaction_bracket`.
pub const MIN_COMMIT_ENTRY_SIZE: usize = crate::ram::entry::ENTRY_HEAD_SIZE
    + (TXN_ID_SIZE + size_of::<u16>()).next_multiple_of(crate::ram::entry::ENTRY_HEAD_SIZE);

/// One member of a commit manifest: a chunk and the seq id of the segment the
/// transaction's bracket lives in there.
#[derive(Copy, Clone, Debug, PartialEq, Eq)]
pub struct ManifestEntry {
    pub chunk_id: u16,
    pub seq_id: u64,
}

const MANIFEST_ENTRY_SIZE: usize = size_of::<u16>() + size_of::<u64>();

/// The id's RAW fields, not `wall_ms()`: that accessor drops the logical
/// counter, and two transactions in the same millisecond would encode
/// identically. An id has to round-trip exactly or a bracket can be
/// attributed to the wrong transaction.
pub fn encode_txn_id(txn: &TxnId, out: &mut Vec<u8>) {
    out.extend_from_slice(&txn.ts.to_le_bytes());
    out.extend_from_slice(&txn.node.to_le_bytes());
}

pub fn decode_txn_id(bytes: &[u8]) -> Option<TxnId> {
    if bytes.len() < TXN_ID_SIZE {
        return None;
    }
    let ts = u64::from_le_bytes(bytes[..8].try_into().ok()?);
    let node = u64::from_le_bytes(bytes[8..16].try_into().ok()?);
    Some(TxnId { ts, node })
}

/// The short id used by `TXN_CONT`, where 24 bytes is the whole budget.
///
/// Safe to truncate because the link is only a cross-check: the COMMIT's
/// manifest is what authoritatively names a chain's members, so a collision
/// would have to happen between two chains in flight at the same instant AND
/// agree with the manifest to matter.
pub fn short_txn_id(txn: &TxnId) -> u64 {
    txn.ts ^ txn.node.rotate_left(32)
}

/// The format of a BEGIN record.
///
/// Version 1 held the transaction id only. Version 2 adds the participant
/// set, so that a site that restarts in doubt knows which site is the commit
/// point of the transaction. Recovery refuses any other version with
/// [`BracketFormatError`]; it does not read version 1.
pub const BEGIN_FORMAT_VERSION: u8 = 2;

/// BEGIN content, version 2:
///
/// ```text
/// txn id        16 bytes
/// version       u8       BEGIN_FORMAT_VERSION
/// reserved      u8
/// count         u16      number of participants
/// participants  u64 * count, server ids
/// padding       to 8-byte alignment
/// ```
///
/// Every part of a transaction carries the same participant set. The set
/// comes from the coordinator at prepare. The commit point is its lowest id.
pub fn encode_begin(txn: &TxnId, participants: &[u64]) -> Vec<u8> {
    let mut out = Vec::with_capacity(TXN_ID_SIZE + 4 + participants.len() * size_of::<u64>());
    encode_txn_id(txn, &mut out);
    out.push(BEGIN_FORMAT_VERSION);
    out.push(0);
    out.extend_from_slice(&(participants.len() as u16).to_le_bytes());
    for participant in participants {
        out.extend_from_slice(&participant.to_le_bytes());
    }
    pad_to_alignment(&mut out);
    out
}

/// Why a bracket record cannot be read.
#[derive(Debug, Clone, PartialEq, Eq)]
pub enum BracketFormatError {
    /// The record is shorter than its fields say.
    Truncated,
    /// The record has a format version that this build does not read.
    UnsupportedBeginVersion(u8),
}

impl std::fmt::Display for BracketFormatError {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        match self {
            BracketFormatError::Truncated => write!(f, "the BEGIN record is truncated"),
            BracketFormatError::UnsupportedBeginVersion(version) => write!(
                f,
                "the BEGIN record has format version {version}; this build reads only version \
                 {BEGIN_FORMAT_VERSION}. Import the store again from its source."
            ),
        }
    }
}

impl std::error::Error for BracketFormatError {}

/// A decoded BEGIN: the transaction and its participant set.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct Begin {
    pub txn: TxnId,
    pub participants: Vec<u64>,
}

pub fn decode_begin(content: &[u8]) -> Result<Begin, BracketFormatError> {
    // Version 1 was the transaction id alone. Name it, so the error says
    // which format the store holds.
    if content.len() == TXN_ID_SIZE {
        return Err(BracketFormatError::UnsupportedBeginVersion(1));
    }
    let txn = decode_txn_id(content).ok_or(BracketFormatError::Truncated)?;
    let fixed = TXN_ID_SIZE + 4;
    if content.len() < fixed {
        return Err(BracketFormatError::Truncated);
    }
    let version = content[TXN_ID_SIZE];
    if version != BEGIN_FORMAT_VERSION {
        return Err(BracketFormatError::UnsupportedBeginVersion(version));
    }
    let count = u16::from_le_bytes([content[TXN_ID_SIZE + 2], content[TXN_ID_SIZE + 3]]) as usize;
    if content.len() < fixed + count * size_of::<u64>() {
        return Err(BracketFormatError::Truncated);
    }
    let participants = (0..count)
        .map(|index| {
            let at = fixed + index * size_of::<u64>();
            u64::from_le_bytes(content[at..at + size_of::<u64>()].try_into().unwrap())
        })
        .collect();
    Ok(Begin { txn, participants })
}

/// COMMIT content: the transaction id, then the manifest.
///
/// The manifest is exact rather than estimated -- a transaction's write set
/// is fully known when it applies, and a chain is pre-allocated -- so
/// "committed" can be decided from any single part: the COMMIT is present
/// and every member it names is present.
pub fn encode_commit(txn: &TxnId, manifest: &[ManifestEntry]) -> Vec<u8> {
    let mut out = Vec::with_capacity(TXN_ID_SIZE + 2 + manifest.len() * MANIFEST_ENTRY_SIZE);
    encode_txn_id(txn, &mut out);
    out.extend_from_slice(&(manifest.len() as u16).to_le_bytes());
    for member in manifest {
        out.extend_from_slice(&member.chunk_id.to_le_bytes());
        out.extend_from_slice(&member.seq_id.to_le_bytes());
    }
    // Entries are 8-byte aligned, and an entry's size is its header plus its
    // content -- so the content itself has to be a multiple of 8 or the NEXT
    // entry starts misaligned and cannot be decoded at all. Trailing zeros
    // are free to add and ignored on the way back in, since the manifest
    // count says where the data ends.
    pad_to_alignment(&mut out);
    out
}

fn pad_to_alignment(out: &mut Vec<u8>) {
    while out.len() % crate::ram::entry::ENTRY_HEAD_SIZE != 0 {
        out.push(0);
    }
}

pub fn decode_commit(content: &[u8]) -> Option<(TxnId, Vec<ManifestEntry>)> {
    let txn = decode_txn_id(content)?;
    let mut cursor = TXN_ID_SIZE;
    if content.len() < cursor + 2 {
        return None;
    }
    let count = u16::from_le_bytes(content[cursor..cursor + 2].try_into().ok()?) as usize;
    cursor += 2;
    // A count the content cannot hold means the record is not what it claims.
    // Refusing beats trusting a length: this is the record that decides
    // whether a transaction is committed.
    if content.len() < cursor + count * MANIFEST_ENTRY_SIZE {
        return None;
    }
    let mut manifest = Vec::with_capacity(count);
    for _ in 0..count {
        let chunk_id = u16::from_le_bytes(content[cursor..cursor + 2].try_into().ok()?);
        cursor += 2;
        let seq_id = u64::from_le_bytes(content[cursor..cursor + 8].try_into().ok()?);
        cursor += 8;
        manifest.push(ManifestEntry { chunk_id, seq_id });
    }
    Some((txn, manifest))
}

/// TXN_CONT content: short id, then the seq id of the PREVIOUS part.
///
/// Previous rather than next, because a part is written before its successor
/// exists as a durable thing to point at, and a backwards link is verifiable
/// the moment it is written.
pub fn encode_txn_cont(txn: &TxnId, prev_seq: u64) -> Vec<u8> {
    let mut out = Vec::with_capacity(TXN_CONT_CONTENT_SIZE);
    out.extend_from_slice(&short_txn_id(txn).to_le_bytes());
    out.extend_from_slice(&prev_seq.to_le_bytes());
    out
}

pub fn decode_txn_cont(content: &[u8]) -> Option<(u64, u64)> {
    if content.len() < TXN_CONT_CONTENT_SIZE {
        return None;
    }
    let short = u64::from_le_bytes(content[..8].try_into().ok()?);
    let prev_seq = u64::from_le_bytes(content[8..16].try_into().ok()?);
    Some((short, prev_seq))
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::server::transactions::test_hlc;

    #[test]
    fn bracket_records_round_trip() {
        let txn = test_hlc(0x0102_0304_0506_0708, 0x1122_3344_5566_7788);
        let begin = decode_begin(&encode_begin(&txn, &[7, 3, u64::MAX])).unwrap();
        assert_eq!(begin.txn, txn);
        assert_eq!(begin.participants, vec![7, 3, u64::MAX]);

        let manifest = vec![
            ManifestEntry { chunk_id: 0, seq_id: 1 },
            ManifestEntry { chunk_id: 513, seq_id: u64::MAX },
        ];
        let (decoded_txn, decoded) = decode_commit(&encode_commit(&txn, &manifest)).unwrap();
        assert_eq!(decoded_txn, txn);
        assert_eq!(decoded, manifest);

        let (short, prev) = decode_txn_cont(&encode_txn_cont(&txn, 42)).unwrap();
        assert_eq!(short, short_txn_id(&txn));
        assert_eq!(prev, 42);
    }

    /// A COMMIT decides whether a transaction happened, so a truncated or
    /// over-claiming one must be refused rather than read as far as it goes.
    #[test]
    fn a_commit_that_cannot_hold_its_manifest_is_refused() {
        let txn = test_hlc(7, 7);
        // Cut into the manifest itself, not the alignment padding behind it.
        let mut bytes = encode_commit(&txn, &[ManifestEntry { chunk_id: 3, seq_id: 9 }]);
        bytes.truncate(TXN_ID_SIZE + 2 + 1);
        assert!(
            decode_commit(&bytes).is_none(),
            "a manifest cut short must not decode"
        );

        // A count larger than the bytes behind it: the same refusal.
        let mut lying = encode_commit(&txn, &[]);
        lying[TXN_ID_SIZE] = 9;
        assert!(decode_commit(&lying).is_none());
    }

    /// Every bracket record must leave the next entry 8-byte aligned, or the
    /// entry after it cannot be decoded -- which takes out the rest of the
    /// segment, not just the record.
    #[test]
    fn every_bracket_record_is_eight_byte_aligned() {
        let txn = test_hlc(11, 12);
        let align = crate::ram::entry::ENTRY_HEAD_SIZE;
        for participants in 0..5u64 {
            let ids: Vec<u64> = (0..participants).collect();
            assert_eq!(encode_begin(&txn, &ids).len() % align, 0, "BEGIN");
        }
        assert_eq!(encode_txn_cont(&txn, 5).len() % align, 0, "TXN_CONT");
        for members in 0..8usize {
            let manifest: Vec<_> = (0..members)
                .map(|i| ManifestEntry { chunk_id: i as u16, seq_id: i as u64 })
                .collect();
            let encoded = encode_commit(&txn, &manifest);
            assert_eq!(
                encoded.len() % align,
                0,
                "COMMIT with {} manifest member(s)",
                members
            );
            // Padding must not corrupt what it pads.
            let (decoded_txn, decoded) = decode_commit(&encoded).unwrap();
            assert_eq!(decoded_txn, txn);
            assert_eq!(decoded, manifest);
        }
    }

    /// The reserve every bracket part keeps must hold the minimal COMMIT
    /// exactly, or a part can fill up with no way left to close it.
    #[test]
    fn the_commit_reserve_is_exactly_a_minimal_commit() {
        let txn = test_hlc(3, 4);
        assert_eq!(
            crate::ram::entry::ENTRY_HEAD_SIZE + encode_commit(&txn, &[]).len(),
            MIN_COMMIT_ENTRY_SIZE
        );
        assert_eq!(MIN_COMMIT_ENTRY_SIZE, 32);
        let (decoded, manifest) = decode_commit(&encode_commit(&txn, &[])).unwrap();
        assert_eq!(decoded, txn);
        assert!(manifest.is_empty());
    }

    /// A BEGIN of another format version is refused with a typed error. The
    /// id-only form of version 1 is not read.
    #[test]
    fn a_begin_of_another_version_is_refused() {
        let txn = test_hlc(5, 6);
        let mut old = Vec::new();
        encode_txn_id(&txn, &mut old);
        assert_eq!(
            decode_begin(&old),
            Err(BracketFormatError::UnsupportedBeginVersion(1))
        );
        let mut future = encode_begin(&txn, &[1]);
        future[TXN_ID_SIZE] = BEGIN_FORMAT_VERSION + 1;
        assert_eq!(
            decode_begin(&future),
            Err(BracketFormatError::UnsupportedBeginVersion(BEGIN_FORMAT_VERSION + 1))
        );
        let mut short = encode_begin(&txn, &[1, 2]);
        short.truncate(TXN_ID_SIZE + 4 + 8);
        assert_eq!(decode_begin(&short), Err(BracketFormatError::Truncated));
    }

    /// The whole point of the fixed tail: the entry is exactly 24 bytes, so
    /// the writer can reserve for it without knowing anything else.
    #[test]
    fn a_continuation_link_is_exactly_twenty_four_bytes() {
        assert_eq!(TXN_CONT_ENTRY_SIZE, 24);
    }
}
