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

/// BEGIN content: just the transaction id.
pub fn encode_begin(txn: &TxnId) -> Vec<u8> {
    let mut out = Vec::with_capacity(TXN_ID_SIZE);
    encode_txn_id(txn, &mut out);
    out
}

pub fn decode_begin(content: &[u8]) -> Option<TxnId> {
    decode_txn_id(content)
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
    out
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
        assert_eq!(decode_begin(&encode_begin(&txn)).unwrap(), txn);

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
        let mut bytes = encode_commit(&txn, &[ManifestEntry { chunk_id: 3, seq_id: 9 }]);
        let full = bytes.len();
        bytes.truncate(full - 1);
        assert!(
            decode_commit(&bytes).is_none(),
            "a manifest cut short must not decode"
        );

        // A count larger than the bytes behind it: the same refusal.
        let mut lying = encode_commit(&txn, &[]);
        lying[TXN_ID_SIZE] = 9;
        assert!(decode_commit(&lying).is_none());
    }

    /// The whole point of the fixed tail: the entry is exactly 24 bytes, so
    /// the writer can reserve for it without knowing anything else.
    #[test]
    fn a_continuation_link_is_exactly_twenty_four_bytes() {
        assert_eq!(TXN_CONT_ENTRY_SIZE, 24);
    }
}
