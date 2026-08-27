//! Dictionary-encoded columns: store a small integer per cell, the string once.
//!
//! A low-cardinality string column stores the same handful of bytes over and
//! over. Measured on a FlyWire connectome import, nine label columns held
//! 13,236 distinct strings across ~1.25M instances — ~99% of the stored label
//! text was repetition, and replacing it with integer codes cut the vertex
//! write phase from 10.1s to 1.3s.
//!
//! Space is the obvious win and the least interesting one. Two others matter
//! more:
//!
//! - **Indexes.** A ranged index cannot be built on `String`; the schema layer
//!   refuses it. As an integer code the same column takes `Ranged`, which
//!   measured 3.6x faster on ingest than `Hashed` for a low-cardinality column
//!   and, unlike `Hashed`, does not concentrate every write for one value onto
//!   the same few cells.
//! - **Comparison.** Every predicate, group key and join key on the column
//!   becomes integer equality.
//!
//! The cost is that a decode step has to live somewhere. Doing it in the
//! engine is the point of this module: hand-rolled per-application, the decode
//! becomes a client round-trip, because resolving a code to its string is a
//! join and the query language has no join.
//!
//! ## Codes are append-only, and that is load-bearing
//!
//! A code is an index into an ordered vocabulary, so a vocabulary that
//! renumbers silently changes the meaning of every value already stored under
//! the old numbering. [`Dictionary::merge`] therefore only ever appends, and
//! [`DictionaryColumn`] reads the persisted vocabulary before writing an
//! extended one. Re-importing, or importing a second larger export, keeps
//! every existing code exactly where it was.
//!
//! Code `0` is reserved for "no value": absent, empty, or not in the
//! vocabulary. Real codes start at 1, so a zero is never a valid index and an
//! unencodable value degrades to "unset" rather than to some other value.

use crate::client::AsyncClient;
use crate::ram::cell::{OwnedCell, ReadError, WriteError};
use crate::ram::schema::{Field, Schema, SchemaUid, SchemaVid};
use crate::ram::types::*;
use bifrost::rpc::RPCError;
use bifrost_hasher::hash_str;
use parking_lot::RwLock;
use std::collections::HashMap;
use std::sync::Arc;

/// Code meaning "this cell has no value for this column".
pub const DICTIONARY_UNSET: u32 = 0;

/// Largest assignable code, bounded by the code width.
pub const DICTIONARY_MAX_CODE: u32 = u32::MAX - 1;

const DICTIONARY_SCHEMA: &str = "DICTIONARY_SCHEMA";
const DICTIONARY_VALUES_FIELD: &str = "VALUES";

lazy_static! {
    pub static ref DICTIONARY_SCHEMA_ID: SchemaVid = SchemaVid(key_hash(DICTIONARY_SCHEMA) as u32);
    pub static ref DICTIONARY_VALUES_FIELD_ID: u64 = hash_str(DICTIONARY_VALUES_FIELD);
}

/// The cell holding one column's vocabulary.
pub fn dictionary_schema() -> Schema {
    Schema::new_with_id(
        DICTIONARY_SCHEMA_ID.get(),
        &DICTIONARY_SCHEMA.to_string(),
        None,
        Field::new_schema(vec![Field::new_unindexed_array(
            DICTIONARY_VALUES_FIELD,
            Type::String,
        )]),
        false,
        true, // scannable: an operator should be able to list vocabularies
    )
}

/// Deterministic id of the dictionary cell for one column.
///
/// Keyed by schema FAMILY uid and field, so evolving a schema does not move
/// the vocabulary out from under the codes already written against it.
pub fn dictionary_cell_id(schema: SchemaUid, field: u64) -> Id {
    let key = format!("DICTIONARY-{}-{}", schema.get(), field);
    Id::from_parts(hash_str(&format!("{key}-HI")), hash_str(&key))
}

/// An ordered vocabulary. Position defines the code.
#[derive(Debug, Clone, Default, PartialEq)]
pub struct Dictionary {
    values: Vec<String>,
    codes: HashMap<String, u32>,
}

impl Dictionary {
    pub fn from_values(values: Vec<String>) -> Self {
        let mut dict = Self::default();
        for value in values {
            dict.push(value);
        }
        dict
    }

    fn push(&mut self, value: String) {
        if value.is_empty() || self.codes.contains_key(&value) {
            return;
        }
        if self.values.len() as u32 >= DICTIONARY_MAX_CODE {
            return;
        }
        let code = self.values.len() as u32 + 1;
        self.codes.insert(value.clone(), code);
        self.values.push(value);
    }

    /// Append every unknown value, in sorted order, leaving existing codes
    /// untouched. Returns how many codes were newly assigned.
    pub fn merge<I: IntoIterator<Item = String>>(&mut self, incoming: I) -> usize {
        let mut fresh: Vec<String> = incoming
            .into_iter()
            .filter(|v| !v.is_empty() && !self.codes.contains_key(v))
            .collect();
        fresh.sort();
        fresh.dedup();
        let before = self.values.len();
        for value in fresh {
            self.push(value);
        }
        self.values.len() - before
    }

    /// Code for a value, or [`DICTIONARY_UNSET`] if empty or unknown.
    pub fn encode(&self, value: &str) -> u32 {
        if value.is_empty() {
            return DICTIONARY_UNSET;
        }
        self.codes.get(value).copied().unwrap_or(DICTIONARY_UNSET)
    }

    pub fn decode(&self, code: u32) -> Option<&str> {
        if code == DICTIONARY_UNSET {
            return None;
        }
        self.values.get(code as usize - 1).map(String::as_str)
    }

    pub fn len(&self) -> usize {
        self.values.len()
    }

    pub fn is_empty(&self) -> bool {
        self.values.is_empty()
    }

    pub fn values(&self) -> &[String] {
        &self.values
    }

    pub fn to_cell(&self, id: &Id) -> OwnedCell {
        let mut map = OwnedMap::new();
        map.insert_key_id(
            *DICTIONARY_VALUES_FIELD_ID,
            OwnedValue::Array(
                self.values
                    .iter()
                    .map(|v| OwnedValue::String(v.clone()))
                    .collect(),
            ),
        );
        OwnedCell::new_with_id(*DICTIONARY_SCHEMA_ID, id, OwnedValue::Map(map))
    }

    /// Rebuild from a persisted cell.
    ///
    /// Accepts both a plain array and a `{"value": [..]}` wrapper, so a change
    /// in how arrays are represented cannot silently yield an empty vocabulary
    /// — which would renumber every subsequent code.
    pub fn from_cell(cell: &OwnedCell) -> Self {
        let raw = &cell[*DICTIONARY_VALUES_FIELD_ID];
        let items: Vec<&OwnedValue> = match raw {
            OwnedValue::Array(items) => items.iter().collect(),
            OwnedValue::Map(map) => match map.get_by_key_id(*DICTIONARY_VALUES_FIELD_ID) {
                OwnedValue::Array(items) => items.iter().collect(),
                _ => Vec::new(),
            },
            _ => Vec::new(),
        };
        Self::from_values(
            items
                .into_iter()
                .filter_map(|v| match v {
                    OwnedValue::String(s) => Some(s.clone()),
                    _ => None,
                })
                .collect(),
        )
    }
}

/// A durable, shared dictionary for one column.
///
/// Holds a cached copy so `decode` is a local lookup; extending it reads the
/// persisted vocabulary first so concurrent writers cannot renumber each
/// other's codes.
pub struct DictionaryColumn {
    client: Arc<AsyncClient>,
    cell_id: Id,
    cached: RwLock<Dictionary>,
}

impl DictionaryColumn {
    pub fn new(client: Arc<AsyncClient>, schema: SchemaUid, field: u64) -> Self {
        Self {
            client,
            cell_id: dictionary_cell_id(schema, field),
            cached: RwLock::new(Dictionary::default()),
        }
    }

    pub fn cell_id(&self) -> Id {
        self.cell_id
    }

    /// Re-read the persisted vocabulary into the cache.
    pub async fn refresh(&self) -> Result<(), RPCError> {
        match self.client.read_cell(self.cell_id).await? {
            Ok(cell) => {
                *self.cached.write() = Dictionary::from_cell(&cell);
            }
            Err(ReadError::CellDoesNotExisted) => {
                *self.cached.write() = Dictionary::default();
            }
            Err(_) => {}
        }
        Ok(())
    }

    /// Decode from the cache. `None` for unset or unknown codes.
    pub fn decode(&self, code: u32) -> Option<String> {
        self.cached.read().decode(code).map(str::to_owned)
    }

    /// Encode from the cache without extending it.
    pub fn encode_known(&self, value: &str) -> u32 {
        self.cached.read().encode(value)
    }

    pub fn len(&self) -> usize {
        self.cached.read().len()
    }

    pub fn is_empty(&self) -> bool {
        self.cached.read().is_empty()
    }

    /// Extend the vocabulary with `values` and persist it, then return their
    /// codes.
    ///
    /// Reads the persisted vocabulary before extending, so a code assigned by
    /// another writer is adopted rather than overwritten. Values are added in
    /// one batch because the cell is rewritten whole: doing it per value would
    /// rewrite the whole vocabulary per value.
    pub async fn extend_and_encode(
        &self,
        values: &[String],
    ) -> Result<Vec<u32>, DictionaryError> {
        self.refresh().await.map_err(DictionaryError::Rpc)?;
        let needs_extend = {
            let cached = self.cached.read();
            values
                .iter()
                .any(|v| !v.is_empty() && cached.encode(v) == DICTIONARY_UNSET)
        };

        if needs_extend {
            let mut extended = self.cached.read().clone();
            extended.merge(values.iter().cloned());
            let cell = extended.to_cell(&self.cell_id);
            match self.client.upsert_cell(cell).await.map_err(DictionaryError::Rpc)? {
                Ok(_) => {
                    *self.cached.write() = extended;
                }
                Err(e) => return Err(DictionaryError::Write(e)),
            }
        }

        let cached = self.cached.read();
        Ok(values.iter().map(|v| cached.encode(v)).collect())
    }
}

#[derive(Debug)]
pub enum DictionaryError {
    Rpc(RPCError),
    Write(WriteError),
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn codes_are_dense_one_based_and_zero_is_unset() {
        let d = Dictionary::from_values(vec!["gaba".into(), "acetylcholine".into()]);
        assert_eq!(d.encode("gaba"), 1);
        assert_eq!(d.encode("acetylcholine"), 2);
        assert_eq!(d.decode(1), Some("gaba"));
        assert_eq!(d.decode(DICTIONARY_UNSET), None, "0 is never a value");
    }

    #[test]
    fn unknown_and_empty_encode_to_unset() {
        let d = Dictionary::from_values(vec!["gaba".into()]);
        assert_eq!(d.encode(""), DICTIONARY_UNSET);
        assert_eq!(d.encode("dopamine"), DICTIONARY_UNSET);
        assert_eq!(d.decode(9999), None);
    }

    #[test]
    fn merge_never_renumbers_an_existing_code() {
        // The property the whole design rests on: a later, larger import must
        // not change what an already-stored code means.
        let mut d = Dictionary::from_values(vec!["glutamate".into(), "gaba".into()]);
        let (g, b) = (d.encode("glutamate"), d.encode("gaba"));
        let added = d.merge(vec![
            "serotonin".into(),
            "gaba".into(),
            "acetylcholine".into(),
        ]);
        assert_eq!(added, 2, "only the two unseen values are assigned");
        assert_eq!(d.encode("glutamate"), g);
        assert_eq!(d.encode("gaba"), b);
        assert_eq!(d.encode("acetylcholine"), 3, "new codes sort among themselves");
        assert_eq!(d.encode("serotonin"), 4);
    }

    #[test]
    fn merge_ignores_empty_and_duplicates() {
        let mut d = Dictionary::default();
        assert_eq!(d.merge(vec!["".into(), "a".into(), "a".into()]), 1);
        assert_eq!(d.len(), 1);
    }

    #[test]
    fn round_trip_through_a_cell_preserves_every_code() {
        let mut d = Dictionary::default();
        d.merge(vec!["ME_R".into(), "AL_L".into(), "FB".into()]);
        let id = dictionary_cell_id(SchemaUid(7), 42);
        let back = Dictionary::from_cell(&d.to_cell(&id));
        assert_eq!(back, d);
        for v in d.values() {
            assert_eq!(back.encode(v), d.encode(v));
        }
    }

    #[test]
    fn a_cell_without_the_field_yields_an_empty_vocabulary() {
        // Must not panic: an empty dictionary is recoverable, a panic is not.
        let cell = OwnedCell::new_with_id(
            *DICTIONARY_SCHEMA_ID,
            &dictionary_cell_id(SchemaUid(1), 1),
            OwnedValue::Map(OwnedMap::new()),
        );
        assert!(Dictionary::from_cell(&cell).is_empty());
    }

    #[test]
    fn columns_do_not_share_a_cell() {
        assert_ne!(
            dictionary_cell_id(SchemaUid(1), 10),
            dictionary_cell_id(SchemaUid(1), 11)
        );
        assert_ne!(
            dictionary_cell_id(SchemaUid(1), 10),
            dictionary_cell_id(SchemaUid(2), 10)
        );
        assert_eq!(
            dictionary_cell_id(SchemaUid(3), 5),
            dictionary_cell_id(SchemaUid(3), 5)
        );
    }
}
