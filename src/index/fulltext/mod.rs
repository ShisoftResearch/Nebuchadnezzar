use std::collections::HashMap;
use std::sync::Arc;

use bifrost::rpc::RPCError;
use bifrost_hasher::hash_str;
use serde::{Deserialize, Serialize};

use crate::index::builder::IndexError;
use crate::ram::cell::{OwnedCell, ReadError};
use crate::ram::schema::{Field, Schema};
use crate::ram::types::{Id, Map, OwnedMap, OwnedPrimArray, OwnedValue, SharedValue};

// Submodules
pub mod coordinator;
pub mod hybrid;
pub mod rpc;

// ============================================================================
// Constants and Configuration
// ============================================================================

pub const BM25_K1: f32 = 1.5;
pub const BM25_B: f32 = 0.75;
const MIN_TOKEN_LEN: usize = 2;

const INVERTED_INDEX_SCHEMA: &str = "INVERTED_INDEX_SCHEMA";
const INVERTED_STATS_SCHEMA: &str = "INVERTED_STATS_SCHEMA";
const INVERTED_DOC_SCHEMA: &str = "INVERTED_DOC_SCHEMA";

const DOC_IDS_FIELD: &str = "DOC_IDS";
const TERM_FREQS_FIELD: &str = "TERM_FREQS";
const DOC_LENGTHS_FIELD: &str = "DOC_LENGTHS";
const DOC_COUNT_FIELD: &str = "DOC_COUNT";
const TOTAL_LENGTH_FIELD: &str = "TOTAL_LENGTH";
const DOC_LENGTH_FIELD: &str = "DOC_LENGTH";

lazy_static! {
    pub static ref INVERTED_INDEX_SCHEMA_ID: u32 = hash_str(INVERTED_INDEX_SCHEMA) as u32;
    pub static ref INVERTED_STATS_SCHEMA_ID: u32 = hash_str(INVERTED_STATS_SCHEMA) as u32;
    pub static ref INVERTED_DOC_SCHEMA_ID: u32 = hash_str(INVERTED_DOC_SCHEMA) as u32;

    // Made public for use in submodules
    pub(crate) static ref DOC_IDS_FIELD_ID: u64 = hash_str(DOC_IDS_FIELD);
    pub(crate) static ref TERM_FREQS_FIELD_ID: u64 = hash_str(TERM_FREQS_FIELD);
    pub(crate) static ref DOC_LENGTHS_FIELD_ID: u64 = hash_str(DOC_LENGTHS_FIELD);
    pub(crate) static ref DOC_COUNT_FIELD_ID: u64 = hash_str(DOC_COUNT_FIELD);
    pub(crate) static ref TOTAL_LENGTH_FIELD_ID: u64 = hash_str(TOTAL_LENGTH_FIELD);
    pub(crate) static ref DOC_LENGTH_FIELD_ID: u64 = hash_str(DOC_LENGTH_FIELD);
}

// ============================================================================
// Public Data Structures
// ============================================================================

#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct BM25Hit {
    pub id: Id,
    pub score: f32,
}

#[derive(Clone, Debug, Hash, PartialEq, Eq)]
pub struct TokenStat {
    pub term_hash: u64,
    pub term_freq: u32,
}

#[derive(Clone, Debug, Hash, PartialEq, Eq)]
pub struct FullTextIndexMeta {
    pub cell_id: Id,
    pub schema_id: u32,
    pub field_id: u64,
    pub doc_length: u32,
    pub tokens: Vec<TokenStat>,
}

pub trait ToOwnedValue {
    fn to_owned_value(&self) -> OwnedValue;
}

impl ToOwnedValue for OwnedValue {
    fn to_owned_value(&self) -> OwnedValue {
        self.clone()
    }
}

impl<'a> ToOwnedValue for SharedValue<'a> {
    fn to_owned_value(&self) -> OwnedValue {
        self.owned()
    }
}

impl<T: ToOwnedValue + ?Sized> ToOwnedValue for &T {
    fn to_owned_value(&self) -> OwnedValue {
        (**self).to_owned_value()
    }
}

// ============================================================================
// Schema Definitions
// ============================================================================

pub fn inverted_index_schema() -> Schema {
    Schema::new_with_id(
        *INVERTED_INDEX_SCHEMA_ID,
        &INVERTED_INDEX_SCHEMA.to_string(),
        None,
        Field::new_schema(vec![
            Field::new_unindexed_array(DOC_IDS_FIELD, dovahkiin::types::Type::Id),
            Field::new_unindexed_array(TERM_FREQS_FIELD, dovahkiin::types::Type::U32),
            Field::new_unindexed_array(DOC_LENGTHS_FIELD, dovahkiin::types::Type::U32),
        ]),
        false,
        false,
    )
}

pub fn inverted_stats_schema() -> Schema {
    Schema::new_with_id(
        *INVERTED_STATS_SCHEMA_ID,
        &INVERTED_STATS_SCHEMA.to_string(),
        None,
        Field::new_schema(vec![
            Field::new_unindexed(DOC_COUNT_FIELD, dovahkiin::types::Type::U64),
            Field::new_unindexed(TOTAL_LENGTH_FIELD, dovahkiin::types::Type::U64),
        ]),
        false,
        false,
    )
}

pub fn inverted_doc_schema() -> Schema {
    Schema::new_with_id(
        *INVERTED_DOC_SCHEMA_ID,
        &INVERTED_DOC_SCHEMA.to_string(),
        None,
        Field::new_schema(vec![Field::new_unindexed(
            DOC_LENGTH_FIELD,
            dovahkiin::types::Type::U32,
        )]),
        false,
        false,
    )
}

// ============================================================================
// Index Metadata Builder (shared by all implementations)
// ============================================================================

pub fn build_index_meta(
    cell_id: Id,
    schema_id: u32,
    field_id: u64,
    value: OwnedValue,
) -> Option<FullTextIndexMeta> {
    let mut term_counts: HashMap<u64, u32> = HashMap::new();
    let mut doc_length: u32 = 0;

    let mut record_tokens = |text: &str| {
        for raw in text.split(|c: char| !c.is_alphanumeric()) {
            if raw.len() < MIN_TOKEN_LEN {
                continue;
            }
            let token = raw.to_lowercase();
            if token.len() < MIN_TOKEN_LEN {
                continue;
            }
            doc_length = doc_length.saturating_add(1);
            let hash = hash_str(&token);
            *term_counts.entry(hash).or_insert(0) += 1;
        }
    };

    match value {
        OwnedValue::String(text) => record_tokens(&text),
        OwnedValue::PrimArray(OwnedPrimArray::String(items)) => {
            for text in items {
                record_tokens(&text);
            }
        }
        OwnedValue::Null => {}
        _ => return None,
    }

    if term_counts.is_empty() {
        return None;
    }

    let mut tokens = term_counts
        .into_iter()
        .map(|(term_hash, term_freq)| TokenStat {
            term_hash,
            term_freq,
        })
        .collect::<Vec<_>>();
    tokens.sort_by_key(|stat| stat.term_hash);

    Some(FullTextIndexMeta {
        cell_id,
        schema_id,
        field_id,
        doc_length,
        tokens,
    })
}

// ============================================================================
// Utility Functions (shared by all implementations)
// ============================================================================

pub(crate) fn tokenize_query(query: &str) -> Vec<u64> {
    let mut seen = HashMap::new();
    for raw in query.split(|c: char| !c.is_alphanumeric()) {
        if raw.len() < MIN_TOKEN_LEN {
            continue;
        }
        let norm = raw.to_lowercase();
        if norm.len() < MIN_TOKEN_LEN {
            continue;
        }
        let hash = hash_str(&norm);
        seen.entry(hash).or_insert(0u32);
    }
    seen.into_iter().map(|(hash, _)| hash).collect()
}

pub(crate) fn compute_idf(doc_count: u64, df: u64) -> f32 {
    if df == 0 || doc_count == 0 {
        return 0.0;
    }
    let numerator = (doc_count as f32 - df as f32 + 0.5).max(0.5);
    let denominator = df as f32 + 0.5;
    ((numerator / denominator) + 1.0).ln()
}

pub(crate) fn bm25_score(tf: f32, doc_len: f32, avg_doc_len: f32, idf: f32) -> f32 {
    if tf <= 0.0 {
        return 0.0;
    }
    let denom = tf + BM25_K1 * (1.0 - BM25_B + BM25_B * (doc_len / avg_doc_len));
    if denom <= 0.0 {
        return 0.0;
    }
    idf * (tf * (BM25_K1 + 1.0)) / denom
}
