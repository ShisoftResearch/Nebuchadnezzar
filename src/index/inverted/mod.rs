use std::collections::HashMap;
use std::sync::Arc;

use bifrost::rpc::RPCError;
use bifrost_hasher::hash_str;

use crate::client::transaction::{Transaction, TxnError};
use crate::client::AsyncClient;
use crate::index::builder::IndexError;
use crate::ram::cell::{OwnedCell, ReadError};
use crate::ram::schema::{Field, Schema};
use crate::ram::types::{Id, Map, OwnedMap, OwnedPrimArray, OwnedValue, SharedValue};

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
    static ref DOC_IDS_FIELD_ID: u64 = hash_str(DOC_IDS_FIELD);
    static ref TERM_FREQS_FIELD_ID: u64 = hash_str(TERM_FREQS_FIELD);
    static ref DOC_LENGTHS_FIELD_ID: u64 = hash_str(DOC_LENGTHS_FIELD);
    static ref DOC_COUNT_FIELD_ID: u64 = hash_str(DOC_COUNT_FIELD);
    static ref TOTAL_LENGTH_FIELD_ID: u64 = hash_str(TOTAL_LENGTH_FIELD);
    static ref DOC_LENGTH_FIELD_ID: u64 = hash_str(DOC_LENGTH_FIELD);
}

#[derive(Debug, Clone)]
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
pub struct InvertedIndexMeta {
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

pub fn build_index_meta(
    cell_id: Id,
    schema_id: u32,
    field_id: u64,
    value: OwnedValue,
) -> Option<InvertedIndexMeta> {
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

    Some(InvertedIndexMeta {
        cell_id,
        schema_id,
        field_id,
        doc_length,
        tokens,
    })
}

pub struct InvertedIndexer {
    neb_client: Arc<AsyncClient>,
}

impl InvertedIndexer {
    pub fn new(neb_client: &Arc<AsyncClient>) -> Self {
        Self {
            neb_client: neb_client.clone(),
        }
    }
}

pub struct InvertedIndexClient {
    neb_client: Arc<AsyncClient>,
    pub indexer: InvertedIndexer,
}

impl InvertedIndexClient {
    pub fn new(neb_client: &Arc<AsyncClient>) -> Self {
        Self {
            neb_client: neb_client.clone(),
            indexer: InvertedIndexer::new(neb_client),
        }
    }

    pub async fn insert(&self, meta: &InvertedIndexMeta) -> Result<(), IndexError> {
        self.indexer
            .add_document(meta)
            .await
            .map_err(IndexError::TxnError)
    }

    pub async fn remove(&self, meta: &InvertedIndexMeta) -> Result<(), IndexError> {
        self.indexer
            .remove_document(meta)
            .await
            .map_err(IndexError::TxnError)
    }

    pub async fn bm25_search(
        &self,
        schema_id: u32,
        field_id: u64,
        query: &str,
        limit: usize,
    ) -> Result<Result<Vec<BM25Hit>, ReadError>, RPCError> {
        if query.trim().is_empty() || limit == 0 {
            return Ok(Ok(vec![]));
        }
        let query_terms = tokenize_query(query);
        if query_terms.is_empty() {
            return Ok(Ok(vec![]));
        }
        let stats_id = InvertedIndexer::stats_cell_id(schema_id, field_id);
        let stats_cell = match self.neb_client.read_cell(stats_id).await {
            Ok(Ok(cell)) => Some(cell),
            Ok(Err(ReadError::CellDoesNotExisted)) => None,
            Ok(Err(e)) => return Ok(Err(e)),
            Err(e) => return Err(e),
        };
        let stats = stats_cell
            .as_ref()
            .and_then(|cell| FieldStats::from_value(&cell.data))
            .unwrap_or_default();
        if stats.doc_count == 0 {
            return Ok(Ok(vec![]));
        }
        let avg_doc_len = stats.avg_length();
        let mut scores: HashMap<Id, f32> = HashMap::new();
        for term_hash in query_terms {
            let term_id = InvertedIndexer::term_cell_id(schema_id, field_id, term_hash);
            let posting_cell = match self.neb_client.read_cell(term_id).await {
                Ok(Ok(cell)) => cell,
                Ok(Err(ReadError::CellDoesNotExisted)) => continue,
                Ok(Err(e)) => return Ok(Err(e)),
                Err(e) => return Err(e),
            };
            if let Some(postings) = PostingList::from_value(&posting_cell.data) {
                let df = postings.len() as u64;
                if df == 0 {
                    continue;
                }
                let idf = compute_idf(stats.doc_count, df);
                if idf <= 0.0 {
                    continue;
                }
                for (doc_id, tf, doc_len) in postings.iter() {
                    let inc = bm25_score(tf as f32, doc_len as f32, avg_doc_len, idf);
                    if inc <= 0.0 {
                        continue;
                    }
                    scores
                        .entry(doc_id)
                        .and_modify(|score| *score += inc)
                        .or_insert(inc);
                }
            }
        }
        let mut hits = scores
            .into_iter()
            .map(|(id, score)| BM25Hit { id, score })
            .collect::<Vec<_>>();
        hits.sort_by(|a, b| {
            b.score
                .partial_cmp(&a.score)
                .unwrap_or(std::cmp::Ordering::Equal)
        });
        if hits.len() > limit {
            hits.truncate(limit);
        }
        Ok(Ok(hits))
    }
}

#[derive(Debug, Default, Clone)]
struct FieldStats {
    doc_count: u64,
    total_length: u64,
}

impl FieldStats {
    fn from_value(value: &OwnedValue) -> Option<Self> {
        if let OwnedValue::Map(_) = value {
            let doc_count = match &value[*DOC_COUNT_FIELD_ID] {
                OwnedValue::U64(v) => *v,
                _ => 0,
            };
            let total_length = match &value[*TOTAL_LENGTH_FIELD_ID] {
                OwnedValue::U64(v) => *v,
                _ => 0,
            };
            Some(FieldStats {
                doc_count,
                total_length,
            })
        } else {
            None
        }
    }

    fn to_value(&self) -> OwnedValue {
        let mut map = OwnedMap::new();
        map.insert_key_id(*DOC_COUNT_FIELD_ID, OwnedValue::U64(self.doc_count));
        map.insert_key_id(*TOTAL_LENGTH_FIELD_ID, OwnedValue::U64(self.total_length));
        OwnedValue::Map(map)
    }

    fn apply_upsert(&mut self, new_len: u32, previous_len: Option<u32>) {
        match previous_len {
            Some(prev) => {
                if self.total_length >= prev as u64 {
                    self.total_length -= prev as u64;
                } else {
                    self.total_length = 0;
                }
                self.total_length += new_len as u64;
            }
            None => {
                self.doc_count += 1;
                self.total_length += new_len as u64;
            }
        }
    }

    fn apply_remove(&mut self, removed_len: u32) {
        if self.doc_count > 0 {
            self.doc_count -= 1;
        }
        if self.total_length >= removed_len as u64 {
            self.total_length -= removed_len as u64;
        } else {
            self.total_length = 0;
        }
    }

    fn avg_length(&self) -> f32 {
        if self.doc_count == 0 {
            return 1.0;
        }
        let avg = self.total_length as f32 / self.doc_count as f32;
        if avg <= 0.0 {
            1.0
        } else {
            avg
        }
    }
}

#[derive(Clone)]
struct PostingList {
    doc_ids: Vec<Id>,
    term_freqs: Vec<u32>,
    doc_lengths: Vec<u32>,
}

impl PostingList {
    fn new() -> Self {
        Self {
            doc_ids: vec![],
            term_freqs: vec![],
            doc_lengths: vec![],
        }
    }

    fn from_value(value: &OwnedValue) -> Option<Self> {
        if let OwnedValue::Map(_) = value {
            let doc_ids = match &value[*DOC_IDS_FIELD_ID] {
                OwnedValue::PrimArray(OwnedPrimArray::Id(ids)) => ids.clone(),
                _ => vec![],
            };
            let term_freqs = match &value[*TERM_FREQS_FIELD_ID] {
                OwnedValue::PrimArray(OwnedPrimArray::U32(freqs)) => freqs.clone(),
                _ => vec![],
            };
            let doc_lengths = match &value[*DOC_LENGTHS_FIELD_ID] {
                OwnedValue::PrimArray(OwnedPrimArray::U32(lengths)) => lengths.clone(),
                _ => vec![],
            };
            Some(PostingList {
                doc_ids,
                term_freqs,
                doc_lengths,
            })
        } else {
            None
        }
    }

    fn len(&self) -> usize {
        self.doc_ids.len()
    }

    fn into_value(self) -> OwnedValue {
        let mut map = OwnedMap::new();
        map.insert_key_id(
            *DOC_IDS_FIELD_ID,
            OwnedValue::PrimArray(OwnedPrimArray::Id(self.doc_ids)),
        );
        map.insert_key_id(
            *TERM_FREQS_FIELD_ID,
            OwnedValue::PrimArray(OwnedPrimArray::U32(self.term_freqs)),
        );
        map.insert_key_id(
            *DOC_LENGTHS_FIELD_ID,
            OwnedValue::PrimArray(OwnedPrimArray::U32(self.doc_lengths)),
        );
        OwnedValue::Map(map)
    }

    fn upsert(&mut self, doc_id: Id, tf: u32, doc_length: u32) {
        if let Some(pos) = self.doc_ids.iter().position(|id| *id == doc_id) {
            self.term_freqs[pos] = tf;
            self.doc_lengths[pos] = doc_length;
        } else {
            self.doc_ids.push(doc_id);
            self.term_freqs.push(tf);
            self.doc_lengths.push(doc_length);
        }
    }

    fn remove(&mut self, doc_id: Id) -> bool {
        if let Some(pos) = self.doc_ids.iter().position(|id| *id == doc_id) {
            self.doc_ids.swap_remove(pos);
            self.term_freqs.swap_remove(pos);
            self.doc_lengths.swap_remove(pos);
            true
        } else {
            false
        }
    }

    fn iter(&self) -> impl Iterator<Item = (Id, u32, u32)> + '_ {
        self.doc_ids
            .iter()
            .cloned()
            .zip(self.term_freqs.iter().cloned())
            .zip(self.doc_lengths.iter().cloned())
            .map(|((doc_id, tf), len)| (doc_id, tf, len))
    }
}

impl InvertedIndexer {
    fn term_cell_id(schema_id: u32, field_id: u64, term_hash: u64) -> Id {
        Id::from_obj(&(schema_id, field_id, term_hash))
    }

    fn stats_cell_id(schema_id: u32, field_id: u64) -> Id {
        Id::from_obj(&(schema_id, field_id, b"stats"))
    }

    fn doc_meta_cell_id(schema_id: u32, field_id: u64, doc_id: &Id) -> Id {
        Id::from_obj(&(schema_id, field_id, doc_id.higher, doc_id.lower))
    }

    async fn load_stats(
        txn: &Transaction,
        schema_id: u32,
        field_id: u64,
    ) -> Result<FieldStats, TxnError> {
        let stats_id = Self::stats_cell_id(schema_id, field_id);
        match txn.read(stats_id).await? {
            Some(cell) => Ok(FieldStats::from_value(&cell.data).unwrap_or_default()),
            None => Ok(FieldStats::default()),
        }
    }

    async fn persist_stats(
        txn: &Transaction,
        schema_id: u32,
        field_id: u64,
        stats: &FieldStats,
    ) -> Result<(), TxnError> {
        let stats_id = Self::stats_cell_id(schema_id, field_id);
        let cell = OwnedCell::new_with_id(*INVERTED_STATS_SCHEMA_ID, &stats_id, stats.to_value());
        match txn.read(stats_id).await? {
            Some(_) => txn.update(cell).await,
            None => txn.write(cell).await,
        }
    }

    async fn upsert_doc_meta(
        txn: &Transaction,
        schema_id: u32,
        field_id: u64,
        doc_id: Id,
        doc_length: u32,
    ) -> Result<Option<u32>, TxnError> {
        let meta_id = Self::doc_meta_cell_id(schema_id, field_id, &doc_id);
        match txn.read(meta_id).await? {
            Some(mut cell) => {
                let prev = extract_doc_length(&cell);
                cell[*DOC_LENGTH_FIELD_ID] = OwnedValue::U32(doc_length);
                txn.update(cell).await?;
                Ok(prev)
            }
            None => {
                let mut map = OwnedMap::new();
                map.insert_key_id(*DOC_LENGTH_FIELD_ID, OwnedValue::U32(doc_length));
                let cell =
                    OwnedCell::new_with_id(*INVERTED_DOC_SCHEMA_ID, &meta_id, OwnedValue::Map(map));
                txn.write(cell).await?;
                Ok(None)
            }
        }
    }

    async fn remove_doc_meta(
        txn: &Transaction,
        schema_id: u32,
        field_id: u64,
        doc_id: Id,
    ) -> Result<Option<u32>, TxnError> {
        let meta_id = Self::doc_meta_cell_id(schema_id, field_id, &doc_id);
        match txn.read(meta_id).await? {
            Some(cell) => {
                let doc_length = extract_doc_length(&cell);
                txn.remove(meta_id).await?;
                Ok(doc_length)
            }
            None => Ok(None),
        }
    }

    async fn upsert_posting(
        txn: &Transaction,
        meta: &InvertedIndexMeta,
        token: &TokenStat,
    ) -> Result<(), TxnError> {
        let term_id = Self::term_cell_id(meta.schema_id, meta.field_id, token.term_hash);
        match txn.read(term_id).await? {
            Some(mut cell) => {
                let mut postings =
                    PostingList::from_value(&cell.data).unwrap_or_else(PostingList::new);
                postings.upsert(meta.cell_id, token.term_freq, meta.doc_length);
                cell.data = postings.into_value();
                txn.update(cell).await
            }
            None => {
                let mut postings = PostingList::new();
                postings.upsert(meta.cell_id, token.term_freq, meta.doc_length);
                let cell = OwnedCell::new_with_id(
                    *INVERTED_INDEX_SCHEMA_ID,
                    &term_id,
                    postings.into_value(),
                );
                txn.write(cell).await
            }
        }
    }

    async fn remove_posting(
        txn: &Transaction,
        meta: &InvertedIndexMeta,
        token: &TokenStat,
    ) -> Result<(), TxnError> {
        let term_id = Self::term_cell_id(meta.schema_id, meta.field_id, token.term_hash);
        match txn.read(term_id).await? {
            Some(mut cell) => {
                let mut postings =
                    PostingList::from_value(&cell.data).unwrap_or_else(PostingList::new);
                if postings.remove(meta.cell_id) {
                    if postings.len() == 0 {
                        txn.remove(term_id).await
                    } else {
                        cell.data = postings.into_value();
                        txn.update(cell).await
                    }
                } else {
                    Ok(())
                }
            }
            None => Ok(()),
        }
    }

    pub async fn add_document(&self, meta: &InvertedIndexMeta) -> Result<(), TxnError> {
        let meta = Arc::new(meta.clone());
        self.neb_client
            .transaction({
                let meta = meta.clone();
                move |txn| {
                    let meta = meta.clone();
                    async move {
                        let meta_ref = meta.as_ref();
                        let prev_length = Self::upsert_doc_meta(
                            txn,
                            meta_ref.schema_id,
                            meta_ref.field_id,
                            meta_ref.cell_id,
                            meta_ref.doc_length,
                        )
                        .await?;
                        let mut stats =
                            Self::load_stats(txn, meta_ref.schema_id, meta_ref.field_id).await?;
                        stats.apply_upsert(meta_ref.doc_length, prev_length);
                        Self::persist_stats(txn, meta_ref.schema_id, meta_ref.field_id, &stats)
                            .await?;
                        for token in meta_ref.tokens.iter() {
                            Self::upsert_posting(txn, meta_ref, token).await?;
                        }
                        Ok(())
                    }
                }
            })
            .await
    }

    pub async fn remove_document(&self, meta: &InvertedIndexMeta) -> Result<(), TxnError> {
        let meta = Arc::new(meta.clone());
        self.neb_client
            .transaction({
                let meta = meta.clone();
                move |txn| {
                    let meta = meta.clone();
                    async move {
                        let meta_ref = meta.as_ref();
                        let existing_length = Self::remove_doc_meta(
                            txn,
                            meta_ref.schema_id,
                            meta_ref.field_id,
                            meta_ref.cell_id,
                        )
                        .await?;
                        let doc_length = match existing_length {
                            Some(len) => len,
                            None => return Ok(()),
                        };
                        let mut stats =
                            Self::load_stats(txn, meta_ref.schema_id, meta_ref.field_id).await?;
                        stats.apply_remove(doc_length);
                        Self::persist_stats(txn, meta_ref.schema_id, meta_ref.field_id, &stats)
                            .await?;
                        for token in meta_ref.tokens.iter() {
                            Self::remove_posting(txn, meta_ref, token).await?;
                        }
                        Ok(())
                    }
                }
            })
            .await
    }
}

fn extract_doc_length(cell: &OwnedCell) -> Option<u32> {
    if let OwnedValue::Map(_) = cell.data {
        match &cell.data[*DOC_LENGTH_FIELD_ID] {
            OwnedValue::U32(v) => Some(*v),
            _ => None,
        }
    } else {
        None
    }
}

fn tokenize_query(query: &str) -> Vec<u64> {
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

fn compute_idf(doc_count: u64, df: u64) -> f32 {
    if df == 0 || doc_count == 0 {
        return 0.0;
    }
    let numerator = (doc_count as f32 - df as f32 + 0.5).max(0.5);
    let denominator = df as f32 + 0.5;
    ((numerator / denominator) + 1.0).ln()
}

fn bm25_score(tf: f32, doc_len: f32, avg_doc_len: f32, idf: f32) -> f32 {
    if tf <= 0.0 {
        return 0.0;
    }
    let denom = tf + BM25_K1 * (1.0 - BM25_B + BM25_B * (doc_len / avg_doc_len));
    if denom <= 0.0 {
        return 0.0;
    }
    idf * (tf * (BM25_K1 + 1.0)) / denom
}
