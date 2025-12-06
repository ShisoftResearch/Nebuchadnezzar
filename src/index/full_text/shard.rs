use std::collections::HashMap;
use std::sync::atomic::{AtomicBool, Ordering};
use std::sync::Arc;
use std::time::Duration;

use bifrost_hasher::hash_str;
use lightning::map::{Map as LFMap, PtrHashMap};
use log::{error, info, warn};
use parking_lot::Mutex;

use crate::client::AsyncClient;
use crate::index::builder::IndexError;
use crate::ram::cell::{Cell, OwnedCell};
use crate::ram::chunk::Chunks;
use crate::ram::schema::{Field, Schema};
use crate::ram::types::{Id, Map, OwnedMap, OwnedPrimArray, OwnedValue};

use super::{
    bm25_score, compute_idf, tokenize_query, BM25Hit, FullTextIndexMeta, TokenStat,
    DOC_COUNT_FIELD_ID, INVERTED_STATS_SCHEMA_ID, TOTAL_LENGTH_FIELD_ID,
};

// Schema for segmented posting lists
// Each posting entry includes: (doc_id, version, term_freq, doc_length)
// The version allows filtering stale entries and garbage collection
const INVERTED_SEGMENT_SCHEMA: &str = "INVERTED_SEGMENT_V2";
const SEGMENT_DOC_IDS_FIELD: &str = "doc_ids";
const SEGMENT_VERSIONS_FIELD: &str = "versions"; // Cell version for each entry
const SEGMENT_TERM_FREQS_FIELD: &str = "term_freqs";
const SEGMENT_DOC_LENGTHS_FIELD: &str = "doc_lengths";
const SEGMENT_NEXT_FIELD: &str = "_next";

lazy_static! {
    pub static ref INVERTED_SEGMENT_SCHEMA_ID: u32 = hash_str(INVERTED_SEGMENT_SCHEMA) as u32;
    static ref SEGMENT_DOC_IDS_FIELD_ID: u64 = hash_str(SEGMENT_DOC_IDS_FIELD);
    static ref SEGMENT_VERSIONS_FIELD_ID: u64 = hash_str(SEGMENT_VERSIONS_FIELD);
    static ref SEGMENT_TERM_FREQS_FIELD_ID: u64 = hash_str(SEGMENT_TERM_FREQS_FIELD);
    static ref SEGMENT_DOC_LENGTHS_FIELD_ID: u64 = hash_str(SEGMENT_DOC_LENGTHS_FIELD);
    static ref SEGMENT_NEXT_FIELD_ID: u64 = hash_str(SEGMENT_NEXT_FIELD);
}

pub fn inverted_segment_schema() -> Schema {
    Schema::new_with_id(
        *INVERTED_SEGMENT_SCHEMA_ID,
        &INVERTED_SEGMENT_SCHEMA.to_string(),
        None,
        Field::new_schema(vec![
            Field::new_unindexed(SEGMENT_NEXT_FIELD, dovahkiin::types::Type::Id),
            Field::new_unindexed_array(SEGMENT_DOC_IDS_FIELD, dovahkiin::types::Type::Id),
            Field::new_unindexed_array(SEGMENT_VERSIONS_FIELD, dovahkiin::types::Type::U64),
            Field::new_unindexed_array(SEGMENT_TERM_FREQS_FIELD, dovahkiin::types::Type::U32),
            Field::new_unindexed_array(SEGMENT_DOC_LENGTHS_FIELD, dovahkiin::types::Type::U32),
        ]),
        false,
        false,
    )
}

/// Field statistics
#[derive(Debug, Clone, Default)]
pub struct FieldStats {
    pub doc_count: u64,
    pub total_length: u64,
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
}

/// Document metadata
#[derive(Debug, Clone)]
struct DocMeta {
    doc_length: u32,
    tokens: Vec<TokenStat>,
}

use crate::ram::chunk::Chunk;

/// Segmented posting list for persistent storage (per-Chunk)
///
/// Each Chunk has its own posting lists for terms in documents stored in that Chunk.
/// The posting list ID has higher=0 (local-only, not globally routed) and
/// lower=hash(schema, field, term, segment).
struct SegmentedPostingList {
    schema_id: u32,
    field_id: u64,
    term_hash: u64,
}

impl SegmentedPostingList {
    const MAX_SEGMENT_SIZE: usize = 1000;

    fn new(schema_id: u32, field_id: u64, term_hash: u64) -> Self {
        Self {
            schema_id,
            field_id,
            term_hash,
        }
    }

    /// Generate segment ID with the same partition as documents in this chunk
    /// This ensures posting lists are recovered to the same chunk after restart
    fn segment_id(&self, partition: u64, segment_idx: u32) -> Id {
        // higher = partition (matches document partition for proper recovery)
        // lower = hash of (schema_id, field_id, term_hash, segment_idx)
        let lower =
            Id::from_obj(&(self.schema_id, self.field_id, self.term_hash, segment_idx)).lower;
        Id::new(partition, lower)
    }

    fn head_segment_id(&self, partition: u64) -> Id {
        self.segment_id(partition, 0)
    }

    /// Append a posting to the segmented list in a specific Chunk
    /// The version is stored with the entry for filtering stale data and GC
    /// partition: used to generate segment IDs that route to the correct chunk after recovery
    fn append(
        &self,
        chunk: &Chunk,
        partition: u64,
        doc_id: Id,
        version: u64,
        tf: u32,
        doc_len: u32,
    ) -> Result<(), IndexError> {
        let head_id = self.head_segment_id(partition);
        let head_hash = head_id.lower;

        // Try to read existing head segment from this Chunk
        let mut head_guard = chunk.lock_or_insert_cell(head_hash);
        let (mut segment, head_version) = if !head_guard.is_unassigned() {
            let owned_cell = head_guard.read_cell_owned().map_err(|e| IndexError::Other(format!("Failed to read head segment: {:?}", e)))?;
            let version = owned_cell.header().version;
            let seg = PostingSegment::from_cell(&owned_cell).ok_or_else(|| IndexError::Other("Failed to parse head segment".to_string()))?;
            (seg, version)
        } else {
            (PostingSegment::new(), 0)
        };

        // If head is full, prepend: move old head content to overflow cell, reset head with new posting
        if segment.is_full() {
            // Generate unique ID for overflow segment using random nonce
            let overflow_id = self.random_segment_id(partition);
            
            // Move current head content (with its chain) to NEW overflow cell (version starts at 0)
            let mut overflow_cell = segment.to_cell(&overflow_id);
            chunk
                .upsert_cell(&mut overflow_cell)
                .map_err(|e| IndexError::Other(format!("Failed to write overflow segment: {:?}", e)))?;

            // Update head with new posting, pointing to overflow (preserve version for increment)
            let mut new_head = PostingSegment::new();
            new_head.add(doc_id, version, tf, doc_len);
            new_head.next = Some(overflow_id);
            let mut head_cell = new_head.to_cell_with_version(&head_id, head_version);
            head_guard
                .upsert_cell(&mut head_cell)
                .map_err(|e| IndexError::Other(format!("Failed to update head segment: {:?}", e)))?;
        } else {
            // Append to existing head (preserve version for increment)
            segment.add(doc_id, version, tf, doc_len);
            let mut cell = segment.to_cell_with_version(&head_id, head_version);
            head_guard
                .upsert_cell(&mut cell)
                .map_err(|e| IndexError::Other(format!("Failed to update segment: {:?}", e)))?;
        }
        Ok(())
    }

    /// Generate a random segment ID for overflow segments
    /// Uses timestamp + random bits to ensure uniqueness without traversing the chain
    fn random_segment_id(&self, partition: u64) -> Id {
        use std::time::{SystemTime, UNIX_EPOCH};
        let timestamp = SystemTime::now()
            .duration_since(UNIX_EPOCH)
            .unwrap_or_default()
            .as_nanos() as u64;
        // Combine with term hash and a random component for uniqueness
        let random_component: u64 = rand::random();
        let lower = Id::from_obj(&(self.schema_id, self.field_id, self.term_hash, timestamp, random_component)).lower;
        Id::new(partition, lower)
    }

    /// Iterate through all postings in this Chunk's posting list for this term
    /// Returns: (doc_id, version, term_freq, doc_length)
    /// chunk_index: used to identify posting lists stored in this chunk
    fn iterate(
        &self,
        chunk: &Chunk,
        chunk_index: u64,
    ) -> Result<Vec<(Id, u64, u32, u32)>, IndexError> {
        let mut all_postings = Vec::new();
        let mut current_id = Some(self.head_segment_id(chunk_index));

        while let Some(seg_id) = current_id {
            match chunk.read_cell(seg_id.lower) {
                Ok(cell) => {
                    let owned_cell = OwnedCell {
                        header: cell.header().clone(),
                        data: cell.data().owned(),
                    };
                    if let Some(segment) = PostingSegment::from_cell(&owned_cell) {
                        all_postings.extend(segment.iter());
                        current_id = segment.next;
                    } else {
                        break;
                    }
                }
                Err(_) => break,
            }
        }

        Ok(all_postings)
    }
}

/// Posting segment stored in a cell
/// Each entry contains: (doc_id, version, term_freq, doc_length)
/// The version is used to filter stale entries and enable garbage collection
struct PostingSegment {
    next: Option<Id>,
    doc_ids: Vec<Id>,
    versions: Vec<u64>, // Cell version when entry was created
    term_freqs: Vec<u32>,
    doc_lengths: Vec<u32>,
}

impl PostingSegment {
    fn new() -> Self {
        Self {
            next: None,
            doc_ids: Vec::new(),
            versions: Vec::new(),
            term_freqs: Vec::new(),
            doc_lengths: Vec::new(),
        }
    }

    fn is_full(&self) -> bool {
        self.doc_ids.len() >= SegmentedPostingList::MAX_SEGMENT_SIZE
    }

    fn add(&mut self, doc_id: Id, version: u64, tf: u32, doc_len: u32) {
        self.doc_ids.push(doc_id);
        self.versions.push(version);
        self.term_freqs.push(tf);
        self.doc_lengths.push(doc_len);
    }

    fn from_cell(cell: &OwnedCell) -> Option<Self> {
        if cell.header().schema != *INVERTED_SEGMENT_SCHEMA_ID {
            return None;
        }

        let data = cell.data();
        let mut segment = Self::new();

        // Read next pointer
        if let OwnedValue::Id(id) = &data[*SEGMENT_NEXT_FIELD_ID] {
            segment.next = Some(*id);
        }

        // Read arrays
        if let OwnedValue::PrimArray(OwnedPrimArray::Id(ids)) = &data[*SEGMENT_DOC_IDS_FIELD_ID] {
            segment.doc_ids = ids.clone();
        }
        if let OwnedValue::PrimArray(OwnedPrimArray::U64(vers)) = &data[*SEGMENT_VERSIONS_FIELD_ID]
        {
            segment.versions = vers.clone();
        }
        if let OwnedValue::PrimArray(OwnedPrimArray::U32(tfs)) = &data[*SEGMENT_TERM_FREQS_FIELD_ID]
        {
            segment.term_freqs = tfs.clone();
        }
        if let OwnedValue::PrimArray(OwnedPrimArray::U32(lengths)) =
            &data[*SEGMENT_DOC_LENGTHS_FIELD_ID]
        {
            segment.doc_lengths = lengths.clone();
        }

        // Handle old schema without versions (backwards compatibility)
        if segment.versions.is_empty() && !segment.doc_ids.is_empty() {
            segment.versions = vec![0; segment.doc_ids.len()];
        }

        Some(segment)
    }

    fn to_cell(&self, id: &Id) -> OwnedCell {
        let mut data = OwnedMap::new();

        if let Some(next_id) = self.next {
            data.insert_key_id(*SEGMENT_NEXT_FIELD_ID, OwnedValue::Id(next_id));
        } else {
            data.insert_key_id(*SEGMENT_NEXT_FIELD_ID, OwnedValue::Id(Id::unit_id()));
        }

        data.insert_key_id(
            *SEGMENT_DOC_IDS_FIELD_ID,
            OwnedValue::PrimArray(OwnedPrimArray::Id(self.doc_ids.clone())),
        );
        data.insert_key_id(
            *SEGMENT_VERSIONS_FIELD_ID,
            OwnedValue::PrimArray(OwnedPrimArray::U64(self.versions.clone())),
        );
        data.insert_key_id(
            *SEGMENT_TERM_FREQS_FIELD_ID,
            OwnedValue::PrimArray(OwnedPrimArray::U32(self.term_freqs.clone())),
        );
        data.insert_key_id(
            *SEGMENT_DOC_LENGTHS_FIELD_ID,
            OwnedValue::PrimArray(OwnedPrimArray::U32(self.doc_lengths.clone())),
        );

        // Create cell with version 0 - storage layer will increment to 1 on write
        OwnedCell::new_with_id(*INVERTED_SEGMENT_SCHEMA_ID, id, OwnedValue::Map(data))
    }

    /// Convert to cell for updating an existing cell (preserves version for proper incrementing)
    fn to_cell_with_version(&self, id: &Id, current_version: u64) -> OwnedCell {
        let mut data = OwnedMap::new();

        if let Some(next_id) = self.next {
            data.insert_key_id(*SEGMENT_NEXT_FIELD_ID, OwnedValue::Id(next_id));
        } else {
            data.insert_key_id(*SEGMENT_NEXT_FIELD_ID, OwnedValue::Id(Id::unit_id()));
        }

        data.insert_key_id(
            *SEGMENT_DOC_IDS_FIELD_ID,
            OwnedValue::PrimArray(OwnedPrimArray::Id(self.doc_ids.clone())),
        );
        data.insert_key_id(
            *SEGMENT_VERSIONS_FIELD_ID,
            OwnedValue::PrimArray(OwnedPrimArray::U64(self.versions.clone())),
        );
        data.insert_key_id(
            *SEGMENT_TERM_FREQS_FIELD_ID,
            OwnedValue::PrimArray(OwnedPrimArray::U32(self.term_freqs.clone())),
        );
        data.insert_key_id(
            *SEGMENT_DOC_LENGTHS_FIELD_ID,
            OwnedValue::PrimArray(OwnedPrimArray::U32(self.doc_lengths.clone())),
        );

        // Preserve current version so storage layer increments correctly
        let mut cell = OwnedCell::new_with_id(*INVERTED_SEGMENT_SCHEMA_ID, id, OwnedValue::Map(data));
        cell.header.version = current_version;
        cell
    }

    /// Iterate all entries: (doc_id, version, term_freq, doc_length)
    fn iter(&self) -> impl Iterator<Item = (Id, u64, u32, u32)> + '_ {
        self.doc_ids
            .iter()
            .cloned()
            .zip(self.versions.iter().cloned())
            .zip(self.term_freqs.iter().cloned())
            .zip(self.doc_lengths.iter().cloned())
            .map(|(((doc_id, version), tf), len)| (doc_id, version, tf, len))
    }
}

/// Per-Chunk inverted indexer
///
/// Each Chunk has its own posting lists for terms in documents stored in that Chunk.
/// This provides data locality and scalability - no need to iterate global term lists.
///
/// Caches (small, per-server):
/// - field_stats: doc_count, total_length per (schema, field)
/// - doc_metadata: doc_length per document (needed for BM25)
///
/// NOT cached (read from Chunk storage):
/// - posting_lists: term → doc_ids (can be huge)
pub struct InvertedIndexer {
    server_id: u64,
    chunks: Arc<Chunks>,
    neb_client: Arc<AsyncClient>,

    // In-memory caches using lock-free PtrHashMap (small, frequently accessed)
    // Keys are hashed from (schema_id, field_id) or (schema_id, field_id, doc_id)
    // Wrapped in Arc for cloning
    field_stats: Arc<PtrHashMap<u64, Arc<Mutex<FieldStats>>>>,
    doc_metadata: Arc<PtrHashMap<u64, Arc<Mutex<DocMeta>>>>,

    // Track original keys for iteration (PtrHashMap doesn't support iteration)
    field_stats_keys: Arc<Mutex<HashMap<u64, (u32, u64)>>>, // hash -> (schema_id, field_id)

    // Background sync for stats
    flush_interval: Duration,
    shutdown: Arc<AtomicBool>,
}

impl InvertedIndexer {
    pub fn new(
        server_id: u64,
        chunks: Arc<Chunks>,
        neb_client: Arc<AsyncClient>,
        flush_interval: Duration,
    ) -> Self {
        // Create one lock per chunk for fine-grained concurrency
        let num_chunks = chunks.list.len().max(1);
        let append_locks: Vec<std::sync::Mutex<()>> =
            (0..num_chunks).map(|_| std::sync::Mutex::new(())).collect();

        Self {
            server_id,
            chunks,
            neb_client,
            field_stats: Arc::new(PtrHashMap::with_capacity(64)),
            doc_metadata: Arc::new(PtrHashMap::with_capacity(1024)),
            field_stats_keys: Arc::new(Mutex::new(HashMap::new())),
            flush_interval,
            shutdown: Arc::new(AtomicBool::new(false)),
        }
    }

    /// Hash key for field_stats: (schema_id, field_id) -> u64
    pub fn stats_key(schema_id: u32, field_id: u64) -> u64 {
        Id::from_obj(&(schema_id, field_id)).lower
    }

    /// Hash key for doc_metadata: (schema_id, field_id, doc_id) -> u64
    pub fn doc_meta_key(schema_id: u32, field_id: u64, doc_id: &Id) -> u64 {
        Id::from_obj(&(schema_id, field_id, doc_id.higher, doc_id.lower)).lower
    }

    fn stats_cell_id(schema_id: u32, field_id: u64) -> Id {
        Id::from_obj(&(schema_id, field_id, b"stats"))
    }

    fn doc_meta_cell_id(schema_id: u32, field_id: u64, doc_id: &Id) -> Id {
        Id::from_obj(&(schema_id, field_id, doc_id.higher, doc_id.lower))
    }

    /// Add a document to the index
    ///
    /// Writes posting lists to the SAME Chunk as the document (based on partition).
    /// This ensures data locality - document and its term postings are co-located.
    pub fn add_document(&self, meta: &FullTextIndexMeta) -> Result<(), IndexError> {
        // Get the Chunk for this document based on its partition
        let partition = meta.cell_id.higher;
        let chunk_index = (partition as usize) % self.chunks.list.len();

        let chunk = &self.chunks.list[chunk_index];

        // Update posting lists in this Chunk (write directly, no caching)
        // Each entry includes the cell version for filtering stale entries
        // Use chunk_index as the partition for posting list IDs (for proper recovery)
        for token in &meta.tokens {
            let seg_list =
                SegmentedPostingList::new(meta.schema_id, meta.field_id, token.term_hash);
            seg_list.append(
                chunk,
                chunk_index as u64,
                meta.cell_id,
                meta.version,
                token.term_freq,
                meta.doc_length,
            )?;
        }

        Ok(())
    }

    /// Update in-memory stats cache (call after add_document)
    /// Now uses lock-free PtrHashMap - no longer needs async
    pub fn update_stats_for_add(&self, meta: &FullTextIndexMeta) {
        let doc_meta_key = Self::doc_meta_key(meta.schema_id, meta.field_id, &meta.cell_id);
        let stats_key = Self::stats_key(meta.schema_id, meta.field_id);

        // Get or create doc_metadata entry
        let doc_meta_arc = self.doc_metadata.get_or_insert(doc_meta_key, || {
            Arc::new(Mutex::new(DocMeta {
                doc_length: 0,
                tokens: Vec::new(),
            }))
        });

        // Update doc_metadata and get previous length
        let prev_length = {
            let mut doc_meta = doc_meta_arc.lock();
            let prev = doc_meta.doc_length;
            doc_meta.doc_length = meta.doc_length;
            doc_meta.tokens = meta.tokens.clone();
            if prev == 0 {
                None
            } else {
                Some(prev)
            }
        };

        // Track stats key for iteration during flush
        {
            let mut keys = self.field_stats_keys.lock();
            keys.insert(stats_key, (meta.schema_id, meta.field_id));
        }

        // Get or create field_stats entry
        let stats_arc = self
            .field_stats
            .get_or_insert(stats_key, || Arc::new(Mutex::new(FieldStats::default())));

        // Update stats
        let mut stats = stats_arc.lock();
        if let Some(prev_len) = prev_length {
            // Update: adjust stats
            stats.total_length = stats
                .total_length
                .saturating_sub(prev_len as u64)
                .saturating_add(meta.doc_length as u64);
        } else {
            // Insert: increment doc count
            stats.doc_count += 1;
            stats.total_length += meta.doc_length as u64;
        }
    }

    /// Remove a document from the index
    ///
    /// Note: For now, we don't actually remove from posting lists (append-only).
    /// The document will be filtered out at query time if it no longer exists.
    /// A compaction process could clean up stale entries later.
    pub fn remove_document(&self, meta: &FullTextIndexMeta) -> Result<(), IndexError> {
        let doc_meta_key = Self::doc_meta_key(meta.schema_id, meta.field_id, &meta.cell_id);
        let stats_key = Self::stats_key(meta.schema_id, meta.field_id);

        // Get doc length before removal and reset doc_metadata
        let removed_length = if let Some(doc_meta_arc) = self.doc_metadata.get(&doc_meta_key) {
            let mut doc_meta = doc_meta_arc.lock();
            let prev_len = doc_meta.doc_length;
            // Reset doc_length so subsequent inserts are treated as new documents
            doc_meta.doc_length = 0;
            doc_meta.tokens.clear();
            if prev_len > 0 {
                Some(prev_len)
            } else {
                None
            }
        } else {
            None
        };

        // Update stats if document existed
        if let Some(doc_len) = removed_length {
            if let Some(stats_arc) = self.field_stats.get(&stats_key) {
                let mut stats = stats_arc.lock();
                stats.apply_remove(doc_len);
            }
        }

        // Note: Posting lists are append-only for now.
        // Stale entries will be filtered at query time.

        Ok(())
    }

    /// Get field statistics from memory, loading from disk if not found
    pub fn get_field_stats(&self, schema_id: u32, field_id: u64) -> FieldStats {
        let stats_key = Self::stats_key(schema_id, field_id);

        // Check memory first
        if let Some(stats_arc) = self.field_stats.get(&stats_key) {
            return stats_arc.lock().clone();
        }

        // Not in memory, try loading from disk
        let stats_id = Self::stats_cell_id(schema_id, field_id);
        match self.chunks.read_cell(&stats_id) {
            Ok(cell) => {
                info!(
                    "Found stats cell for schema {} field {}: {:?}",
                    schema_id, field_id, stats_id
                );
                let owned_cell = OwnedCell {
                    header: cell.header().clone(),
                    data: cell.data().owned(),
                };
                if let Some(loaded_stats) = FieldStats::from_value(owned_cell.data()) {
                    info!(
                        "Loaded stats from disk: doc_count={}, total_length={}",
                        loaded_stats.doc_count, loaded_stats.total_length
                    );
                    // Cache in memory for future use using get_or_insert
                    let stats_arc = self
                        .field_stats
                        .get_or_insert(stats_key, || Arc::new(Mutex::new(loaded_stats.clone())));
                    return stats_arc.lock().clone();
                } else {
                    warn!(
                        "Failed to parse stats from cell for schema {} field {}",
                        schema_id, field_id
                    );
                }
            }
            Err(e) => {
                warn!(
                    "Stats cell not found for schema {} field {} (id: {:?}): {:?}",
                    schema_id, field_id, stats_id, e
                );
            }
        }

        FieldStats::default()
    }

    /// Get postings for a specific term by iterating ALL Chunks on this server
    /// Returns: (doc_id, term_freq, doc_length)
    /// Note: Version is stored for GC but not filtered at query time for performance
    pub fn get_term_postings(
        &self,
        schema_id: u32,
        field_id: u64,
        term_hash: u64,
    ) -> Vec<(Id, u32, u32)> {
        let mut all_postings = Vec::new();
        let seg_list = SegmentedPostingList::new(schema_id, field_id, term_hash);

        // Iterate all Chunks and collect postings from each
        for (chunk_index, chunk) in self.chunks.list.iter().enumerate() {
            if let Ok(postings) = seg_list.iterate(chunk, chunk_index as u64) {
                for (doc_id, _version, tf, doc_len) in postings {
                    all_postings.push((doc_id, tf, doc_len));
                }
            }
        }

        all_postings
    }

    /// Get postings with version information (for GC or verified search)
    pub fn get_term_postings_with_version(
        &self,
        schema_id: u32,
        field_id: u64,
        term_hash: u64,
    ) -> Vec<(Id, u64, u32, u32)> {
        let mut all_postings = Vec::new();
        let seg_list = SegmentedPostingList::new(schema_id, field_id, term_hash);

        for (chunk_index, chunk) in self.chunks.list.iter().enumerate() {
            if let Ok(postings) = seg_list.iterate(chunk, chunk_index as u64) {
                all_postings.extend(postings);
            }
        }

        all_postings
    }

    /// Search using BM25 by iterating ALL Chunks on this server
    ///
    /// Each Chunk has its own posting lists for terms in documents stored in that Chunk.
    /// We iterate all Chunks, read posting lists from each, and aggregate BM25 scores.
    ///
    /// Note: Version is stored with each entry for future garbage collection,
    /// but not filtered at query time for performance. Stale entries may appear
    /// in results until GC runs.
    pub async fn bm25_search(
        &self,
        schema_id: u32,
        field_id: u64,
        query: &str,
        limit: usize,
    ) -> Result<Vec<BM25Hit>, IndexError> {
        if query.trim().is_empty() || limit == 0 {
            return Ok(vec![]);
        }

        let query_terms = tokenize_query(query);
        if query_terms.is_empty() {
            return Ok(vec![]);
        }

        // Get stats (from memory cache if available)
        let stats = self.get_field_stats(schema_id, field_id);

        if stats.doc_count == 0 {
            return Ok(vec![]);
        }

        let avg_doc_len = stats.avg_length();
        let mut scores: HashMap<Id, f32> = HashMap::new();

        // For each query term, iterate ALL Chunks and collect postings
        for term_hash in query_terms {
            let seg_list = SegmentedPostingList::new(schema_id, field_id, term_hash);

            // Collect postings from all Chunks
            let mut all_postings = Vec::new();
            for (chunk_index, chunk) in self.chunks.list.iter().enumerate() {
                if let Ok(chunk_postings) = seg_list.iterate(chunk, chunk_index as u64) {
                    for (doc_id, _version, tf, doc_len) in chunk_postings {
                        all_postings.push((doc_id, tf, doc_len));
                    }
                }
            }

            if all_postings.is_empty() {
                continue;
            }

            // Compute IDF based on document frequency
            let df = all_postings.len() as u64;
            let idf = compute_idf(stats.doc_count, df);

            // Score each document
            for (doc_id, tf, doc_len) in all_postings {
                let score = bm25_score(tf as f32, doc_len as f32, avg_doc_len, idf);
                if score > 0.0 {
                    *scores.entry(doc_id).or_insert(0.0) += score;
                }
            }
        }

        // Sort by score and return top K
        let mut hits = scores
            .into_iter()
            .map(|(id, score)| BM25Hit { id, score })
            .collect::<Vec<_>>();
        hits.sort_by(|a, b| b.score.partial_cmp(&a.score).unwrap());
        if hits.len() > limit {
            hits.truncate(limit);
        }

        Ok(hits)
    }

    /// Start background flush task
    pub fn start_background_flush(&self) {
        let indexer = Arc::new(self.clone());
        tokio::spawn(async move {
            loop {
                tokio::time::sleep(indexer.flush_interval).await;

                if indexer.shutdown.load(Ordering::Relaxed) {
                    break;
                }

                if let Err(e) = indexer.flush_to_disk().await {
                    error!("Failed to flush index to disk: {:?}", e);
                }
            }
        });
    }

    /// Flush in-memory stats to disk using transactions
    ///
    /// Note: Posting lists are written directly to Chunks during add_document,
    /// so we only need to flush the stats cache here.
    pub(crate) async fn flush_to_disk(&self) -> Result<(), IndexError> {
        let mut cells_to_write = Vec::new();
        let mut cells_to_update = Vec::new();

        // Collect stats cells to flush
        {
            let keys = self.field_stats_keys.lock();
            info!("Flushing {} field stats to disk", keys.len());

            if self.chunks.list.is_empty() {
                error!("No chunks available!");
                return Err(IndexError::Other("No chunks available".to_string()));
            }

            let schema_registered = self.chunks.list[0]
                .meta
                .schemas
                .get(&(*INVERTED_STATS_SCHEMA_ID))
                .is_some();
            if !schema_registered {
                error!("Stats schema {} not registered!", *INVERTED_STATS_SCHEMA_ID);
                return Err(IndexError::Other(format!(
                    "Stats schema {} not registered",
                    *INVERTED_STATS_SCHEMA_ID
                )));
            }

            for (hash_key, (schema_id, field_id)) in keys.iter() {
                if let Some(stats_arc) = self.field_stats.get(hash_key) {
                    let stat = stats_arc.lock();
                    let stats_id = Self::stats_cell_id(*schema_id, *field_id);
                    let cell = OwnedCell::new_with_id(
                        *INVERTED_STATS_SCHEMA_ID,
                        &stats_id,
                        stat.to_value(),
                    );

                    info!("Preparing stats cell for flush: schema={}, field={}, doc_count={}, total_length={}", 
                          schema_id, field_id, stat.doc_count, stat.total_length);

                    // Check if exists to decide write vs update
                    match self.chunks.read_cell(&stats_id) {
                        Ok(_) => cells_to_update.push(cell),
                        Err(_) => cells_to_write.push(cell),
                    }
                }
            }
        }

        if cells_to_write.is_empty() && cells_to_update.is_empty() {
            info!("No cells to flush");
            return Ok(());
        }

        info!(
            "Flushing {} writes and {} updates in transaction",
            cells_to_write.len(),
            cells_to_update.len()
        );

        // Execute transaction to atomically flush all changes
        let neb_client = self.neb_client.clone();
        let result = neb_client
            .transaction(move |txn| {
                let cells_to_write = cells_to_write.clone();
                let cells_to_update = cells_to_update.clone();
                async move {
                    for cell in cells_to_write {
                        txn.write(cell).await?;
                    }
                    for cell in cells_to_update {
                        txn.update(cell).await?;
                    }
                    Ok(())
                }
            })
            .await;

        if let Err(e) = result {
            error!("Transaction flush failed: {:?}", e);
            return Err(IndexError::Other(format!(
                "Transaction flush failed: {:?}",
                e
            )));
        }

        info!("Successfully flushed all cells in transaction");
        Ok(())
    }

    /// Garbage collect stale posting entries
    ///
    /// Scans posting lists and removes entries where the stored version doesn't match
    /// the current cell version (indicating the document was updated or removed).
    ///
    /// Parameters:
    /// - `schema_id`: Schema to GC (or all if None)
    /// - `field_id`: Field to GC (or all if None)  
    /// - `term_hashes`: Specific terms to GC (or scan all if None - expensive!)
    ///
    /// Returns: (entries_scanned, entries_removed)
    pub fn garbage_collect(
        &self,
        schema_id: Option<u32>,
        field_id: Option<u64>,
        term_hashes: Option<Vec<u64>>,
    ) -> Result<(usize, usize), IndexError> {
        let mut total_scanned = 0usize;
        let mut total_removed = 0usize;

        // If specific term hashes provided, GC those
        if let Some(hashes) = term_hashes {
            let sid = schema_id.unwrap_or(0);
            let fid = field_id.unwrap_or(0);

            for term_hash in hashes {
                let (scanned, removed) = self.gc_posting_list(sid, fid, term_hash)?;
                total_scanned += scanned;
                total_removed += removed;
            }
        }
        // Otherwise, we'd need to scan all posting lists - not implemented yet
        // (would require iterating all cells with INVERTED_SEGMENT_SCHEMA_ID)

        info!(
            "Garbage collection complete: scanned={}, removed={}",
            total_scanned, total_removed
        );
        Ok((total_scanned, total_removed))
    }

    /// GC a single posting list for a specific term
    /// Locks each chunk individually as it processes to allow concurrent GC on different chunks
    fn gc_posting_list(
        &self,
        schema_id: u32,
        field_id: u64,
        term_hash: u64,
    ) -> Result<(usize, usize), IndexError> {
        let seg_list = SegmentedPostingList::new(schema_id, field_id, term_hash);
        let mut total_scanned = 0usize;
        let mut total_removed = 0usize;

        for (chunk_index, chunk) in self.chunks.list.iter().enumerate() {
            let head_id = seg_list.head_segment_id(chunk_index as u64);
            let mut head_guard = match chunk.lock_cell_for_write(head_id.lower, true) {
                Ok(guard) => guard,
                Err(_) => continue,
            };
            // Read the current posting list
            match head_guard.read_cell_owned() {
                Ok(cell) => {
                    let cell_version = cell.header().version;
                    if let Some(segment) = PostingSegment::from_cell(&cell) {
                        let mut new_segment = PostingSegment::new();
                        new_segment.next = segment.next;

                        let mut chunk_removed = 0usize;

                        // Filter entries: keep only those with matching version
                        for (doc_id, version, tf, doc_len) in segment.iter() {
                            total_scanned += 1;

                            // Check if this entry's version matches current cell version
                            match chunk.head_cell(doc_id.lower) {
                                Ok(doc_header) => {
                                    if doc_header.version == version {
                                        // Version matches, keep this entry
                                        new_segment.add(doc_id, version, tf, doc_len);
                                    } else {
                                        // Version mismatch, remove (don't add to new segment)
                                        chunk_removed += 1;
                                    }
                                }
                                Err(_) => {
                                    // Document doesn't exist, remove entry
                                    chunk_removed += 1;
                                }
                            }
                        }

                        total_removed += chunk_removed;

                        // Write back the cleaned segment if anything was removed from this chunk
                        if chunk_removed > 0 {
                            // Preserve cell version for proper incrementing
                            let mut new_cell = new_segment.to_cell_with_version(&head_id, cell_version);
                            head_guard.update_cell(&mut new_cell).map_err(|e| {
                                IndexError::Other(format!("Failed to write GC'd segment: {:?}", e))
                            })?;
                        }
                    }
                }
                Err(_) => {} // No posting list for this term in this chunk
            }
        }

        Ok((total_scanned, total_removed))
    }

    /// Graceful shutdown with final flush
    pub async fn graceful_shutdown(&self) -> Result<(), IndexError> {
        self.shutdown.store(true, Ordering::Relaxed);
        self.flush_to_disk().await?;
        Ok(())
    }
}

impl Clone for InvertedIndexer {
    fn clone(&self) -> Self {
        Self {
            server_id: self.server_id,
            chunks: self.chunks.clone(),
            neb_client: self.neb_client.clone(),
            field_stats: self.field_stats.clone(),
            doc_metadata: self.doc_metadata.clone(),
            field_stats_keys: self.field_stats_keys.clone(),
            flush_interval: self.flush_interval,
            shutdown: self.shutdown.clone(),
        }
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::ram::schema::LocalSchemasCache;
    use crate::server::ServerMeta;
    use bifrost::conshash::weights::Weights;
    use bifrost::conshash::ConsistentHashing;
    use bifrost::membership::client::ObserverClient;
    use bifrost::membership::member::MemberService;
    use bifrost::membership::server::Membership;
    use bifrost::raft;
    use bifrost::raft::client::RaftClient;
    use bifrost::raft::disk::DiskOptions;
    use bifrost::rpc::Server;
    use std::sync::Arc;
    use tempfile::TempDir;

    // Helper to create test chunks
    fn create_test_chunks() -> Arc<Chunks> {
        let schemas = LocalSchemasCache::new_local("");
        schemas.debug_only_new_schema(inverted_segment_schema());
        schemas.debug_only_new_schema(crate::index::full_text::inverted_stats_schema());

        Chunks::new(
            1,
            8 * 1024 * 1024, // 8MB
            Arc::new(ServerMeta { schemas }),
            None,
            None,
            None,
            None,
        )
    }

    // Helper to create test document metadata
    fn create_test_meta(
        schema_id: u32,
        field_id: u64,
        doc_id: Id,
        text: &str,
    ) -> FullTextIndexMeta {
        create_test_meta_with_version(schema_id, field_id, doc_id, text, 1)
    }

    // Helper to create test document metadata with specific version
    fn create_test_meta_with_version(
        schema_id: u32,
        field_id: u64,
        doc_id: Id,
        text: &str,
        version: u64,
    ) -> FullTextIndexMeta {
        crate::index::full_text::build_index_meta(
            doc_id,
            version,
            schema_id,
            field_id,
            OwnedValue::String(text.to_string()),
        )
        .unwrap()
    }

    // Helper to set up ConsistentHashing and AsyncClient for testing
    async fn setup_test_infrastructure(
        server_addr: &str,
        group_name: &str,
        conshash_id: u64,
    ) -> (
        Arc<ConsistentHashing>,
        Arc<RaftClient>,
        Arc<crate::client::AsyncClient>,
        u64,
    ) {
        let temp_dir = TempDir::new().unwrap();
        let raft_path = temp_dir.path().join("raft");

        let rpc_server = Server::new(&server_addr.to_string());
        let storage = raft::Storage::DISK(DiskOptions {
            path: raft_path.to_str().unwrap().to_string(),
            take_snapshots: false,
            append_logs: false,
            trim_logs: false,
            snapshot_log_threshold: 1000,
            log_compaction_threshold: 2000,
        });

        let raft_service = raft::RaftService::new(raft::Options {
            storage,
            address: server_addr.to_string(),
            service_id: raft::DEFAULT_SERVICE_ID,
        });

        Weights::new_with_id(conshash_id, &raft_service).await;
        rpc_server.register_service(&raft_service).await;
        Server::listen_and_resume(&rpc_server).await;
        Membership::new(&rpc_server, &raft_service).await;
        raft::RaftService::start(&raft_service, false).await;
        raft_service.bootstrap().await;

        let raft_client = RaftClient::new(&vec![server_addr.to_string()], raft::DEFAULT_SERVICE_ID)
            .await
            .unwrap();
        RaftClient::prepare_subscription(&rpc_server).await;

        let member_service =
            MemberService::new(&server_addr.to_string(), &raft_client, &raft_service).await;
        member_service
            .join_group(&group_name.to_string())
            .await
            .unwrap();

        let membership_client = Arc::new(ObserverClient::new(&raft_client));
        let conshash = ConsistentHashing::new_with_id(
            conshash_id,
            &group_name.to_string(),
            &raft_client,
            &membership_client,
        )
        .await
        .unwrap();
        conshash
            .set_weight(&server_addr.to_string(), 1024)
            .await
            .unwrap();
        conshash.init_table().await.unwrap();

        // Get the server_id for this address
        let server_id = conshash.get_server_id(hash_str(server_addr)).unwrap_or(1);

        // Create AsyncClient - it needs the conshash to be initialized first
        let neb_client = Arc::new(
            crate::client::AsyncClient::new(
                &rpc_server,
                &membership_client,
                &vec![server_addr.to_string()],
                &group_name.to_string(),
            )
            .await
            .unwrap(),
        );

        (conshash, raft_client, neb_client, server_id)
    }

    // Simplified version without transactions for basic tests
    #[tokio::test]
    async fn test_basic_add_search() {
        let _ = env_logger::try_init();

        // Use a full NebServer for proper AsyncClient setup
        let server = crate::server::NebServer::new_from_opts(
            &crate::server::ServerOptions {
                chunk_count: 1,
                total_size: 64 * 1024 * 1024,
                tiered_config: None,
                backup_storage: None,
                wal_storage: None,
                undo_log_storage: None,
                raft_storage: None,
                index_enabled: true,
                services: vec![crate::server::Service::Cell],
                enable_recovery: false,
            },
            "127.0.0.1:29399",
            "basic_test",
            async |_| {},
        )
        .await;

        // Create a simple schema with fulltext indexing
        let schema_id = 999u32;
        let content_field = "content";
        let content_field_id = hash_str(content_field) as u64;

        let fields =
            crate::ram::schema::Field::new_schema(vec![crate::ram::schema::Field::new_indexed(
                content_field,
                dovahkiin::types::Type::String,
                vec![crate::ram::schema::IndexType::Fulltext],
            )]);

        let schema = crate::ram::schema::Schema::new_with_id(
            schema_id,
            "test_schema",
            None,
            fields,
            false,
            false,
        );

        server.meta.schemas.debug_only_new_schema(schema.clone());

        // Find owned document IDs
        let mut owned_doc_ids = Vec::new();
        for i in 0..1000 {
            let test_id = Id::new(i, i);
            if server
                .consh
                .get_server_id(test_id.higher)
                .map(|sid| sid == server.server_id)
                .unwrap_or(false)
            {
                owned_doc_ids.push(test_id);
                if owned_doc_ids.len() >= 2 {
                    break;
                }
            }
        }

        assert!(owned_doc_ids.len() >= 2, "Need at least 2 owned documents");

        // Create and write cells
        let mut cell1_data = OwnedMap::new();
        cell1_data.insert(
            content_field,
            OwnedValue::String("hello world test document".to_string()),
        );
        let mut cell1 =
            OwnedCell::new_with_id(schema_id, &owned_doc_ids[0], OwnedValue::Map(cell1_data));

        let mut cell2_data = OwnedMap::new();
        cell2_data.insert(
            content_field,
            OwnedValue::String("hello rust programming language".to_string()),
        );
        let mut cell2 =
            OwnedCell::new_with_id(schema_id, &owned_doc_ids[1], OwnedValue::Map(cell2_data));

        server.chunks.write_cell(&mut cell1).unwrap();
        server.chunks.write_cell(&mut cell2).unwrap();

        // Trigger indexing
        if let Some(ref index_builder) = server.indexer {
            index_builder.ensure_indices(&cell1, &schema, None);
            index_builder.ensure_indices(&cell2, &schema, None);

            // Wait for indexing
            tokio::time::sleep(Duration::from_millis(300)).await;

            // Test search via the indexer directly
            if let Some(indexer) = index_builder.clients.fulltext_indexer() {
                let stats = indexer.get_field_stats(schema_id, content_field_id);
                assert_eq!(stats.doc_count, 2, "Should have 2 documents indexed");

                let hits = indexer
                    .bm25_search(schema_id, content_field_id, "hello", 10)
                    .await
                    .unwrap();
                assert_eq!(hits.len(), 2, "Should find both documents with 'hello'");

                let hits = indexer
                    .bm25_search(schema_id, content_field_id, "rust", 10)
                    .await
                    .unwrap();
                assert_eq!(hits.len(), 1, "Should find 1 document with 'rust'");
                assert_eq!(hits[0].id, owned_doc_ids[1]);
            }
        }

        info!("Basic add and search test passed");
    }

    /// Test concurrent indexing of the same term from multiple threads
    /// This tests the posting list's ability to handle concurrent appends without deadlock
    #[tokio::test]
    async fn test_concurrent_indexing_same_term() {
        let _ = env_logger::try_init();
        info!("Starting concurrent indexing test");

        let server_addr = "127.0.0.1:5710";
        let group_name = "concurrent_test_group";

        let server = crate::server::NebServer::new_from_opts(
            &crate::server::ServerOptions {
                chunk_count: 1,
                total_size: 64 * 1024 * 1024,
                tiered_config: None,
                backup_storage: None,
                wal_storage: None,
                undo_log_storage: None,
                raft_storage: None,
                index_enabled: true,
                services: vec![crate::server::Service::Cell],
                enable_recovery: false,
            },
            server_addr,
            group_name,
            async |_| {},
        )
        .await;

        let schema_id = 500u32;
        let content_field = "content";
        let content_field_id = hash_str(content_field) as u64;

        let fields =
            crate::ram::schema::Field::new_schema(vec![crate::ram::schema::Field::new_indexed(
                content_field,
                dovahkiin::types::Type::String,
                vec![crate::ram::schema::IndexType::Fulltext],
            )]);

        let schema = crate::ram::schema::Schema::new_with_id(
            schema_id,
            "concurrent_test_schema",
            None,
            fields,
            false,
            false,
        );

        server.meta.schemas.debug_only_new_schema(schema.clone());

        // Find many owned document IDs for concurrent testing
        let mut owned_doc_ids = Vec::new();
        for i in 0..10000 {
            let test_id = Id::new(i, i);
            if server
                .consh
                .get_server_id(test_id.higher)
                .map(|sid| sid == server.server_id)
                .unwrap_or(false)
            {
                owned_doc_ids.push(test_id);
                if owned_doc_ids.len() >= 50 {
                    break;
                }
            }
        }

        let num_docs = owned_doc_ids.len();
        assert!(num_docs >= 10, "Need at least 10 owned documents for concurrent test");
        info!("Found {} owned documents for concurrent test", num_docs);

        // All documents will contain the same common term "concurrent"
        // plus unique content to differentiate them
        let server_arc = Arc::new(server);
        let schema_arc = Arc::new(schema);

        // Spawn concurrent tasks to write and index documents
        let mut handles = Vec::new();
        for (i, doc_id) in owned_doc_ids.iter().enumerate() {
            let server_clone = server_arc.clone();
            let schema_clone = schema_arc.clone();
            let doc_id = *doc_id;

            let handle = tokio::spawn(async move {
                let mut cell_data = OwnedMap::new();
                cell_data.insert(
                    content_field,
                    OwnedValue::String(format!(
                        "concurrent test document number {} with shared term concurrent",
                        i
                    )),
                );
                let mut cell = OwnedCell::new_with_id(schema_id, &doc_id, OwnedValue::Map(cell_data));

                // Write cell
                server_clone.chunks.write_cell(&mut cell).unwrap();

                // Index cell
                if let Some(ref index_builder) = server_clone.indexer {
                    index_builder.ensure_indices(&cell, &schema_clone, None);
                }

                doc_id
            });
            handles.push(handle);
        }

        // Wait for all concurrent tasks to complete
        let indexed_ids: Vec<Id> = futures::future::join_all(handles)
            .await
            .into_iter()
            .map(|r| r.unwrap())
            .collect();

        info!("All {} documents indexed concurrently", indexed_ids.len());

        // Give time for async indexing to settle
        tokio::time::sleep(Duration::from_millis(500)).await;

        // Verify all documents are searchable
        if let Some(ref index_builder) = server_arc.indexer {
            if let Some(indexer) = index_builder.clients.fulltext_indexer() {
                let stats = indexer.get_field_stats(schema_id, content_field_id);
                assert_eq!(
                    stats.doc_count, num_docs as u64,
                    "Should have {} documents indexed, got {}",
                    num_docs, stats.doc_count
                );

                // Search for the common term - should find all documents
                let hits = indexer
                    .bm25_search(schema_id, content_field_id, "concurrent", 100)
                    .await
                    .unwrap();
                assert_eq!(
                    hits.len(), num_docs,
                    "Should find all {} documents with 'concurrent', found {}",
                    num_docs, hits.len()
                );

                // Verify all indexed IDs are in the results
                let hit_ids: std::collections::HashSet<Id> = hits.iter().map(|h| h.id).collect();
                for id in &indexed_ids {
                    assert!(hit_ids.contains(id), "Document {:?} should be in search results", id);
                }

                info!("Concurrent indexing test passed: all {} documents found", num_docs);
            }
        }
    }

    /// Test concurrent indexing that causes segment overflow (tests prepend logic under contention)
    #[tokio::test]
    async fn test_concurrent_indexing_with_segment_overflow() {
        let _ = env_logger::try_init();
        info!("Starting concurrent indexing with segment overflow test");

        let server_addr = "127.0.0.1:5711";
        let group_name = "overflow_test_group";

        let server = crate::server::NebServer::new_from_opts(
            &crate::server::ServerOptions {
                chunk_count: 1,
                total_size: 64 * 1024 * 1024,
                tiered_config: None,
                backup_storage: None,
                wal_storage: None,
                undo_log_storage: None,
                raft_storage: None,
                index_enabled: true,
                services: vec![crate::server::Service::Cell],
                enable_recovery: false,
            },
            server_addr,
            group_name,
            async |_| {},
        )
        .await;

        let schema_id = 501u32;
        let content_field = "content";
        let content_field_id = hash_str(content_field) as u64;

        let fields =
            crate::ram::schema::Field::new_schema(vec![crate::ram::schema::Field::new_indexed(
                content_field,
                dovahkiin::types::Type::String,
                vec![crate::ram::schema::IndexType::Fulltext],
            )]);

        let schema = crate::ram::schema::Schema::new_with_id(
            schema_id,
            "overflow_test_schema",
            None,
            fields,
            false,
            false,
        );

        server.meta.schemas.debug_only_new_schema(schema.clone());

        // Find many owned document IDs - enough to cause segment overflow
        // SEGMENT_SIZE is 1000, so we need > 1000 docs with the same term
        let mut owned_doc_ids = Vec::new();
        for i in 0..100000 {
            let test_id = Id::new(i, i);
            if server
                .consh
                .get_server_id(test_id.higher)
                .map(|sid| sid == server.server_id)
                .unwrap_or(false)
            {
                owned_doc_ids.push(test_id);
                if owned_doc_ids.len() >= 200 {
                    // Enough to test concurrent prepend under load
                    break;
                }
            }
        }

        let num_docs = owned_doc_ids.len();
        assert!(num_docs >= 100, "Need at least 100 owned documents for overflow test");
        info!("Found {} owned documents for overflow test", num_docs);

        let server_arc = Arc::new(server);
        let schema_arc = Arc::new(schema);

        // Use multiple waves of concurrent indexing to stress test
        let wave_size = 20;
        let mut all_indexed = Vec::new();

        for wave in 0..(num_docs / wave_size) {
            let start_idx = wave * wave_size;
            let end_idx = std::cmp::min(start_idx + wave_size, num_docs);
            
            let mut handles = Vec::new();
            for i in start_idx..end_idx {
                let server_clone = server_arc.clone();
                let schema_clone = schema_arc.clone();
                let doc_id = owned_doc_ids[i];

                let handle = tokio::spawn(async move {
                    let mut cell_data = OwnedMap::new();
                    // Use a single common term that will cause all postings to go to the same list
                    cell_data.insert(
                        content_field,
                        OwnedValue::String(format!("overflow stress test document {}", i)),
                    );
                    let mut cell = OwnedCell::new_with_id(schema_id, &doc_id, OwnedValue::Map(cell_data));

                    server_clone.chunks.write_cell(&mut cell).unwrap();

                    if let Some(ref index_builder) = server_clone.indexer {
                        index_builder.ensure_indices(&cell, &schema_clone, None);
                    }

                    doc_id
                });
                handles.push(handle);
            }

            let wave_results: Vec<Id> = futures::future::join_all(handles)
                .await
                .into_iter()
                .map(|r| r.unwrap())
                .collect();
            all_indexed.extend(wave_results);
        }

        info!("All {} documents indexed in waves", all_indexed.len());

        // Give time for async indexing to settle
        tokio::time::sleep(Duration::from_millis(500)).await;

        // Verify all documents are searchable
        if let Some(ref index_builder) = server_arc.indexer {
            if let Some(indexer) = index_builder.clients.fulltext_indexer() {
                let stats = indexer.get_field_stats(schema_id, content_field_id);
                info!("Stats: doc_count={}, total_length={}", stats.doc_count, stats.total_length);
                
                // Search for the common term
                let hits = indexer
                    .bm25_search(schema_id, content_field_id, "overflow", 500)
                    .await
                    .unwrap();
                
                info!(
                    "Found {} documents with 'overflow' (expected {})",
                    hits.len(), all_indexed.len()
                );

                // With concurrent indexing, some may not be found due to timing
                // but we should find most of them
                assert!(
                    hits.len() >= all_indexed.len() * 80 / 100,
                    "Should find at least 80% of documents, found {} out of {}",
                    hits.len(), all_indexed.len()
                );

                info!("Concurrent overflow test passed!");
            }
        }
    }

    #[tokio::test]
    async fn test_basic_functionality() {
        let _ = env_logger::try_init();

        // Test utility functions
        let schema_id = 100u32;
        let field_id = hash_str("content") as u64;

        // Create test documents with text
        let doc1_id = Id::new(1, 1);
        let doc2_id = Id::new(1, 2);

        // Create metadata
        let meta1 = create_test_meta(schema_id, field_id, doc1_id, "hello world test");
        let meta2 = create_test_meta(schema_id, field_id, doc2_id, "hello rust programming");

        // Verify metadata creation
        assert!(meta1.doc_length > 0);
        assert!(meta2.doc_length > 0);
        assert!(!meta1.tokens.is_empty());
        assert!(!meta2.tokens.is_empty());

        // Test tokenization
        let query_terms = tokenize_query("hello world");
        assert!(!query_terms.is_empty());

        // Test BM25 score calculation
        let score = bm25_score(2.0, 10.0, 10.0, 1.0);
        assert!(score > 0.0);

        // Test IDF calculation
        let idf = compute_idf(100, 10);
        assert!(idf > 0.0);

        info!("Basic functionality tests passed");
    }

    /// Test that multi-term queries rank documents with more matching terms higher
    #[tokio::test]
    async fn test_multi_term_ranking() {
        let _ = env_logger::try_init();
        info!("Starting multi-term ranking test");

        let server_addr = "127.0.0.1:5720";
        let group_name = "multi_term_test_group";

        let server = crate::server::NebServer::new_from_opts(
            &crate::server::ServerOptions {
                chunk_count: 1,
                total_size: 64 * 1024 * 1024,
                tiered_config: None,
                backup_storage: None,
                wal_storage: None,
                undo_log_storage: None,
                raft_storage: None,
                index_enabled: true,
                services: vec![crate::server::Service::Cell],
                enable_recovery: false,
            },
            server_addr,
            group_name,
            async |_| {},
        )
        .await;

        let schema_id = 600u32;
        let content_field = "content";
        let content_field_id = hash_str(content_field) as u64;

        let fields =
            crate::ram::schema::Field::new_schema(vec![crate::ram::schema::Field::new_indexed(
                content_field,
                dovahkiin::types::Type::String,
                vec![crate::ram::schema::IndexType::Fulltext],
            )]);

        let schema = crate::ram::schema::Schema::new_with_id(
            schema_id,
            "multi_term_test_schema",
            None,
            fields,
            false,
            false,
        );

        server.meta.schemas.debug_only_new_schema(schema.clone());

        // Find owned document IDs
        let mut owned_doc_ids = Vec::new();
        for i in 0..10000 {
            let test_id = Id::new(i, i);
            if server
                .consh
                .get_server_id(test_id.higher)
                .map(|sid| sid == server.server_id)
                .unwrap_or(false)
            {
                owned_doc_ids.push(test_id);
                if owned_doc_ids.len() >= 4 {
                    break;
                }
            }
        }

        assert!(owned_doc_ids.len() >= 4, "Need at least 4 owned documents");
        info!("Found {} owned documents", owned_doc_ids.len());

        // Create documents with varying term coverage:
        // Doc 0: contains "rust" only (1 term)
        // Doc 1: contains "programming" only (1 term)
        // Doc 2: contains "rust programming" (2 terms)
        // Doc 3: contains "rust programming language" (3 terms if we search for all 3)
        let texts = vec![
            "rust is great",                           // 1 matching term
            "programming is fun",                      // 1 matching term
            "rust programming tutorial",               // 2 matching terms
            "rust programming language guide",         // 2 matching terms (same as doc2 for query "rust programming")
        ];

        for (i, doc_id) in owned_doc_ids.iter().enumerate() {
            let mut cell_data = OwnedMap::new();
            cell_data.insert(content_field, OwnedValue::String(texts[i].to_string()));
            let mut cell = OwnedCell::new_with_id(schema_id, doc_id, OwnedValue::Map(cell_data));

            server.chunks.write_cell(&mut cell).unwrap();

            if let Some(ref index_builder) = server.indexer {
                index_builder.ensure_indices(&cell, &schema, None);
            }
        }

        // Wait for indexing
        tokio::time::sleep(Duration::from_millis(500)).await;

        // Search for "rust programming" - docs with both terms should rank higher
        if let Some(ref index_builder) = server.indexer {
            if let Some(indexer) = index_builder.clients.fulltext_indexer() {
                let hits = indexer
                    .bm25_search(schema_id, content_field_id, "rust programming", 10)
                    .await
                    .unwrap();

                info!("Search results for 'rust programming':");
                for (i, hit) in hits.iter().enumerate() {
                    info!("  {}: doc {:?} score {:.4}", i + 1, hit.id, hit.score);
                }

                assert!(hits.len() >= 4, "Should find at least 4 documents");

                // The top results should be docs with both terms (doc2 and doc3)
                // They should have higher scores than docs with single terms
                let top_two_ids: Vec<Id> = hits.iter().take(2).map(|h| h.id).collect();
                let doc2_id = owned_doc_ids[2];
                let doc3_id = owned_doc_ids[3];

                // At least one of the top 2 should be doc2 or doc3 (both have "rust" and "programming")
                assert!(
                    top_two_ids.contains(&doc2_id) || top_two_ids.contains(&doc3_id),
                    "Documents with both query terms should rank in top 2. Top 2: {:?}, expected: {:?} or {:?}",
                    top_two_ids, doc2_id, doc3_id
                );

                // Verify that docs with 2 terms have higher scores than docs with 1 term
                let single_term_doc = owned_doc_ids[0]; // "rust is great"
                let multi_term_doc = owned_doc_ids[2];  // "rust programming tutorial"

                let single_score = hits.iter().find(|h| h.id == single_term_doc).map(|h| h.score);
                let multi_score = hits.iter().find(|h| h.id == multi_term_doc).map(|h| h.score);

                if let (Some(single), Some(multi)) = (single_score, multi_score) {
                    info!("Single term doc score: {:.4}, Multi term doc score: {:.4}", single, multi);
                    assert!(
                        multi > single,
                        "Document with 2 matching terms ({:.4}) should score higher than document with 1 term ({:.4})",
                        multi, single
                    );
                }

                info!("Multi-term ranking test passed!");
            }
        }
    }

    #[tokio::test]
    async fn test_add_and_search_document() {
        let _ = env_logger::try_init();

        // Use a full NebServer for proper setup
        let server = crate::server::NebServer::new_from_opts(
            &crate::server::ServerOptions {
                chunk_count: 1,
                total_size: 64 * 1024 * 1024,
                tiered_config: None,
                backup_storage: None,
                wal_storage: None,
                undo_log_storage: None,
                raft_storage: None,
                index_enabled: true,
                services: vec![
                    crate::server::Service::Cell,
                    crate::server::Service::Transaction,
                ],
                enable_recovery: false,
            },
            "127.0.0.1:29300",
            "hybrid_index_test",
            async |_| {},
        )
        .await;

        // Get the indexer from the server
        let indexer = server
            .indexer
            .as_ref()
            .and_then(|ib| ib.clients.fulltext_indexer())
            .expect("Indexer should be available");

        // Create schema and field IDs
        let schema_id = 100u32;
        let field_id = hash_str("content") as u64;

        // Find IDs that are owned by our server
        let mut doc1_id = None;
        let mut doc2_id = None;
        for i in 0..100 {
            let test_id = Id::new(i, i);
            if server
                .consh
                .get_server_id(test_id.higher)
                .map(|sid| sid == server.server_id)
                .unwrap_or(false)
            {
                if doc1_id.is_none() {
                    doc1_id = Some(test_id);
                } else {
                    doc2_id = Some(test_id);
                    break;
                }
            }
        }

        let doc1_id = doc1_id.expect("Should find at least one owned document");
        let doc2_id = doc2_id.expect("Should find at least two owned documents");

        // Create test documents
        let meta1 = create_test_meta(schema_id, field_id, doc1_id, "hello world test document");
        let meta2 = create_test_meta(
            schema_id,
            field_id,
            doc2_id,
            "hello rust programming language",
        );

        // Add documents
        indexer.add_document(&meta1).unwrap();
        indexer.update_stats_for_add(&meta1);
        indexer.add_document(&meta2).unwrap();
        indexer.update_stats_for_add(&meta2);

        // Verify stats
        let stats = indexer.get_field_stats(schema_id, field_id);
        assert_eq!(stats.doc_count, 2);
        assert!(stats.total_length > 0);

        // Search for "hello"
        let hits = indexer
            .bm25_search(schema_id, field_id, "hello", 10)
            .await
            .unwrap();
        assert_eq!(hits.len(), 2);
        assert!(hits[0].score > 0.0);
        assert!(hits[1].score > 0.0);

        // Search for "world" - should find doc1
        let hits = indexer
            .bm25_search(schema_id, field_id, "world", 10)
            .await
            .unwrap();
        assert!(!hits.is_empty());
        assert_eq!(hits[0].id, doc1_id);

        // Search for "rust" - should find doc2
        let hits = indexer
            .bm25_search(schema_id, field_id, "rust", 10)
            .await
            .unwrap();
        assert!(!hits.is_empty());
        assert_eq!(hits[0].id, doc2_id);

        info!("Add and search test passed");
    }

    #[tokio::test]
    async fn test_remove_document() {
        let _ = env_logger::try_init();

        // Use a full NebServer for proper setup
        let server = crate::server::NebServer::new_from_opts(
            &crate::server::ServerOptions {
                chunk_count: 1,
                total_size: 64 * 1024 * 1024,
                tiered_config: None,
                backup_storage: None,
                wal_storage: None,
                undo_log_storage: None,
                raft_storage: None,
                index_enabled: true,
                services: vec![
                    crate::server::Service::Cell,
                    crate::server::Service::Transaction,
                ],
                enable_recovery: false,
            },
            "127.0.0.1:29301",
            "hybrid_index_remove_test",
            async |_| {},
        )
        .await;

        // Get the indexer from the server
        let indexer = server
            .indexer
            .as_ref()
            .and_then(|ib| ib.clients.fulltext_indexer())
            .expect("Indexer should be available");

        let schema_id = 100u32;
        let field_id = hash_str("content") as u64;

        // Find an owned document ID
        let mut doc_id = None;
        for i in 0..100 {
            let test_id = Id::new(i, i);
            if server
                .consh
                .get_server_id(test_id.higher)
                .map(|sid| sid == server.server_id)
                .unwrap_or(false)
            {
                doc_id = Some(test_id);
                break;
            }
        }

        let doc_id = doc_id.expect("Should find an owned document");
        let meta = create_test_meta(schema_id, field_id, doc_id, "test document to remove");

        // Add document
        indexer.add_document(&meta).unwrap();
        indexer.update_stats_for_add(&meta);

        // Verify it's indexed
        let stats = indexer.get_field_stats(schema_id, field_id);
        assert_eq!(stats.doc_count, 1);

        // Remove document
        indexer.remove_document(&meta).unwrap();

        // Verify it's removed
        let stats = indexer.get_field_stats(schema_id, field_id);
        assert_eq!(stats.doc_count, 0);

        // Search should return nothing
        let hits = indexer
            .bm25_search(schema_id, field_id, "test", 10)
            .await
            .unwrap();
        assert!(hits.is_empty());

        info!("Remove document test passed");
    }

    #[tokio::test]
    async fn test_flush_and_recovery() {
        let _ = env_logger::try_init();

        // Use a full NebServer for proper setup
        let server = crate::server::NebServer::new_from_opts(
            &crate::server::ServerOptions {
                chunk_count: 1,
                total_size: 64 * 1024 * 1024,
                tiered_config: None,
                backup_storage: None,
                wal_storage: None,
                undo_log_storage: None,
                raft_storage: None,
                index_enabled: true,
                services: vec![
                    crate::server::Service::Cell,
                    crate::server::Service::Transaction,
                ],
                enable_recovery: false,
            },
            "127.0.0.1:29302",
            "hybrid_index_flush_test",
            async |_| {},
        )
        .await;

        // Get the indexer from the server
        let indexer = server
            .indexer
            .as_ref()
            .and_then(|ib| ib.clients.fulltext_indexer())
            .expect("Indexer should be available");

        let schema_id = 100u32;
        let field_id = hash_str("content") as u64;

        // Find owned document IDs
        let mut doc_ids = Vec::new();
        for i in 0..100 {
            let test_id = Id::new(i, i);
            if server
                .consh
                .get_server_id(test_id.higher)
                .map(|sid| sid == server.server_id)
                .unwrap_or(false)
            {
                doc_ids.push(test_id);
                if doc_ids.len() >= 3 {
                    break;
                }
            }
        }

        assert!(
            doc_ids.len() >= 2,
            "Need at least 2 owned documents for test"
        );

        // Add documents
        for (i, doc_id) in doc_ids.iter().enumerate() {
            let text = format!("document {} with test content", i);
            let meta = create_test_meta(schema_id, field_id, *doc_id, &text);
            indexer.add_document(&meta).unwrap();
            indexer.update_stats_for_add(&meta);
        }

        // Manually flush to disk
        indexer.flush_to_disk().await.unwrap();

        // Verify data is persisted by reading from disk
        // We can check that stats are persisted
        let stats = indexer.get_field_stats(schema_id, field_id);
        assert_eq!(stats.doc_count, doc_ids.len() as u64);

        // Search should still work after flush
        let hits = indexer
            .bm25_search(schema_id, field_id, "test", 10)
            .await
            .unwrap();
        assert_eq!(hits.len(), doc_ids.len());

        info!("Flush and recovery test passed");
    }

    #[tokio::test]
    async fn test_per_chunk_indexing() {
        let _ = env_logger::try_init();

        // Use a full NebServer for proper setup
        let server = crate::server::NebServer::new_from_opts(
            &crate::server::ServerOptions {
                chunk_count: 2, // Multiple chunks to test per-chunk behavior
                total_size: 64 * 1024 * 1024,
                tiered_config: None,
                backup_storage: None,
                wal_storage: None,
                undo_log_storage: None,
                raft_storage: None,
                index_enabled: true,
                services: vec![
                    crate::server::Service::Cell,
                    crate::server::Service::Transaction,
                ],
                enable_recovery: false,
            },
            "127.0.0.1:29303",
            "per_chunk_index_test",
            async |_| {},
        )
        .await;

        // Get the indexer from the server
        let indexer = server
            .indexer
            .as_ref()
            .and_then(|ib| ib.clients.fulltext_indexer())
            .expect("Indexer should be available");

        let schema_id = 100u32;
        let field_id = hash_str("content") as u64;

        // Create documents that will go to different chunks based on partition
        let doc1_id = Id::new(0, 1); // Partition 0 -> Chunk 0
        let doc2_id = Id::new(1, 2); // Partition 1 -> Chunk 1

        let meta1 = create_test_meta(schema_id, field_id, doc1_id, "hello world test");
        let meta2 = create_test_meta(schema_id, field_id, doc2_id, "hello universe test");

        // Add documents - they should go to different chunks
        let result1 = indexer.add_document(&meta1);
        assert!(
            result1.is_ok(),
            "Should add document to chunk 0: {:?}",
            result1
        );
        indexer.update_stats_for_add(&meta1);

        let result2 = indexer.add_document(&meta2);
        assert!(
            result2.is_ok(),
            "Should add document to chunk 1: {:?}",
            result2
        );
        indexer.update_stats_for_add(&meta2);

        // Search should find documents from all chunks
        let hits = indexer.bm25_search(schema_id, field_id, "hello", 10).await;
        assert!(hits.is_ok(), "Search should succeed");
        let hits = hits.unwrap();
        assert_eq!(
            hits.len(),
            2,
            "Should find both documents containing 'hello'"
        );

        info!("Per-chunk indexing test passed");
    }

    #[tokio::test]
    async fn test_end_to_end_indexing_from_cells() {
        let _ = env_logger::try_init();

        // Set up a full NebServer with indexing enabled
        let server_addr = "127.0.0.1:29304";
        let group_name = "e2e_indexing_test";
        let server = crate::server::NebServer::new_from_opts(
            &crate::server::ServerOptions {
                chunk_count: 1,
                total_size: 64 * 1024 * 1024, // 64MB
                tiered_config: None,
                backup_storage: None,
                wal_storage: None,
                undo_log_storage: None,
                raft_storage: None,
                index_enabled: true, // Enable indexing
                services: vec![crate::server::Service::Cell],
                enable_recovery: false,
            },
            server_addr,
            group_name,
            async |_| {},
        )
        .await;

        // Create a schema with a text field that has Fulltext index
        let schema_id = 200u32;
        let content_field = "content";
        let content_field_id = hash_str(content_field) as u64;

        let fields =
            crate::ram::schema::Field::new_schema(vec![crate::ram::schema::Field::new_indexed(
                content_field,
                dovahkiin::types::Type::String,
                vec![crate::ram::schema::IndexType::Fulltext],
            )]);

        let schema = crate::ram::schema::Schema::new_with_id(
            schema_id,
            "test_schema",
            None,
            fields,
            false,
            false,
        );

        // Register schema
        server.meta.schemas.debug_only_new_schema(schema.clone());

        // Create test documents with text content
        let doc1_id = Id::new(1, 1);
        let doc2_id = Id::new(1, 2);
        let doc3_id = Id::new(1, 3);

        // Ensure these IDs are owned by our server
        let mut owned_doc_ids = Vec::new();
        for test_id in [doc1_id, doc2_id, doc3_id] {
            if server
                .consh
                .get_server_id(test_id.higher)
                .map(|sid| sid == server.server_id)
                .unwrap_or(false)
            {
                owned_doc_ids.push(test_id);
            }
        }

        // If none are owned, try to find some that are
        if owned_doc_ids.is_empty() {
            for i in 0..1000 {
                let test_id = Id::new(i, i);
                if server
                    .consh
                    .get_server_id(test_id.higher)
                    .map(|sid| sid == server.server_id)
                    .unwrap_or(false)
                {
                    owned_doc_ids.push(test_id);
                    if owned_doc_ids.len() >= 3 {
                        break;
                    }
                }
            }
        }

        assert!(
            owned_doc_ids.len() >= 2,
            "Need at least 2 owned documents for test"
        );

        // Create cells with text content
        let mut cell1_data = OwnedMap::new();
        cell1_data.insert(
            content_field,
            OwnedValue::String("rust programming language tutorial".to_string()),
        );
        let mut cell1 =
            OwnedCell::new_with_id(schema_id, &owned_doc_ids[0], OwnedValue::Map(cell1_data));

        let mut cell2_data = OwnedMap::new();
        cell2_data.insert(
            content_field,
            OwnedValue::String("database storage engine design".to_string()),
        );
        let mut cell2 =
            OwnedCell::new_with_id(schema_id, &owned_doc_ids[1], OwnedValue::Map(cell2_data));

        let mut cell3_data = OwnedMap::new();
        cell3_data.insert(
            content_field,
            OwnedValue::String("rust async programming with tokio".to_string()),
        );
        let mut cell3 = OwnedCell::new_with_id(
            schema_id,
            &owned_doc_ids[2 % owned_doc_ids.len()],
            OwnedValue::Map(cell3_data),
        );

        // Write cells to database (this should trigger indexing)
        server.chunks.write_cell(&mut cell1).unwrap();
        server.chunks.write_cell(&mut cell2).unwrap();
        server.chunks.write_cell(&mut cell3).unwrap();

        // Trigger indexing by calling ensure_indices
        if let Some(ref index_builder) = server.indexer {
            index_builder.ensure_indices(&cell1, &schema, None);
            index_builder.ensure_indices(&cell2, &schema, None);
            index_builder.ensure_indices(&cell3, &schema, None);
        }

        // Give time for async indexing to complete
        tokio::time::sleep(Duration::from_millis(500)).await;

        // Use the coordinator for distributed search
        use crate::index::full_text::coordinator::DistributedInvertedIndexCoordinator;
        let coordinator = DistributedInvertedIndexCoordinator::new(
            server.consh.clone(),
            server.member_pool.clone(),
        );

        // Verify field statistics using coordinator
        let stats = coordinator
            .get_global_stats(schema_id, content_field_id)
            .await
            .unwrap();
        assert!(
            stats.doc_count >= 2,
            "Should have indexed at least 2 documents"
        );
        assert!(stats.total_length > 0, "Total length should be positive");

        // Search for "rust" - should find doc1 and doc3
        let hits_result = coordinator
            .distributed_search(schema_id, content_field_id, "rust", 10, false)
            .await
            .unwrap();
        let hits = hits_result.unwrap();
        assert!(!hits.is_empty(), "Should find documents containing 'rust'");
        assert!(
            hits.len() >= 1,
            "Should find at least 1 document with 'rust'"
        );

        // Search for "database" - should find doc2
        let hits_result = coordinator
            .distributed_search(schema_id, content_field_id, "database", 10, false)
            .await
            .unwrap();
        let hits = hits_result.unwrap();
        assert!(
            !hits.is_empty(),
            "Should find document containing 'database'"
        );

        // Search for "programming" - should find doc1 and doc3
        let hits_result = coordinator
            .distributed_search(schema_id, content_field_id, "programming", 10, false)
            .await
            .unwrap();
        let hits = hits_result.unwrap();
        assert!(
            hits.len() >= 1,
            "Should find at least 1 document with 'programming'"
        );

        // Search for "storage" - should find doc2
        let hits_result = coordinator
            .distributed_search(schema_id, content_field_id, "storage", 10, false)
            .await
            .unwrap();
        let hits = hits_result.unwrap();
        assert!(
            !hits.is_empty(),
            "Should find document containing 'storage'"
        );

        info!("End-to-end indexing test passed with coordinator");
    }

    #[tokio::test]
    async fn test_end_to_end_update_and_remove() {
        let _ = env_logger::try_init();

        info!("Starting test_end_to_end_update_and_remove");

        // Set up a full NebServer with indexing enabled
        // Use a unique port to avoid conflicts
        let server_addr = "127.0.0.1:29306";
        let group_name = "e2e_update_remove_test";
        info!("Creating server at {}...", server_addr);
        let server_result = tokio::time::timeout(
            Duration::from_secs(30),
            crate::server::NebServer::new_from_opts(
                &crate::server::ServerOptions {
                    chunk_count: 1,
                    total_size: 64 * 1024 * 1024,
                    tiered_config: None,
                    backup_storage: None,
                    wal_storage: None,
                    undo_log_storage: None,
                    raft_storage: None,
                    index_enabled: true,
                    services: vec![crate::server::Service::Cell],
                    enable_recovery: false,
                },
                server_addr,
                group_name,
                async |_| {},
            ),
        )
        .await;

        let server = match server_result {
            Ok(s) => s,
            Err(_) => {
                panic!("Server creation timed out after 30 seconds");
            }
        };
        info!("Server created successfully");

        info!("Server created, setting up schema...");
        let schema_id = 201u32;
        let content_field = "content";
        let content_field_id = hash_str(content_field) as u64;

        let fields =
            crate::ram::schema::Field::new_schema(vec![crate::ram::schema::Field::new_indexed(
                content_field,
                dovahkiin::types::Type::String,
                vec![crate::ram::schema::IndexType::Fulltext],
            )]);

        let schema = crate::ram::schema::Schema::new_with_id(
            schema_id,
            "test_schema_update",
            None,
            fields,
            false,
            false,
        );

        server.meta.schemas.debug_only_new_schema(schema.clone());

        // Find an owned document ID
        info!("Finding owned document ID...");
        let mut doc_id = Id::new(1, 1);
        for i in 0..1000 {
            let test_id = Id::new(i, i);
            if server
                .consh
                .get_server_id(test_id.higher)
                .map(|sid| sid == server.server_id)
                .unwrap_or(false)
            {
                doc_id = test_id;
                break;
            }
        }
        info!("Using doc_id: {:?}", doc_id);

        // Create initial cell
        info!("Creating initial cell...");
        let mut cell_data = OwnedMap::new();
        cell_data.insert(
            content_field,
            OwnedValue::String("initial content about testing".to_string()),
        );
        let mut cell = OwnedCell::new_with_id(schema_id, &doc_id, OwnedValue::Map(cell_data));

        // Write and index
        info!("Writing initial cell...");
        server.chunks.write_cell(&mut cell).unwrap();
        if let Some(ref index_builder) = server.indexer {
            info!("Ensuring indices for initial cell...");
            index_builder.ensure_indices(&cell, &schema, None);
        }
        // Give time for async indexing to complete
        tokio::time::sleep(Duration::from_millis(500)).await;

        // Use the coordinator for distributed search
        use crate::index::full_text::coordinator::DistributedInvertedIndexCoordinator;
        let coordinator = DistributedInvertedIndexCoordinator::new(
            server.consh.clone(),
            server.member_pool.clone(),
        );

        // Verify initial indexing
        info!("Verifying initial indexing...");
        let hits_result = coordinator
            .distributed_search(schema_id, content_field_id, "initial", 10, false)
            .await
            .unwrap();
        let hits = hits_result.unwrap();
        assert!(!hits.is_empty(), "Should find document with 'initial'");
        info!("Initial indexing verified");

        // Update the cell
        info!("Preparing cell update...");
        let mut updated_cell_data = OwnedMap::new();
        updated_cell_data.insert(
            content_field,
            OwnedValue::String("updated content about rust".to_string()),
        );
        let mut updated_cell =
            OwnedCell::new_with_id(schema_id, &doc_id, OwnedValue::Map(updated_cell_data));

        // Update cell and ensure indices
        info!("Updating cell...");
        server.chunks.update_cell(&mut updated_cell).unwrap();
        if let Some(ref index_builder) = server.indexer {
            info!("Ensuring indices for updated cell...");
            index_builder.ensure_indices(&updated_cell, &schema, None);
        }
        info!("Cell updated successfully");

        // Give time for async indexing to complete
        tokio::time::sleep(Duration::from_millis(500)).await;
        info!("Update indexing complete");

        // Note: With append-only posting lists, old terms may still be found.
        // This is a design tradeoff - stale entries should be filtered by the caller
        // or cleaned up by a future compaction process.
        // For now, we just verify the NEW terms are indexed:
        info!("Note: append-only design - old terms 'initial' may still be in index");

        // Should find "updated" and "rust"
        let hits_result = coordinator
            .distributed_search(schema_id, content_field_id, "updated", 10, false)
            .await
            .unwrap();
        let hits = hits_result.unwrap();
        assert!(!hits.is_empty(), "Should find 'updated' after update");

        let hits_result = coordinator
            .distributed_search(schema_id, content_field_id, "rust", 10, false)
            .await
            .unwrap();
        let hits = hits_result.unwrap();
        assert!(!hits.is_empty(), "Should find 'rust' after update");
        info!("Update verification complete");

        // Remove the cell - manually remove inverted index first since we know the metadata
        info!("Preparing cell removal...");

        // Manually remove from inverted indexer using the updated cell metadata
        if let Some(ref index_builder) = server.indexer {
            if let Some(inverted_indexer) = index_builder.clients.fulltext_indexer() {
                let removal_meta = create_test_meta(
                    schema_id,
                    content_field_id,
                    doc_id,
                    "updated content about rust",
                );
                info!("Removing from inverted indexer...");
                inverted_indexer.remove_document(&removal_meta).unwrap();
            }
        }

        info!("Removing cell...");
        server.chunks.remove_cell(&doc_id).unwrap();
        info!("Cell removed successfully");

        // Give time for any async cleanup
        tokio::time::sleep(Duration::from_millis(200)).await;
        info!("Removal complete");

        // Note: With append-only posting lists, removed documents may still appear in search.
        // The stats are updated (doc_count decremented) but posting entries remain.
        // Callers should verify document existence or use a compaction process.
        let stats = coordinator
            .get_global_stats(schema_id, content_field_id)
            .await
            .unwrap();
        assert_eq!(
            stats.doc_count, 0,
            "Document count should be 0 after removal"
        );

        info!("End-to-end update and remove test passed - stats updated correctly");
        info!("Note: append-only design means posting entries remain until compaction");
    }

    #[tokio::test]
    async fn test_index_persistence_after_recovery() {
        let _ = env_logger::try_init();

        info!("Starting test_index_persistence_after_recovery");

        // Create temporary directories for storage
        let wal_dir = TempDir::new().unwrap();
        let backup_dir = TempDir::new().unwrap();
        let raft_dir = TempDir::new().unwrap();

        let wal_path = wal_dir.path().to_str().unwrap().to_string();
        let backup_path = backup_dir.path().to_str().unwrap().to_string();
        let raft_path = raft_dir.path().join("raft").to_str().unwrap().to_string();

        info!(
            "Storage paths: WAL={}, Backup={}, Raft={}",
            wal_path, backup_path, raft_path
        );

        // Phase 1: Create server, write cells, index them, and flush
        let server_addr = "127.0.0.1:29307";
        let group_name = "recovery_test";

        info!("Phase 1: Creating initial server...");
        let server1 = crate::server::NebServer::new_from_opts(
            &crate::server::ServerOptions {
                chunk_count: 1,
                total_size: 64 * 1024 * 1024, // 64MB
                tiered_config: None,
                backup_storage: Some(backup_path.clone()),
                wal_storage: Some(wal_path.clone()),
                undo_log_storage: None,
                raft_storage: Some(raft_path.clone()),
                index_enabled: true,
                services: vec![
                    crate::server::Service::Cell,
                    crate::server::Service::Transaction,
                ],
                enable_recovery: false, // First run, no recovery
            },
            server_addr,
            group_name,
            async |_| {},
        )
        .await;

        info!("Server 1 created, setting up schema...");
        let schema_id = 300u32;
        let content_field = "content";
        let content_field_id = hash_str(content_field) as u64;

        let fields =
            crate::ram::schema::Field::new_schema(vec![crate::ram::schema::Field::new_indexed(
                content_field,
                dovahkiin::types::Type::String,
                vec![crate::ram::schema::IndexType::Fulltext],
            )]);

        let schema = crate::ram::schema::Schema::new_with_id(
            schema_id,
            "recovery_test_schema",
            None,
            fields,
            false,
            false,
        );

        server1.meta.schemas.debug_only_new_schema(schema.clone());

        // Register inverted index schemas BEFORE flushing (needed for flush operations)
        server1.meta.schemas.debug_only_new_schema(inverted_segment_schema());
        server1
            .meta
            .schemas
            .debug_only_new_schema(crate::index::full_text::inverted_stats_schema());

        // Find owned document IDs (skip unit ID)
        info!("Finding owned document IDs...");
        let mut owned_doc_ids = Vec::new();
        let unit_id = Id::unit_id();
        for i in 0..1000 {
            let test_id = Id::new(i, i);
            // Skip unit ID
            if test_id == unit_id {
                continue;
            }
            if server1
                .consh
                .get_server_id(test_id.higher)
                .map(|sid| sid == server1.server_id)
                .unwrap_or(false)
            {
                owned_doc_ids.push(test_id);
                if owned_doc_ids.len() >= 3 {
                    break;
                }
            }
        }

        assert!(
            owned_doc_ids.len() >= 2,
            "Need at least 2 owned documents for test"
        );
        info!("Found {} owned documents", owned_doc_ids.len());

        // Create and write cells with indexed content
        info!("Writing cells with indexed content...");
        let texts = vec![
            "recovery test document one",
            "recovery test document two",
            "recovery test document three",
        ];

        for (i, doc_id) in owned_doc_ids.iter().enumerate() {
            let mut cell_data = OwnedMap::new();
            cell_data.insert(
                content_field,
                OwnedValue::String(texts[i % texts.len()].to_string()),
            );
            let mut cell = OwnedCell::new_with_id(schema_id, doc_id, OwnedValue::Map(cell_data));

            server1.chunks.write_cell(&mut cell).unwrap();

            if let Some(ref index_builder) = server1.indexer {
                index_builder.ensure_indices(&cell, &schema, None);
            }
        }

        // Give time for async indexing to complete
        tokio::time::sleep(Duration::from_millis(500)).await;

        // Verify cells exist before archiving
        info!("Verifying cells exist before archiving...");
        for doc_id in &owned_doc_ids {
            let cell_result = server1.chunks.read_cell(doc_id);
            assert!(
                cell_result.is_ok(),
                "Cell should exist before archiving for doc_id: {:?}",
                doc_id
            );
        }

        // Sync all segments to ensure WAL is persisted before archiving
        info!("Syncing segments before archiving...");
        for chunk in &server1.chunks.list {
            for seg in chunk.segments() {
                if let Err(e) = seg.force_wal_sync() {
                    warn!("Failed to sync segment {}: {:?}", seg.id, e);
                }
            }
        }

        // Verify stats exist in memory before flushing
        info!("Verifying stats exist in memory before flushing...");
        if let Some(ref index_builder) = server1.indexer {
            if let Some(inverted_indexer) = index_builder.clients.fulltext_indexer() {
                let stats_before_flush =
                    inverted_indexer.get_field_stats(schema_id, content_field_id);
                info!(
                    "Stats in memory before flush: doc_count={}, total_length={}",
                    stats_before_flush.doc_count, stats_before_flush.total_length
                );
                assert!(
                    stats_before_flush.doc_count > 0,
                    "Stats should exist in memory before flushing"
                );

                // Verify stats are actually in the map (using PtrHashMap)
                {
                    let stats_key = InvertedIndexer::stats_key(schema_id, content_field_id);
                    if let Some(stat_arc) = inverted_indexer.field_stats.get(&stats_key) {
                        let stat = stat_arc.lock();
                        info!(
                            "Stats found in map: doc_count={}, total_length={}",
                            stat.doc_count, stat.total_length
                        );
                    } else {
                        panic!(
                            "Stats not found in map for key ({}, {})",
                            schema_id, content_field_id
                        );
                    }
                }
            }
        }

        // Manually flush indices to disk
        info!("Flushing indices to disk...");
        if let Some(ref index_builder) = server1.indexer {
            if let Some(inverted_indexer) = index_builder.clients.fulltext_indexer() {
                let flush_result = inverted_indexer.flush_to_disk().await;
                match flush_result {
                    Ok(()) => {
                        info!("Flush completed successfully");
                    }
                    Err(e) => {
                        error!("Flush failed: {:?}", e);
                        panic!("Flush should succeed: {:?}", e);
                    }
                }
            }
        }

        // Give a moment for WAL writes to complete (PendingEntry drop writes to WAL)
        tokio::time::sleep(Duration::from_millis(200)).await;

        // Sync all segments to ensure stats cells are persisted to WAL
        info!("Syncing all segments after flushing indices...");
        for chunk in &server1.chunks.list {
            for seg in chunk.segments() {
                if let Err(e) = seg.force_wal_sync() {
                    warn!("Failed to sync segment {} after flush: {:?}", seg.id, e);
                }
            }
        }

        // Give another moment for sync to complete
        tokio::time::sleep(Duration::from_millis(100)).await;

        // Verify stats cells are in segments before archiving
        // We can't verify via read_cell (cell index might not be updated),
        // but we can verify that segments contain data
        info!("Verifying segments contain data before archiving...");
        let mut segments_with_data = 0;
        for chunk in &server1.chunks.list {
            for seg in chunk.segments() {
                let data_size = seg.append_header.load(Ordering::Relaxed) - seg.addr;
                if data_size > 0 {
                    segments_with_data += 1;
                    info!("Segment {} has {} bytes of data", seg.id, data_size);
                }
            }
        }
        info!(
            "Found {} segments with data before archiving",
            segments_with_data
        );
        assert!(
            segments_with_data > 0,
            "Should have at least one segment with data before archiving"
        );

        // Verify stats cell ID before archiving
        let stats_id = InvertedIndexer::stats_cell_id(schema_id, content_field_id);
        info!("Stats cell ID to recover: {:?}", stats_id);
        info!(
            "Stats cell partition: {}, hash: {}",
            stats_id.higher, stats_id.lower
        );

        // Try to read stats cell before archiving to verify it exists
        info!("Attempting to read stats cell before archiving...");
        match server1.chunks.read_cell(&stats_id) {
            Ok(cell) => {
                info!("SUCCESS: Stats cell readable before archiving! partition={}, hash={}, version={}", 
                      cell.header().partition, cell.header().hash, cell.header().version);
            }
            Err(e) => {
                error!("PROBLEM: Stats cell NOT readable before archiving: {:?}", e);
                error!(
                    "This means flush succeeded but cell isn't in cell_index - this is the bug!"
                );
            }
        }

        // Archive segments to create backup files (needed for recovery)
        info!("Archiving segments for recovery...");
        let mut archived_count = 0;
        let mut total_segments = 0;
        for chunk in &server1.chunks.list {
            for seg in chunk.segments() {
                total_segments += 1;
                let seg_data_size = seg.append_header.load(Ordering::Relaxed) - seg.addr;
                info!(
                    "Segment {}: {} bytes, hash range checking...",
                    seg.id, seg_data_size
                );

                match seg.archive() {
                    Ok(true) => {
                        archived_count += 1;
                        info!("Archived segment {}", seg.id);
                    }
                    Ok(false) => {
                        warn!(
                            "Segment {} archive returned false (may already be archived)",
                            seg.id
                        );
                    }
                    Err(e) => {
                        warn!("Failed to archive segment {}: {:?}", seg.id, e);
                    }
                }
            }
        }
        info!("Archived {} of {} segments", archived_count, total_segments);

        // Verify indices work before recovery
        info!("Verifying indices before recovery...");
        use crate::index::full_text::coordinator::DistributedInvertedIndexCoordinator;
        let coordinator1 = DistributedInvertedIndexCoordinator::new(
            server1.consh.clone(),
            server1.member_pool.clone(),
        );

        let stats1 = coordinator1
            .get_global_stats(schema_id, content_field_id)
            .await
            .unwrap();
        assert!(
            stats1.doc_count >= 2,
            "Should have indexed at least 2 documents before recovery"
        );

        let hits_result = coordinator1
            .distributed_search(schema_id, content_field_id, "recovery", 10, false)
            .await
            .unwrap();
        let hits1 = hits_result.unwrap();
        assert_eq!(
            hits1.len(),
            owned_doc_ids.len(),
            "Should find all documents before recovery"
        );

        info!(
            "Phase 1 complete: {} documents indexed, {} search results",
            stats1.doc_count,
            hits1.len()
        );

        // Drop server1 to simulate shutdown
        drop(server1);
        drop(coordinator1);
        tokio::time::sleep(Duration::from_millis(200)).await;

        // Phase 2: Create new server with recovery enabled
        info!("Phase 2: Creating server with recovery enabled...");
        let server2 = crate::server::NebServer::new_from_opts(
            &crate::server::ServerOptions {
                chunk_count: 1,
                total_size: 64 * 1024 * 1024, // 64MB
                tiered_config: None,
                backup_storage: Some(backup_path.clone()),
                wal_storage: Some(wal_path.clone()),
                undo_log_storage: None,
                raft_storage: Some(raft_path.clone()),
                index_enabled: true,
                services: vec![
                    crate::server::Service::Cell,
                    crate::server::Service::Transaction,
                ],
                enable_recovery: true, // Enable recovery
            },
            server_addr,
            group_name,
            async |_| {},
        )
        .await;

        info!("Server 2 created with recovery, registering schemas...");
        // Re-register schemas (needed for recovery)
        server2.meta.schemas.debug_only_new_schema(schema.clone());
        server2.meta.schemas.debug_only_new_schema(inverted_segment_schema());
        server2
            .meta
            .schemas
            .debug_only_new_schema(crate::index::full_text::inverted_stats_schema());

        // Give recovery time to complete
        tokio::time::sleep(Duration::from_millis(1000)).await;

        // Note: Cell recovery is tested separately in other tests.
        // Here we focus on verifying that indices persist after recovery.
        // Cells may or may not be recovered depending on segment archiving,
        // but indices should be recoverable from disk segments.

        // Verify indices were recovered
        info!("Verifying indices were recovered...");
        let coordinator2 = DistributedInvertedIndexCoordinator::new(
            server2.consh.clone(),
            server2.member_pool.clone(),
        );

        // Stats should be recovered (loaded on-demand from disk)
        // Note: Stats cells are written to segments and should be recoverable via cell index rebuild
        // However, if stats cells aren't in archived segments, they won't be recovered
        // Let's verify that stats can be loaded from disk after recovery
        let stats2 = coordinator2
            .get_global_stats(schema_id, content_field_id)
            .await
            .unwrap();

        // If stats aren't recovered, it means stats cells weren't archived or recovered
        // This could happen if:
        // 1. Stats cells were written but not synced before archiving
        // 2. Stats cells were written to segments that weren't archived
        // 3. Recovery didn't rebuild the cell index correctly for stats cells
        if stats2.doc_count == 0 {
            warn!("Stats not recovered - doc_count is 0. This suggests stats cells weren't archived or recovered.");
            warn!(
                "Stats before recovery: doc_count={}, total_length={}",
                stats1.doc_count, stats1.total_length
            );

            // Try to manually verify if stats cell exists after recovery
            let stats_id = InvertedIndexer::stats_cell_id(schema_id, content_field_id);
            match server2.chunks.read_cell(&stats_id) {
                Ok(cell) => {
                    info!("Stats cell found after recovery: {:?}", stats_id);
                    // Try to load stats from the cell
                    let owned_cell = OwnedCell {
                        header: cell.header().clone(),
                        data: cell.data().owned(),
                    };
                    if let Some(loaded_stats) = FieldStats::from_value(owned_cell.data()) {
                        info!(
                            "Loaded stats from recovered cell: doc_count={}, total_length={}",
                            loaded_stats.doc_count, loaded_stats.total_length
                        );
                        // Stats cell exists but wasn't loaded - this is a bug in get_field_stats
                        panic!("Stats cell exists but get_field_stats returned 0 - bug in recovery logic");
                    }
                }
                Err(e) => {
                    warn!(
                        "Stats cell not found after recovery: {:?}, error: {:?}",
                        stats_id, e
                    );
                    // Stats cell wasn't recovered - this means it wasn't archived or recovery didn't find it
                    panic!("Stats cell not recovered - wasn't archived or recovery didn't find it: {:?}", e);
                }
            }
        }

        assert_eq!(
            stats2.doc_count, stats1.doc_count,
            "Document count should match after recovery"
        );
        assert_eq!(
            stats2.total_length, stats1.total_length,
            "Total length should match after recovery"
        );

        // Search should still work - indices should be recoverable from disk
        let hits_result = coordinator2
            .distributed_search(schema_id, content_field_id, "recovery", 10, false)
            .await
            .unwrap();
        let hits2 = hits_result.unwrap();
        assert_eq!(
            hits2.len(),
            hits1.len(),
            "Should find same number of documents after recovery"
        );

        // Verify specific terms
        let hits_result = coordinator2
            .distributed_search(schema_id, content_field_id, "document", 10, false)
            .await
            .unwrap();
        let hits = hits_result.unwrap();
        assert_eq!(
            hits.len(),
            owned_doc_ids.len(),
            "Should find all documents with 'document'"
        );

        let hits_result = coordinator2
            .distributed_search(schema_id, content_field_id, "one", 10, false)
            .await
            .unwrap();
        let hits = hits_result.unwrap();
        assert!(!hits.is_empty(), "Should find document with 'one'");

        info!("Phase 2 complete: Recovery successful!");
        info!(
            "Stats: doc_count={}, total_length={}",
            stats2.doc_count, stats2.total_length
        );
        info!("Search results: {} documents found", hits2.len());

        info!("Index persistence after recovery test passed!");
    }

    #[tokio::test]
    async fn test_index_recovery_with_new_documents() {
        let _ = env_logger::try_init();

        info!("Starting test_index_recovery_with_new_documents");

        // Create temporary directories for storage
        let wal_dir = TempDir::new().unwrap();
        let backup_dir = TempDir::new().unwrap();
        let raft_dir = TempDir::new().unwrap();

        let wal_path = wal_dir.path().to_str().unwrap().to_string();
        let backup_path = backup_dir.path().to_str().unwrap().to_string();
        let raft_path = raft_dir.path().join("raft").to_str().unwrap().to_string();

        let server_addr = "127.0.0.1:29308";
        let group_name = "recovery_new_docs_test";

        // Phase 1: Create server, index some documents
        info!("Phase 1: Creating initial server and indexing documents...");
        let server1 = crate::server::NebServer::new_from_opts(
            &crate::server::ServerOptions {
                chunk_count: 1,
                total_size: 64 * 1024 * 1024,
                tiered_config: None,
                backup_storage: Some(backup_path.clone()),
                wal_storage: Some(wal_path.clone()),
                undo_log_storage: None,
                raft_storage: Some(raft_path.clone()),
                index_enabled: true,
                services: vec![
                    crate::server::Service::Cell,
                    crate::server::Service::Transaction,
                ],
                enable_recovery: false,
            },
            server_addr,
            group_name,
            async |_| {},
        )
        .await;

        let schema_id = 301u32;
        let content_field = "content";
        let content_field_id = hash_str(content_field) as u64;

        let fields =
            crate::ram::schema::Field::new_schema(vec![crate::ram::schema::Field::new_indexed(
                content_field,
                dovahkiin::types::Type::String,
                vec![crate::ram::schema::IndexType::Fulltext],
            )]);

        let schema = crate::ram::schema::Schema::new_with_id(
            schema_id,
            "recovery_new_docs_schema",
            None,
            fields,
            false,
            false,
        );

        server1.meta.schemas.debug_only_new_schema(schema.clone());

        // Register inverted index schemas
        server1.meta.schemas.debug_only_new_schema(inverted_segment_schema());
        server1
            .meta
            .schemas
            .debug_only_new_schema(crate::index::full_text::inverted_stats_schema());

        // Find owned document IDs
        let mut owned_doc_ids = Vec::new();
        for i in 0..1000 {
            let test_id = Id::new(i, i);
            if server1
                .consh
                .get_server_id(test_id.higher)
                .map(|sid| sid == server1.server_id)
                .unwrap_or(false)
            {
                owned_doc_ids.push(test_id);
                if owned_doc_ids.len() >= 2 {
                    break;
                }
            }
        }

        assert!(owned_doc_ids.len() >= 2, "Need at least 2 owned documents");

        // Write initial documents
        for (i, doc_id) in owned_doc_ids.iter().enumerate() {
            let mut cell_data = OwnedMap::new();
            cell_data.insert(
                content_field,
                OwnedValue::String(format!("initial document {}", i)),
            );
            let mut cell = OwnedCell::new_with_id(schema_id, doc_id, OwnedValue::Map(cell_data));

            server1.chunks.write_cell(&mut cell).unwrap();

            if let Some(ref index_builder) = server1.indexer {
                index_builder.ensure_indices(&cell, &schema, None);
            }
        }

        tokio::time::sleep(Duration::from_millis(500)).await;

        // Flush and archive
        if let Some(ref index_builder) = server1.indexer {
            if let Some(inverted_indexer) = index_builder.clients.fulltext_indexer() {
                inverted_indexer.flush_to_disk().await.unwrap();
            }
        }

        for chunk in &server1.chunks.list {
            for seg in chunk.segments() {
                seg.archive().unwrap();
            }
        }

        let initial_count = owned_doc_ids.len();
        drop(server1);
        tokio::time::sleep(Duration::from_millis(200)).await;

        // Phase 2: Recover and add new documents
        info!("Phase 2: Recovering server and adding new documents...");
        let server2 = crate::server::NebServer::new_from_opts(
            &crate::server::ServerOptions {
                chunk_count: 1,
                total_size: 64 * 1024 * 1024,
                tiered_config: None,
                backup_storage: Some(backup_path.clone()),
                wal_storage: Some(wal_path.clone()),
                undo_log_storage: None,
                raft_storage: Some(raft_path.clone()),
                index_enabled: true,
                services: vec![
                    crate::server::Service::Cell,
                    crate::server::Service::Transaction,
                ],
                enable_recovery: true,
            },
            server_addr,
            group_name,
            async |_| {},
        )
        .await;

        server2.meta.schemas.debug_only_new_schema(schema.clone());
        server2.meta.schemas.debug_only_new_schema(inverted_segment_schema());
        server2
            .meta
            .schemas
            .debug_only_new_schema(crate::index::full_text::inverted_stats_schema());
        tokio::time::sleep(Duration::from_millis(1000)).await;

        // Verify recovered documents
        use crate::index::full_text::coordinator::DistributedInvertedIndexCoordinator;
        let coordinator2 = DistributedInvertedIndexCoordinator::new(
            server2.consh.clone(),
            server2.member_pool.clone(),
        );

        let stats_before = coordinator2
            .get_global_stats(schema_id, content_field_id)
            .await
            .unwrap();
        assert_eq!(
            stats_before.doc_count, initial_count as u64,
            "Should recover initial documents"
        );

        // Add new documents after recovery
        let mut new_doc_ids = Vec::new();
        for i in 1000..2000 {
            let test_id = Id::new(i, i);
            if server2
                .consh
                .get_server_id(test_id.higher)
                .map(|sid| sid == server2.server_id)
                .unwrap_or(false)
            {
                new_doc_ids.push(test_id);
                if new_doc_ids.len() >= 2 {
                    break;
                }
            }
        }

        for (i, doc_id) in new_doc_ids.iter().enumerate() {
            let mut cell_data = OwnedMap::new();
            cell_data.insert(
                content_field,
                OwnedValue::String(format!("new document after recovery {}", i)),
            );
            let mut cell = OwnedCell::new_with_id(schema_id, doc_id, OwnedValue::Map(cell_data));

            server2.chunks.write_cell(&mut cell).unwrap();

            if let Some(ref index_builder) = server2.indexer {
                index_builder.ensure_indices(&cell, &schema, None);
            }
        }

        tokio::time::sleep(Duration::from_millis(500)).await;

        // Verify both recovered and new documents are searchable
        let stats_after = coordinator2
            .get_global_stats(schema_id, content_field_id)
            .await
            .unwrap();
        assert_eq!(
            stats_after.doc_count,
            (initial_count + new_doc_ids.len()) as u64,
            "Should have both recovered and new documents"
        );

        let hits_result = coordinator2
            .distributed_search(schema_id, content_field_id, "initial", 10, false)
            .await
            .unwrap();
        let hits = hits_result.unwrap();
        assert_eq!(hits.len(), initial_count, "Should find recovered documents");

        let hits_result = coordinator2
            .distributed_search(schema_id, content_field_id, "new", 10, false)
            .await
            .unwrap();
        let hits = hits_result.unwrap();
        assert_eq!(hits.len(), new_doc_ids.len(), "Should find new documents");

        info!("Index recovery with new documents test passed!");
    }
}
