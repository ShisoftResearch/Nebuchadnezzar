# Hybrid Inverted Index Design

## Goal
Create a distributed inverted indexer that **avoids expensive transactions** while maintaining consistency through:
- Neb Cells for persistence
- Rust data structures for runtime performance
- Partition-aware loading (each node only loads its own documents)

## Problem with Current Design

The current implementation uses full transactions for every index operation:

```rust
pub async fn add_document(&self, meta: &InvertedIndexMeta) -> Result<(), TxnError> {
    self.neb_client.transaction(|txn| async move {
        // Read stats, update posting lists, update doc metadata
        // All within expensive distributed transaction
    }).await
}
```

**Issues:**
- Every index update requires a distributed transaction
- Transaction overhead for simple append operations
- Lock contention on frequently accessed terms
- Poor scalability for high-throughput indexing

## Hybrid Approach

### Core Principle: Partition Ownership

Each node is **authoritative** for documents it owns:
```rust
fn owns_document(&self, doc_id: &Id) -> bool {
    self.conshash.get_server_id(doc_id.higher) == self.server_id
}
```

**Why this works:**
- Documents are partitioned by consistent hashing
- Only the owning node can modify a document
- No cross-node conflicts for document updates
- Local in-memory structures are sufficient for owned documents

### Architecture

```
┌─────────────────────────────────────────────────────┐
│           HybridInvertedIndexer                      │
├─────────────────────────────────────────────────────┤
│                                                       │
│  Runtime (In-Memory):                                │
│  ┌─────────────────────────────────────────────┐   │
│  │ HashMap<(schema, field, term), PostingList>  │   │
│  │   - Only loaded for owned documents          │   │
│  │   - RwLock for concurrent access             │   │
│  └─────────────────────────────────────────────┘   │
│                                                       │
│  ┌─────────────────────────────────────────────┐   │
│  │ HashMap<(schema, field), FieldStats>         │   │
│  │   - doc_count, total_length                  │   │
│  └─────────────────────────────────────────────┘   │
│                                                       │
│  ┌─────────────────────────────────────────────┐   │
│  │ HashMap<(schema, field, doc_id), DocMeta>    │   │
│  │   - doc_length, tokens                       │   │
│  └─────────────────────────────────────────────┘   │
│                                                       │
│  Persistence (Neb Cells):                            │
│  ┌─────────────────────────────────────────────┐   │
│  │ Segmented Linked List (_NEB_ID_LIST)        │   │
│  │   - term -> [segment1, segment2, ...]        │   │
│  │   - Each segment stores batch of doc IDs     │   │
│  └─────────────────────────────────────────────┘   │
│                                                       │
│  Background Sync:                                    │
│  ┌─────────────────────────────────────────────┐   │
│  │ Periodic flush: memory -> cells              │   │
│  │ Batch updates to minimize write overhead     │   │
│  └─────────────────────────────────────────────┘   │
└─────────────────────────────────────────────────────┘
```

### Data Structures

#### 1. In-Memory Posting List

```rust
struct RuntimePostingList {
    // Owned documents only
    postings: Vec<(Id, u32, u32)>,  // (doc_id, term_freq, doc_length)
    dirty: bool,  // Needs to be flushed to disk
}

impl RuntimePostingList {
    fn add(&mut self, doc_id: Id, tf: u32, doc_len: u32) {
        // Simple in-place update
        if let Some(pos) = self.postings.iter().position(|(id, _, _)| *id == doc_id) {
            self.postings[pos] = (doc_id, tf, doc_len);
        } else {
            self.postings.push((doc_id, tf, doc_len));
        }
        self.dirty = true;
    }
}
```

#### 2. Persistent Posting List (Segmented)

Using `_NEB_ID_LIST` pattern:

```rust
// Schema: _NEB_ID_LIST (ID: 100)
// Fields:
//   _next: Id         // pointer to next segment (or NULL)
//   _list: Id[]       // array of document IDs

// For inverted index, we extend this with parallel arrays:
const INVERTED_SEGMENT_SCHEMA: &str = "INVERTED_SEGMENT";
// Fields:
//   _next: Id              // pointer to next segment
//   doc_ids: Id[]          // document IDs
//   term_freqs: u32[]      // term frequencies (parallel to doc_ids)
//   doc_lengths: u32[]     // document lengths (parallel to doc_ids)

struct SegmentedPostingList {
    schema_id: u32,
    field_id: u64,
    term_hash: u64,
    head_segment_id: Id,   // Deterministic: derived from (schema, field, term, 0)
}

impl SegmentedPostingList {
    const MAX_SEGMENT_SIZE: usize = 1000;  // Max docs per segment
    
    fn segment_id(&self, segment_idx: u32) -> Id {
        // Deterministic ID: hash of (schema, field, term, segment_index)
        Id::from_obj(&(self.schema_id, self.field_id, self.term_hash, segment_idx))
    }
    
    async fn append(&self, chunks: &Chunks, doc_id: Id, tf: u32, doc_len: u32) 
        -> Result<(), WriteError> 
    {
        // Load head segment
        let head_id = self.segment_id(0);
        let mut segment = match chunks.read(head_id).await {
            Ok(cell) => PostingSegment::from_cell(cell),
            Err(ReadError::CellDoesNotExisted) => PostingSegment::new(),
            Err(e) => return Err(e.into()),
        };
        
        // If head segment is full, create new head and link
        if segment.is_full() {
            let new_head = PostingSegment::new();
            let new_head_id = self.segment_id(self.next_segment_index());
            
            // Write new head with _next pointing to old head
            new_head._next = Some(head_id);
            chunks.upsert(new_head_id, new_head.to_cell()).await?;
            
            segment = new_head;
        }
        
        // Append to head segment
        segment.add(doc_id, tf, doc_len);
        chunks.upsert(head_id, segment.to_cell()).await?;
        
        Ok(())
    }
    
    async fn iterate(&self, chunks: &Chunks) -> impl Iterator<Item = (Id, u32, u32)> {
        // Read linked list of segments
        let mut segments = vec![];
        let mut current_id = Some(self.head_segment_id);
        
        while let Some(seg_id) = current_id {
            match chunks.read(seg_id).await {
                Ok(cell) => {
                    let segment = PostingSegment::from_cell(cell);
                    current_id = segment._next;
                    segments.push(segment);
                }
                Err(_) => break,
            }
        }
        
        // Return iterator over all segments
        segments.into_iter().flat_map(|seg| seg.postings.into_iter())
    }
}
```

### Ownership-Based Loading

```rust
pub struct HybridInvertedIndexer {
    server_id: u64,
    conshash: Arc<ConsistentHashing>,
    chunks: Arc<Chunks>,
    
    // In-memory indices (only for owned documents)
    posting_lists: Arc<RwLock<HashMap<(u32, u64, u64), RuntimePostingList>>>,
    field_stats: Arc<RwLock<HashMap<(u32, u64), FieldStats>>>,
    doc_metadata: Arc<RwLock<HashMap<(u32, u64, Id), DocMeta>>>,
    
    // Background sync
    flush_interval: Duration,
    shutdown: Arc<AtomicBool>,
}

impl HybridInvertedIndexer {
    fn owns_document(&self, doc_id: &Id) -> bool {
        self.conshash.get_server_id(doc_id.higher)
            .map(|sid| sid == self.server_id)
            .unwrap_or(false)
    }
    
    pub async fn load_owned_documents(&self) -> Result<(), String> {
        // Scan chunks for all cells
        for chunk in self.chunks.iter() {
            for cell in chunk.cells() {
                // Only load if we own this document
                if !self.owns_document(&cell.id) {
                    continue;
                }
                
                // Check if cell has indexed fields
                let schema = self.get_schema(cell.schema_id)?;
                for (field_id, index_types) in &schema.index_fields {
                    if index_types.contains(&IndexType::InvertedBM25) {
                        // Load this document's metadata
                        self.load_document_metadata(&cell, *field_id).await?;
                    }
                }
            }
        }
        
        Ok(())
    }
    
    async fn load_document_metadata(&self, cell: &OwnedCell, field_id: u64) 
        -> Result<(), String> 
    {
        // Build index metadata from cell
        let meta = build_index_meta(
            cell.id,
            cell.schema_id,
            field_id,
            cell.data[field_id].clone(),
        )?;
        
        // Load into memory structures
        let mut doc_meta = self.doc_metadata.write().await;
        doc_meta.insert(
            (cell.schema_id, field_id, cell.id),
            DocMeta {
                doc_length: meta.doc_length,
                tokens: meta.tokens.clone(),
            },
        );
        
        // Update field stats
        let mut stats = self.field_stats.write().await;
        let stat = stats.entry((cell.schema_id, field_id))
            .or_insert_with(FieldStats::default);
        stat.doc_count += 1;
        stat.total_length += meta.doc_length as u64;
        
        // Update posting lists (in memory only)
        let mut postings = self.posting_lists.write().await;
        for token in &meta.tokens {
            let key = (cell.schema_id, field_id, token.term_hash);
            let posting = postings.entry(key)
                .or_insert_with(RuntimePostingList::new);
            posting.add(cell.id, token.term_freq, meta.doc_length);
        }
        
        Ok(())
    }
}
```

### Adding Documents (No Transactions!)

```rust
impl HybridInvertedIndexer {
    pub async fn add_document(&self, meta: &InvertedIndexMeta) -> Result<(), IndexError> {
        // Check ownership
        if !self.owns_document(&meta.cell_id) {
            return Err(IndexError::NotOwned);
        }
        
        // Update in-memory structures (fast!)
        {
            let mut doc_meta = self.doc_metadata.write().await;
            let prev = doc_meta.insert(
                (meta.schema_id, meta.field_id, meta.cell_id),
                DocMeta {
                    doc_length: meta.doc_length,
                    tokens: meta.tokens.clone(),
                },
            );
            
            let mut stats = self.field_stats.write().await;
            let stat = stats.entry((meta.schema_id, meta.field_id))
                .or_insert_with(FieldStats::default);
            
            if let Some(prev_meta) = prev {
                // Update: adjust stats
                stat.total_length = stat.total_length
                    .saturating_sub(prev_meta.doc_length as u64)
                    .saturating_add(meta.doc_length as u64);
            } else {
                // Insert: increment doc count
                stat.doc_count += 1;
                stat.total_length += meta.doc_length as u64;
            }
        }
        
        // Update posting lists
        {
            let mut postings = self.posting_lists.write().await;
            for token in &meta.tokens {
                let key = (meta.schema_id, meta.field_id, token.term_hash);
                let posting = postings.entry(key)
                    .or_insert_with(RuntimePostingList::new);
                posting.add(meta.cell_id, token.term_freq, meta.doc_length);
            }
        }
        
        // Mark dirty - will be flushed by background task
        // No immediate persistence required!
        
        Ok(())
    }
}
```

### Background Flush

```rust
impl HybridInvertedIndexer {
    pub fn start_background_flush(&self) {
        let indexer = self.clone();
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
    
    async fn flush_to_disk(&self) -> Result<(), IndexError> {
        // Flush posting lists
        let postings = self.posting_lists.read().await;
        for ((schema, field, term), posting) in postings.iter() {
            if !posting.dirty {
                continue;
            }
            
            // Write to segmented list
            let seg_list = SegmentedPostingList {
                schema_id: *schema,
                field_id: *field,
                term_hash: *term,
                head_segment_id: Self::term_segment_id(*schema, *field, *term, 0),
            };
            
            // Batch write all postings
            for (doc_id, tf, doc_len) in &posting.postings {
                seg_list.append(&self.chunks, *doc_id, *tf, *doc_len).await?;
            }
        }
        
        // Flush stats
        let stats = self.field_stats.read().await;
        for ((schema, field), stat) in stats.iter() {
            let stats_id = Self::stats_cell_id(*schema, *field);
            let cell = OwnedCell::new_with_id(
                *INVERTED_STATS_SCHEMA_ID,
                &stats_id,
                stat.to_value(),
            );
            self.chunks.upsert(stats_id, cell).await?;
        }
        
        Ok(())
    }
}
```

### Querying (Read from Memory + Disk)

```rust
impl HybridInvertedIndexer {
    pub async fn bm25_search(
        &self,
        schema_id: u32,
        field_id: u64,
        query: &str,
        limit: usize,
    ) -> Result<Vec<BM25Hit>, IndexError> {
        let query_terms = tokenize_query(query);
        
        // Get stats (from memory if available, else disk)
        let stats = {
            let stats = self.field_stats.read().await;
            stats.get(&(schema_id, field_id))
                .cloned()
                .unwrap_or_default()
        };
        
        if stats.doc_count == 0 {
            return Ok(vec![]);
        }
        
        let avg_doc_len = stats.avg_length();
        let mut scores: HashMap<Id, f32> = HashMap::new();
        
        for term_hash in query_terms {
            // Check memory first
            let postings = {
                let lists = self.posting_lists.read().await;
                lists.get(&(schema_id, field_id, term_hash))
                    .map(|p| p.postings.clone())
            };
            
            if let Some(postings) = postings {
                // Use in-memory postings (owned documents)
                let df = postings.len() as u64;
                let idf = compute_idf(stats.doc_count, df);
                
                for (doc_id, tf, doc_len) in postings {
                    let score = bm25_score(tf as f32, doc_len as f32, avg_doc_len, idf);
                    *scores.entry(doc_id).or_insert(0.0) += score;
                }
            } else {
                // Fall back to disk (for cross-node queries)
                let seg_list = SegmentedPostingList {
                    schema_id,
                    field_id,
                    term_hash,
                    head_segment_id: Self::term_segment_id(schema_id, field_id, term_hash, 0),
                };
                
                for (doc_id, tf, doc_len) in seg_list.iterate(&self.chunks).await {
                    let idf = compute_idf(stats.doc_count, 1);
                    let score = bm25_score(tf as f32, doc_len as f32, avg_doc_len, idf);
                    *scores.entry(doc_id).or_insert(0.0) += score;
                }
            }
        }
        
        let mut hits = scores
            .into_iter()
            .map(|(id, score)| BM25Hit { id, score })
            .collect::<Vec<_>>();
        hits.sort_by(|a, b| b.score.partial_cmp(&a.score).unwrap());
        hits.truncate(limit);
        
        Ok(hits)
    }
}
```

## Consistency Guarantees

### Within a Node
- **Ownership guarantee**: Only owner node modifies its documents
- **No conflicts**: No distributed locking needed
- **Eventually consistent**: Background flush ensures persistence

### Cross-Node
- **Partition isolation**: Each node manages its own partition
- **Query aggregation**: Search can query multiple nodes and merge results
- **No distributed transactions**: Each node operates independently

### Failure Handling

```rust
impl HybridInvertedIndexer {
    pub async fn recover_from_disk(&self) -> Result<(), IndexError> {
        // On startup, load all owned documents from disk
        self.load_owned_documents().await?;
        
        // Disk is always authoritative source
        // In-memory structures are just a cache
        
        Ok(())
    }
    
    pub async fn graceful_shutdown(&self) -> Result<(), IndexError> {
        // Signal shutdown
        self.shutdown.store(true, Ordering::Relaxed);
        
        // Final flush
        self.flush_to_disk().await?;
        
        Ok(())
    }
}
```

## Advantages

1. **No transaction overhead** - direct memory updates
2. **Partition-aware** - each node only loads what it owns
3. **Fast writes** - in-memory updates with async persistence
4. **Scalable** - no cross-node coordination for writes
5. **Recoverable** - disk is authoritative, memory is cache

## Trade-offs

1. **Eventual consistency** - updates visible in memory before disk
2. **Memory overhead** - must keep index in memory for owned docs
3. **Recovery time** - must reload from disk on startup
4. **Query complexity** - cross-partition queries require RPC

## Implementation Checklist

- [ ] Define `INVERTED_SEGMENT_SCHEMA` with segmented linked list fields
- [ ] Implement `RuntimePostingList` for in-memory operations
- [ ] Implement `SegmentedPostingList` for persistent operations
- [ ] Implement `HybridInvertedIndexer` with ownership checking
- [ ] Implement background flush task
- [ ] Implement recovery from disk on startup
- [ ] Update `InvertedIndexClient` to use hybrid indexer
- [ ] Add tests for partition-aware loading
- [ ] Add tests for cross-node queries
- [ ] Add benchmarks comparing transactional vs hybrid approach

