# Inverted Index with BM25 Implementation Guide

## Overview

The inverted index implementation provides full-text search capabilities with BM25 ranking. It stores documents as **posting lists** (inverted from document→terms to term→documents) and uses BM25 scoring to rank search results by relevance.

## Architecture

### Three Core Data Structures

The system uses three specialized schemas stored as cells in Nebuchadnezzar:

1. **Posting Lists** (`INVERTED_INDEX_SCHEMA`)
   - Maps each term (token hash) to documents containing it
   - Stores: `[doc_ids, term_frequencies, doc_lengths]` arrays
   - One cell per unique term

2. **Field Statistics** (`INVERTED_STATS_SCHEMA`)
   - Global statistics for a field: total document count and total length
   - Used to compute average document length for BM25
   - One cell per (schema_id, field_id) pair

3. **Document Metadata** (`INVERTED_DOC_SCHEMA`)
   - Per-document length tracking
   - Used during updates/deletes to maintain statistics
   - One cell per document

## Data Flow

### 1. Document Indexing (Insert/Update)

When a document is written or updated with an `InvertedBM25` index:

```
Document: "modern database storage engine"
    ↓
Tokenization & Normalization
    ↓
Tokens: ["modern", "database", "storage", "engine"]
    ↓
Hash each token → [hash1, hash2, hash3, hash4]
    ↓
Build InvertedIndexMeta {
    cell_id: doc_id,
    doc_length: 4,
    tokens: [(hash1, tf=1), (hash2, tf=1), ...]
}
```

**Transaction Steps:**

1. **Update Document Metadata**
   - Store/update doc_length for this document
   - Track previous length if updating

2. **Update Field Statistics**
   - Increment doc_count (if new document)
   - Update total_length: `total_length = total_length - old_length + new_length`
   - Persist stats cell

3. **Update Posting Lists** (for each token)
   - Read posting list cell for token hash
   - Upsert entry: `(doc_id, term_frequency, doc_length)`
   - If posting list doesn't exist, create new cell
   - Persist updated posting list

**Example Posting List Structure:**
```
Term: "database" (hash: 0x1234...)
Posting List:
  doc_ids:      [doc1, doc2, doc3]
  term_freqs:   [1,    2,    1]
  doc_lengths:  [10,   15,   8]
```

### 2. Document Removal

When a document is deleted:

1. **Remove Document Metadata**
   - Read doc_length from metadata cell
   - Delete metadata cell

2. **Update Field Statistics**
   - Decrement doc_count
   - Subtract doc_length from total_length
   - Persist stats cell

3. **Update Posting Lists** (for each token)
   - Remove doc_id from each posting list
   - If posting list becomes empty, delete the cell
   - Otherwise, update the cell

### 3. Query Processing (BM25 Search)

When searching with `bm25_search(schema_id, field_id, query, limit)`:

```
Query: "database ranking"
    ↓
Tokenize query → ["database", "ranking"]
    ↓
Hash tokens → [hash1, hash2]
    ↓
Load Field Statistics
    ↓
For each query term:
  1. Load posting list for term hash
  2. Compute IDF (Inverse Document Frequency)
  3. For each document in posting list:
     - Compute BM25 score contribution
     - Accumulate score in document score map
    ↓
Sort documents by score (descending)
    ↓
Return top N results
```

## BM25 Scoring Formula

The BM25 score combines three factors:

### Components

1. **TF (Term Frequency)**: How often the term appears in the document
2. **IDF (Inverse Document Frequency)**: How rare/common the term is across all documents
3. **Document Length Normalization**: Penalizes very long documents

### Formula

```
BM25(doc, term) = IDF(term) × (TF × (k1 + 1)) / (TF + k1 × (1 - b + b × (doc_len / avg_doc_len)))
```

Where:
- `k1 = 1.5` (term frequency saturation parameter)
- `b = 0.75` (length normalization parameter)
- `doc_len` = number of tokens in document
- `avg_doc_len` = average document length in collection

### IDF Calculation

```
IDF = ln((N - df + 0.5) / (df + 0.5) + 1)
```

Where:
- `N` = total document count
- `df` = document frequency (how many documents contain the term)

### Multi-Term Queries

For queries with multiple terms, scores are **additive**:
```
Final Score(doc) = Σ BM25(doc, term_i) for all query terms
```

## Code Flow Examples

### Indexing a Document

```rust
// 1. User writes a cell with indexed text field
let mut value = OwnedValue::Map(OwnedMap::new());
value["body"] = OwnedValue::String("modern database storage".to_string());
let cell = OwnedCell::new_with_id(schema_id, &doc_id, value);
client.write_cell(cell).await?;

// 2. IndexBuilder automatically detects InvertedBM25 index
// 3. Extracts text, tokenizes, builds InvertedIndexMeta
let meta = build_index_meta(doc_id, schema_id, field_id, text_value)?;

// 4. Transaction updates:
//    - Document metadata cell
//    - Field statistics cell  
//    - Posting list cells (one per unique token)
InvertedIndexer::add_document(&meta).await?;
```

### Searching

```rust
// Query for "database ranking"
let hits = idx_client.bm25_search(
    schema_id,
    field_id,
    "database ranking",
    10  // top 10 results
).await?;

// Returns: Vec<BM25Hit> sorted by score descending
// BM25Hit { id: doc_id, score: 2.45 }
```

## Key Design Decisions

### 1. Transactional Consistency

All index updates happen within transactions, ensuring:
- Atomic updates to posting lists, stats, and metadata
- Consistency even if operations fail mid-way
- Proper handling of concurrent updates

### 2. Cell-Based Storage

Each posting list is stored as a separate cell, allowing:
- Efficient updates (only affected terms need changes)
- Distributed storage across cluster
- Independent scaling per term

### 3. Document Length Tracking

Storing doc_length per document enables:
- Accurate BM25 scoring (requires doc length)
- Efficient updates (can recompute stats without re-scanning)
- Proper handling of document updates

### 4. Hash-Based Terms

Terms are stored as hashes (u64) rather than strings:
- Fixed-size keys (efficient storage)
- Fast lookups
- Collision-resistant with good hash function

## Performance Characteristics

### Indexing
- **Time Complexity**: O(T) where T = number of unique tokens in document
- **Space Complexity**: O(T) per document
- **Update Cost**: Proportional to number of changed tokens

### Querying
- **Time Complexity**: O(Q × D) where Q = query terms, D = average documents per term
- **Space Complexity**: O(D) for score accumulation
- **Optimization**: Early termination possible (not yet implemented)

## Integration Points

### Schema Definition

```rust
let fields = Field::new_schema(vec![
    Field::new_indexed(
        "body",
        Type::String,
        vec![IndexType::InvertedBM25]
    )
]);
```

### Automatic Indexing

When cells are written/updated, `IndexBuilder::probe_cell_indices()`:
1. Detects fields with `InvertedBM25` index type
2. Extracts text values
3. Tokenizes and builds metadata
4. Triggers async index updates

### Query API

```rust
// Via IndexedDataClient
let hits = idx_data_client.bm25_search(
    schema_id,
    field_id,
    "search query",
    limit
).await?;
```

## Limitations & Future Enhancements

### Current Limitations
- No phrase matching (only term-based)
- No stemming or lemmatization
- Fixed BM25 parameters (k1=1.5, b=0.75)
- No query-time boosting
- No stop-word filtering

### Potential Enhancements
- Phrase queries with positional information
- Custom BM25 parameters per field
- Query expansion/synonyms
- Faceted search
- Highlighting support
- Incremental index updates without full re-indexing

## Example Walkthrough

### Scenario: Indexing and Searching Documents

**Step 1: Create Schema**
```rust
let schema = Schema::new_with_id(
    100,
    "articles",
    None,
    Field::new_schema(vec![
        Field::new_indexed("title", Type::String, vec![IndexType::InvertedBM25]),
        Field::new_indexed("body", Type::String, vec![IndexType::InvertedBM25]),
    ]),
    false,
    false
);
```

**Step 2: Insert Documents**
```rust
// Doc 1: "modern database storage engine"
// Doc 2: "distributed transactions and consensus"
// Doc 3: "ranking algorithms for search and bm25 scoring"
```

**Step 3: Indexing Happens Automatically**
- Each document is tokenized
- Posting lists are created/updated for each term
- Statistics are maintained

**Step 4: Search**
```rust
let results = bm25_search(100, field_id, "database ranking", 5);
// Returns documents ranked by relevance:
// 1. Doc 1 (contains "database")
// 2. Doc 3 (contains "ranking")
```

The BM25 algorithm ensures documents matching multiple query terms rank higher, and common terms contribute less to the score than rare terms.

