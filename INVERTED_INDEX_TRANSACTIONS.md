# How Inverted Indexer Uses Transactions

## Overview

The inverted indexer **depends on transactions** to ensure **atomicity** and **consistency** when updating multiple related data structures. Each index update involves modifying several cells simultaneously, and transactions guarantee they all succeed or all fail together.

## Why Transactions Are Required

### The Multi-Cell Update Problem

When indexing a single document, we need to update **multiple cells**:

1. **Document Metadata Cell** - Store document length
2. **Field Statistics Cell** - Update total document count and total length  
3. **Posting List Cells** - One per unique token (could be many!)

**Example:** Indexing "modern database storage engine" requires:
- 1 document metadata update
- 1 statistics update  
- 4 posting list updates (one per token)

**Without transactions:** If any update fails mid-way, the index becomes inconsistent:
- Statistics might be wrong
- Some posting lists updated, others not
- Document metadata might be missing

**With transactions:** All updates succeed together or all rollback → **consistent state**

## Transaction Flow

### High-Level Pattern

```rust
self.neb_client.transaction(|txn| async move {
    // All operations use the same transaction handle
    // They all succeed or all fail together
    txn.read(...).await?;
    txn.write(...).await?;
    txn.update(...).await?;
    Ok(())
}).await
```

### Transaction Lifecycle

```
1. BEGIN
   ↓
2. EXECUTE (read/write/update operations)
   ↓
3. PREPARE (validate no conflicts)
   ↓
4. COMMIT (make changes permanent)
   OR
   ABORT (rollback all changes)
```

## Detailed Operation: Adding a Document

### Step-by-Step Transaction Flow

```rust
pub async fn add_document(&self, meta: &InvertedIndexMeta) -> Result<(), TxnError> {
    self.neb_client.transaction(|txn| async move {
        // STEP 1: Update Document Metadata
        let prev_length = Self::upsert_doc_meta(txn, ...).await?;
        // Reads existing doc metadata (if any)
        // Writes/updates doc length
        
        // STEP 2: Update Field Statistics  
        let mut stats = Self::load_stats(txn, ...).await?;
        // Reads current statistics
        stats.apply_upsert(new_length, prev_length);
        Self::persist_stats(txn, ...).await?;
        // Updates total_length and doc_count
        
        // STEP 3: Update All Posting Lists
        for token in meta.tokens.iter() {
            Self::upsert_posting(txn, meta, token).await?;
            // For each token:
            // - Read posting list cell
            // - Add/update document entry
            // - Write/update posting list cell
        }
        
        Ok(())  // Transaction commits if all steps succeed
    }).await
}
```

### What Happens Inside the Transaction

#### 1. Document Metadata Update

```rust
async fn upsert_doc_meta(txn: &Transaction, ...) -> Result<Option<u32>, TxnError> {
    let meta_id = Self::doc_meta_cell_id(...);
    
    // Read within transaction
    match txn.read(meta_id).await? {
        Some(mut cell) => {
            // Update existing
            let prev = extract_doc_length(&cell);
            cell[DOC_LENGTH_FIELD_ID] = OwnedValue::U32(doc_length);
            txn.update(cell).await?;  // Update within transaction
            Ok(prev)
        }
        None => {
            // Create new
            let cell = OwnedCell::new_with_id(...);
            txn.write(cell).await?;  // Write within transaction
            Ok(None)
        }
    }
}
```

**Transaction Benefits:**
- If another transaction is updating the same doc metadata, conflict detection prevents corruption
- Read sees consistent snapshot (no partial updates)

#### 2. Statistics Update

```rust
async fn persist_stats(txn: &Transaction, ...) -> Result<(), TxnError> {
    let stats_id = Self::stats_cell_id(...);
    
    // Read current stats
    match txn.read(stats_id).await? {
        Some(_) => {
            // Update existing stats cell
            txn.update(cell).await?
        }
        None => {
            // Create new stats cell
            txn.write(cell).await?
        }
    }
}
```

**Critical:** Statistics must be updated **atomically** with document metadata. If stats update fails but doc metadata succeeds, counts become wrong.

#### 3. Posting List Updates

```rust
async fn upsert_posting(txn: &Transaction, meta: &InvertedIndexMeta, token: &TokenStat) -> Result<(), TxnError> {
    let term_id = Self::term_cell_id(meta.schema_id, meta.field_id, token.term_hash);
    
    // Read posting list within transaction
    match txn.read(term_id).await? {
        Some(mut cell) => {
            // Update existing posting list
            let mut postings = PostingList::from_value(&cell.data)?;
            postings.upsert(meta.cell_id, token.term_freq, meta.doc_length);
            cell.data = postings.into_value();
            txn.update(cell).await?  // Update within transaction
        }
        None => {
            // Create new posting list
            let mut postings = PostingList::new();
            postings.upsert(meta.cell_id, token.term_freq, meta.doc_length);
            let cell = OwnedCell::new_with_id(..., postings.into_value());
            txn.write(cell).await?  // Write within transaction
        }
    }
}
```

**Transaction Benefits:**
- Multiple documents indexing the same term concurrently are handled safely
- No race conditions when updating posting lists
- If one posting list update fails, entire transaction rolls back

## Transaction Isolation & Consistency

### Read Consistency

All reads within a transaction see a **consistent snapshot**:

```rust
// Read 1: Get current stats
let stats = load_stats(txn, ...).await?;
// stats.doc_count = 100

// Read 2: Get doc metadata  
let prev = upsert_doc_meta(txn, ...).await?;
// Sees consistent state, not affected by concurrent transactions

// Write: Update stats based on reads
stats.apply_upsert(new_length, prev_length);
persist_stats(txn, ...).await?;
```

### Write Atomicity

All writes are **atomic** - they all succeed or all fail:

```rust
// If ANY of these fail, ALL rollback:
txn.write(doc_meta_cell).await?;      // ✓
txn.update(stats_cell).await?;        // ✓
txn.update(posting_list_1).await?;    // ✓
txn.update(posting_list_2).await?;    // ✗ FAILS HERE
// → ALL previous writes rollback
// → Index remains consistent
```

### Conflict Detection

The transaction system detects conflicts:

```rust
// Transaction A: Reading stats
txn_a.read(stats_id).await?;

// Transaction B: Updating stats (concurrent)
txn_b.update(stats_cell).await?;
txn_b.commit().await?;

// Transaction A: Trying to update based on stale read
txn_a.update(stats_cell).await?;  // ❌ Conflict detected!
txn_a.prepare().await?;            // ❌ Fails: NotRealizable
// → Transaction A retries with fresh data
```

## Removing a Document

Similar pattern, but in reverse:

```rust
pub async fn remove_document(&self, meta: &InvertedIndexMeta) -> Result<(), TxnError> {
    self.neb_client.transaction(|txn| async move {
        // STEP 1: Remove document metadata
        let doc_length = Self::remove_doc_meta(txn, ...).await?;
        
        // STEP 2: Update statistics (decrement counts)
        let mut stats = Self::load_stats(txn, ...).await?;
        stats.apply_remove(doc_length);
        Self::persist_stats(txn, ...).await?;
        
        // STEP 3: Remove from all posting lists
        for token in meta.tokens.iter() {
            Self::remove_posting(txn, meta, token).await?;
        }
        
        Ok(())
    }).await
}
```

**Atomicity:** If removing from posting lists fails, document metadata and statistics rollback too.

## Error Handling & Retries

### Automatic Retry on Conflicts

The transaction system automatically retries on conflicts:

```rust
// In AsyncClient::transaction()
while retried < TRANSACTION_MAX_RETRY {
    txn.tid = txn.client.begin().await?;
    
    match func(txn_ref).await {
        Ok(val) => {
            txn.prepare().await?;  // May fail with NotRealizable
            txn.commit().await?;   // Success!
            return Ok(val);
        }
        Err(TxnError::NotRealizable(_)) => {
            txn.abort().await?;
            // Exponential backoff
            sleep(backoff_ms).await;
            retried += 1;
            // Retry with fresh transaction
        }
    }
}
```

**Example Retry Scenario:**

```
Attempt 1:
  - Begin transaction
  - Read stats (doc_count = 100)
  - Update stats (doc_count = 101)
  - Prepare → ❌ Conflict! Another txn updated stats
  - Abort & retry

Attempt 2 (after backoff):
  - Begin new transaction  
  - Read stats (doc_count = 101) ← Fresh data!
  - Update stats (doc_count = 102)
  - Prepare → ✓ Success
  - Commit → ✓ Success
```

## Why Not Use Plain Writes?

### Without Transactions (Hypothetical)

```rust
// ❌ BAD: No atomicity
async fn add_document_bad(meta: &InvertedIndexMeta) {
    // Update doc metadata
    client.write_cell(doc_meta_cell).await?;  // ✓ Succeeds
    
    // Update stats
    client.update_cell(stats_cell).await?;   // ✓ Succeeds
    
    // Update posting list 1
    client.update_cell(posting_1).await?;    // ✓ Succeeds
    
    // Update posting list 2
    client.update_cell(posting_2).await?;    // ❌ FAILS!
    
    // Problem: First 3 updates are committed, but 4th failed
    // → Index is now INCONSISTENT!
    // → Statistics show doc_count = 101
    // → But posting list 2 doesn't have this document
    // → Queries will return wrong results!
}
```

### With Transactions (Current Implementation)

```rust
// ✓ GOOD: Atomic updates
async fn add_document_good(meta: &InvertedIndexMeta) {
    client.transaction(|txn| async move {
        txn.write(doc_meta_cell).await?;     // ✓
        txn.update(stats_cell).await?;       // ✓
        txn.update(posting_1).await?;         // ✓
        txn.update(posting_2).await?;         // ❌ FAILS!
        
        // Transaction automatically aborts
        // → ALL changes rollback
        // → Index remains CONSISTENT
        // → Can retry later
    }).await
}
```

## Performance Considerations

### Transaction Overhead

**Costs:**
- Network round-trips for begin/prepare/commit
- Lock management overhead
- Conflict detection checks

**Benefits:**
- **Correctness:** Guaranteed consistency
- **Concurrency:** Safe parallel indexing
- **Reliability:** Automatic retry on conflicts

### Optimization Opportunities

1. **Batch Updates:** Update multiple posting lists in parallel (within same transaction)
2. **Read-Only Transactions:** For queries, use read-only transactions (lighter weight)
3. **Transaction Pooling:** Reuse transaction connections

## Summary

### Key Points

1. **Atomicity:** All index updates succeed or all fail together
2. **Consistency:** Index always reflects correct state
3. **Isolation:** Concurrent transactions don't interfere
4. **Durability:** Committed changes persist

### Transaction Dependencies

The inverted indexer requires transactions because:

- ✅ **Multi-cell updates** need atomicity
- ✅ **Statistics consistency** requires coordinated updates  
- ✅ **Concurrent indexing** needs conflict detection
- ✅ **Error recovery** needs rollback capability

Without transactions, the index could become corrupted with:
- Wrong document counts
- Missing posting list entries
- Inconsistent statistics
- Race conditions on concurrent updates

**Transactions ensure the inverted index remains reliable and consistent!**

