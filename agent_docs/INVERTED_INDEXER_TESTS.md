# InvertedIndexer Atomic Operations Tests

## Overview

This document describes the focused tests for the InvertedIndexer that specifically test **lock-based atomic operations** through `lock_or_insert_cell` and `upsert_cell`.

### Concurrency Strategy

Unlike HashIndexer which uses explicit compare-and-swap with retry loops (`compare_version_and_set_field`), the InvertedIndexer uses **cell-level locking** for atomicity:

1. **`lock_or_insert_cell(hash)`** - Atomically locks a cell or inserts a new one
2. **`upsert_cell(&mut cell)`** - Updates the cell while holding the lock
3. Lock is released when `CellGuard` is dropped

This approach provides strong atomicity guarantees while avoiding the complexity of retry loops.

## Test Coverage

### 1. `test_cas_concurrent_append_same_term`
**Focus**: Tests atomicity of concurrent appends to the same posting list

**Scenario**: 20 concurrent threads all trying to append to the same term's posting list

**Atomic Operation**: 
- `lock_or_insert_cell(head_hash)` - Each thread atomically locks the head segment
- While holding lock: read current segment, append entry, call `upsert_cell`
- Lock ensures only one thread modifies the segment at a time

**Expected**: All 20 appends succeed without data loss or corruption

---

### 2. `test_cas_segment_overflow_concurrent`
**Focus**: Tests prepend logic when segments overflow under high concurrency

**Scenario**: 1200 concurrent appends to trigger segment overflow (max segment size = 1000)

**Atomic Operation**: While holding lock on head:
- Detect segment is full
- Create overflow cell with old head content
- Create new head pointing to overflow
- Both operations complete atomically before lock release

**Expected**: All 1200 entries are successfully stored across multiple segments

---

### 3. `test_cas_version_preservation`
**Focus**: Verifies cell versions are correctly stored with each posting entry

**Scenario**: Add documents with different versions (1, 5, 10, 15, 20)

**Atomic Operation**: 
- `to_cell_with_version` preserves current cell version
- Lock ensures version is read and written atomically
- Cell version increments happen atomically in the storage layer

**Expected**: All versions are correctly stored and retrievable

---

### 4. `test_cas_concurrent_stats_updates`
**Focus**: Tests field stats cache updates under concurrent modifications

**Scenario**: 50 concurrent document additions with stats updates

**CAS Operation**: Lock-free PtrHashMap operations for stats cache

**Expected**: Final stats correctly reflect all 50 documents (doc_count=50, total_length=500)

---

### 5. `test_cas_document_update_version_increment`
**Focus**: Tests document updates with version increments

**Scenario**: Add document v1, then update to v2

**CAS Operation**: Append-only posting lists store both versions

**Expected**: 
- Both versions present in posting list (append-only)
- Stats only count document once (updated, not added)

---

### 6. `test_cas_concurrent_mixed_operations`
**Focus**: Tests concurrent additions and removals

**Scenario**: 
- Phase 1: Add 30 documents
- Phase 2: Concurrently remove 10 and add 10 new ones

**CAS Operation**: Concurrent stats updates (add vs remove)

**Expected**: Final stats show 30 documents (30 - 10 + 10)

---

### 7. `test_cas_high_contention_stress`
**Focus**: Stress test with extreme contention on single posting list

**Scenario**: 100 threads, each performing 5 rapid appends to the same term (500 total operations)

**CAS Operation**: Lock-based atomicity under extreme contention

**Expected**: All 500 operations succeed without deadlock or corruption

---

### 8. `test_cas_idempotent_stats_update`
**Focus**: Verifies stats updates are idempotent for document updates

**Scenario**: Add document with length 10, then update to length 15

**CAS Operation**: Stats cache update distinguishes insert vs update

**Expected**: 
- Doc count remains 1 (not 2)
- Total length is 15 (not 25)

---

## Key Differences from Existing Tests

The existing tests in `shard.rs` focus on:
- End-to-end integration with NebServer
- BM25 search functionality
- Recovery and persistence
- Distributed coordinator behavior

These new tests focus specifically on:
- **Lock-based atomic operations** (`lock_or_insert_cell` + `upsert_cell`)
- **Concurrent behavior** under various contention scenarios
- **Version tracking** and preservation
- **Stats consistency** under concurrent modifications
- **Segment overflow** handling during concurrent writes

## Comparison: InvertedIndexer vs HashIndexer Concurrency

| Aspect | HashIndexer | InvertedIndexer |
|--------|-------------|-----------------|
| **Strategy** | Compare-and-swap with retry | Cell-level locking |
| **Functions** | `compare_version_and_set_field` | `lock_or_insert_cell` + `upsert_cell` |
| **Retry Logic** | Explicit retry loop (MAX_CAS_RETRIES) | Implicit (lock waits) |
| **Contention** | May fail after max retries | Always succeeds (waits for lock) |
| **Overhead** | Re-reads on conflict | Lock acquisition overhead |

Both strategies provide atomicity. HashIndexer uses optimistic concurrency (CAS), while InvertedIndexer uses pessimistic concurrency (locking). For posting lists, locking is appropriate since operations are quick and conflicts are resolved by waiting rather than retrying.

## No Overlapping Coverage

These tests are complementary to existing tests:
- No duplicate BM25 search tests
- No duplicate server setup tests
- No duplicate tokenization tests
- Focus exclusively on concurrent CAS behavior
- Target specific edge cases in atomic operations

## Running the Tests

```bash
# Run all InvertedIndexer CAS tests
cargo test --package neb --lib index::full_text::tests::tests

# Run individual test
cargo test --package neb --lib index::full_text::tests::tests::test_cas_concurrent_append_same_term
```

## Test Results

All 8 tests pass successfully:
- ✅ `test_cas_concurrent_append_same_term`
- ✅ `test_cas_segment_overflow_concurrent`
- ✅ `test_cas_version_preservation`
- ✅ `test_cas_concurrent_stats_updates`
- ✅ `test_cas_document_update_version_increment`
- ✅ `test_cas_concurrent_mixed_operations`
- ✅ `test_cas_high_contention_stress`
- ✅ `test_cas_idempotent_stats_update`

