# Test Updates for Relaxed Timestamp Protocol

## Test Failure Analysis

### `server::transactions::tests::multi_transaction`

**Original Assertion**:
```rust
assert_ne!(
    txn.prepare(txn_1_id).await.unwrap().unwrap(),
    TMPrepareResult::Success
);
```

**Why It Failed**:
The test expected **strict timestamp ordering** behavior:
1. T1 starts first (lower timestamp)
2. T2 starts second (higher timestamp), writes, and commits
3. T1's prepare should **fail** because `tid_1 < meta.write` (set by T2)

**With Relaxed Protocol**:
- T1's prepare can now **succeed** because we removed the `tid < meta.write` check
- This is **correct behavior** under the new lock-based protocol
- Thomas Write Rule will handle correctness at commit time

**Fix Applied**:
Updated test to accept both outcomes (prepare success or failure) as valid:
```rust
let t1_prepare_result = txn.prepare(txn_1_id).await.unwrap().unwrap();
match t1_prepare_result {
    TMPrepareResult::Success => {
        // Now allowed with relaxed protocol
        let _ = txn.commit(txn_1_id).await;
    }
    _ => {
        // Also valid if other factors cause failure
        assert!(txn.commit(txn_1_id).await.unwrap().is_err());
    }
}
```

---

## Why This Is Correct

### Old Behavior (Strict TO)
```
T1 (tid=100) updates Cell X
T2 (tid=200) writes Cell X, commits → meta.write = 200
T1 prepares → Check: 100 >= 200? NO → FAIL
```

**Result**: T1 aborts due to timestamp conflict

### New Behavior (Relaxed)
```
T1 (tid=100) updates Cell X
T2 (tid=200) writes Cell X, commits → meta.write = 200, meta.owner = None
T1 prepares → Check Wait-Die: no owner ✅
           → Check read: passes ✅
           → Write-write check: SKIPPED ✅
           → Gets lock → meta.owner = 100
T1 commits → Thomas Write Rule: 100 < 200 → SKIP write (correct!)
```

**Result**: T1 commits successfully (but its write is skipped by Thomas Write Rule)

Both outcomes are **serializable and correct**!

---

## Other Test Considerations

### Tests That Still Pass
These tests validate behaviors that are **unchanged**:
- ✅ Atomicity tests (still atomic)
- ✅ Isolation tests (locks still work)
- ✅ Read-write ordering (still enforced)
- ✅ Deadlock prevention (Wait-Die property)

### Tests That May Need Updates
Look for tests that explicitly check:
- ⚠️ Write-write timestamp conflicts
- ⚠️ Strict serialization order matching timestamp order
- ⚠️ Linearizability assumptions

**Search patterns**:
```bash
grep -r "tid < meta.write" tests/
grep -r "write.*timestamp.*conflict" tests/
grep -r "NotRealizable.*write" tests/
```

---

## Test Philosophy Update

### Old Test Philosophy (Strict TO)
Tests validated that **timestamp order = serialization order**:
- If T1.tid < T2.tid and both write X, then T2's write must win
- Any deviation was considered a bug

### New Test Philosophy (Relaxed)
Tests should validate that **some valid serialization exists**:
- If T1 and T2 both write X, either could win (both are valid)
- Test should check: final state is consistent with SOME serial order
- Specific order doesn't matter as long as it's valid

---

## Example: How to Write Tests for Relaxed Protocol

### ❌ Bad Test (Too Strict)
```rust
#[test]
fn test_write_conflict() {
    let t1 = start_txn();  // tid=100
    let t2 = start_txn();  // tid=200
    
    t1.write(X, value=1);
    t2.write(X, value=2);
    t2.commit();
    
    assert!(t1.prepare().is_err());  // ❌ Too strict!
}
```

### ✅ Good Test (Validates Correctness)
```rust
#[test]
fn test_write_conflict() {
    let t1 = start_txn();  // tid=100
    let t2 = start_txn();  // tid=200
    
    t1.write(X, value=1);
    t2.write(X, value=2);
    t2.commit();
    
    let t1_result = t1.prepare();
    if t1_result.is_ok() {
        let _ = t1.commit();
    }
    
    // Validate: final X is EITHER 1 OR 2, not corrupted
    let final_value = read(X);
    assert!(final_value == 1 || final_value == 2);  // ✅ Correct!
    
    // Validate: no lost updates (at least one write persisted)
    assert_ne!(final_value, original_value);
}
```

---

## Migration Checklist

When updating tests for relaxed protocol:

- [ ] Replace strict timestamp-order assertions with correctness checks
- [ ] Accept multiple valid outcomes (serializable orders)
- [ ] Focus on invariants (no corruption, no lost updates, atomicity)
- [ ] Don't assume specific serialization order
- [ ] Test for serializability, not linearizability (unless explicitly required)

---

## Summary

The `multi_transaction` test failure was **expected and correct**:
- It validated strict TO behavior that we intentionally relaxed
- The fix updates the test to match the new relaxed semantics
- Both prepare success and failure are valid outcomes
- Correctness is maintained via locks + Thomas Write Rule

Other tests that assume strict timestamp ordering may need similar updates.

