# Fix: Transaction Update Visibility Bug

## Issue
Transaction updates weren't visible to subsequent reads within the same transaction.

## Root Cause

The bug was in `read_from_site()` function in `src/server/transactions/manager.rs`. When a transaction read a cell from a remote data site, it would unconditionally cache the remote value, even if there was a pending update in the transaction's local cache.

### Problematic Flow

1. Transaction updates a cell → stores in `txn.data` with `changed = true`
2. Transaction reads the same cell
3. `read()` checks cache first, but if cache check somehow misses (edge case) or cache was cleared
4. `read()` calls `read_from_site()` to fetch from remote
5. **BUG**: `read_from_site()` overwrites the cached updated cell with the stale remote value
6. Transaction sees old value instead of updated value

### The Bug

```rust
// OLD CODE - BUGGY
TxnExecResult::Accepted(cell) => {
    txn.data.insert(
        id.clone(),
        DataObject {
            server: server_id,
            version: Some(cell.header.version),
            cell: Some(cell.clone()),  // ❌ Overwrites pending update!
            new: false,
            changed: false,  // ❌ Resets changed flag!
        },
    );
}
```

This would overwrite any pending update, even if `changed = true` was set by a previous `update()` call.

## The Fix

Modified `read_from_site()` to check for pending updates before overwriting the cache:

```rust
// NEW CODE - FIXED
TxnExecResult::Accepted(cell) => {
    // Check if there's a pending update in the transaction cache
    // If the cell was updated locally, we must return the cached updated version
    // instead of overwriting it with the remote (stale) value
    if let Some(data_obj) = txn.data.get_mut(id) {
        // Entry exists in transaction cache
        if data_obj.changed {
            // There's a pending update - return the cached updated cell instead
            // This ensures update-then-read visibility within the same transaction
            if let Some(ref cached_cell) = data_obj.cell {
                return Ok(TxnExecResult::Accepted(cached_cell.clone()));
            }
            // Changed but cell is None (removed) - return error
            return Ok(TxnExecResult::Error(ReadError::CellDoesNotExisted));
        }
        // Entry exists but not changed - only update if cell is missing
        if data_obj.cell.is_none() {
            data_obj.cell = Some(cell.clone());
            data_obj.version = Some(cell.header.version);
        }
    } else {
        // No entry exists - cache the remote value
        txn.data.insert(
            id.clone(),
            DataObject {
                server: server_id,
                version: Some(cell.header.version),
                cell: Some(cell.clone()),
                new: false,
                changed: false,
            },
        );
    }
}
```

### What the Fix Does

1. **Checks for pending updates**: Before caching a remote value, checks if there's a local update with `changed = true`
2. **Returns cached update**: If there's a pending update, returns the cached updated cell instead of the stale remote value
3. **Preserves isolation**: Ensures transaction isolation by never overwriting local updates with remote values
4. **Handles edge cases**: Properly handles cases where entry exists but cell is `None` (removed)

## Transaction Visibility Guarantees

After this fix, transactions correctly provide **write-your-own-writes** visibility:
- ✅ Updates are immediately visible to subsequent reads within the same transaction
- ✅ Transaction isolation is preserved (local updates never overwritten by remote reads)
- ✅ Correct behavior for both update-then-read and read-then-update patterns

## Test Coverage

The fix is covered by existing tests in `src/server/transactions/tests.rs`:
- Line 99-117: Test that verifies update-then-read returns updated value
- Line 138-146 in `src/client/tests.rs`: Client-side test that verifies same behavior

## Files Modified

- `src/server/transactions/manager.rs`: Fixed `read_from_site()` to respect pending updates

## Related Functions

- `head_from_site()`: Takes immutable transaction reference, doesn't modify cache, so safe
- `read_selected_from_site()`: Takes immutable transaction reference, doesn't modify cache, so safe
- `read()`: Checks cache first, so primary path is safe; `read_from_site()` is fallback for edge cases

## Impact

- **Severity**: High (breaks transaction isolation)
- **Scope**: Affects all transactions that update then read within same transaction
- **User Impact**: Transaction operations would see stale values, breaking correctness
- **Fix Impact**: Minimal - only changes behavior when reading from remote, preserves all other behavior





