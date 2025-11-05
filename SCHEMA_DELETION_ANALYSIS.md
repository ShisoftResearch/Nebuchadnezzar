# How Schemas Can Be Deleted/Cleared from Local Cache

## Summary
I found **3 potential issues** that could cause schemas to disappear from the local cache, plus added safeguards.

## Ways Schemas Can Be Deleted

### 1. ✅ **Intentional Deletion via `del_schema` Command**
**Status:** By design, but dangerous

```rust
// In SchemasSM (sm.rs:73-82)
fn del_schema(&mut self, name: String) -> BoxFuture<'_, Result<(), DelSchemaError>> {
    async move {
        self.map.del_schema(&name)?;
        self.callback.notify(commands::on_schema_deleted::new(), name).await?;
        Ok(())
    }
}
```

**How it works:**
1. Someone calls `client.del_schema(name)`
2. SchemasSM deletes it from the state machine
3. Raft propagates the deletion
4. All nodes receive `on_schema_deleted` notification
5. Local cache deletes the schema

**Danger:** If existing cells reference a deleted schema, they become unreadable!

**Recommendation:** 
- Never delete schemas that have active cells
- Implement reference counting before deletion
- Add a "soft delete" that marks schemas as deprecated but keeps them readable

---

### 2. ⚠️ **Subscription Callback Silent Failure** (FIXED)
**Status:** Fixed with enhanced error logging

**Before fix:**
```rust
fn del_schema(&self, name: &str) {
    if let Some(id) = self.name_map.remove(&(name.to_owned())) {
        self.schema_map.remove(&id);
        debug!("Deleted local schema {} with id {}", name, id);
    }
    // Silently fails if name doesn't exist!
}
```

**Issues:**
- If schema name doesn't match exactly, deletion silently fails
- No error logging for failed deletions
- Could create inconsistency between SchemasSM and local cache
- If callback panics, the subscription dies (subscription handle dropped)

**After fix:**
```rust
fn del_schema(&self, name: &str) {
    if let Some(id) = self.name_map.remove(&(name.to_owned())) {
        self.schema_map.remove(&id);
        warn!("Deleted local schema '{}' with id {}", name, id);
    } else {
        error!("Attempted to delete schema '{}' but it doesn't exist in local cache. \
                This may indicate a subscription inconsistency or the schema was already deleted.", 
               name);
    }
}
```

Now you'll see error logs if deletion attempts fail!

---

### 3. ⚠️ **Schema Addition Race Condition** (HARDENED)
**Status:** Added duplicate detection

**Scenario:**
1. Schema added via subscription callback
2. Same schema added via initial load
3. Or schema updated with different ID

**Before fix:**
```rust
fn new_schema(&self, schema: Schema) {
    self.name_map.insert(schema.name.clone(), schema.id);
    self.schema_map.insert(schema.id, Arc::new(schema));
    // Blindly overwrites!
}
```

**After fix:**
```rust
fn new_schema(&self, schema: Schema) {
    // Check if schema already exists
    if let Some(existing_id) = self.name_map.get(&name) {
        if existing_id != id {
            error!("Schema name collision: name '{}' already mapped to ID {} but new schema has ID {}", 
                   name, existing_id, id);
            return; // Don't overwrite
        } else {
            debug!("Updating existing schema {} ({})", id, name);
        }
    }
    
    self.name_map.insert(name, id);
    self.schema_map.insert(id, Arc::new(schema));
}
```

Now detects and prevents ID collisions!

---

### 4. ❌ **Subscription Handle Dropped** (ALREADY FIXED)
**Status:** Fixed in previous commit

**The Critical Bug:**
```rust
// BROKEN CODE:
let _ = sm.on_schema_deleted(move |schema| { ... }).await?;
```

The `let _ =` immediately drops the subscription handle, causing:
- Callbacks stop working at random times
- Schema deletions not received
- Schemas appear to "disappear"

**Fix:** Store handles in struct to keep them alive forever.

---

### 5. 🔹 **Raft Snapshot Recovery Replaces State**
**Status:** Legitimate operation, not a bug

When Raft loads a snapshot during recovery:

```rust
// In SchemasSM::recover (sm.rs:108-132)
fn recover(&mut self, data: Vec<u8>) -> BoxFuture<'_, ()> {
    let schemas: Vec<Schema> = utils::serde::deserialize(&data)?;
    self.map.load_from_list(schemas.clone()); // Replaces ALL schemas
    self.id_count = schemas.iter().map(|s| s.id).max().unwrap_or(0);
}
```

This **replaces the entire schema map** with the snapshot data.

**When it happens:**
- Server restart
- Raft follower catching up
- Snapshot restoration

**Impact:** 
- If local cache queries before recovery completes, gets empty/stale schemas
- **Already fixed** with retry logic in `LocalSchemasCache::new()`

---

### 6. ⚠️ **LFHashMap Concurrency Issues** (UNLIKELY)
**Status:** Low risk, using lock-free map

The `LFHashMap` (lock-free hash map from lightning crate) is thread-safe, but:

**Potential issues:**
- Race between `insert` and `remove` on same key
- Memory ordering issues (should be handled by library)
- If LFHashMap has bugs (unlikely in mature library)

**Mitigation:** The Arc wrapper provides proper memory barriers.

---

### 7. ❌ **ServerMeta Recreation** (DOESN'T HAPPEN)
**Status:** Confirmed safe

I verified that `ServerMeta` is created **once** during server initialization and stored in `Arc`:

```rust
let meta_rc = Arc::new(ServerMeta { schemas });
// Passed to chunks, cleaner, etc. - never recreated
```

The Arc is cloned but never replaced, so schemas persist for server lifetime.

---

## What We Added for Safety

### Enhanced Logging
- ✅ `WARN` on successful deletion (was `DEBUG`)
- ✅ `ERROR` on failed deletion attempts (was silent)
- ✅ `ERROR` on schema name/ID collisions (was silent)
- ✅ `INFO` on subscription events (was `DEBUG`)

### Duplicate Detection
- ✅ Detect schema name collisions during addition
- ✅ Prevent overwriting schemas with different IDs
- ✅ Allow updates of existing schemas (same ID)

### Subscription Safeguards
- ✅ Keep subscription handles alive (previous fix)
- ✅ Subscribe before loading schemas (previous fix)
- ✅ Retry logic for SchemasSM recovery (previous fix)

## Monitoring Checklist

Watch for these log patterns indicating problems:

1. **Schema Deletion Failures:**
   ```
   ERROR: Attempted to delete schema 'X' but it doesn't exist in local cache
   ```
   → Subscription inconsistency or duplicate deletion attempt

2. **Schema Name Collisions:**
   ```
   ERROR: Schema name collision: name 'X' already mapped to ID Y but new schema has ID Z
   ```
   → Multiple schemas with same name but different IDs (serious bug!)

3. **Missing Schemas During Reads:**
   ```
   ERROR: Schema X does not existed to read
   ```
   → Schema was deleted but cells still reference it, OR subscription dropped

4. **Subscription Issues:**
   ```
   WARN: Schema cache initialization: get_all() returned 0 schemas
   ```
   → SchemasSM still recovering (should resolve within 10s)

## Recommendations

### Immediate Actions
1. ✅ Monitor logs for the new ERROR messages
2. ✅ Never delete schemas that have active cells
3. ✅ Restart server to pick up fixes

### Future Enhancements
1. **Schema Reference Counting:**
   - Track which cells reference which schemas
   - Prevent deletion of in-use schemas
   - Add `force_delete` option for manual cleanup

2. **Soft Delete:**
   - Mark schemas as deprecated instead of deleting
   - Keep them readable but prevent new cell creation
   - Cleanup after verifying no active references

3. **Subscription Health Monitoring:**
   - Periodic heartbeat to verify subscriptions are alive
   - Auto-reconnect if subscription dies
   - Alert if no schema events received for extended period

4. **Schema Version Migration:**
   - Support schema updates that maintain backward compatibility
   - Automatically upgrade cells when schemas change
   - Track schema versions in cell headers

## Testing Recommendations

### Test Schema Deletion
```bash
# Should see WARN log when schema is deleted
client.del_schema("test_schema")

# Should see ERROR if trying to read cells with deleted schema
cell = txn.read(id_with_deleted_schema)
```

### Test Subscription Resilience
```bash
# Create schema after server starts
# Should see: INFO "Received schema_added event for schema X"
client.new_schema(...)

# Delete schema
# Should see: WARN "Received schema_deleted event for schema: X"
client.del_schema(...)
```

### Test Duplicate Detection
```bash
# Try to add schema with same name but different ID (should fail)
# Should see: ERROR "Schema name collision"
```

## Conclusion

The main ways schemas can be deleted/cleared:

1. ✅ **Intentional deletion** (by design, dangerous if cells exist)
2. ✅ **Subscription dropped** (FIXED - handles now kept alive)
3. ⚠️ **Silent deletion failures** (FIXED - now logged)
4. ⚠️ **Schema collisions** (HARDENED - now detected)
5. 🔹 **Raft recovery** (legitimate, handled by retry logic)

All critical issues have been fixed. The remaining risk is intentional deletion of schemas that still have active cells - this should be prevented at the application level by implementing reference counting.

