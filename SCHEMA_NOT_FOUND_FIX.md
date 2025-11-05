# Schema Not Found Issues - Root Cause Analysis and Fix

## Problem Summary

Multiple workers experiencing `SchemaDoesNotExisted` errors during transaction execution, causing transactions to get stuck in `read_selected`. Errors showed various schema IDs missing, including schema ID 0.

## Root Causes Identified

### 1. **CRITICAL: Subscription Handle Leak (Runtime Loss)**
**Location:** `src/ram/schema/mod.rs:355-367`

```rust
// BEFORE (BROKEN):
let _ = sm.on_schema_added(move |schema| { ... }).await?;
let _ = sm.on_schema_deleted(move |schema| { ... }).await?;
```

**Problem:** The `let _ =` pattern immediately drops the subscription handles, causing:
- Subscription callbacks to stop working at unpredictable times
- New schemas added after initialization won't be received by local cache
- Schema deletion events won't be processed
- Callbacks can be garbage collected by Rust runtime

**Impact:** Schemas appear to "disappear" during runtime even though they exist in SchemasSM.

**Fix:** Store subscription handles in `LocalSchemasCache` struct to keep them alive:
```rust
pub struct LocalSchemasCache {
    map: Arc<LocalSchemasMap>,
    _schema_added_subscription: Box<dyn std::any::Any + Send + Sync>,
    _schema_deleted_subscription: Box<dyn std::any::Any + Send + Sync>,
}
```

### 2. **Race Condition: Schema Loading vs Raft Recovery**
**Location:** `src/server/mod.rs:337-349` and `src/ram/schema/mod.rs:352`

**Timeline:**
1. `raft::RaftService::start()` - Starts Raft, begins async WAL/snapshot replay to SchemasSM
2. 5-second arbitrary sleep (line 349) - May not be sufficient for large schema sets
3. `LocalSchemasCache::new()` calls `sm.get_all()` immediately
4. If SchemasSM hasn't finished recovering, returns empty or incomplete schema list

**Problem:** The initialization code assumes schemas are ready, but SchemasSM may still be replaying Raft log entries.

**Fix:** Added retry logic with exponential backoff:
```rust
const MAX_RETRIES: u32 = 50; // 50 * 200ms = 10 seconds max
const RETRY_DELAY_MS: u64 = 200;

loop {
    match sm.get_all().await {
        Ok(schemas) => {
            if schemas.is_empty() && retries < MAX_RETRIES {
                warn!("Schema cache initialization: get_all() returned 0 schemas. \
                       SchemasSM may still be recovering from Raft. Retrying...");
                tokio::time::sleep(Duration::from_millis(RETRY_DELAY_MS)).await;
                retries += 1;
                continue;
            }
            break schemas;
        }
        Err(e) if retries < MAX_RETRIES => { /* retry */ }
        Err(e) => return Err(e),
    }
}
```

### 3. **Subscription Order Issue**
**Problem:** Original code subscribed to events AFTER loading initial schemas, creating a window where new schemas could be missed.

**Fix:** Changed order to subscribe FIRST, then load schemas:
```rust
// 1. Subscribe to events (catch updates during initialization)
let schema_added_sub = sm.on_schema_added(...).await?;
let schema_deleted_sub = sm.on_schema_deleted(...).await?;

// 2. Load initial schemas with retry
let sm_data = /* retry logic */;

// 3. Import schemas
for schema in sm_data { map.new_schema(schema); }

// 4. Store subscription handles (keep alive!)
LocalSchemasCache { 
    map,
    _schema_added_subscription: Box::new(schema_added_sub),
    _schema_deleted_subscription: Box::new(schema_deleted_sub),
}
```

## Why Schemas Can Be Lost At Runtime

Yes, schemas can disappear at runtime due to:

1. **Subscription handle dropped** (FIXED) - Most critical issue
2. **Schema deletion via `del_schema` command** - Intentional but can cause errors if cells reference deleted schemas
3. **Raft leader changes** - Could temporarily disrupt subscription callbacks
4. **Network partitions** - In distributed setups, nodes may become isolated
5. **Subscription connection failures** - No automatic reconnection logic (still a potential issue)

## Remaining Concerns

### Schema Deletion Safety
When a schema is deleted via `del_schema`:
- The deletion event triggers `on_schema_deleted` callback
- Local cache removes the schema
- **BUT:** Existing cells in memory may still reference the deleted schema ID
- Attempting to read these cells will fail with `SchemaDoesNotExisted`

**Recommendation:** Implement schema reference counting or prevent deletion of schemas that have active cells.

### No Subscription Reconnection
If the subscription connection to SchemasSM fails after initialization:
- There's no automatic reconnection logic
- Schema updates will silently stop arriving
- The cache will become stale

**Recommendation:** Add subscription health monitoring and automatic reconnection.

## Testing Recommendations

1. **Startup Test:** Restart server multiple times, verify schema count is consistent
2. **Load Test:** Create schemas while server is starting up
3. **Runtime Test:** Monitor for "Received schema_added event" logs during operation
4. **Deletion Test:** Delete a schema, verify it's removed from all nodes
5. **Partition Test:** Network partition recovery - verify schemas resync

## Log Monitoring

Look for these new log messages:
- `INFO`: "Received schema_added event for schema X (name)"
- `WARN`: "Received schema_deleted event for schema: X"
- `WARN`: "Schema cache initialization: get_all() returned 0 schemas (attempt X/50)"
- `INFO`: "Local schema initialization completed with X schemas"

If you see continuous retry warnings, SchemasSM recovery is taking longer than expected.

## Files Modified

1. `src/ram/schema/mod.rs`:
   - Added subscription handle storage to `LocalSchemasCache` struct
   - Added retry logic with exponential backoff in `LocalSchemasCache::new()`
   - Reordered subscription before schema loading
   - Kept subscription handles alive
   - Enhanced logging for debugging

## Expected Behavior After Fix

✅ Schemas load reliably on startup even during Raft recovery
✅ Schema updates propagate to all nodes via subscriptions
✅ Subscriptions remain active for the lifetime of the server
✅ Better logging for debugging schema synchronization issues
✅ Graceful handling of SchemasSM recovery delays

