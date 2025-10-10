# State Machine Recovery Status

**Date**: October 10, 2025  
**Test**: `schema_persistence_and_recovery`

---

## Summary

✅ **Bifrost Fix #1: Leader Election** - WORKING  
❌ **Bifrost Fix #2: State Machine Recovery** - NOT WORKING

---

## Evidence: recover() Method is NOT Called

### Instrumentation Added

Added detailed error logging to `SchemasSM::recover()` in `src/ram/schema/sm.rs:101-126`:

```rust
fn recover(&mut self, data: Vec<u8>) -> BoxFuture<()> {
    error!("========== SchemasSM::recover() CALLED ==========");
    error!("Received {} bytes of snapshot data", data.len());
    
    let schemas: Vec<Schema> = match utils::serde::deserialize::<Vec<Schema>>(&data) {
        Some(s) => {
            error!("Successfully deserialized {} schemas", s.len());
            s
        },
        None => {
            error!("Failed to deserialize schemas from snapshot data");
            return future::ready(()).boxed();
        }
    };
    
    error!("Loading {} schemas into map...", schemas.len());
    self.map.load_from_list(schemas.clone());
    error!("Schemas loaded into map");
    
    self.id_count = schemas.iter().map(|s| s.id).max().unwrap_or(0);
    error!("Set id_count to {}", self.id_count);
    error!("========== SchemasSM::recover() COMPLETE ==========");
    
    future::ready(()).boxed()
}
```

Also added logging to `load_from_list()` at line 176-187.

### Test Output (Phase 2 Recovery)

```
[INFO  neb::server::tests] PHASE 2: Restarting server and verifying schema recovery
[INFO  bifrost::rpc] Registering service RaftService with id 10662549175266302366
[INFO  bifrost::tcp::server] TCP server listening on 127.0.0.1:18800
[WARN  bifrost::raft::disk] Failed to read snapshot checksum, file may be corrupted  ⚠️
[INFO  bifrost::raft] Single-node cluster detected after recovery (term=20, logs=true, members=1)
[INFO  bifrost::raft] Successfully transitioned to Leader state  ✅
[INFO  neb::server] Single-server resumed cluster, waiting for leader election...
[INFO  bifrost::raft::client] UPDATE_INFO Setting leader to 5702953806423631046  ✅
[INFO  neb::ram::schema] Initializing local schema cache
[INFO  neb::ram::schema] Local schema initialization completed
[INFO  neb::server::tests] Waiting for Raft recovery to complete...
[INFO  neb::server::tests] Verifying recovered schemas...
[INFO  neb::server::tests] Found 0 schemas after recovery  ❌
```

**NOTICE**: 
- ❌ NO "========== SchemasSM::recover() CALLED ==========" message
- ❌ NO deserialization messages
- ❌ NO load_from_list messages

**Conclusion**: The `recover()` method was NEVER called.

---

## What's Still Broken

### 1. Snapshot Checksum Failure
```
[WARN bifrost::raft::disk] Failed to read snapshot checksum, file may be corrupted
```

The snapshot file exists (confirmed in Phase 1), but checksum validation fails during recovery.

### 2. recover() Not Called

Even though the snapshot file exists:
- Checksum fails →  Snapshot rejected
- No fallback to WAL replay
- State machine `recover()` never invoked
- State machine stays empty

### 3. No WAL Replay

Logs show WAL entries being applied (log_id=21, 22, 29), but these are **new operations during Phase 2**, not replayed operations from Phase 1.

---

## What's Working ✅

### Leader Election
```
[INFO  bifrost::raft] Single-node cluster detected after recovery (term=20, logs=true, members=1)
[INFO  bifrost::raft] Successfully transitioned to Leader state
[INFO  bifrost::raft::client] UPDATE_INFO Setting leader to 5702953806423631046
```

Leader election after recovery works correctly now!

### Shutdown/Restart
```
[INFO  bifrost::raft] Shutting down RaftService on 127.0.0.1:18800
[INFO  bifrost::tcp::server] TCP server shut down gracefully
```

Clean shutdown and restart works properly.

---

## Issues for Bifrost Team

### Critical Issue #1: Snapshot Checksum
**File**: `bifrost/src/raft/disk.rs` (or similar)

The snapshot checksum validation is failing:
```
Failed to read snapshot checksum, file may be corrupted
```

**Questions**:
1. What checksum algorithm is used?
2. Is the checksum written correctly during snapshot creation?
3. Is there an endianness issue?
4. Is the file format version compatible?

**Debug**: Add logging to snapshot write/read:
```rust
fn write_snapshot(...) {
    // ... write snapshot data ...
    let checksum = calculate_checksum(&data);
    error!("Writing snapshot: size={}, checksum={:x}", data.len(), checksum);
    // ... write checksum ...
}

fn read_snapshot(...) {
    let data = read_file()?;
    let stored_checksum = read_checksum()?;
    let calculated_checksum = calculate_checksum(&data);
    error!("Reading snapshot: size={}, stored_checksum={:x}, calculated={:x}", 
           data.len(), stored_checksum, calculated_checksum);
    if stored_checksum != calculated_checksum {
        error!("CHECKSUM MISMATCH!");
        return None;
    }
}
```

### Critical Issue #2: recover() Not Called
**File**: `bifrost/src/raft/mod.rs` (or similar)

When checksum fails, bifrost should:
1. Try to recover from WAL logs instead
2. Or at least call `recover()` with empty data

Currently:
- Snapshot fails → Silently ignored
- WAL logs exist → Not replayed to state machine
- State machine → Stays in default state

**Expected code** (that's missing):
```rust
async fn recover_state_machines(&self) {
    for (sm_id, sm) in &self.state_machines {
        error!("Attempting to recover SM {}", sm_id);
        
        if let Some(snapshot) = self.load_snapshot(*sm_id) {
            error!("Recovering SM {} from snapshot", sm_id);
            sm.write().await.recover(snapshot).await;
        } else {
            error!("No snapshot for SM {}, replaying from WAL", sm_id);
            self.replay_wal_to_sm(*sm_id, sm).await;
        }
    }
}
```

---

## Test Commands

### Run with Instrumentation
```bash
cd Nebuchadnezzar
RUST_LOG=error,neb::ram::schema::sm=error cargo test schema_persistence_and_recovery -- --nocapture
```

**Expected (when fixed)**:
```
[ERROR neb::ram::schema::sm] ========== SchemasSM::recover() CALLED ==========
[ERROR neb::ram::schema::sm] Received 12345 bytes of snapshot data
[ERROR neb::ram::schema::sm] Successfully deserialized 3 schemas
[ERROR neb::ram::schema::sm] Loading 3 schemas into map...
[ERROR neb::ram::schema::sm] load_from_list: Loading 3 schemas
[ERROR neb::ram::schema::sm] load_from_list: All schemas loaded
[ERROR neb::ram::schema::sm] Schemas loaded into map
[ERROR neb::ram::schema::sm] Set id_count to 300
[ERROR neb::ram::schema::sm] ========== SchemasSM::recover() COMPLETE ==========
```

**Actual (currently)**:
```
(none of the above appears)
```

---

## Next Steps for Bifrost

1. Fix snapshot checksum validation bug
2. Add code to call `sm.recover()` after loading snapshot
3. Implement WAL replay as fallback when snapshot fails
4. Test with: `cargo test schema_persistence_and_recovery` in Nebuchadnezzar

---

## Summary

**Bifrost progress**:
- ✅ Leader election fix: DONE
- ❌ State machine recovery: STILL BROKEN

**Proof**: Added extensive error logging to `recover()` method - **no logs appear**, proving it's never called.

**Blocking**: All Raft persistence functionality in Nebuchadnezzar

