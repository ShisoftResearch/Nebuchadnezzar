# Cell Index Operations - Complete Analysis

## Overview

The `cell_index` is a `WordMap<System>` that maps `hash (u64) -> cell_location (usize)`

```rust
cell_index: WordMap<System>  // Maps hash to memory address where cell is stored
```

## All Functions Using cell_index

### 1. **location_for_read** (Line 358)
**Purpose**: Get read-only access to cell location for reading cell data

```rust:358:386:src/ram/chunk.rs
pub fn location_for_read<'a>(&self, hash: u64) -> Result<CellReadGuard<'_>, ReadError> {
    let guard = self.cell_index.lock(hash as usize);
    match guard {
        Some(index) => {
            if *index == 0 {
                warn!("Cannot find cell with hash {} for index is zero", hash);
                return Err(ReadError::CellDoesNotExisted);
            }
            
            // Reference bit tracking is handled by mprotect + SIGSEGV for ALL segments:
            // - Hot segments (anonymous memory): mprotect works
            // - Cold segments (file-backed memory): mprotect works! Kernel pages in from disk transparently
            // CLOCK re-arms segments with mprotect(PROT_NONE) after clearing reference bits
            
            return Ok(index);
        }
        None => {
            if hash == 0 {
                Err(ReadError::CellIdIsUnitId)
            } else {
                trace!(
                    "Cannot find cell with hash {} for it is not in the map",
                    hash
                );
                Err(ReadError::CellDoesNotExisted)
            }
        }
    }
}
```

**Operation**: `self.cell_index.lock(hash as usize)`
- Locks the entry for hash
- Returns `Option<WordMutexGuard>` where `*guard` is the `cell_location: usize`
- The returned guard is **READ-ONLY** in this function
- **VALUE RETRIEVED**: `*index` contains the cell memory address

### 2. **location_for_write** (Line 388)
**Purpose**: Get write access to cell location for updating cell data

```rust:388:399:src/ram/chunk.rs
pub fn location_for_write(&self, hash: u64) -> Option<CellWriteGuard<'_>> {
    let guard = self.cell_index.lock(hash as usize);
    match guard {
        Some(index) => {
            if *index == 0 {
                return None;
            }
            return Some(index);
        }
        None => None,
    }
}
```

**Operation**: `self.cell_index.lock(hash as usize)`
- Same as location_for_read but returns guard for potential write operations
- **VALUE RETRIEVED**: `*index` contains the cell memory address

### 3. **write_cell** (Line 469)
**Purpose**: Write a NEW cell and store its location in cell_index

```rust:469:493:src/ram/chunk.rs
fn write_cell(&self, cell: &mut OwnedCell) -> Result<CellHeader, WriteError> {
    debug!("Writing cell {:?} to chunk {}", cell.id(), self.id);
    let (cell_loc, schema) = self.write_cell_to_chunk(cell)?;
    
    #[cfg(debug_assertions)]
    {
        debug_assert!(
            self.validate_cell_location(cell_loc, &format!("write_cell(hash={})", cell.header.hash)),
            "Attempting to store invalid cell location 0x{:x} in cell index for hash {}",
            cell_loc,
            cell.header.hash
        );
    }
    
    match self.cell_index.try_insert_locked(cell.header.hash as usize) {
        Some(mut guard) => {
            *guard = cell_loc;
            drop(guard);
            self.ensure_indices(cell, None, &*schema);
            self.refresh_statistics();
        }
        None => return Err(WriteError::CellAlreadyExisted),
    }
    Ok(cell.header)
}
```

**Operation**: `self.cell_index.try_insert_locked(cell.header.hash as usize)`
- Tries to INSERT a new entry in the map
- Returns `Some(guard)` only if key doesn't exist
- **VALUE STORED**: `*guard = cell_loc` where `cell_loc` comes from `write_cell_to_chunk()`
- ⚠️ **CRITICAL WRITE POINT** - This is where cell_loc enters the index

### 4. **update_cell** (Line 509)
**Purpose**: Update an existing cell's location

```rust:509:556:src/ram/chunk.rs
fn update_cell(&self, cell: &mut OwnedCell) -> Result<CellHeader, WriteError> {
    let hash = cell.header.hash;
    // Write first, lock second to avoid deadlock with cleaner
    let (new_cell_loc, schema) = self.write_cell_to_chunk(cell)?;
    
    #[cfg(debug_assertions)]
    {
        debug_assert!(
            self.validate_cell_location(new_cell_loc, &format!("update_cell(hash={})", hash)),
            "Attempting to store invalid cell location 0x{:x} in cell index for hash {} (update)",
            new_cell_loc,
            hash
        );
    }
    
    if let Some(mut guard) = self.location_for_write(hash) {
        let cell_location = *guard;
        
        #[cfg(debug_assertions)]
        {
            // Also validate the old location we're about to mark dead
            if cell_location != 0 {
                let is_valid = self.validate_cell_location(
                    cell_location,
                    &format!("update_cell old location(hash={})", hash)
                );
                if !is_valid {
                    error!(
                        "Found corrupted old cell location 0x{:x} for hash {} - this indicates prior corruption",
                        cell_location, hash
                    );
                }
            }
        }
        
        let old_indices = self.old_index_res(&guard, &*schema)?;
        self.ensure_indices_with_res(cell, old_indices, &*schema);
        *guard = new_cell_loc;
        drop(guard);
        self.mark_dead_entry_with_cell(cell_location, cell);
        self.refresh_statistics();
    } else {
        // Optimistic update will remove the new inserted one
        self.mark_dead_entry_with_cell(new_cell_loc, cell);
        return Err(WriteError::CellDoesNotExisted);
    }
    Ok(cell.header)
}
```

**Operations**:
1. Calls `location_for_write(hash)` - **RETRIEVES** old `cell_location`
2. **VALUE STORED**: `*guard = new_cell_loc` - **OVERWRITES** with new location
- ⚠️ **CRITICAL WRITE POINT** - Old value retrieved, new value written

### 5. **upsert_cell** (Line 558)
**Purpose**: Update existing cell OR insert new cell

```rust:558:616:src/ram/chunk.rs
fn upsert_cell(&self, cell: &mut OwnedCell) -> Result<CellHeader, WriteError> {
    let hash = cell.header.hash;
    // Write first, lock second to avoid deadlock with cleaner
    let (new_cell_loc, schema) = self.write_cell_to_chunk(cell)?;
    
    #[cfg(debug_assertions)]
    {
        debug_assert!(
            self.validate_cell_location(new_cell_loc, &format!("upsert_cell(hash={})", hash)),
            "Attempting to store invalid cell location 0x{:x} in cell index for hash {} (upsert)",
            new_cell_loc,
            hash
        );
    }
    
    loop {
        if let Some(mut guard) = self.location_for_write(hash) {
            trace!("Cell {} exists, will update for upsert", hash);
            let cell_location = *guard;
            
            #[cfg(debug_assertions)]
            {
                if cell_location != 0 {
                    let is_valid = self.validate_cell_location(
                        cell_location,
                        &format!("upsert_cell old location(hash={})", hash)
                    );
                    if !is_valid {
                        error!(
                            "Found corrupted old cell location 0x{:x} for hash {} during upsert",
                            cell_location, hash
                        );
                    }
                }
            }
            
            let old_indices = self.old_index_res(&guard, &*schema)?;
            *guard = new_cell_loc;
            drop(guard);
            self.ensure_indices_with_res(cell, old_indices, &*schema);
            self.mark_dead_entry_with_cell(cell_location, cell);
            self.refresh_statistics();
        } else {
            let reservation = self.cell_index.try_insert_locked(hash as usize);
            if let Some(mut guard) = reservation {
                // New cell
                trace!("Cell {} does not exists, will insert for upsert", hash);
                *guard = new_cell_loc;
                drop(guard);
                self.ensure_indices(cell, None, &*schema);
                self.refresh_statistics();
            } else {
                trace!("Cell {} was not exists, but found exists, will try", hash);
                continue;
            }
        }
        return Ok(cell.header);
    }
}
```

**Operations**:
- **Path 1 (Update)**: Uses `location_for_write()`, **RETRIEVES** old, **WRITES** new via `*guard = new_cell_loc`
- **Path 2 (Insert)**: Uses `try_insert_locked()`, **WRITES** `*guard = new_cell_loc`
- ⚠️ **CRITICAL WRITE POINTS** - Two possible write paths

### 6. **update_cell_by** (Line 618)
**Purpose**: Update cell using a closure/callback

```rust:618:681:src/ram/chunk.rs
fn update_cell_by<U>(&self, hash: u64, update: U) -> Result<OwnedCell, WriteError>
where
    U: FnOnce(&SharedCell) -> Option<OwnedCell>,
{
    if let Some(cell_guard) = self.location_for_write(hash) {
        let old_loc = *cell_guard;
        
        #[cfg(debug_assertions)]
        {
            // Validate old location before we try to read it
            if old_loc != 0 {
                let is_valid = self.validate_cell_location(
                    old_loc,
                    &format!("update_cell_by old location(hash={})", hash)
                );
                if !is_valid {
                    error!(
                        "Corrupted cell location 0x{:x} detected for hash {} in update_cell_by - aborting to prevent further corruption",
                        old_loc, hash
                    );
                    return Err(WriteError::ReadError(ReadError::ExecError(
                        format!("Corrupted cell location: 0x{:x}", old_loc)
                    )));
                }
            }
        }
        
        match SharedCell::from_chunk_raw(cell_guard, self) {
            Ok((cell, schema)) => {
                let old_indices = self
                    .index_builder
                    .as_ref()
                    .map(|_| probe_cell_indices(&cell, &*schema));
                let new_cell = update(&cell);
                if let Some(mut new_cell) = new_cell {
                    let (new_cell_loc, schema) = self.write_cell_to_chunk(&mut new_cell)?;
                    
                    #[cfg(debug_assertions)]
                    {
                        debug_assert!(
                            self.validate_cell_location(new_cell_loc, &format!("update_cell_by new location(hash={})", hash)),
                            "Attempting to store invalid cell location 0x{:x} in cell index for hash {} (update_cell_by)",
                            new_cell_loc,
                            hash
                        );
                    }
                    
                    *cell.into_guard() = new_cell_loc;
                    if let Some(indexer) = &self.index_builder {
                        indexer.ensure_indices(&new_cell, &*schema, old_indices);
                    }
                    self.mark_dead_entry_with_cell(old_loc, &new_cell);
                    self.refresh_statistics();
                    return Ok(new_cell);
                } else {
                    return Err(WriteError::UserCanceledUpdate);
                }
            }
            Err(e) => return Err(WriteError::ReadError(e)),
        }
    } else {
        return Err(WriteError::CellDoesNotExisted);
    }
}
```

**Operations**:
1. **RETRIEVES**: `let old_loc = *cell_guard` from `location_for_write()`
2. **WRITES**: `*cell.into_guard() = new_cell_loc`
- ⚠️ **CRITICAL READ AND WRITE POINT**

### 7. **remove_cell** (Line 683)
**Purpose**: Remove cell from index and mark it dead

```rust:683:703:src/ram/chunk.rs
fn remove_cell(&self, hash: u64) -> Result<(), WriteError> {
    let hash_key = hash as usize;
    let guard_opt = self.cell_index.lock(hash_key);
    if let Some(mut guard) = guard_opt {
        let cell_location = *guard;
        if let Some(indexer) = &self.index_builder {
            match SharedCell::from_chunk_raw(guard, self) {
                Ok((cell, schema)) => {
                    indexer.remove_indices(&cell, &*schema);
                    guard = cell.into_guard();
                }
                Err(e) => return Err(WriteError::ReadError(e)),
            }
        }
        self.put_tombstone_by_cell_loc(cell_location)?;
        guard.remove();
        Ok(())
    } else {
        Err(WriteError::CellDoesNotExisted)
    }
}
```

**Operations**:
1. **RETRIEVES**: `let cell_location = *guard`
2. **REMOVES**: `guard.remove()` - removes entry from map
- ⚠️ **READ AND DELETE POINT**

### 8. **remove_cell_by** (Line 705)
**Purpose**: Conditional remove based on predicate

```rust:705:727:src/ram/chunk.rs
fn remove_cell_by<P>(&self, hash: u64, predict: P) -> Result<(), WriteError>
where
    P: Fn(&SharedCell) -> bool,
{
    let guard = self.cell_index.lock(hash as usize);
    if let Some(guard) = guard {
        let cell_location = *guard;
        match SharedCell::from_chunk_raw(guard, self) {
            Ok((cell, schema)) => {
                if predict(&cell) {
                    let put_tombstone_result = self.put_tombstone_by_cell_loc(cell_location);
                    if put_tombstone_result.is_err() {
                        put_tombstone_result
                    } else {
                        self.remove_indices(&cell, &schema);
                        cell.into_guard().remove();
                        Ok(())
                    }
                } else {
                    Err(WriteError::CellDoesNotExisted)
                }
            }
```

**Operations**:
1. **RETRIEVES**: `let cell_location = *guard`
2. **REMOVES**: `cell.into_guard().remove()` if predicate true

### 9. **cell_count** / **count** (Lines 1073, 1082)
**Purpose**: Get number of cells in index

```rust
pub fn cell_count(&self) -> usize {
    self.cell_index.len()
}

pub fn count(&self) -> usize {
    self.cell_index.len()
}
```

**Operation**: `self.cell_index.len()` - Just reads count, no data access

## Critical Observations

### Write Points (Where corruption could be introduced)
1. ✅ **write_cell**: `*guard = cell_loc` (line 485)
2. ✅ **update_cell**: `*guard = new_cell_loc` (line 546)
3. ✅ **upsert_cell** (update path): `*guard = new_cell_loc` (line 595)
4. ✅ **upsert_cell** (insert path): `*guard = new_cell_loc` (line 605)
5. ✅ **update_cell_by**: `*cell.into_guard() = new_cell_loc` (line 665)

### Read Points (Where corrupted value would be used)
1. ✅ **location_for_read**: `*index` (line 362)
2. ✅ **location_for_write**: `*index` (line 392)
3. ✅ **update_cell**: `let cell_location = *guard` (line 525)
4. ✅ **upsert_cell**: `let cell_location = *guard` (line 576)
5. ✅ **update_cell_by**: `let old_loc = *cell_guard` (line 623)
6. ✅ **remove_cell**: `let cell_location = *guard` (line 687)
7. ✅ **remove_cell_by**: `let cell_location = *guard` (line 711)

## Source of cell_loc Values

All `cell_loc` values come from ONE function:

```rust
let (cell_loc, schema) = self.write_cell_to_chunk(cell)?;
```

Which calls:

```rust
cell.write_to_chunk_with_schema(self, &*schema)?
```

Which returns:

```rust
return Ok(addr);  // From pending_entry.addr
```

## The Chain of Trust

```
1. Segment::try_acquire(size)          → Returns 8-byte aligned address ✓ (VERIFIED)
2. Chunk::try_acquire(size)            → Wraps in PendingEntry { addr, ... }
3. OwnedCell::write_to_chunk_with_schema → Uses pending_entry.addr
4. Returns addr to write_cell_to_chunk  → Returns (addr, schema)
5. write_cell/update_cell/upsert_cell  → Stores in *guard = addr
6. WordMap stores the value             → VERIFIED CORRECT ✓
7. Later: *guard retrieves value        → VERIFIED CORRECT ✓
8. Used for reading cell data           → Panics if misaligned!
```

## Hypothesis: Where is the +6 coming from?

Since WordMap is proven correct, the +6 offset must be added:

**BEFORE storage** (during address calculation):
- In `Chunk::try_acquire`?
- In `PendingEntry` struct access?
- In `write_to_chunk_with_schema`?

**AFTER retrieval** (during address usage):
- In `header_from_chunk_raw`?
- In `Entry::content_pos`?
- In cell header reading?

## Next Investigation

Need to instrument:
1. The value of `pending_entry.addr` at line 170 of cell.rs
2. The value stored via `*guard = cell_loc` at all write points
3. The value retrieved via `*guard` at all read points

Add logging like:
```rust
debug!("TRACE: pending_entry.addr = 0x{:016x}", pending_entry.addr);
debug!("TRACE: storing in cell_index = 0x{:016x}", cell_loc);
debug!("TRACE: retrieved from cell_index = 0x{:016x}", *guard);
```

