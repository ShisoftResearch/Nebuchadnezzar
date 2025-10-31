# CRITICAL: Schema Mismatch Between Old Data and New Code

## The Root Cause of `len=1700921344` Error

### What's Happening

You have a **schema version mismatch** problem:

1. **Old cells** in your database were written with schemas having:
   - wikidata_link: `static_bound: 44` ❌
   - _NEB_ID_LIST: `static_bound: 20` ❌
   - _NEB_TYPE_ID_LIST: `static_bound: 4` ❌

2. **New code** is trying to read those cells with fixed schemas having:
   - wikidata_link: `static_bound: 48` ✓
   - _NEB_ID_LIST: `static_bound: 24` ✓
   - _NEB_TYPE_ID_LIST: `static_bound: 8` ✓

3. **Result**: The code looks for variable data at the wrong offset!

### Example: Reading a wikidata_link Cell

```
OLD CELL (written with static_bound: 44):
=========================================
Offset  Content
------  -------
0-15    _inbound (Id)
16-31   _outbound (Id)
32-35   statement_id pointer (u32)
36-39   property_id pointer (u32)
40-43   literal_id pointer (u32)
44-47   STRING LENGTH (u32) = 15    ← ACTUAL DATA HERE
48-62   STRING DATA "P31-Q5..."

NEW CODE (expects static_bound: 48):
====================================
Reads variable region starting at offset 48
Tries to read string length from offset 48
But offset 48 contains STRING DATA, not the length!
Result: len = 0x50333150 = 1347571024 (garbage!)
```

## The Error You're Seeing

```
string_io: invalid UTF-8 at ptr=139097638242740 len=1700921344 
err=Utf8Error { valid_up_to: 8, error_len: Some(1) }
```

- **len=1700921344**: This is garbage bytes interpreted as u32 length
- **valid_up_to: 8**: Only first 8 bytes happen to be valid UTF-8 by accident
- **Root cause**: Reading from wrong offset due to schema mismatch

## Why This Happens

### Schema Storage vs Schema Usage

```rust
// When a schema is created:
Schema::new(...) {
    bound = align_address(8, bound);  // Fixed: now 48, not 44
    static_bound: bound  // stored in the Schema struct
}
```

**BUT**: If the schema was already stored in the database (serialized), it still has the old `static_bound` value!

### The Flow

```
1. Server starts
2. Loads schemas from database (OLD schemas with static_bound: 44, 20, 4)
3. Code tries to create new schemas → gets new values (48, 24, 8)
4. Which schema is used for reading cells?
   - If schema cache uses OLD deserialized schema: MISMATCH!
   - If code uses NEW created schema: ALSO MISMATCH (old data, new schema)!
```

## Solutions

### Option 1: Schema Migration (Cleanest, but requires rewrite)

Migrate all existing cells to new schema layout:

1. Read all cells with OLD schema (static_bound: 44, 20, 4)
2. Rewrite them with NEW schema (static_bound: 48, 24, 8)
3. Update schema metadata in database

**Pros**: Clean, permanent fix
**Cons**: Requires full database migration, complex

### Option 2: Schema Versioning (Best Long-term)

Store schema version with each cell:

```rust
struct CellHeader {
    schema_id: u32,
    schema_version: u16,  // NEW: track schema version
    // ...
}
```

Then:
- Old cells (version 1): use static_bound: 44, 20, 4
- New cells (version 2): use static_bound: 48, 24, 8

**Pros**: Supports mixed old/new cells, clean architecture
**Cons**: Requires schema versioning system, changes to cell format

### Option 3: Runtime Schema Fixup (Quick Fix)

Detect and fix schemas when loaded from database:

```rust
impl LocalSchemasCache {
    pub fn get(&self, id: &u32) -> Option<Arc<Schema>> {
        let schema = self.schema_map.get(id)?;
        
        // FIXUP: Check if schema has wrong static_bound
        if schema.static_bound % 8 != 0 {
            warn!("Schema {} has misaligned static_bound: {}, NOT FIXING", 
                  schema.name, schema.static_bound);
            // Don't fix! Use the original misaligned value to read old cells
        }
        
        Some(schema)
    }
}
```

**IMPORTANT**: DON'T fix old schemas! Use them as-is to read old cells correctly!

**Pros**: Preserves ability to read old cells
**Cons**: Creates two classes of schemas (old/new), messy

### Option 4: Fresh Start (Simplest)

1. Export important data (if any)
2. Delete database
3. Restart with fixed code
4. Reimport data

**Pros**: Clean slate, guaranteed to work
**Cons**: Loses existing data

## Current Status

### What's Fixed ✓
- New schemas are created with proper 8-byte aligned static_bound
- No NEW cells will have this problem

### What's Broken ❌
- Old cells in database were written with misaligned schemas
- Those cells cannot be read correctly with new schemas
- Reading attempts result in garbage length values and crashes

## Recommended Action

**Immediate**: 
1. Determine if you have valuable data in the current database
2. If NO: Use Option 4 (Fresh Start)
3. If YES: Use Option 3 (Runtime Fixup) temporarily

**Long-term**:
- Implement Option 2 (Schema Versioning)
- This prevents future schema evolution issues

## How to Check Your Database

Add this diagnostic at startup:

```rust
fn check_schema_alignment(cache: &LocalSchemasCache) {
    for (id, name, expected) in &[
        (3313777299, "wikidata_link", 48),
        (100, "_NEB_ID_LIST", 24),
        (150, "_NEB_TYPE_ID_LIST", 8),
    ] {
        if let Some(schema) = cache.get(id) {
            eprintln!("Schema {}: static_bound = {} (expected {})",
                     name, schema.static_bound, expected);
            if schema.static_bound != *expected {
                eprintln!("  ❌ MISMATCH! Database has old schema!");
            }
        }
    }
}
```

If you see mismatches, your database has old schemas!

