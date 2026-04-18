# Blob Schema Design

Date: 2026-04-18

## Goal

Add schema-wide blob storage to Nebuchadnezzar so selected schemas can store larger cells, use blob-only segments inside the existing universal chunk layout, and bias those segments toward early eviction without changing the existing cell header format or the normal read semantics.

## Non-Goals

- No separate blob chunk collection.
- No new persisted segment metadata for blob identification.
- No change to `CellHeader` layout.
- No change to promotion-on-read semantics for cold blob segments.
- No separate blob memory budget.

## Summary

Blob support is schema-wide through a new `Schema.blobs` flag.

When `blobs=true`:

- cells in that schema may grow up to 2 MiB instead of 1 MiB;
- writes allocate into blob-only segments;
- blob segments share the same chunk, allocator, WAL, backup, recovery, and address model as regular segments;
- blob segments are preferred victims during eviction under the shared hot-memory budget;
- blob segments recovered from storage remain cold and are not promoted into anonymous memory during recovery.

The system identifies blob cells by resolving `cell.header.schema` through the schema table. The system identifies blob segments by examining their cells and deriving an in-memory runtime class. No blob bit is persisted in the cell header, WAL, or segment snapshot.

## Architecture

### Universal Chunks

Chunks remain universal. A chunk may contain both regular segments and blob segments. Partition-to-chunk routing remains unchanged, so the current key-to-chunk behavior and cell address stability model stay intact.

The change happens inside the chunk write path: each chunk now maintains one writable head per segment class instead of one global writable head.

### Two Segment Classes

Segments are classified at runtime as one of:

- regular
- blob

Blob segments must contain only blob cells. Regular segments must contain only regular cells. Mixed segments are invalid and should be treated as an invariant violation if detected during recovery or validation.

### Hot-Path Head Selection

Chunk write allocation is on the hot path, so the multi-head design must stay branch-light and avoid dynamic structures.

The chunk should not use a map keyed by segment type. It should store exactly two head pointers in a fixed layout, for example:

- regular head segment id
- blob head segment id

This can be implemented as two atomics or a two-slot array indexed by a branch-free lane computation from `schema.blobs as usize`.

The write path should do only:

1. schema lookup
2. derive lane index from `schema.blobs`
3. read the matching head id
4. allocate from that head

No extra lock, hash lookup, or segment rescan should be added to the steady-state write path.

## Data Model

### Schema

Add a new schema property:

- `blobs: bool`

Rules:

- default is `false`
- serialized with the schema snapshot and replicated schema metadata
- validated as a storage-policy flag only; it does not change indexing semantics

The existing `Schema::new` call surface should remain practical for the current codebase. If direct constructor expansion would cause excessive churn, add a narrow helper or builder-style setter so existing callers default to `blobs=false` while new blob schemas opt in explicitly.

### Cell Headers

`CellHeader` remains unchanged.

Blob or regular cell identity is derived by:

1. reading `CellHeader.schema`
2. resolving that schema id from the schema table
3. checking `schema.blobs`

This same lookup rule is used in:

- write planning
- recovery scanning
- runtime segment-class derivation

### Segment Runtime Class

Segments may keep a runtime-only derived class field once known. This is not persisted. Its purpose is to avoid repeated rescans during eviction, cleaner decisions, and head-lane validation.

Sources of truth for that runtime class:

- new segments: known from the head lane that allocated them
- recovered segments: derived once by scanning cells and resolving schema ids

## Size Limits

Current normal cell limit remains unchanged:

- regular schema: 1 MiB

New blob cell limit:

- blob schema: 2 MiB

The size check remains centralized in cell write planning so behavior stays consistent for inserts, updates, and transactional writes.

## Write Path

### Schema-Aware Planning

`OwnedCell::plan_write` becomes schema-aware for size policy:

- if `schema.blobs == false`, reject cells larger than 1 MiB
- if `schema.blobs == true`, reject cells larger than 2 MiB

No header encoding changes are required.

### Head Lane Allocation

When a chunk allocates space for a new entry, it selects the head lane from the schema policy:

- regular schema -> regular head
- blob schema -> blob head

If the selected head has room, allocation proceeds normally.

If the selected head is full:

- allocate a fresh segment from the existing segment allocator
- tag its runtime class from the chosen lane
- publish it as the new head for that lane only
- leave the other lane's head unchanged

This preserves the blob-only or regular-only segment invariant while keeping universal chunks.

### Empty Recovered Heads

Recovery does not need to persist head class information. If a chunk has no writable head for a lane after recovery, the first write on that lane may lazily create one.

This avoids adding persisted metadata just to reconstruct empty writable heads.

## Eviction Policy

### Shared Budget

Blob and regular segments share the existing server-wide hot-memory budget.

No separate blob budget is introduced.

### Priority Rule

When the tiered manager selects eviction victims under memory pressure, it should prefer eligible hot blob segments before eligible hot regular segments.

Existing safety checks remain intact:

- never evict active head segments for either lane
- never evict segments with active references
- never evict already cold segments
- still honor the promotion cooldown window

If no eligible blob victim exists, normal CLOCK behavior continues over regular segments.

### Cleaner Awareness

Cleaner logic that currently protects the single active head must be extended to protect both active heads in the chunk.

This prevents compaction or reclamation from touching:

- the current regular write head
- the current blob write head

## Promotion Semantics

Blob segments use the same promotion-on-read semantics as regular segments.

If a cold blob segment is accessed through the normal read path, the existing promotion mechanism still applies. No special read API is needed and no blob-specific read bypass is introduced.

## Recovery

### Segment Classification

Recovery classifies a segment by scanning its cells and resolving each cell's schema id.

Classification rules:

- if all cells map to regular schemas, the segment is regular
- if all cells map to blob schemas, the segment is blob
- if both classes are observed in one segment, recovery should fail that segment as an invariant violation

Tombstones do not define the segment class. Only cells do.

### Blob Recovery Rule

Blob segments must not be promoted into hot anonymous memory during recovery.

For blob segments:

- recovery scans from file data
- recovery rebuilds cell index and version tracking using virtual addresses
- recovery marks the segment cold
- recovery does not copy the segment back into anonymous hot memory

For regular segments:

- keep the current hot or cold recovery behavior

### Recovery Without Persisted Blob Metadata

Because segment class is derived from recovered cells, no blob flag is required in:

- backup file naming
- WAL metadata
- segment header metadata
- cell header fields

This keeps storage compatibility narrow while moving class inference into recovery logic.

## Invariants

The implementation should enforce these invariants:

1. A blob schema writes only to blob-class segments.
2. A regular schema writes only to regular-class segments.
3. A segment must not contain both blob and regular cells.
4. `CellHeader` layout remains unchanged.
5. Blob classification always resolves through the schema table.
6. Blob segments recovered from storage remain cold until a normal runtime read promotes them.
7. Both chunk heads are treated as protected from eviction and cleaner compaction.

## Performance Constraints

The multi-head chunk design must remain efficient because allocation is a hot path.

Required performance properties:

- exactly two fixed head lanes, not an extensible container
- O(1) lane selection from schema policy
- no per-write segment-content inspection
- no per-write schema revalidation beyond the existing schema lookup already needed for write planning
- no additional global coordination between the regular and blob head lanes beyond using the existing allocator

The runtime-derived segment class should be cached on the segment after it is known so eviction and cleaner policy do not need repeated scans.

## Testing Strategy

Minimum required test coverage:

### Schema Metadata

- schema registration persists and recovers `blobs=false` and `blobs=true`
- existing non-blob schema creation remains backward-compatible

### Cell Size Policy

- regular schema still rejects cells larger than 1 MiB
- blob schema accepts cells above 1 MiB up to 2 MiB
- blob schema rejects cells larger than 2 MiB

### Segment Separation

- regular writes allocate into regular segments only
- blob writes allocate into blob segments only
- chunk can maintain both active heads without cross-lane interference

### Eviction

- under shared pressure, eligible blob segments are selected before eligible regular segments
- active blob head and active regular head are both protected from eviction

### Promotion

- reads from cold blob segments still trigger the normal promotion path

### Recovery

- recovered blob segments stay cold
- recovered regular segments preserve current behavior
- recovery derives blob classification from schema ids in cell headers
- recovery fails or flags mixed-class segments as invalid

## Rollout Notes

This design intentionally keeps the public data model small:

- one new schema flag
- no new cell header fields
- no new persisted segment metadata

The main implementation complexity is local to:

- chunk head management
- schema-aware size policy
- runtime segment classification
- eviction preference
- recovery classification

This is the desired tradeoff because it keeps compatibility risk low while preserving universal chunks and the existing read model.