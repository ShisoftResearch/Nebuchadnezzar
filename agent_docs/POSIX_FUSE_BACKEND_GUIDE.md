# Neb as a Backend for a POSIX Filesystem via FUSE

## Purpose

This guide describes how to use Nebuchadnezzar as the storage backend for a POSIX-style filesystem implemented in a separate FUSE project.

The short version is:

- Treat Neb as a transactional metadata and chunk store.
- Treat FUSE as the layer that owns POSIX behavior.
- Keep metadata and file payload in different schemas and different runtime lanes.
- Use Neb transactions for every filesystem mutation that must be crash-safe and user-visible.

Neb is a good substrate for this role because it already provides:

- rich typed schemas
- hash lookup for point operations
- ranged indexes for ordered scans
- version-certified optimistic commits with repeatable cell reads
- tiered memory with blob-first eviction behavior
- explicit durability for transactional writes at commit time

Neb is not, by itself, a POSIX filesystem. The FUSE layer must define and enforce the filesystem contract.

## What Neb Should Be Responsible For

Neb should be the durable store for:

- inode metadata
- directory entries
- file data chunks
- optional extended attributes
- optional orphan cleanup records
- optional lease or recovery metadata

Neb should not be treated as the layer that directly implements:

- pathname lookup semantics exposed to the kernel
- open file handle lifetime rules
- page cache behavior
- `mmap` coherence
- `flock` or POSIX byte-range locks
- mount-local cache invalidation
- distributed client lease management

Those behaviors belong in the FUSE daemon, or in a separate coordination service if the filesystem is mounted concurrently from multiple hosts.

## Recommended Layering

The recommended split is:

1. FUSE daemon
2. Filesystem object model
3. Neb storage adapter
4. Neb runtime

In practice:

- FUSE translates kernel requests into filesystem operations.
- The filesystem layer owns inode, dentry, link count, path resolution, and flush semantics.
- The storage adapter maps those operations into Neb reads, writes, scans, and transactions.
- Neb stores typed rows and chunks and handles crash recovery of committed state.

If you follow this boundary, Neb stays simple and reusable, and the FUSE layer remains the only place where POSIX semantics are interpreted.

## Core Design Rule

The single most important rule is:

**Store metadata in regular schemas and bulk payload in blob schemas.**

That gives you the intended runtime behavior:

- metadata stays on the regular segment lane
- file payload goes to the blob lane
- blob segments are the preferred eviction target under tiered memory
- recovery preserves the segment class discovered from actual cell contents

Do not mix inode metadata and large file payload in the same schema.

## Recommended Data Model

### 1. Inode Table

Use one row per inode.

Suggested logical key:

- `inode_number`

Suggested fields:

- `inode_number`
- `kind` (`file`, `dir`, `symlink`, `device`, etc.)
- `mode`
- `uid`
- `gid`
- `nlink`
- `size`
- `blocks` or logical block accounting
- `atime`
- `mtime`
- `ctime`
- `generation`
- `flags`
- `rdev` for device nodes when needed
- optional short symlink target inline

Notes:

- `generation` should be incremented on visible metadata changes and used as a cache invalidation token.
- `size` must be authoritative for EOF handling.
- Small symlink targets can be stored inline in the inode row.

### 2. Directory Entry Table

Use a separate table for dentries.

Suggested logical key:

- `(parent_inode, name)`

Suggested fields:

- `parent_inode`
- `name`
- `target_inode`
- `target_generation`
- `kind`

Recommended indexes:

- hash lookup on `(parent_inode, name)` for `lookup`
- ranged index on `(parent_inode, name)` for ordered `readdir`

Notes:

- Do not embed directory contents inside inode rows.
- Ordered `readdir` is a direct fit for Neb's ranged index support.

### 3. File Chunk Table

Use a blob schema for file payload.

Suggested logical key:

- `(inode, chunk_index)`

Suggested fields:

- `inode`
- `chunk_index`
- `payload`
- optional `logical_len`
- optional `checksum`
- optional `written_at_generation`

Recommended indexes:

- hash lookup on `(inode, chunk_index)`
- optional ranged index on `(inode, chunk_index)` if extent or sequential scan logic benefits from it

Notes:

- The chunk table is the data plane.
- This schema should enable `blobs=true`.
- Chunks beyond EOF are invalid even if rows still exist temporarily during a transaction.

### 4. Extended Attribute Table

If you need xattrs, use a separate metadata table.

Suggested logical key:

- `(inode, xattr_name)`

Suggested fields:

- `inode`
- `name`
- `value`

Keep this on the regular lane unless xattrs are known to be very large.

### 5. Optional Orphan Table

If you need correct POSIX behavior for unlink-on-open files, maintain an orphan table.

Suggested logical key:

- `inode`

Suggested fields:

- `inode`
- `finalize_after_last_handle`
- `pending_delete_generation`

This lets the FUSE layer delay physical cleanup until the last handle closes.

## Chunk Sizing Guidelines

Neb supports blob cells up to 2 MiB, but that is an upper bound, not the default target size.

Recommended starting point:

- `512 KiB` chunks for general-purpose filesystems

Use smaller chunks when:

- workloads do heavy random overwrites
- partial writes are common
- rewrite amplification matters more than scan efficiency

Use larger chunks when:

- workloads are dominated by large immutable or append-mostly media
- read throughput matters more than write granularity

Practical guidance:

- `256 KiB`: heavy random overwrite workloads
- `512 KiB`: balanced default
- `1 MiB` to `1.5 MiB`: large media, low rewrite rate

Avoid using the 2 MiB ceiling as the default chunk size unless the workload is clearly optimized for that tradeoff.

## Transaction Boundaries

Neb's transactional durability is the foundation for the filesystem mutation model.

### Use a Neb Transaction For

- `create`
- `mkdir`
- `unlink`
- `rmdir`
- `rename`
- `link`
- `symlink`
- `truncate`
- `setattr`
- file writes that change chunk rows and inode metadata together
- xattr updates that must be atomically visible with inode metadata

### What Must Be In the Same Transaction

At minimum, a visible filesystem mutation should commit:

- all changed metadata rows
- all changed chunk rows
- inode `size` if data length changed
- inode timestamps and generation
- directory timestamps and generation when namespace changes

Examples:

#### `create`

One transaction should:

- allocate an inode
- write the inode row
- insert the parent dentry
- update parent directory metadata

#### `rename`

One transaction should:

- remove the old dentry
- insert or replace the new dentry
- update source and destination directory metadata
- update replaced inode metadata if overwrite semantics apply

#### partial `write`

One transaction should:

- read the affected chunk rows
- perform read-modify-write in the FUSE layer
- write updated chunk rows
- update inode `size`, `mtime`, `ctime`, and `generation`

### What Not To Do

Do not rely on non-transactional writes for POSIX-visible mutations.

Neb batches non-transactional WAL sync for throughput. That is acceptable for internal best-effort writes, but it is not the right contract for filesystem operations that users will treat as committed once `write`, `rename`, or `fsync` returns.

## `fsync` and Durability Contract

This is where the FUSE project must be explicit.

Neb currently gives you two relevant durability modes:

- non-transactional writes use batched WAL sync
- transactional writes explicitly sync at commit

For a POSIX filesystem backend, the safe rule is:

**Map all user-visible mutations that require durable acknowledgement onto Neb transactions, and define `fsync` in terms of successful Neb commit for the relevant data and metadata set.**

Recommended semantics:

- file `fsync`: commit all dirty chunks and the inode row
- directory `fsync`: commit dentry and directory inode mutations affecting that directory
- `close` without `fsync`: may use writeback policy, but the FUSE layer must document it clearly

If you want stronger guarantees such as cross-host visibility ordering or external audit ordering, Neb alone is not enough. Add a higher-level coordination rule.

## Read and Cache Semantics

Neb provides repeatable cell reads and version-certified optimistic commits. It does not provide predicate/range phantom protection or full external linearizability.

That means:

- transactions reread the same committed cell versions consistently
- optimistic commit certification rejects stale cell updates
- predicate or range scans still need higher-level handling if phantoms matter
- the database does not, by itself, define all POSIX cache and coherence rules

For a FUSE filesystem, the daemon must therefore own:

- inode attribute cache invalidation
- dentry cache invalidation
- open-handle state
- page cache writeback ordering
- `mmap` consistency policy

If the filesystem is mounted from only one host, a mount-local cache policy may be sufficient.

If the filesystem is mounted from multiple hosts at once, add a lease or lock service. Do not assume Neb's transactional model alone is enough to provide cluster-wide POSIX locking or real-time ordering.

## Locking and Multi-Host Coordination

Single-node FUSE is straightforward: keep lock state in the daemon.

Multi-node FUSE is where you must be strict:

- do not use Neb alone as the lock authority for POSIX byte-range locks
- do not assume real-time ordering from timestamp transactions
- do not build lease correctness on top of Neb transaction semantics alone

If multiple clients mount the same namespace concurrently, use a separate lease or coordination layer for:

- inode leases
- directory mutation exclusion where needed
- advisory locks
- cache invalidation notifications

Neb remains the durable store of record, but it should not be forced to act as a linearizable distributed lock manager.

## Sparse Files and Holes

The simplest sparse-file rule is:

- absent chunk row means logical zeroes
- inode `size` defines EOF

This is enough for a first implementation.

If hole punching or extent-heavy optimization matters later, add extent metadata in a separate table. Do not complicate the first design unless the workload requires it.

## Recommended Operation Mapping

### `lookup(parent, name)`

- hash lookup in dentry table
- read target inode row

### `getattr(inode)`

- point read on inode row

### `readdir(dir)`

- ranged scan on `(parent_inode, name)`

### `read(inode, offset, len)`

- map byte range to chunk indices
- fetch relevant chunk rows
- synthesize holes as zeroes
- trim to inode `size`

### `write(inode, offset, data)`

- map to chunk indices
- read-modify-write partial chunks in userspace
- write updated chunk rows in one transaction
- update inode row in the same transaction

### `truncate(inode, new_size)`

- update inode `size`
- remove or logically invalidate tail chunk rows
- commit in one transaction

### `unlink(parent, name)`

- remove dentry row
- decrement inode `nlink`
- if `nlink == 0` and no open handles remain, schedule chunk cleanup

### `rename(old_parent, old_name, new_parent, new_name)`

- one transaction for both dentry updates and metadata updates
- treat overwrite carefully and update link counts consistently

### `link(parent, name, target_inode)`

- insert new dentry
- increment inode `nlink`
- commit together

## Performance Guidance

### Keep Metadata Small and Hot

- inode rows should stay compact
- dentries should remain compact
- xattrs should be separate
- do not place file payload in inode rows

### Use Blob Schemas Only for Payload

- file data chunks should be blob rows
- large symlink or alternate stream payload may also use blob rows if needed

This aligns with Neb's tiered-memory behavior, where blob segments are better eviction candidates than metadata segments.

### Use Ranged Indexes for Directory Traversal

Directory scans are naturally ranged operations.

Model them that way instead of reconstructing directory contents from unordered metadata scans.

## Recovery and Startup Rules

The FUSE layer should assume:

- committed Neb transactions survive recovery
- recovered blob segments may come back cold
- metadata required for mount and namespace traversal should be regular-lane data

Startup recommendations:

- verify root inode exists
- verify root directory entries are readable
- lazily warm file payload as files are accessed
- avoid full-data scans during mount if only metadata is needed

## What to Avoid

Avoid these patterns:

- storing whole files in one row by default
- mixing metadata and payload in one schema
- embedding directory listings in inode rows
- depending on non-transactional writes for POSIX durability points
- assuming Neb provides `mmap` coherence semantics
- assuming Neb alone provides distributed file locking correctness
- designing around maximum blob size instead of chunk rewrite behavior

## Validation Checklist for the FUSE Project

Before calling the design production-ready, validate:

- correct `lookup`, `getattr`, `readdir`, `read`, `write`, `rename`, `unlink`, and `truncate`
- crash safety of metadata and data transactions
- correct behavior for unlink-on-open files
- correct link count behavior for hard links
- durable `fsync` behavior
- cache invalidation across concurrent opens
- sparse file reads and truncation

Recommended external test tools:

- `pjdfstest`
- `fsx`
- `fsstress`
- selected `xfstests` cases that match the implemented feature set

Also re-run Neb's own isolated test sweep whenever backend schema or storage behavior changes.

## Recommended First Milestone

For the first usable version, keep the scope tight:

1. single-host mount
2. inode table
3. dentry table
4. blob chunk table
5. transactional create, rename, unlink, read, write, truncate
6. explicit `fsync`
7. no distributed locks yet
8. no `mmap` coherence guarantees beyond mount-local behavior

That gives you a real POSIX-like filesystem backed by Neb without prematurely solving multi-host coordination.

## Bottom Line

Neb is suitable as the backend of a POSIX-style filesystem if you use it in the right role:

- Neb is the transactional object and chunk store.
- FUSE is the POSIX contract layer.
- Metadata belongs on the regular lane.
- File payload belongs on the blob lane.
- All visible mutations belong in transactions.
- Strong cross-host coordination needs a higher-level lease or lock mechanism.

If you keep those boundaries clean, the design is practical and aligned with Neb's current guarantees.
