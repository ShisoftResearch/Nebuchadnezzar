# Known residuals

What is deliberately **not** fixed, and what is fixed but not fully closed.
Written at the end of the `feat/head-pool` work (2026-08-25) so the next
person does not have to reconstruct it from a 1,500-line campaign log.

Each entry says what is known, what was ruled out and by what evidence, and
what would actually close it. Entries here are not TODOs to be cleared by
guessing; every one of them is where it is because the next step needs
something that was not available at the time.

---

## 1. `NodeCellRef`'s pointer is still read non-atomically

**Status: partially fixed** (`c01156f6`). `src/index/ranged/tree/btree/cell_ref.rs`.

`try_clone_speculative` read the `inner` field THREE times -- the default
check, the deref, and the ref it returned -- on bytes that are mid-write by
definition. Two consequences, both closed:

- the deref saw NULL after the default check saw non-null and
  `as_ref().unwrap()` panicked (`cell_ref.rs:174`);
- the refcount was incremented through the pointer read at the deref while
  the ref RETURNED carried the pointer read afterwards, so a different object
  owed the reference -- an under-count on one node and an over-count on
  another out of one race.

One `read_volatile` now feeds all three uses.

**What is still open.** `inner` is a `*mut NodeRefInner<dyn AnyNode>` -- a FAT
pointer -- and a non-atomic read of one can still tear between its data
pointer and its vtable. `try_clone_speculative` itself does not use the
vtable (the counter is reached through the data pointer at a fixed offset),
but the ref it RETURNS carries both halves, and dropping a ref whose vtable
does not match its data pointer calls the wrong drop glue.

**Why it has not bitten.** All children of a level are the same node kind, and
a split replaces a slot with a node of the same kind, so the old and new
values almost always share a vtable. "Almost always" is doing real work in
that sentence.

**What would close it.** Make the field thin so it fits an `AtomicPtr`: move
the vtable into the pointee (`ptr_metadata`, already available on this
toolchain) and rebuild `&dyn AnyNode` via `ptr::from_raw_parts`. That keeps
one allocation -- boxing `obj` instead would add an indirection to the
hottest path in the tree.

**Evidence for the part that is fixed.** 8 concurrent copies of
`level_merge_insertion`: 1 panic + 1 SIGSEGV in 1,344 runs before, 0 in 1,704
after. It needs LOAD -- 0 in 40 single-threaded runs -- so a test that passes
alone says nothing here.

---

## 2. `write_skew` refused a commit with `CannotEnd`, once in 46 runs

**Status: open lead, unattributed.** Recorded in `crash-safety-plan.md`.

```
client::tests::write_skew
  PrepareError(DMCommitError(CheckFailed(CannotEnd)))
```

That result in the COMMIT phase means one of the transaction's cells no
longer had this transaction as its lock `owner`.

**Ruled out from the failing round's own counters**, not by argument:

| candidate | count in that round |
|---|---|
| stale-lock reclaim by a concurrent prepare | 0 |
| `commit_transaction_brackets` failing | 0 |
| lease sweeper expiring a live transaction | 0 |
| cooperative termination deciding it | 0 |

The last two mean the Phase 6b code never ran in that round at all.

**Rate.** One occurrence across 46 full-suite logs spanning both the
candidate and control arms. `CheckFailed(CannotEnd)` appears in that one log
and nowhere else.

**The remaining lead.** The only production path that clears a cell's owner
is `attempt_lock_release` during `end`, and it clears only when the owner
MATCHES. So either an `end` ran for this transaction before its commit, or a
concurrent `prepare` took the lock through a window that is not the stale
reclaim. Reproducing it needs full-suite load; it does not appear in module
runs.

---

## 3. Cooperative termination cannot resolve a whole-cluster restart

**Status: by design, and stated in the code.**

An in-doubt participant asks its peers. If every peer is down or itself
restarting with no live decision record, nobody can answer and the
transaction is presumed abort. That is the limit of cooperative termination
without a durable coordinator decision, and it is what the code did
UNCONDITIONALLY before this work -- the change shrinks the window rather than
removing it. The 120 s lease sweeper remains the backstop.

Closing it needs a durable decision or an elected terminator, which is a
different protocol and was explicitly not chosen: any node can coordinate
here, so there is no distinguished owner to fail over to.

---

## 4. A resolved COMMIT is per-CHUNK, not per-segment

**Status: known imprecision** (`198be237`).

When recovery resolves an in-doubt transaction to COMMITTED, it writes a
COMMIT record into every chunk that holds a part. The existing rule for
bracket parts is stronger -- every PART carries its own COMMIT, so the
cleaner cannot orphan one by compacting the segment that held the only
record.

Chunk granularity is as precise as this path can be: the transaction's own
segments are full or sealed and cannot take another entry. The residual is
that compacting a chunk's COMMIT-bearing segment while another segment in the
same chunk still holds a BEGIN would put the transaction back in doubt at the
next restart, where the peers may no longer remember it.

Closing it properly means being able to append to a sealed segment, or
rewriting the resolved cells as ordinary entries.

---

## 5. `test_large_scale_transactions_with_natural_tiered_memory` lives at the edge

**Status: not a bug, but do not read its failures as durability findings.**

Under a full 8-way suite this test shows, in EVERY round including passing
ones:

- ~2,100 `could not place a NNNN-byte transactional entry` on chunk 0
- 14-32 `Segment N archive returned false before eviction - aborting`
- occasionally `SEAL ARCHIVE FAILED ... No such file or directory`

It drives 512 MB through a 64 MB tier, uses a FIXED shared backup path it
`remove_dir_all`s at start, and calls `std::env::set_var` while seven other
tests run. A batch failing to write is a normal outcome here.

Its verification counter was fixed (`bcc94cfa`) so a failed batch's cells are
no longer verified, but the test's assertion message still says
"Serializability check failed" for what may be absent cells. **Before
believing it, compare `grep -c "Serializability violation"` against
`grep -c "Failed to read cell"`** -- the first is a real mismatch, the second
is an absent cell. When this was chased, the counts were 0 and 15.

---

## 6. Not covered by a test, covered by repetition

The recovery half of "silence is not a vote" (`4fd8ac7d`) -- waiting for the
membership roster to grow before believing it is empty -- has no dedicated
test. It was verified by running the module 6x clean where it had previously
failed roughly 1 run in 4, and by the `asking N peer(s)` log line. The sweeper
half does have a test
(`a_silent_peer_holds_the_transaction_open_but_not_forever`).

Worth a test if someone touches that path.
