# Task 5 Report: History-Safe Cleaner Relocation

## Implementation

- `SegmentCandidate` now acquires exclusive segment reference state before its
  hot lock and releases that state only after restoring the segment to hot.
  Cleaner selection no longer relies on a racy `no_references()` observation.
- Cleaner liveness and deduplication use the full logical revision key
  `(Id, revision_ts)`. Raw source scans retain every cell or tombstone selected
  by history, plus a physical cell selected by the current-index mirror.
- `HistoryIndex::is_live_at` validates an exact retained address.
  `HistoryIndex::relocate` uses a tagged-word CAS that preserves concurrent
  state transitions and reports a lost race when expiration or another move
  changed the node.
- Every retained cell and tombstone is copied before publication. Tombstone
  counters are preserved on destination segments. A copied entry that loses
  the history CAS is marked dead exactly once.
- Physical cells reconcile the current-index mirror under its lower-key guard.
  The helper validates the full `Id` and revision header before moving the
  expected address. A differing or absent mirror permits source reclamation
  only when history proves, under the same guard, that the relocated node is no
  longer logically current.
- If a logically current cell cannot reconcile its mirror, cleaner reverses the
  history CAS, marks the unreachable destination dead, retains every source,
  and reports no reduction. A lost reverse CAS still suppresses reclamation.
- A test-only pre-relocation hook makes expiration, historical-reader, and
  inconsistent-mirror interleavings deterministic without changing the
  production interface.

## TDD Evidence

- Initial retained-revision RED:
  `cargo test --lib ram::cleaner::tests::combine_relocates_current_and_historical_revisions_for_one_id -- --test-threads=1`
  failed 0/1 because revision 100 still referenced its reclaimed source.
  After history-aware collection and relocation it passed 1/1.
- The prerequisite lease regression
  `cargo test --lib ram::segs::tests::shared_reference_fails_while_exclusive_guard_is_held -- --test-threads=1`
  passed 1/1 before cleaner changes.
- The expiration and historical-reader race tests initially failed to compile
  with two E0599 errors because the deterministic relocation hook did not
  exist. After adding the test-only hook and CAS path, each passed 1/1.
- The unreconciled-current regression initially failed 0/1 because history was
  left at the destination instead of the registered source. After reverse-CAS,
  destination dead accounting, and source-reclamation suppression, it passed
  1/1.
- The expanded cleaner suite initially passed 8/9. Its legacy full-clean test
  still expected retained historical cells to be reclaimed immediately. The
  test now first asserts no reduction, explicitly expires the obsolete
  revisions, and then verifies retained current cells and tombstones. The final
  cleaner suite passed 9/9.

## Final Verification

- `cargo test --lib ram::cleaner -- --test-threads=1`: 9 passed, 0 failed.
- Exact exclusive/shared lease regression: 1 passed, 0 failed.
- `cargo test --lib ram::history -- --test-threads=1`: 24 passed, 0 failed.
- `cargo test --lib ram::tiered -- --test-threads=1`: 30 passed, 0 failed,
  5 ignored.
- `cargo test --lib ram::tests::cell -- --test-threads=1`: 22 passed,
  0 failed.
- `cargo test --lib ram::tests::chunk -- --test-threads=1`: 28 passed,
  0 failed.
- The two focused blob cleaner tests each passed 1/1.
- The listed test commands total 116 passed, 0 failed, 5 ignored.
- `cargo check --lib`: passed with the repository's existing warning set.
- `rustfmt --edition 2021 --check` on every touched Rust file and
  `git diff --check`: passed.
- The `rg` audit found no `no_references()` precondition in cleaner/chunk
  selection and no `SegmentReferenceGuard::try_new` use in transaction code.

## Concurrency and Accounting Review

- Publication order is destination copy and archive, destination registration,
  history-location CAS, then current-index reconciliation for physical cells.
  Exclusive source reference state remains held through that entire sequence
  and source cleanup.
- A historical reader either holds a shared source lease before exclusivity or
  fails that lease, reloads the node, and obtains a short destination lease.
  Snapshot APIs materialize owned data and retain neither raw addresses nor
  leases.
- Expiration winning the history CAS owns the source dead record; cleaner owns
  only the losing destination copy. Cleaner winning makes the destination the
  node location before the old source can be freed.
- Current mirror reconciliation and the proof that a moved node is historical
  occur under the same per-lower-key guard used by writers. Full header
  validation prevents a lower-hash collision from moving another `Id`.
- Secondary indexes are keyed by logical values and do not embed physical
  storage addresses, so cleaner has no secondary address to rewrite. The
  current cell-index mirror is the sole address-bearing secondary structure.
- On successful relocation the source is reclaimed as a segment and the
  destination remains live. On a lost relocation the destination alone becomes
  dead. On unreconciled current publication the destination becomes dead after
  rollback and all sources stay registered.
- Repeated cleaning is stable: destination-only retained revisions are not
  recopied when fewer than two candidates remain, and retained source
  duplicates cease to be live once history selects the destination.

## Accepted-Head Blob Audit

- The approved Task 4 head was checked in a temporary detached worktree at
  `a64faa51cab0aeae050dc81df545594385791ef2`.
- `blob_schema_combine_preserves_blob_segment_class` passed 1/1 on that head.
  Task 5's correct history retention exposed that its deleted filler revisions
  were still retained, so the test now expires and drains those setup-only
  revisions before exercising destination segment-class preservation.
- `blob_schema_partial_cleaner_candidates_stay_class_aware_in_mixed_workloads`
  failed 0/1 on the approved head because no candidates were produced. This was
  accepted-head test debt. Its setup now performs the same explicit expiration
  before inspecting class-aware candidates.
- These are test-only setup changes; production retention policy is unchanged.

## Scope and Self-Review

- Production changes are limited to segment guarding, cleaner relocation,
  history CAS support, and current-mirror reconciliation. The only additional
  source file is the approved blob test setup adaptation.
- Bifrost, Dovahkiin, the approved plan/spec, transactions, recovery, and
  retention policy were not modified.
- The final diff was reviewed for full-`Id` collision handling, current and
  historical tombstones, aborted physical-cell kind, dead-space double
  accounting, source registration on rollback, lock ordering, repeated
  cleaning, and accidental long-lived segment guards. No unresolved Task 5
  defect was found.

## Review Fixes Round 1

### Implementation

- Cleaner relocation records whether collection was selected by an exact
  history node or only by the current cell-index mirror. A history
  `LostRace` for a current-only physical cell now enters guarded mirror
  reconciliation instead of unconditionally retiring the copied destination.
- Under the lower-key guard, current-only reconciliation verifies the expected
  source address plus full source and destination `(Id, revision_ts)` before
  publishing the destination. A changed mirror is accepted as superseding only
  when it is a registered, fully materialized same-lower cell outside the
  candidate source set. Same-ID older revisions, absent/zero mirrors,
  unregistered addresses, incomplete headers, and changed mirrors inside any
  candidate source are unresolved.
- A successfully published current-only destination remains live without
  synthesizing history. The next normal assigned-revision update lazily
  installs that relocated predecessor, after which normal snapshot reads
  select it.
- An unpublished current-only destination is dead-accounted exactly once.
  Unresolved mirrors suppress cleanup of every exclusive source, leaving the
  source registered and readable. A valid successor or full-ID/lower-key
  collision outside the cleanup set proves the old source is no longer
  mirrored and permits reclamation.
- Relocation and snapshot/write synchronization hooks, their dynamic callback
  types, fields, parameters, and invocations are all `#[cfg(test)]`. Expanded
  non-test library output contains none of their symbols; the production
  cleaner path has no no-op callback or indirect call.

### TDD Evidence

- Current-only RED:
  `cargo test --lib ram::cleaner::tests::combine_relocates_a_current_only_cell_without_a_history_node -- --exact --test-threads=1`
  failed 0/1 because the mirror still equaled the reclaimed source after a
  successful two-to-one combine. After provenance-aware guarded publication it
  passed 1/1, including a normal current read, normal assigned update, and
  snapshot read of the relocated predecessor.
- The first GREEN attempt exposed a test-only setup error: the test had left
  the deliberately invalid cleaner head ID installed before asking the normal
  writer to allocate. Restoring the published destination as the write head
  removed that allocation wait without changing relocation code.
- The current-only collision, valid same-ID successor, and unresolved-mirror
  decision tests each passed 1/1. Self-review then added the stricter
  candidate-source case. Its RED failed 0/1 because cleaner returned
  `(16777096, 1)` rather than retaining `(0, 0)` while the changed mirror
  pointed into another source. Passing the candidate set into guarded
  reconciliation made it GREEN 1/1.
- Production-hook RED audit found unconditional `&dyn Fn` parameters in both
  combine phases and a normal-path no-op invocation. After the cfg-only split,
  `cargo check --lib` passed and expanded non-test output reported
  `no test-hook symbols in expanded production library`.
- Reader-boundary RED failed to compile with two E0599 errors because no
  post-lease-attempt synchronization method existed. The cfg-only point now
  fires immediately after `SegmentReferenceGuard::try_new` returns and before
  node revalidation/materialization. The normal snapshot retry regression
  passed 1/1, observing one or more exact `(source, false)` attempts followed by
  `(destination, true)`. A second normal-API test passed 1/1 while pausing with
  `(source, true)` and proving cleaner returned `(0, 0)` without unregistering
  the leased source.
- Writer-boundary RED failed to compile with three E0599 errors for the missing
  cfg-only post-history hook and failed-CellGuard-attempt hook. After adding
  those test-only points, the normal assigned-update race passed 1/1.
- A final serial blob-suite rerun exposed the filler-expiration setup race:
  two runs failed different cleaner-candidate assertions while each exact test
  passed alone. The test-only setup now stops and joins the background history
  worker before its explicit `u64::MAX` expiration/drain, eliminating the
  worker's temporary ownership of queued records without changing production
  retention.

### Concurrency and Accounting Protocol

- Source `SegmentCandidate` exclusivity is held through copy, destination
  registration, history CAS, mirror reconciliation, the decision to retain or
  reclaim all sources, and physical cleanup. Current-only mirror
  reconciliation intentionally takes only the lower-key word lock and never
  tries to acquire a source segment reference under cleaner exclusivity.
- A historical reader that acquired a shared source lease first prevents
  cleaner exclusivity and materializes the source. If cleaner exclusivity won
  first, the reader observes a failed source lease, reloads the history node
  after relocation, acquires a short destination lease, revalidates the node,
  and materializes owned data.
- With default tiered-memory guards, a normal writer cannot hold or read the
  source after cleaner exclusivity. In the deterministic interleaving, revision
  200 history relocates first, the assigned-revision-300 update fails its source
  `CellGuard` attempt and retries, cleaner reconciles the current mirror and
  releases/removes sources, and that same normal update acquires the relocated
  destination and publishes its successor. Revision 300 remains current,
  revision 200 remains snapshot-readable at the copied destination, both
  source segments are absent, and the predecessor destination has zero dead
  bytes.
- Changed current-only mirrors are decoded only after proving the address lies
  in a registered segment and its complete header is below the append
  boundary. A valid changed mirror inside any candidate segment is unresolved,
  because bulk cleanup would otherwise reclaim its storage.
- Every current-only lost-publication path marks only its copied destination
  dead once. Unresolved paths leave all sources registered. Successful
  current-only publication keeps the destination live. History-backed
  publication and exact reverse-CAS rollback retain their previous accounting
  ownership.

### Final Verification

- `cargo test --lib ram::cleaner -- --test-threads=1`: 16 passed, 0 failed.
- Exact exclusive/shared lease regression: 1 passed, 0 failed.
- `cargo test --lib ram::history -- --test-threads=1`: 24 passed, 0 failed.
- Bounded `cargo test --lib ram::tiered -- --test-threads=1`: 30 passed,
  0 failed, 5 ignored in 159.44 seconds.
- `cargo test --lib ram::tests::cell -- --test-threads=1`: 22 passed,
  0 failed.
- `cargo test --lib ram::tests::chunk -- --test-threads=1`: 28 passed,
  0 failed.
- `cargo test --lib ram::tests::blob_schema -- --test-threads=1`: 8 passed,
  0 failed.
- The listed serial test commands total 129 passed, 0 failed, 5 ignored.
- `cargo check --lib`: passed with the repository's existing warning set.
- Touched-file `rustfmt --edition 2021 --check` and `git diff --check`:
  passed.
- The `rg` audit found no `no_references()` cleaner/chunk precondition.
  Test callback dynamic types and invocations are all adjacent to
  `#[cfg(test)]`; expanded production output contains no hook symbols.
