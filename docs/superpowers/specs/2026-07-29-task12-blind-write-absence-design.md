# Task 12 Blind-Write Absence Certification

## Context

The first full candidate comparison run stopped in `mvcc/blind_remove`.
Criterion invokes the workload repeatedly and reuses its fixed ID set. A prior
remove therefore leaves `SnapshotRead::Absent(Some(tombstone_revision_ts))`.
The next untimed transactional seed calls `write`, which currently records
`CellExpectation::Absent(None)` without reading the cell. Participant prepare
compares that value exactly with the current tombstone and returns
`DMPrepareResult::NotRealizable`.

This is independent of the cleanup-floor sentinels. The one-operation local
test-mode benchmark did not reuse an ID, and the earlier filtered remote gate
did not include `blind_remove`, so neither gate exercised the transition.

## Decision

Add `CellExpectation::UnobservedAbsent` for a blind `write` whose transaction
has not read the cell.

- `UnobservedAbsent` means “the cell must be absent when this transaction
  acquires its prepare lock.” It matches either a never-created absence or a
  current tombstone.
- Participants accept `UnobservedAbsent` only with `PrepareIntent::Write`.
  A malformed read intent carrying this expectation is rejected.
- `Absent(None)` and `Absent(Some(revision_ts))` remain exact observations made
  at the transaction snapshot. They continue to detect an intervening
  create/delete cycle.
- A blind write may serialize after any earlier transaction that leaves the
  cell absent. Once prepare succeeds, the participant ownership lock prevents
  another create, update, or remove from crossing the install.
- A present cell still rejects the blind write at prepare. Two concurrent blind
  writers cannot both commit: the first owns and installs the cell, and the
  second no longer observes a current absence.

This keeps the public create-if-currently-absent behavior and avoids adding a
participant snapshot RPC or history lookup to every blind write. It does not
weaken read validation because a cell that was actually read never receives
`UnobservedAbsent`.

No compatibility encoding or migration is required. The user explicitly
ruled out backward compatibility for this point-cell MVCC redesign.

## Required implementation

- Add `UnobservedAbsent` to `CellExpectation`.
- Emit it only when `TransactionManager::write` first touches an uncached cell.
- Treat it as an absent write in commit payload and installed-state checks.
- At prepare and commit prevalidation, match it against any current
  `CellExpectation::Absent(_)` only for a write intent; keep every other
  expectation exact.
- Treat it as having no certified revision timestamp when checking commit-HLC
  ordering.
- Do not alter snapshot reads, read certification, distributed prepare/commit/
  end phases, retry rules, retention, indexes, or Bifrost.

## Regression requirements

1. Create and remove a cell, then blind-write that ID in a fresh transaction.
   Prepare and commit must succeed and publish the recreated cell.
2. A transaction reads a never-created cell as absent. Another operation
   transactionally creates that ID and a third transaction removes it. The
   first transaction then writes it. Prepare must return `NotRealizable`,
   proving the real absence read remains exact without relying on unsupported
   transaction/non-transaction interaction.
3. `UnobservedAbsent` with a read intent must be rejected at the participant.
4. A blind write to a currently present cell must be rejected.
5. Two blind writers for one absent ID must not both prepare and commit.
6. The local compatible benchmark must pass all 13 scenarios.
7. A remote candidate-only `blind_remove` Criterion gate must exercise ID reuse
   and pass before the three candidate comparison runs resume.

## Comparison handling

The three completed baseline reports remain valid: they use the accepted
byte-identical harness, exact baseline product and dependency revisions, and
all passed the strict 13-scenario predicate. Preserve the failed candidate
report and log as RED evidence. Rebuild only the disposable candidate
comparison revision with the product fix, prove the five harness files are
still byte-identical to baseline, run the filtered candidate gate, then run
`mvcc-1`, `mvcc-2`, and `mvcc-3` against the preserved baseline reports.
Record the exact native fix SHA and prove that the disposable candidate carries
the identical product diff.
