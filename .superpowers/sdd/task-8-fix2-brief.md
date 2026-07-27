# Task 8 Review Fixes — Round 2

## Scope

Fix the sole remaining Important finding from the full Task 8 rereview at head
`97b1265d1f98ec0d4411ebf2e67b4bd11f42b080`.

Read:

- `docs/superpowers/specs/2026-07-25-point-cell-mvcc-design.md`
- `docs/superpowers/plans/2026-07-26-point-cell-mvcc.md`
- `.superpowers/sdd/task-8-brief.md`
- `.superpowers/sdd/task-8-fix-brief.md`
- `.superpowers/sdd/task-8-report.md`
- `.superpowers/sdd/review-76970a3f..97b1265d.diff`

Use test-driven development and systematic debugging. Keep the implementation
strictly within Task 8. Do not implement Task 9 compensation or Task 10
stale-owner resolution.

## Important Finding: Exact Same-HLC Payload Identity

`src/server/transactions/data_site.rs` currently compares `OwnedCell.data` with
ordinary `PartialEq`. That is not an exact identity relation for all valid
Dovahkiin values:

- `F32`/`F64` NaN values can be bit-identical but compare unequal.
- `+0.0` and `-0.0` compare equal despite different bit patterns.
- `OwnedMap::PartialEq` ignores its public `fields` vector.
- The same concerns apply recursively inside arrays, primitive arrays, and
  nested maps.

Implement a deterministic, logically complete request-identity comparison (or
equivalent canonical representation) for `CommitOp`:

- Compare operation discriminant.
- Compare full `Id` and every `CellHeader` field.
- Compare every supported `OwnedValue` variant recursively.
- Compare floating-point values by `to_bits()`.
- Compare map contents independent of hash-map iteration order, while also
  comparing the complete ordered `OwnedMap.fields` metadata.
- Preserve exact element order for arrays and primitive arrays.
- Avoid a debug/release-dependent serializer. In particular, do not rely on
  Bifrost's JSON-in-debug serializer because valid NaN values cannot be encoded
  there.
- Preserve the existing canonical operation ordering and reject mismatched
  retries without installing, promoting, rolling back, or otherwise mutating
  state.

First add RED tests that cover at minimum:

1. A same-HLC retry containing the same NaN bit pattern is accepted.
2. A retry changing the NaN payload bits is rejected.
3. A retry changing `+0.0` to `-0.0` (or vice versa) is rejected.
4. A retry changing only `OwnedMap.fields` is rejected.
5. At least one recursive/nested case proves the identity relation is applied
   below the top level.
6. Mismatched requests leave installed revision/state unchanged.

Also address the reviewer’s Minor test gap if it can be done narrowly:

- In `commit_stage_failure_preserves_installed_peer_barrier`, expose a
  `#[cfg(test)]`-only participant owner inspection and assert participant A
  retains the exact expected transaction owner after participant B fails.

Do not add production observability solely for this test.

## Verification

Run focused RED/GREEN tests first. Then run, strictly serial and bounded:

- `cargo test --lib server::transactions::data_site -- --test-threads=1`
- `cargo test --lib server::transactions::occ_tests -- --test-threads=1`
- `cargo test --lib server::transactions::manager -- --test-threads=1`
- `cargo test --lib server::transactions::tests -- --test-threads=1`
- `cargo check --lib`
- scoped rustfmt/checks for every owned Rust file
- `git diff --check`

Do not run server test suites concurrently.

Append a concise `## Review Fixes Round 2` section to
`.superpowers/sdd/task-8-report.md`, including RED/GREEN evidence, exact test
counts, files changed, and any pre-existing full-worktree formatting debt.

Commit only the scoped changes with exact subject:

`fix(mvcc): compare commit retries bit-exactly`

Return the commit SHA and leave the worktree clean.
