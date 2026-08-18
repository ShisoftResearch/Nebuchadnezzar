# Linearizable Slot Placement Refresh Implementation Plan

> **For agentic workers:** REQUIRED SUB-SKILL: Use `superpowers:executing-plans` to implement this plan task-by-task. Keep the regression-test correction as its own first commit.

**Goal:** Prevent a lagging Raft replica's slot snapshot from overwriting newer ownership in Nebuchadnezzar while preserving the existing zero-consensus request path.

**Architecture:** Bifrost will expose the applied log index already returned with each Raft command and add a read-only `all_slots_consistent` command. Slot override caches will track a Raft index per slot: a full snapshot updates only slots not newer than the snapshot, and authoritative owner pushes carry the migration command's index. Neb will load one indexed snapshot per refresh, install it monotonically in the server ring, and fan the same snapshot out to hosted clients.

**Tech Stack:** Rust, Bifrost Raft/state-machine macros, Tarpc RPCs, Nebuchadnezzar migration integration tests. Bifrost and Nebuchadnezzar are path dependencies and are upgraded together.

**Normative design:** `docs/superpowers/specs/2026-08-18-linearizable-slot-placement-refresh-design.md`

---

## Task 1: Make the migration regression report the real failure

**Files:**

- Modify: `src/migration.rs` (`cluster_tests::a_transaction_reads_a_migrated_cell`)

- [x] Replace the conversion from a missing cell to `TxnError::NotRealizable(ReadTooLate)` with an `expect` whose message says the migrated cell must exist.
- [x] Build the test, then run four concurrent ignored copies with the existing query-based refresh. Confirm any reproduction fails immediately as a missing cell instead of retrying 1,000 times.
- [x] Keep the test ignored until the placement fix is green, but do not weaken its post-migration refresh.
- [x] Commit only `src/migration.rs` with `test(migration): report missing migrated cell directly`.

## Task 2: Return applied indices from Bifrost commands

**Files:**

- Modify: `../bifrost/src/raft/mod.rs`
- Modify: `../bifrost/src/state_machine.rs` (or the file containing the generated `SMClient` macro)
- Modify: `../bifrost/src/conshash/slots.rs`

- [x] Add a Raft test that executes a command through an `SMClient`, receives `(result, applied_log_index)`, and proves later commands return strictly greater indices.
- [x] Refactor the internal command response path to retain `ClientCmdResponse::Success.last_log_id`; keep existing `execute` methods source-compatible by discarding the index there.
- [x] Add a generated command-only `execute_with_index`/`execute_command_with_index` method to `SMClient`.
- [x] Add the non-mutating slot command `all_slots_consistent(group)` and typed slot-client helpers that return the command result plus applied index. Add indexed helpers for `complete_slot_migration` and `complete_slot_migrations` so owner pushes can carry their authoritative version.
- [x] Test that `all_slots_consistent` returns stable and migrating states without mutating the state machine.
- [x] Run focused Bifrost Raft and slot-state tests.

## Task 3: Make the Bifrost slot cache monotonic per slot

**Files:**

- Modify: `../bifrost/src/conshash/mod.rs`

- [x] Add failing tests for: an older full snapshot cannot replace a newer snapshot; a snapshot at index N cannot replace a pushed owner at N+1; the same snapshot still updates unrelated older slots; and an older owner push cannot replace a newer push.
- [x] Replace the bare `Option<Vec<u64>>` cache with owners plus per-slot Raft indices and a snapshot watermark.
- [x] Change `set_slot_overrides` to accept an applied index and merge only slots whose stored index is not newer. Change `note_slot_owner` to accept an applied index and reject older per-slot updates.
- [x] Expose the cached owner and index together for `NotSlotOwner` responses.
- [x] Run `cargo fmt`, focused conshash tests, and `cargo test --lib` in Bifrost. Commit the Bifrost changes together after all 213 library tests pass.

## Task 4: Install and propagate indexed placement in Neb

**Files:**

- Modify: `src/slots.rs`
- Modify: `src/server/mod.rs`
- Modify: `src/client/mod.rs`
- Modify: `src/server/cell_rpc.rs`
- Modify: `src/ram/cell.rs`
- Modify: `src/migration.rs`
- Modify additional compile-error call sites only where the Bifrost API requires the index.

- [x] Change the loader to use `all_slots_consistent` and return `(owners, applied_log_index)` without losing the distinction between `Ok(None)` and an error.
- [x] Keep the failed-load branch non-mutating, and add tests proving indexed installs preserve newer per-slot pushes.
- [x] Make `NebServer::refresh_slot_placement` perform one consistent load, install it into the server ring, and fan the same indexed vector to every database client without per-runtime Raft calls.
- [x] Have single and bulk migration completion use the indexed completion helpers. Include the returned applied index in local `note_slot_owner` calls and donor/recipient owner-notification RPCs.
- [x] Carry the cached owner index in `WriteError::NotSlotOwner` so redirect learning is also monotonic; update its producers, consumers, and assertions.
- [x] Adapt tests that deliberately manufacture stale routing without bypassing production monotonicity.
- [x] Run focused unit tests and `cargo check --all-targets` with `SHADERC_LIB_DIR=/usr/lib/x86_64-linux-gnu`.

## Task 5: Prove the regression and document the boundary

**Files:**

- Modify: `src/migration.rs` (remove the regression test's `#[ignore]` after it is stable)
- Modify: `docs/superpowers/specs/2026-08-18-linearizable-slot-placement-refresh-design.md`

- [x] Run four concurrent copies of `a_transaction_reads_a_migrated_cell` repeatedly and run the unmigrated control; require all to pass.
- [x] Update the design to specify the returned applied index, per-slot monotonic install rule, versioned owner pushes, and single-snapshot fan-out.
- [x] State explicitly that this fix does not address the deterministic ranged-index/sidecar failure and does not make real-store migration safe.
- [x] Run final gates:
  - `SHADERC_LIB_DIR=/usr/lib/x86_64-linux-gnu cargo test --lib` in Nebuchadnezzar (baseline: 672 passed, 0 failed, 30 ignored before unignoring this test).
  - `SHADERC_LIB_DIR=/usr/lib/x86_64-linux-gnu cargo check --all-targets` in Nebuchadnezzar.
  - `cargo test --lib` in Bifrost (baseline: 213 passed, 0 failed).
  - The relevant Morpheus tests/full suite if time permits; compare against 2955 passed / 1 known failpoint flake and verify that flake in isolation.
- [x] Inspect both diffs and ensure neither `.superpowers/` nor `Lightning/lightning-ppopp27/` is staged.
- [x] Commit Neb code/docs separately from the already-isolated regression-test correction.
