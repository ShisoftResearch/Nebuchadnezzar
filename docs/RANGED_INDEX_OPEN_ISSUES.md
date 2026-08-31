# Ranged index: open issues behind the 2026-08-31 workarounds

Two operational workarounds went in during the BANC bulk-import debugging
night of 2026-08-30/31. Both stand in for real, unresolved defects in the
ranged index under concurrent load. This is the handoff for fixing them
properly. The four fixes that DID land that night (`e2d5dd2c`,
`fd24832b`, `ecf89772`, `92504013`) are prerequisites to understanding
this document — read their commit messages first; each one converted a
silent hang or silent data loss into either correct behaviour or a
bounded, named failure. What remains below is what the bounded failures
still point at.

The operational recipe that works today (and why) is at the end. Do not
"simplify" an import back onto the fragile paths without fixing the
issues here first.

---

## Issue 1: trees go transiently out of order under splits + concurrent readers

**Workaround in place:** `NEB_TREE_DEPTH=4` at server start (capacity
~2.1B keys, so no split ever fires at import scale). Depth is read once
from the env in `btree::tree_depth()`.

**What was observed.** With depth 3 (capacity 4,194,304), five splits
fire during a BANC synapse import (13.6M edges → ~27M scannable+index
keys). During and after those splits, **fresh root-descent seeks return
keys at or before their search key** — not stale-cursor artifacts: the
monotonic-progress guard added in `ecf89772` restarts a regressed scan
by re-seeking from the ROOT past the last yielded key, and those fresh
descents regressed again, 65 times in a row, ~8,700 give-ups in a
38-second window (`server_final2.log`, 2026-08-30 ~21:0x; again in
`server_final4.log` 04:33:38–53). Equal-key replays from LSM level
merges are already tolerated (skipped) since `92504013`'s sibling change
— these were strictly-lesser keys.

**Why the existing stress test misses it.**
`test_concurrent_writes_during_split_remain_scannable` (green 3×) drives
8.4M concurrent INSERTS through splits and then scans — but it has **no
readers concurrent with the splits**. The bulk import does: every insert
spawns an `ensure_scannable` verification seek (`index/builder.rs`), so
splits always run under a storm of range seeks. That is the missing
ingredient.

**What is already ruled out.**
- In-flight writers racing `split_off`: drained via `DistTree::in_flight`
  (`fd24832b`). The observed corruption happened WITH the drain active.
- Lost entries: the failing imports' scans and `contains()` disagreed
  (contains=false) only in the pre-`fd24832b` orphaning cases; in the
  depth-3 regression windows the data eventually scanned complete (the
  final flush block counts matched the healthy 7,394), so this looks
  like **transient structural inconsistency**, not loss.

**Suspects, in rough order.**
1. `split_off`'s structural re-parenting of shared leaves vs OPTIMISTIC
   readers: readers hold `NodeCellRef`s into pages whose parent/sibling
   links are being rewired. The drain stops *counted tree operations*,
   but a reader between two seek RPCs (client cursor) holds page refs
   across the split with no coordination. A fresh ROOT descent should
   still be correct though — so suspect the INTERNAL node separator
   updates during/after `split_off` (a window where internal separators
   disagree with leaf contents routes a descent to the wrong leaf, which
   then yields keys below the search key).
2. Boundary vs physical content: immediately after a split commits, the
   source's `prop.boundary.upper` drops to the pivot while leaf chains
   are still being seam-fixed; the seek closure clamps by boundary but
   walks the physical chain.
3. `LeafKeys` prefix compression (`b601c75e`) interacting with
   mid-split page states — lower probability; would corrupt comparisons
   rather than ordering per se.

**How to reproduce properly (the missing test).** Clone the existing
stress test and add, alongside the insert storm, N tasks that
continuously seek with 16-byte patterns over random already-inserted
prefixes and ASSERT each block is strictly monotonic and contains no key
below the seek key. Run at depth 2 (`btree::set_tree_depth(2)`) so
splits fire early and often. The regression guard can be made to panic
in tests (feature/env flag) instead of restarting, so the test catches
the first inconsistency red-handed with the tree id and keys.

**Acceptance for removing the workaround.**
- New reader+writer stress test green 3× at depth 2.
- A depth-3 BANC import (full 3-table chain, inline rebuilds) completes
  with ZERO `chain restarts`/`gave up` warnings in the server log.
- Then keep the seek guard as a safety net, but page anyone who sees its
  warning again.

---

## Issue 2: sidecar rebuild over a live store fails verification

**Workaround in place:** `connectome_cli ... --skip-sidecar` on every
import, then one `connectome_cli rebuild-sidecar` after the store goes
quiet. Rebuild-from-empty over a quiescent store succeeded 6/6 that
night; rebuild #2/#3 over a live store failed 4/4.

**What was observed.** An `import-edges` (562K junction rows) triggers a
global sidecar rebuild while the junction inserts' 562K async
`ensure_scannable` verification tasks are still draining. The rebuild's
own block-cell writes each spawn verifications too. Under that combined
merge/verify pressure, ~131 of ~20K sidecar-block scannable keys "never
became visible" within `ensure_scannable_insert`'s 32 attempts / 2.4s
(`index/builder.rs:688-737`), the rebuild's family persist fails with
`IndexIncomplete`, and (post the morpheus CLI fix) the import correctly
aborts. At concurrency 8 it still failed. The verification seeks are
range seeks with a 16-byte pattern; under merge storms they return
bounded partial blocks (the `ecf89772`/`92504013` guards), whose first
element is then not the expected id → miss → retry → timeout.

**The probable real fix is small:** `ensure_scannable_insert` should
verify with **`RangedIndexerClient::contains(&key)`** — the exact,
read-only point lookup added for the index scrub — instead of a range
seek + first-element comparison. `contains` does a single-tree exact
probe (`tree.seek(entry).current() == Some(entry)`), does not depend on
scan-window ordering, and is immune to the partial-block degradation.
The seek-based check predates `contains`. Swap it, keep the retry loop,
and the verification becomes insensitive to merge storms. (The scrub
already trusts `contains` for exactly this question.)

Secondary hardening, if needed after the swap:
- Bound the index-task inflight count (builder currently spawns
  unboundedly; 562K concurrent verifications starve the shared tokio
  runtime — HTTP polls timed out repeatedly that night at 950%–2700%
  CPU).
- Let `rebuild_sidecar` refuse to start while the index-task queue is
  above a threshold, or drain it first server-side (the CLI-side
  quiescence gate is doing this job today).

**Acceptance for removing the workaround.**
- `import-edges` at concurrency 64 with an INLINE rebuild green 3× in a
  row on the BANC junction+delay tables.
- Zero `never became visible` lines at any concurrency.
- The `--skip-sidecar`/`rebuild-sidecar` flags can stay — they are good
  operational tools — but the default inline path must be trustworthy.

---

## Also unexplained (lower priority)

- **The cargo-target wipe.** At 2026-08-31 04:33:13 the entire
  `/mnt/optane/cargo-target` tree was deleted mid-import by an unknown
  actor (mount healthy, no fs errors, no tmpfiles/cron/journal trace; it
  emptied while a build had JUST succeeded and the server ran from a
  then-deleted inode). Binaries are insurance-copied to `~/banc-ws/bin/`
  after every build now. If it recurs, `auditctl -w /mnt/optane/cargo-target`
  (needs root) is the tool; a poll watcher existed in-session only.
- **Verification cost.** Even healthy, per-insert `ensure_scannable`
  makes bulk import O(n) extra seeks. The `contains` swap above helps;
  batch verification (one scan per prefix at the end of a batch) would
  help more.

---

## The operational recipe that works today (2026-08-31)

```bash
# server (binaries live OUTSIDE cargo-target on purpose)
NEB_TREE_DEPTH=4 ~/banc-ws/bin/morpheus --config ~/banc-ws/local_banc.yaml
# from the Morpheus repo dir (log4rs config path is CWD-relative)

CLI=~/banc-ws/bin/connectome_cli
$CLI import       --neurons ... --connections ... --annotations ... --database banc --skip-sidecar
$CLI import-edges --connections gap_junctions.csv --schema connectome_gap_junction_innexin --database banc --skip-sidecar
$CLI import-edges --connections delays.csv        --schema connectome_synapse_delay       --database banc --skip-sidecar
# wait for server CPU to go quiet (verification backlog draining), then:
$CLI rebuild-sidecar --database banc
```

Evidence files from the night, all under `~/banc-ws/`: `server_final*.log`
(warn storms, timings), `import*.log` (per-attempt outcomes),
`server_gdb2.log` (the SIGUSR1 all-threads dump that caught Issue-0's
duplicate-skip loop red-handed — the model for how to catch Issue 1).
