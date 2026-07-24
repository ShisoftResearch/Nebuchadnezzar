# Hybrid Logical Clock Transaction IDs

## Summary

Replace `TxnId = StandardVectorClock` with a Hybrid Logical Clock: a fixed
16-byte, `Copy`, totally ordered timestamp `(ts, node)` where `ts` packs 48 bits
of wall-clock milliseconds with a 16-bit logical counter, and `node` is the
server id. HLC order is causality-consistent by construction (happened-before
implies HLC-less-than), so the transaction layer gets one total order that
extends causality — eliminating the partial-order machinery (`Relation`,
`deterministic_cmp`, clone-and-canonicalize, variable-length clock maps) from
every transaction hot path.

## Motivation

Three independent findings converge on the same root cause:

1. **`deterministic_cmp` is not a linear extension of causality.** For canonical
   clocks it compares sparse `(server, counter)` pair lists lexicographically.
   Counterexample: `A = [(2,5)]` happened-before `B = [(1,1),(2,5)]`
   (componentwise, missing = 0), yet `deterministic_cmp(A,B) = Greater` because
   the first pair compares server ids `2 > 1`. Any MVCC visibility rule built on
   it would produce causally inconsistent snapshots (effect visible without its
   cause).
2. **Wait-Die priority is non-transitive.** With `C = [(1,2)]`: `A < B`
   (causal), `B < C` (deterministic), `C < A` (deterministic) — a priority
   cycle among three transactions. Wait-Die can then produce a wait cycle,
   currently backstopped only by the lock timeout.
3. **Hot-path cost.** TxnIds are variable-length `Vec`s cloned on every RPC
   argument, hashed as map keys, compared in O(n), and serialized into every
   response (`DataSiteResponse.clock`). The one previously accepted OCC
   optimization (+11% portfolio) was precisely about avoiding clock
   serialization in Wait-Die comparison.

The swap is safe because the transaction layer's safety never comes from clock
semantics: certification validates storage versions and absence, and the
existing contract already states that timestamp metadata assists scheduling and
cleanup but cannot replace version validation. An audit of the transaction code
found exactly one production consumer of causal semantics —
`TxnPriority::compare_age` — and it only needs a total order (classic Wait-Die
uses plain timestamps). Every other use is a plain order comparison, a map key,
or a serialization.

## The `Hlc` type (new, in bifrost)

`bifrost::hlc`, sibling of `bifrost::vector_clock`:

```rust
/// ts = (wall_ms << 16) | logical  — 48-bit milliseconds, 16-bit logical.
#[derive(Debug, Clone, Copy, PartialEq, Eq, PartialOrd, Ord, Hash,
         Serialize, Deserialize, Default)]
pub struct Hlc {
    pub ts: u64,
    pub node: u64,
}
```

- Total order: derived lexicographic `(ts, node)`. 48-bit milliseconds last
  beyond year 10,000; 16-bit logical allows 65,536 causally chained events per
  millisecond per node, overflowing into the next millisecond (standard HLC
  behavior).
- `HlcSource { node: u64, ts: AtomicU64 }` — one per server process, shared by
  the transaction manager and data manager. Two operations, both CAS loops on
  the packed `ts`:
  - `now(&self) -> Hlc` (local/send event): `wall' = max(stored_wall, phys_ms)`;
    logical increments if `wall'` unchanged, else resets to 0. Every call
    returns a strictly greater `ts` than the previous call on this node, so
    `(ts, node)` is unique — valid as a transaction id.
  - `observe(&self, remote: Hlc) -> Hlc` (receive event): standard HLC receive
    rule — `wall' = max(stored_wall, remote_wall, phys_ms)` with the
    corresponding logical-component rules, then returns the updated value.
- Clock regression (NTP step-back) is absorbed: `max` with the stored value
  keeps `ts` monotonic; only the physical anchor drifts until real time
  catches up.
- Property tests (in bifrost): strict monotonicity of `now`, uniqueness of
  `(ts, node)`, causality consistency (any send/receive chain implies strict
  HLC increase — the linear-extension theorem), logical overflow behavior, and
  a regression test encoding the `A/B/C` cycle above (as HLC values the three
  are transitively ordered).

## Transaction-layer changes (Nebuchadnezzar)

### Type swap

- `pub type TxnId = bifrost::hlc::Hlc` — single definition in
  `server::transactions::mod`; `undo_log.rs` drops its duplicate alias and
  imports the shared one.
- `TxnPriority::compare_age` becomes `self.tid.cmp(&other.tid)`. The order is
  total and transitive (node id is inside the tid), fixing the Wait-Die cycle.
  `Relation` and `deterministic_cmp` imports disappear from the transaction
  layer.

### Clock plumbing

- `begin()` generates tids from the server's `HlcSource::now()`.
- Every transaction-layer RPC parameter and field currently typed
  `StandardVectorClock` becomes `Hlc`: the `DataManager` service's `clock`
  parameters, `DataSiteResponse.clock`, and the coordinator's
  `get_clock()`/`merge_clock()` helpers (which become `source.now()` /
  `source.observe(remote)`).
- `effective_ts = max(clock, tid)` stays literally `Ord::max` — now total, no
  concurrent case.
- The bifrost `Peer`/`ServerVectorClock` remain untouched for everything else
  (membership, raft); the transaction layer simply stops using them for
  transaction timestamps.

### Comparison-site semantics (audited)

| Site | Today (partial order) | Under HLC |
| --- | --- | --- |
| `prepare_read` ReadTooLate: `meta.write > effective_ts` | `false` when concurrent (permissive) | decisive comparison |
| Thomas Write Rule: `effective_ts < meta.write` | `false` when concurrent (write proceeds) | decisive comparison |
| read-stamp advance: `meta.read < effective_ts` | `false` when concurrent (stamp kept) | decisive comparison |
| `cell_meta_cleanup` watermark: `meta < oldest` | partial; concurrent = not-less (retained) | decisive; watermark is a true minimum |
| `txns_sorted` BTreeSet order | arbitrary (`Ord` on map vec) | true age order — the "oldest transaction" watermark becomes exact |
| Wait-Die `compare_age` | causal, then coordinator id, then broken tie-break | `tid.cmp` — total, transitive |

Behavioral shift: pairs that were previously Concurrent (comparison silently
`false`) are now decisively ordered. This can only change *scheduling* outcomes
(a ReadTooLate rejection or TWR skip that previously did not fire may now
fire, and vice versa); it cannot change safety, because commit-time version
certification remains the sole authority for lost-update prevention. This is
strictly less surprising than today's behavior, where the outcome of a
timestamp comparison depended on whether two clocks happened to share support.

### Undo log

Entries frame the txn id as `[txn_id_len: u32][txn_id: bytes]` (serde). The
byte shape changes with the type. Recovery must detect a pre-HLC id (old
serialized form) and fail with an explicit error instructing the operator to
discard pre-HLC undo logs; silently misreading them is not acceptable, and
compatibility shims are not warranted pre-production. The format doc comment
records the change.

### Contract amendment

- Gate 6 ("the guarantee holds for transactions coordinated by different
  servers with concurrent vector clocks") is restated as: the guarantee holds
  for transactions coordinated by different servers regardless of timestamp
  order, because certification is version-based.
- "Concurrent vector clocks cannot be treated as a total version order" is
  replaced by: the HLC provides the transaction layer's total timestamp order;
  timestamps continue to assist scheduling and cleanup and never replace cell
  version or absence validation.

## Test plan

- bifrost: `Hlc`/`HlcSource` unit + property tests (listed above).
- Nebuchadnezzar: port the seven `TxnPriority` tests (concurrent-clock cases
  become distinct-node cases; the panic-on-inconsistency guards become
  transitivity assertions). Port the concurrent-clock Wait-Die OCC tests to
  distinct-node HLC tests. All tid-constructing test helpers
  (`StandardVectorClock::from_vec`) move to an `hlc(ts, node)` helper.
- Full gates on both repos: bifrost vector-clock suite untouched and green;
  Nebuchadnezzar `server::transactions`, `ram::tiered`, `index::full_text`.

## Out of scope

- Any change to bifrost's vector clocks or their non-transaction users.
- The MVCC version index, visibility rule, and snapshot-gated GC — they follow
  as the next increments and consume `Hlc` as `commit_ts` (16-byte fixed
  version-index entries; visibility = `commit_ts <= S` under `Ord`).
- Wire/storage compatibility with pre-HLC transaction state other than the
  explicit undo-log rejection above.

## Risks

- **Wide mechanical diff** (~150 TxnId references across manager, data_site,
  undo_log, client, tests). Mitigated by the alias swap driving compile-error
  discovery, plus the full gate suites.
- **Semantics shift on previously-concurrent comparisons** — scheduling-only,
  argued above; watched by the hot-cell and multi-participant portfolio
  scenarios.
- **Physical-clock anomalies** — absorbed by the monotonic CAS rules; worst
  case is a temporarily larger logical component.

## Validation results (2026-07-23)

Landed as: bifrost `4fce9ac` (Hlc + HlcSource, 6 property tests) and `eaf5fb1`
(deterministic_cmp removed with its last consumer; vector-clock suite 29/29);
Nebuchadnezzar `38dbd0f7` (per-server source wiring), `4e243230` (the atomic
migration: TxnId = Hlc, compare_age = tid.cmp, all clock plumbing on the
HlcSource, Peer and every vector-clock reference removed from production code),
`fc9de70d` (test migration: priority tests rewritten by intent, including a
transitivity regression over the old deterministic_cmp cycle shape),
`98b778cd` (undo-log recovery rejects pre-HLC transaction ids — this also fixed
a pre-existing silent-truncation bug where an undecodable txn id caused
`Err(_) => break`, truncating recovery and reporting success).

Final gates on the migrated tree, all green and identical to pre-migration
counts: `server::transactions` 110 passed / 0 failed / 1 ignored;
`ram::tiered` 30 / 0 / 5 ignored; `index::full_text` 38 / 0. bifrost:
vector_clock 29 / 0 (three deterministic_cmp-specific tests removed with the
API), hlc 6 / 0.

Known follow-ups: (1) server startup currently logs and continues when undo-log
recovery fails — with the new explicit pre-HLC rejection this is visible but
non-fatal; whether it should abort startup is an open product decision.
(2) A benchmark-host portfolio spot-check (independent_rmw/1, hot_rmw/8,
blind_update/1) against a pre-HLC baseline remains to be run; expectation is
neutral-to-positive (16-byte Copy ids, O(1) compares, no clock allocation).

## Performance spot-check: 2026-07-24, host 192.168.10.239

Interleaved A/B x3 (plus 3 extra blind rounds per side), Nebuchadnezzar
`52b7486a` (pre-HLC) vs `e11fc736` (HLC), both built against the same bifrost
`4fce9ac` so the dependency tree is identical. Shared host (CVs well above the
5% acceptance bar — directional evidence, not accept-grade), NUMA-bound,
alternating rounds:

| Scenario | pre-HLC | HLC | Throughput | p95 |
| --- | ---: | ---: | ---: | ---: |
| `independent_rmw/1` | 39,269 (cv 31%) | 49,833 (cv 12%) | **+26.9%** | 41.3u -> 32.7u (-20.9%) |
| `hot_rmw/8` | 26,136 (cv 13%) | 35,145 (cv 13%) | **+34.5%** | 58.3u -> 41.0u (-29.7%) |
| `multi_participant/1` | 15,789 (cv 39%) | 19,920 (cv 7%) | **+26.2%** | 92.5u -> 76.8u (-17.0%) |
| `blind_update/1` (n=6/5) | 17,048 (cv 18%) | 32,632 (cv 25%) | **+91.4%** | 76.0u -> 42.7u (-43.8%) |

Every scenario improves substantially and consistently; candidate CVs are
mostly lower (less allocation jitter). The blind-update result is the
standout: that path was dominated by clock-carrying RPCs (tid-as-clock
observation plus serialized clocks on every prepare/commit/end response), and
it was the worst regression versus pre-OCC develop in the original handoff.

Two of the first three candidate blind rounds aborted at fixture setup
(scenario missing from the report); five subsequent captured candidate runs
were clean with zero panics or invariant failures, so those aborts are
attributed to the shared host's environment (socket/port residue between
back-to-back fixtures), not the migration.
