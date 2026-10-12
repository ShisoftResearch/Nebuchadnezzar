# Startup reconcile and clean shutdown

## Decision

**Read:** Keep the full reconcile after clean shutdown. A successful flush does not prove that the index agrees with the cells.

**Read:** Commit `efa94c77` recorded 721 missing entries among 3,139,279 skipped cells. The previous reconcile reported complete repairs. Index task failures and abandoned pages did not increase. The write-back barrier succeeded. Commit `2a9cf143` removed that marker.

**Hypothesis:** An all-or-nothing marker can hide the same holes. A durable marker changes when the claim applies. It does not strengthen the claim.

## Counterexample

**Read:** `IndexBuilder::with_request_index_scope` performs the cell operation before its returned future executes index tasks. Request tasks do not enter `PENDING_INDEX_TASKS` or `INDEX_TASKS_INFLIGHT`.

**Read:** `NebServer::shutdown` closes client writes and drains the global task list. It does not drain request scopes. `Chunks::shutdown_refuses` checks admission before the mutation. Closing admission does not wait for admitted operations.

**Observed:** `clean_shutdown_does_not_certify_request_scoped_index_work` reproduces the following interleaving through the production request-scope API. The isolated test passed. Steps 6 and 8 describe the proposed marker, which is not implemented:

1. Complete the startup reconcile with no incomplete verdict.
2. Admit a request and persist its data cell.
3. Suspend its request scope before the hash insert.
4. Close client admission and drain the global index tasks.
5. Flush all existing index pages, checkpoints, and buckets successfully.
6. Archive the cell and write the proposed marker last.
7. Stop the process before the pending request inserts its hash member.
8. Consume the marker and skip the next reconcile.

**Observed:** After graceful shutdown, the recovered cell retains `sku=42`, but its bucket is absent. The global index debt counter does not increase. A later startup with reconciliation restores the missing member.

**Hypothesis:** Writing and consuming the proposed marker would preserve this hole. The marker would certify an incomplete index.

## Crash, restore, and cluster conditions

**Hypothesis:** Durable removal before startup writes prevents reuse after a skipped start. Atomic publication after all persistence prevents a partial shutdown from creating a new marker. These rules do not repair the counterexample above. A crash after publication can preserve its false claim.

**Read:** Shutdown also logs, but does not return, errors from WAL sync and segment archives. A marker needs explicit successful results from all persistence barriers.

**Hypothesis:** Copying an internally consistent clean store preserves its truth. A mixed restore can combine a marker with older index files. A store identity alone cannot detect that combination. A manifest binding every persisted component, or explicit restore invalidation, is required.

**Read:** Destructive repair requires confirmed stable-singleton placement. Cluster repair reports incomplete work. Stable ownership can precede donor carry-over. A local marker cannot certify another member's index writes, an offline member, or migration in flight.

**Hypothesis:** A future marker needs request quiescence, complete error accounting, a persistent identity for the exact store image, and a cluster-wide consistency boundary. A shutdown reconcile could establish agreement after quiescence, but moves the full scan to shutdown.

**Read:** This change adds no marker format. It adds no version decoder, compatibility path, or migration. If a future marker is added, its version mismatch must produce a typed error. Present malformed records must stop startup under `AGENTS.md` section 10.1.

## Measured optimization

**Read:** The hash pass now runs at most 4 segment workers. All prune workers finish before any fill worker starts. Each worker releases cell guards before an RPC. The authority checks, deletion recheck, inspected-segment CAS, derivation, and walk cap remain unchanged.

**Observed:** Genoa release measurements use 1.2 million initial cells, 4 A/A pairs, and 8 interleaved pairs against `83e3cb71` for each stop type.

| Stop | Baseline startup | Candidate startup | Median paired change |
|---|---:|---:|---:|
| Clean | 21.04 s | 17.90 s | -15.04 % |
| SIGKILL | 21.555 s | 18.215 s | -15.91 % |

**Observed:** Every startup pair improved. The clean paired range is -20.43 % to -13.93 %. The SIGKILL range is -19.96 % to -14.25 %. Separate hash verification fell from 8.09 s to 3.88 s after clean shutdown, and from 8.14 s to 3.99 s after SIGKILL.

**Observed:** A separate whole-machine gate used 4 A/A pairs and 20 write pairs on the same 48 CPUs. Insert, update, delete, shutdown, and write-phase RSS show no regression under the protocol's sign rule. Shutdown has a +6.39 % paired median with a -23.19 % to +35.84 % range. A/A shutdown differences are +5.63 % to +17.77 %. These measurements cannot resolve a general 3 % effect.

**Observed:** The first 8-pair loader RSS result has a +3.26 % regression flag. That peak includes verification. The 20-pair gate reports +0.58 % for write-phase RSS and +1.36 % for the whole loader, both classified as no change. The initial RSS flag does not reproduce in that gate.

**Read:** Parallel work retains up to 4 batches instead of 1. Repeated key derivation and repeated long-bucket walks remain. No write-path function changes.

**Observed:** All 34 scrub tests pass. All 24 compiled mutants are rejected, with 26 assertion failures and 0 timeouts. A 10-cycle campaign includes 5 SIGKILL recoveries and checks up to 1,114,124 hash memberships. Every recovery scrub reports 0 missing, 0 stale, and 0 incomplete entries.

**Observed:** Full logs, scripts, source hashes, sample statistics, and suite audits are in `/mnt/ext/morpheus-fixes/indexperf/`.
