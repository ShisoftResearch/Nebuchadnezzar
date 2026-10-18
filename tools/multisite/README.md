# Multi-process correctness gate

Read: this harness uses a Neb example plus a Python controller. The example uses the same dependency graph as Neb.
The controller owns the processes, port reservation, forwarding proxies, and persistent operation ledger.
This keeps test dependencies outside production binaries and makes source heads explicit.

Build with `RUSTFLAGS='--cfg disable_shortcut --cfg multisite_test'`.
Use `CARGO_TARGET_DIR=/mnt/optane/cargo-target/multisite`.
Run Cargo under `flock /mnt/ext/morpheus-fixes/cargo.lock`.
Extract the executable from Cargo JSON. Preserve the JSON if extraction fails.
Copy the executable to an immutable path before a run.

Set `MULTISITE_BINARY`, `MULTISITE_HEAD`, and `MULTISITE_OUTPUT`.
Set `TEST_PREBUILT_BINARY` to the absolute path of `isolated.py`.
Require both executables to exist before the isolated runner starts.
Run `./run_tests_isolated.sh multisite_smoke_3` inside a unique user systemd scope with `MemoryMax=24G`.
The adapter never compiles.

`run.py --help` lists the configuration. `--head` records the tested source receipt.
It does not check out or modify another head. Build that head in a sibling workspace and pass its immutable executable.
Record all 4 source hashes in the receipt string. This also applies to `feat/onepc` and future commit protocols.

Every member owns its `store-N/{backup,wal,raft}` directories. Restarts retain these directories and advertised addresses.
`pids.jsonl` records process starts, signals, and exits. The controller signals only subprocess handles that it owns.
Stores and failed logs stay in the output directory. Output directories cannot be reused.

The proxy reads the 4-byte big-endian length and the entire frame before forwarding or dropping it.
The route map belongs to each source process. Each directed member pair has its own listener.
A dropped frame is a lost application message. It is not packet corruption or an operating system network fault.
A cut blackholes complete frames until restore. Existing connections remain open.

Test hooks write protocol events to `MULTISITE_EVENTS`.
Create `MULTISITE_CONTROL/arm` with a point name to pause the first matching event.
The hook claims the file with a rename. Create `MULTISITE_CONTROL/resume` to release it.
Points include `before_p0`, `after_p0`, `during_end`, `before_status_record`, `after_status_record`,
`coordinator_before_end`, `coordinator_after_end`, `before_status_release`, and `during_slot_transfer`.
Read: D31 writes the status record at the participant, not the coordinator.
The coordinator points surround end fanout. The participant points surround the actual status write.

Set `MULTISITE_CLOCK_OFFSET_MS` for a process-local HLC wall offset in a test build.
This does not modify the host clock.

The current baseline schedule checks acknowledged values after each stop or link event.
It is not the complete D19/F1/F4/D33 campaign. The report records the remaining qualification work.
