use super::*;

fn review_options(dir: &std::path::Path, tiered: bool) -> ServerOptions {
    ServerOptions {
        chunk_size: 16 * SEGMENT_SIZE,
        db_size: 16 * SEGMENT_SIZE,
        tiered_config: tiered.then(|| crate::ram::tiered::TieredConfig {
            threshold: 0.9,
            lower_watermark: 0.8,
            physical_memory_limit: 16 * SEGMENT_SIZE,
            promotion_cooldown_ms: 0,
        }),
        backup_storage: Some(dir.join("backup").to_string_lossy().into_owned()),
        wal_storage: Some(dir.join("wal").to_string_lossy().into_owned()),
        raft_storage: Some(dir.join("raft").to_string_lossy().into_owned()),
        index_enabled: false,
        services: vec![NebService::Cell, NebService::Transaction],
        enable_recovery: true,
        disable_storage_locks: true,
    }
}

/// One address per store: a restart of the same raft storage reuses it (a store
/// reopened under another address is the explicit restore, not a restart). The
/// address is kept in a file of the store, because a killed child process and
/// the test that restarts its store do not share memory.
fn review_address(opts: &ServerOptions) -> String {
    let raft = std::path::PathBuf::from(opts.raft_storage.clone().unwrap_or_default());
    let root = raft.parent().map(|p| p.to_path_buf()).unwrap_or_default();
    let file = root.join("review-address.txt");
    if let Ok(address) = std::fs::read_to_string(&file) {
        return address;
    }
    let address = crate::utils::test_port::unique_localhost_addr();
    let _ = std::fs::create_dir_all(&root);
    std::fs::write(&file, &address).unwrap();
    address
}

async fn review_server(opts: &ServerOptions, group: &str) -> Arc<NebServer> {
    let server = NebServer::new_from_opts(
        opts,
        &review_address(opts),
        group,
        async |_| {},
    )
    .await
    .unwrap();
    server.cleaner().pause();
    server
}

async fn review_apply_update(
    manager: &Arc<DataManager>,
    schema: &Schema,
    tid: &TxnId,
    id: Id,
    version: u64,
) {
    assert_eq!(
        prepare_ops_local(
            manager,
            0,
            tid,
            vec![PrepareOp {
                id,
                expectation: CellExpectation::Present(version),
                intent: PrepareIntent::Write
            }]
        )
        .await,
        DMPrepareResult::Success
    );
    assert_eq!(
        commit_ops_local(
            manager,
            tid,
            vec![CommitOp::Update(counter_cell(
                schema.vid.get(),
                id,
                50,
                "applied"
            ))]
        )
        .await,
        DMCommitResult::Success
    );
}

#[tokio::test(flavor = "multi_thread")]
async fn old_segment_eviction_is_refused_while_a_transaction_is_applied() {
    let dir = tempfile::TempDir::new().unwrap();
    let server = review_server(&review_options(dir.path(), true), "review_eviction").await;
    let runtime = server.current_database();
    let schema = install_prepare_test_schema(&runtime);
    let manager = DataManager::new(runtime.clone(), server.hlc.clone());
    let id = Id::allocated(0, 0, 9291);
    let version = seed_cell_version(&runtime, schema.vid.get(), id, 5, 0);
    let previous = runtime.chunks().address_of(&id);
    let source = segment_holding(&runtime, &id);
    let tid = server.hlc.now();
    review_apply_update(&manager, &schema, &tid, id, version).await;
    let chunk = &runtime.chunks().list[0];
    let error = crate::ram::tiered::eviction::evict_segment(&source, chunk).unwrap_err();
    assert!(error.to_string().contains("active references"));
    assert!(source.references_count() > 0);
    abort_and_end_local(&manager, &tid).await;
    assert_eq!(runtime.chunks().address_of(&id), previous);
    assert_eq!(runtime.chunks().head_cell(&id).unwrap().version, version);
    assert_eq!(source.references_count(), 0);
    chunk.release_append_heads_for_test();
    crate::ram::tiered::eviction::evict_segment(&source, chunk).unwrap();
    assert!(source.is_cold());
    assert_eq!(
        runtime.chunks().read_cell(&id).unwrap().data["score"]
            .u64()
            .copied(),
        Some(5)
    );
    server.shutdown().await;
}

#[tokio::test(flavor = "multi_thread")]
async fn occ_reader_validates_after_a_newer_writer_aborts() {
    let server = start_lease_test_server(
        &crate::utils::test_port::unique_localhost_addr(),
        "review_occ_reader",
    )
    .await;
    let runtime = server.current_database();
    let schema = install_prepare_test_schema(&runtime);
    let manager = DataManager::new(runtime.clone(), server.hlc.clone());
    let id = Id::allocated(0, 0, 9292);
    let version = seed_cell_version(&runtime, schema.vid.get(), id, 5, 0);
    let reader = server.hlc.now();
    let read = <DataManager as Service>::read(&manager, 0, reader, reader, id)
        .await
        .payload;
    assert!(matches!(read, TxnExecResult::Accepted(ref cell) if cell.header.version == version));
    let writer = server.hlc.now();
    assert!(writer > reader);
    review_apply_update(&manager, &schema, &writer, id, version).await;
    abort_and_end_local(&manager, &writer).await;
    assert_eq!(
        prepare_ops_local(
            &manager,
            0,
            &reader,
            vec![PrepareOp {
                id,
                expectation: CellExpectation::Present(version),
                intent: PrepareIntent::Read
            }]
        )
        .await,
        DMPrepareResult::Success
    );
    assert_eq!(
        commit_ops_local(&manager, &reader, vec![CommitOp::Read(id, version)]).await,
        DMCommitResult::Success
    );
    assert_eq!(
        <DataManager as Service>::end(&manager, reader, reader, false)
            .await
            .payload,
        EndResult::Success
    );
    server.shutdown().await;
}

#[tokio::test(flavor = "multi_thread")]
async fn pinned_read_of_the_superseded_version_survives_commit_and_a_full_clean() {
    let server = start_lease_test_server(
        &crate::utils::test_port::unique_localhost_addr(),
        "review_pinned_commit",
    )
    .await;
    server.cleaner().pause();
    let runtime = server.current_database();
    let schema = install_prepare_test_schema(&runtime);
    let manager = DataManager::new(runtime.clone(), server.hlc.clone());
    let id = Id::allocated(0, 0, 9293);
    let version = seed_cell_version(&runtime, schema.vid.get(), id, 5, 0);
    let source = segment_holding(&runtime, &id);
    let previous = runtime.chunks().address_of(&id);
    let reader = server.hlc.now();
    assert!(
        matches!(<DataManager as Service>::head(&manager, 0, reader, reader, id, true).await.payload, TxnExecResult::Accepted(ref header) if header.version == version)
    );
    let writer = server.hlc.now();
    review_apply_update(&manager, &schema, &writer, id, version).await;
    assert_eq!(
        <DataManager as Service>::end(&manager, writer, writer, false)
            .await
            .payload,
        EndResult::Success
    );
    let chunk = &runtime.chunks().list[0];
    for slot in chunk.head_pool.iter() {
        let _ = slot.compare_exchange(
            source.id,
            u64::MAX,
            std::sync::atomic::Ordering::AcqRel,
            std::sync::atomic::Ordering::Acquire,
        );
    }
    assert!(source.references_count() > 0);
    assert!(
        source.is_dead_at(previous),
        "commit retires the pinned old version"
    );
    crate::ram::cleaner::Cleaner::clean(chunk, true, true);
    assert!(chunk.segs.get(&(source.id as usize)).is_some());
    let read = <DataManager as Service>::read(&manager, 0, reader, reader, id)
        .await
        .payload;
    assert!(
        matches!(read, TxnExecResult::Accepted(ref cell) if cell.header.version == version && cell.data["score"].u64().copied() == Some(5))
    );
    assert_eq!(
        runtime.chunks().read_cell(&id).unwrap().data["score"]
            .u64()
            .copied(),
        Some(50)
    );
    abort_and_end_local(&manager, &reader).await;
    server.shutdown().await;
}

#[tokio::test(flavor = "multi_thread")]
async fn rollback_failure_on_one_cell_retires_heads_and_recovery_discards_the_bracket() {
    let dir = tempfile::TempDir::new().unwrap();
    let opts = review_options(dir.path(), false);
    let server = review_server(&opts, "review_failed_restore").await;
    let runtime = server.current_database();
    let schema = install_prepare_test_schema(&runtime);
    let manager = DataManager::new(runtime.clone(), server.hlc.clone());
    let ids = [Id::allocated(0, 0, 9294), Id::allocated(0, 0, 9295)];
    let versions = ids.map(|id| seed_cell_version(&runtime, schema.vid.get(), id, 5, 0));
    let tid = server.hlc.now();
    assert_eq!(
        prepare_ops_local(
            &manager,
            0,
            &tid,
            ids.iter()
                .zip(versions)
                .map(|(id, version)| PrepareOp {
                    id: *id,
                    expectation: CellExpectation::Present(version),
                    intent: PrepareIntent::Write
                })
                .collect()
        )
        .await,
        DMPrepareResult::Success
    );
    assert_eq!(
        commit_ops_local(
            &manager,
            &tid,
            ids.iter()
                .map(|id| CommitOp::Update(counter_cell(schema.vid.get(), *id, 50, "new")))
                .collect()
        )
        .await,
        DMCommitResult::Success
    );
    let heads: Vec<_> = runtime.chunks().list[0]
        .segments()
        .into_iter()
        .filter(|seg| seg.bracket_is_open())
        .collect();
    assert!(!heads.is_empty());
    // Inject one validation failure without changing any persisted bytes.
    manager
        .txns
        .get(&tid)
        .unwrap()
        .lock()
        .history
        .get_mut(&ids[0])
        .unwrap()
        .current_version += 1;
    let abort = <DataManager as Service>::abort(&manager, tid, tid)
        .await
        .payload;
    assert_eq!(
        abort,
        AbortResult::Success(Some(vec![RollbackFailure {
            id: ids[0],
            error: WriteError::CellVersionMismatch
        }]))
    );
    assert_eq!(
        crate::ram::chunk::transaction_lease_count(&tid, &runtime.chunks().address_range()),
        0
    );
    for head in heads {
        let path = runtime.chunks().list[0]
            .file_manager
            .wal_path(head.chunk_id, head.id, head.seq_id)
            .unwrap();
        let wal = std::fs::read(path).unwrap();
        let replay = crate::ram::wal_format::scan_framed(&wal, SEGMENT_SIZE).unwrap();
        let mut offset = 0;
        let mut begins = 0;
        let mut commits = 0;
        while offset < replay.used_len {
            let word = u32::from_le_bytes(replay.image[offset..offset + 4].try_into().unwrap());
            let kind = match crate::ram::entry::unpack_type_word(word) {
                crate::ram::entry::TypeWord::Bare(kind)
                | crate::ram::entry::TypeWord::Checked(kind, _) => kind,
                crate::ram::entry::TypeWord::Invalid => panic!("invalid replayed entry header"),
            };
            let len = u32::from_le_bytes(replay.image[offset + 4..offset + 8].try_into().unwrap())
                as usize;
            begins += usize::from(kind == crate::ram::entry::EntryType::BEGIN);
            commits += usize::from(kind == crate::ram::entry::EntryType::COMMIT);
            offset += crate::ram::entry::ENTRY_HEAD_SIZE + len;
        }
        assert_eq!(begins, 1, "failure must retain the journaled BEGIN");
        assert_eq!(commits, 0, "failure must not persist a commit decision");
        assert!(
            !head.can_still_append(),
            "a retired head must refuse later appends"
        );
    }
    assert_eq!(
        <DataManager as Service>::end(&manager, tid, tid, false)
            .await
            .payload,
        EndResult::Success
    );
    runtime.chunks().sync_all().unwrap();
    server.shutdown().await;
    let recovered = review_server(&opts, "review_failed_restore").await;
    recovered.meta().schemas.register_internal_schema(schema);
    for (id, version) in ids.into_iter().zip(versions) {
        let cell = recovered.chunks().read_cell(&id).unwrap().to_owned();
        assert_eq!(cell.header.version, version);
        assert_eq!(cell.data["score"].u64().copied(), Some(5));
    }
    recovered.shutdown().await;
}

#[tokio::test(flavor = "multi_thread")]
async fn applied_undecided_delete_killed_before_end_recovers_the_old_cell() {
    const CHILD: &str = "NEB_REVIEW_UNDECIDED_DELETE_CHILD";
    let id = Id::allocated(0, 0, 9296);
    if let Some(path) = std::env::var_os(CHILD) {
        let path = std::path::PathBuf::from(path);
        let server = review_server(&review_options(&path, false), "review_killed_delete").await;
        let runtime = server.current_database();
        let schema = install_prepare_test_schema(&runtime);
        let manager = DataManager::new(runtime.clone(), server.hlc.clone());
        let version = seed_cell_version(&runtime, schema.vid.get(), id, 5, 0);
        runtime.chunks().sync_all().unwrap();
        let tid = server.hlc.now();
        assert_eq!(
            prepare_ops_local(
                &manager,
                0,
                &tid,
                vec![PrepareOp {
                    id,
                    expectation: CellExpectation::Present(version),
                    intent: PrepareIntent::Write
                }]
            )
            .await,
            DMPrepareResult::Success
        );
        assert_eq!(
            commit_ops_local(&manager, &tid, vec![CommitOp::Remove(id)]).await,
            DMCommitResult::Success
        );
        assert!(runtime.chunks().read_cell(&id).is_err());
        runtime.chunks().sync_all().unwrap();
        std::fs::write(path.join("applied"), version.to_string()).unwrap();
        loop {
            std::thread::park();
        }
    }
    let dir = tempfile::TempDir::new().unwrap();
    let output = std::fs::File::create(dir.path().join("child.log")).unwrap();
    let mut child = std::process::Command::new(std::env::current_exe().unwrap())
        .arg("applied_undecided_delete_killed_before_end_recovers_the_old_cell")
        .arg("--nocapture")
        .env(CHILD, dir.path())
        .stdout(output.try_clone().unwrap())
        .stderr(output)
        .spawn()
        .unwrap();
    let deadline = std::time::Instant::now() + std::time::Duration::from_secs(30);
    while !dir.path().join("applied").exists() && std::time::Instant::now() < deadline {
        if child.try_wait().unwrap().is_some() {
            break;
        }
        std::thread::sleep(std::time::Duration::from_millis(10));
    }
    // Kill only the child created by this test. No shutdown or end can write a decision.
    let ready = dir.path().join("applied").exists();
    let _ = child.kill();
    let status = child.wait().unwrap();
    assert!(
        ready,
        "child failed before apply: {}",
        std::fs::read_to_string(dir.path().join("child.log")).unwrap()
    );
    assert!(!status.success());
    let version: u64 = std::fs::read_to_string(dir.path().join("applied"))
        .unwrap()
        .parse()
        .unwrap();
    let opts = review_options(dir.path(), false);
    let server = review_server(&opts, "review_killed_delete").await;
    let runtime = server.current_database();
    let schema = install_prepare_test_schema(&runtime);
    let old = runtime.chunks().read_cell(&id).unwrap().to_owned();
    assert_eq!(old.header.version, version);
    assert_eq!(old.data["score"].u64().copied(), Some(5));
    let floor = runtime.chunks().list[0].deleted_version_for_test(id.bits());
    assert_eq!(
        floor, version,
        "recovery must retain the physical tombstone's floor"
    );
    runtime.chunks().remove_cell(&id).unwrap();
    let mut recreated = counter_cell(schema.vid.get(), id, 15, "recreated");
    let header = runtime.chunks().write_cell(&mut recreated).unwrap();
    assert!(header.version > floor);
    assert_eq!(
        runtime.chunks().read_cell(&id).unwrap().data["score"]
            .u64()
            .copied(),
        Some(15)
    );
    server.shutdown().await;
}
