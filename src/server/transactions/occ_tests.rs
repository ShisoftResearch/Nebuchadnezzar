use super::*;
use crate::ram::cell::{OwnedCell, ReadError};
use crate::ram::schema::Schema;
use crate::ram::tests::default_fields;
use crate::ram::types::{Id, OwnedMap, OwnedValue};
use crate::server::transactions;
use crate::server::{DatabaseRuntime, NebServer, ServerOptions, Service};
use bifrost_hasher::hash_str;
use dovahkiin::types::{Map, Value};
use std::collections::HashMap;
use std::path::Path;
use std::sync::Arc;
use tempfile::TempDir;
use tokio::time::{timeout, Duration};

async fn scoped_txn_client_for_database(
    address: &str,
    group_name: &str,
    database_name: &str,
) -> Arc<transactions::manager::AsyncServiceClient> {
    transactions::new_async_client_for_database(&address.to_string(), group_name, database_name)
        .await
        .unwrap()
}

async fn scoped_data_site_client_for_database(
    address: &str,
    group_name: &str,
    database_name: &str,
) -> Arc<transactions::data_site::AsyncServiceClient> {
    let client = bifrost::rpc::DEFAULT_CLIENT_POOL
        .get(&address.to_string())
        .await
        .unwrap();
    transactions::data_site::AsyncServiceClient::new_with_service_id(
        transactions::data_site::generate_scoped_service_id(group_name, database_name),
        &client,
    )
}

async fn start_occ_test_server(address: &str, group: &str) -> Arc<NebServer> {
    start_occ_test_server_with_options(address, group, None, 1).await
}

async fn start_occ_test_server_with_options(
    address: &str,
    group: &str,
    undo_log_storage: Option<String>,
    chunk_count: usize,
) -> Arc<NebServer> {
    NebServer::new_from_opts(
        &ServerOptions {
            chunk_size: crate::ram::segs::SEGMENT_SIZE,
            db_size: crate::ram::segs::SEGMENT_SIZE * chunk_count,
            history_retention_ms: 300_000,
            tiered_config: None,
            backup_storage: None,
            wal_storage: None,
            undo_log_storage,
            raft_storage: None,
            index_enabled: false,
            services: vec![Service::Cell, Service::Transaction],
            enable_recovery: false,
            disable_storage_locks: true,
        },
        &address.to_string(),
        &group.to_string(),
        async |_| {},
    )
    .await
    .unwrap()
}

async fn start_durable_occ_test_server(
    address: &str,
    group: &str,
    storage: &TempDir,
) -> Arc<NebServer> {
    NebServer::new_from_opts(
        &ServerOptions {
            chunk_size: crate::ram::segs::SEGMENT_SIZE,
            db_size: crate::ram::segs::SEGMENT_SIZE,
            history_retention_ms: 300_000,
            tiered_config: None,
            backup_storage: Some(storage.path().join("backup").to_string_lossy().into_owned()),
            wal_storage: Some(storage.path().join("wal").to_string_lossy().into_owned()),
            undo_log_storage: Some(storage.path().join("undo").to_string_lossy().into_owned()),
            raft_storage: None,
            index_enabled: false,
            services: vec![Service::Cell, Service::Transaction],
            enable_recovery: false,
            disable_storage_locks: true,
        },
        &address.to_string(),
        &group.to_string(),
        async |_| {},
    )
    .await
    .unwrap()
}

async fn start_occ_test_cluster(addresses: &[&str], group: &str) -> Vec<Arc<NebServer>> {
    let opts = ServerOptions {
        chunk_size: crate::ram::segs::SEGMENT_SIZE,
        db_size: crate::ram::segs::SEGMENT_SIZE,
        history_retention_ms: 300_000,
        tiered_config: None,
        backup_storage: None,
        wal_storage: None,
        undo_log_storage: None,
        raft_storage: None,
        index_enabled: false,
        services: vec![Service::Cell, Service::Transaction],
        enable_recovery: false,
        disable_storage_locks: true,
    };
    let meta_servers = addresses
        .iter()
        .map(|address| address.to_string())
        .collect();
    let mut servers = Vec::with_capacity(addresses.len());
    for address in addresses {
        servers.push(
            NebServer::new_cluster_from_opts(&opts, address, &meta_servers, group, async |_| {})
                .await
                .unwrap(),
        );
    }
    let _ = ids_on_stably_routed_distinct_servers(&servers).await;
    servers
}

fn durable_occ_cluster_options(storage: &Path, enable_recovery: bool) -> ServerOptions {
    ServerOptions {
        chunk_size: crate::ram::segs::SEGMENT_SIZE,
        db_size: crate::ram::segs::SEGMENT_SIZE,
        history_retention_ms: 300_000,
        tiered_config: None,
        backup_storage: Some(storage.join("backup").to_string_lossy().into_owned()),
        wal_storage: Some(storage.join("wal").to_string_lossy().into_owned()),
        undo_log_storage: Some(storage.join("undo").to_string_lossy().into_owned()),
        raft_storage: Some(storage.join("raft").to_string_lossy().into_owned()),
        index_enabled: false,
        services: vec![Service::Cell, Service::Transaction],
        enable_recovery,
        disable_storage_locks: true,
    }
}

async fn start_durable_occ_test_cluster(
    addresses: &[&str],
    group: &str,
    storage: &Path,
) -> Vec<Arc<NebServer>> {
    let meta_servers = addresses
        .iter()
        .map(|address| address.to_string())
        .collect();
    let mut servers = Vec::with_capacity(addresses.len());
    for (index, address) in addresses.iter().enumerate() {
        let opts = durable_occ_cluster_options(&storage.join(format!("server-{index}")), false);
        servers.push(
            NebServer::new_cluster_from_opts(&opts, address, &meta_servers, group, async |_| {})
                .await
                .unwrap(),
        );
    }
    let _ = ids_on_stably_routed_distinct_servers(&servers).await;
    servers
}

fn install_occ_schema(runtime: &Arc<DatabaseRuntime>) -> Schema {
    install_occ_schema_with_dynamic(runtime, false)
}

fn install_occ_schema_on_servers(servers: &[Arc<NebServer>]) -> Schema {
    let schema = Schema::new_with_id(
        901,
        &String::from("txn_occ_repeatable"),
        None,
        default_fields(),
        false,
        false,
    );
    for server in servers {
        server
            .current_database()
            .meta()
            .schemas
            .debug_only_new_schema(schema.clone());
    }
    schema
}

fn install_occ_schema_with_dynamic(runtime: &Arc<DatabaseRuntime>, dynamic: bool) -> Schema {
    let schema = Schema::new_with_id(
        901,
        &String::from("txn_occ_repeatable"),
        None,
        default_fields(),
        dynamic,
        false,
    );
    runtime.meta().schemas.debug_only_new_schema(schema.clone());
    schema
}

fn counter_cell(schema_id: u32, id: Id, score: u64, name: &str) -> OwnedCell {
    let mut data = OwnedMap::new();
    data.insert(&String::from("id"), OwnedValue::I64(id.lower as i64));
    data.insert(&String::from("score"), OwnedValue::U64(score));
    data.insert(&String::from("name"), OwnedValue::String(name.to_string()));
    OwnedCell::new_with_id(schema_id, &id, OwnedValue::Map(data))
}

fn ids_on_distinct_servers(server: &Arc<NebServer>) -> ((Id, u64), (Id, u64)) {
    try_ids_on_distinct_servers(server)
        .expect("expected at least two routed servers in the test cluster")
}

fn try_ids_on_distinct_servers(server: &Arc<NebServer>) -> Option<((Id, u64), (Id, u64))> {
    let mut first = None;
    for partition in 1..8192u64 {
        let id = Id::new(partition, 90_000 + partition);
        let server_id = server.get_server_id_by_id(&id)?;
        if let Some((first_id, first_server_id)) = first {
            if server_id != first_server_id {
                return Some(((first_id, first_server_id), (id, server_id)));
            }
        } else {
            first = Some((id, server_id));
        }
    }
    None
}

async fn ids_on_stably_routed_distinct_servers(
    servers: &[Arc<NebServer>],
) -> ((Id, u64), (Id, u64)) {
    timeout(Duration::from_secs(5), async {
        let mut last_candidate = None;
        let mut confirmations = 0;
        loop {
            let candidate = try_ids_on_distinct_servers(&servers[0]).filter(
                |((first_id, first_server_id), (second_id, second_server_id))| {
                    servers
                        .iter()
                        .any(|server| server.server_id == *first_server_id)
                        && servers
                            .iter()
                            .any(|server| server.server_id == *second_server_id)
                        && servers.iter().all(|server| {
                            server.get_server_id_by_id(first_id) == Some(*first_server_id)
                                && server.get_server_id_by_id(second_id) == Some(*second_server_id)
                        })
                },
            );
            if candidate.is_some() && candidate == last_candidate {
                confirmations += 1;
            } else {
                last_candidate = candidate;
                confirmations = usize::from(last_candidate.is_some());
            }
            if confirmations >= 3 {
                return last_candidate.unwrap();
            }
            tokio::time::sleep(Duration::from_millis(10)).await;
        }
    })
    .await
    .expect(
        "cluster routing should converge on both current server IDs with identical ownership maps",
    )
}

fn observe_distributed_seed_revisions<I>(coordinator: &Arc<NebServer>, revision_timestamps: I)
where
    I: IntoIterator<Item = u64>,
{
    let max_revision_ts = revision_timestamps
        .into_iter()
        .max()
        .expect("distributed seed set should not be empty");
    coordinator.hlc.observe(bifrost::hlc::Hlc {
        ts: max_revision_ts,
        node: coordinator.hlc.node(),
    });
}

fn score_of(cell: &OwnedCell) -> u64 {
    *cell.data["score"].u64().unwrap()
}

fn selected_score_of(cell: &OwnedCell) -> u64 {
    *cell.data.uni_array().unwrap()[0].u64().unwrap()
}

fn selected_value(cell: &OwnedCell, index: usize) -> &OwnedValue {
    &cell.data.uni_array().unwrap()[index]
}

fn accepted_cell(result: TxnExecResult<OwnedCell, ReadError>) -> OwnedCell {
    match result {
        TxnExecResult::Accepted(cell) => cell,
        other => panic!("expected accepted cell, got {:?}", other),
    }
}

fn accepted_head(
    result: TxnExecResult<crate::ram::cell::CellHeader, ReadError>,
) -> crate::ram::cell::CellHeader {
    match result {
        TxnExecResult::Accepted(head) => head,
        other => panic!("expected accepted header, got {:?}", other),
    }
}

fn assert_missing(result: TxnExecResult<OwnedCell, ReadError>) {
    assert!(
        matches!(result, TxnExecResult::Error(ReadError::CellDoesNotExisted)),
        "expected missing cell result, got {:?}",
        result
    );
}

fn assert_missing_head(result: TxnExecResult<crate::ram::cell::CellHeader, ReadError>) {
    assert!(
        matches!(result, TxnExecResult::Error(ReadError::CellDoesNotExisted)),
        "expected missing header result, got {:?}",
        result
    );
}

async fn abort_txn(txn: &Arc<transactions::manager::AsyncServiceClient>, tid: transactions::TxnId) {
    let _ = txn.abort(tid).await;
}

async fn wait_for_transaction_count(
    manager: &Arc<transactions::manager::TransactionManager>,
    expected: usize,
    timeout_duration: Duration,
) {
    let started = tokio::time::Instant::now();
    loop {
        let current = manager.transaction_count();
        if current == expected {
            return;
        }
        assert!(
            started.elapsed() < timeout_duration,
            "timed out waiting for transaction count {expected}, current {current}"
        );
        tokio::time::sleep(Duration::from_millis(10)).await;
    }
}

#[tokio::test(flavor = "multi_thread")]
async fn full_read_validation_prevents_point_write_skew() {
    let _ = env_logger::try_init();
    let address = "127.0.0.1:5393";
    let group = "txn_occ_full_read_write_skew";
    let server = start_occ_test_server(address, group).await;
    let runtime = server.current_database();
    let schema = install_occ_schema(&runtime);
    let first_id = Id::new(0, 90142);
    let second_id = Id::new(0, 90143);

    let mut first_seed = counter_cell(schema.id, first_id, 1, "write-skew-first");
    runtime.chunks().write_cell(&mut first_seed).unwrap();
    let mut second_seed = counter_cell(schema.id, second_id, 1, "write-skew-second");
    runtime.chunks().write_cell(&mut second_seed).unwrap();

    let txn = scoped_txn_client_for_database(address, group, group).await;
    let left = txn.begin().await.unwrap().unwrap();
    let right = txn.begin().await.unwrap().unwrap();

    for tid in [left, right] {
        assert_eq!(
            score_of(&accepted_cell(
                txn.read(tid, first_id).await.unwrap().unwrap()
            )),
            1
        );
        assert_eq!(
            score_of(&accepted_cell(
                txn.read(tid, second_id).await.unwrap().unwrap()
            )),
            1
        );
    }

    assert_eq!(
        txn.update(
            left,
            counter_cell(schema.id, first_id, 0, "write-skew-left"),
        )
        .await
        .unwrap()
        .unwrap(),
        TxnExecResult::Accepted(())
    );
    assert_eq!(
        txn.update(
            right,
            counter_cell(schema.id, second_id, 0, "write-skew-right"),
        )
        .await
        .unwrap()
        .unwrap(),
        TxnExecResult::Accepted(())
    );

    let (left_result, right_result) = tokio::join!(txn.prepare(left), txn.prepare(right));
    let left_result = left_result.unwrap().unwrap();
    let right_result = right_result.unwrap().unwrap();
    let successful = usize::from(left_result == TMPrepareResult::Success)
        + usize::from(right_result == TMPrepareResult::Success);
    assert_eq!(
        successful, 1,
        "exactly one write-skewing transaction may prepare: left={left_result:?}, right={right_result:?}"
    );

    if left_result == TMPrepareResult::Success {
        let _ = txn.abort(left).await;
    }
    if right_result == TMPrepareResult::Success {
        let _ = txn.abort(right).await;
    }
    server.shutdown().await;
}

#[tokio::test(flavor = "multi_thread")]
async fn all_participants_install_the_same_commit_timestamp() {
    let _ = env_logger::try_init();
    let addresses = ["127.0.0.1:5394", "127.0.0.1:5395"];
    let group = "txn_occ_shared_distributed_commit_hlc";
    let servers = start_occ_test_cluster(&addresses, group).await;
    let schema = install_occ_schema_on_servers(&servers);
    let ((first_id, first_server_id), (second_id, second_server_id)) =
        ids_on_distinct_servers(&servers[0]);
    let servers_by_id: HashMap<u64, Arc<NebServer>> = servers
        .iter()
        .map(|server| (server.server_id, server.clone()))
        .collect();

    let mut first_seed = counter_cell(schema.id, first_id, 1, "shared-hlc-first");
    let first_seed_header = servers_by_id[&first_server_id]
        .current_database()
        .chunks()
        .write_cell(&mut first_seed)
        .unwrap();
    let mut second_seed = counter_cell(schema.id, second_id, 2, "shared-hlc-second");
    let second_seed_header = servers_by_id[&second_server_id]
        .current_database()
        .chunks()
        .write_cell(&mut second_seed)
        .unwrap();
    observe_distributed_seed_revisions(
        &servers[0],
        [
            first_seed_header.revision_ts,
            second_seed_header.revision_ts,
        ],
    );

    let txn = scoped_txn_client_for_database(addresses[0], group, group).await;
    let tid = txn.begin().await.unwrap().unwrap();
    let first = accepted_cell(txn.read(tid, first_id).await.unwrap().unwrap());
    let second = accepted_cell(txn.read(tid, second_id).await.unwrap().unwrap());
    assert_eq!(
        txn.update(
            tid,
            counter_cell(
                schema.id,
                first_id,
                score_of(&first) + 10,
                "shared-hlc-first-new"
            ),
        )
        .await
        .unwrap()
        .unwrap(),
        TxnExecResult::Accepted(())
    );
    assert_eq!(
        txn.update(
            tid,
            counter_cell(
                schema.id,
                second_id,
                score_of(&second) + 10,
                "shared-hlc-second-new",
            ),
        )
        .await
        .unwrap()
        .unwrap(),
        TxnExecResult::Accepted(())
    );
    assert_eq!(
        txn.prepare(tid).await.unwrap().unwrap(),
        TMPrepareResult::Success
    );
    assert_eq!(txn.commit(tid).await.unwrap().unwrap(), EndResult::Success);

    let first_revision = servers_by_id[&first_server_id]
        .current_database()
        .chunks()
        .head_cell(&first_id)
        .unwrap()
        .revision_ts;
    let second_revision = servers_by_id[&second_server_id]
        .current_database()
        .chunks()
        .head_cell(&second_id)
        .unwrap()
        .revision_ts;
    assert_eq!(
        first_revision, second_revision,
        "every participant must install the coordinator's one shared commit HLC"
    );

    for server in servers {
        server.shutdown().await;
    }
}

#[tokio::test(flavor = "multi_thread")]
async fn participant_delay_after_peer_install_never_exposes_partial_commit() {
    let _ = env_logger::try_init();
    let addresses = ["127.0.0.1:5401", "127.0.0.1:5402"];
    let group = "txn_occ_distributed_pending_visibility";
    let servers = start_occ_test_cluster(&addresses, group).await;
    let schema = install_occ_schema_on_servers(&servers);
    let ((first_id, first_server_id), (second_id, second_server_id)) =
        ids_on_distinct_servers(&servers[0]);
    let servers_by_id: HashMap<u64, Arc<NebServer>> = servers
        .iter()
        .map(|server| (server.server_id, server.clone()))
        .collect();

    let mut first_seed = counter_cell(schema.id, first_id, 1, "partial-first-old");
    let first_seed_header = servers_by_id[&first_server_id]
        .current_database()
        .chunks()
        .write_cell(&mut first_seed)
        .unwrap();
    let mut second_seed = counter_cell(schema.id, second_id, 2, "partial-second-old");
    let second_seed_header = servers_by_id[&second_server_id]
        .current_database()
        .chunks()
        .write_cell(&mut second_seed)
        .unwrap();
    observe_distributed_seed_revisions(
        &servers[0],
        [
            first_seed_header.revision_ts,
            second_seed_header.revision_ts,
        ],
    );

    let txn = scoped_txn_client_for_database(addresses[0], group, group).await;
    let observer = txn.begin().await.unwrap().unwrap();
    assert_eq!(
        score_of(&accepted_cell(
            txn.read(observer, first_id).await.unwrap().unwrap()
        )),
        1
    );
    assert_eq!(
        score_of(&accepted_cell(
            txn.read(observer, second_id).await.unwrap().unwrap()
        )),
        2
    );

    let writer = txn.begin().await.unwrap().unwrap();
    let first = accepted_cell(txn.read(writer, first_id).await.unwrap().unwrap());
    let second = accepted_cell(txn.read(writer, second_id).await.unwrap().unwrap());
    assert_eq!(
        txn.update(
            writer,
            counter_cell(
                schema.id,
                first_id,
                score_of(&first) + 10,
                "partial-first-new"
            ),
        )
        .await
        .unwrap()
        .unwrap(),
        TxnExecResult::Accepted(())
    );
    assert_eq!(
        txn.update(
            writer,
            counter_cell(
                schema.id,
                second_id,
                score_of(&second) + 10,
                "partial-second-new",
            ),
        )
        .await
        .unwrap()
        .unwrap(),
        TxnExecResult::Accepted(())
    );

    let delayed_commit = transactions::data_site::install_commit_delay_for_cell(writer, second_id);
    let prepare_client = txn.clone();
    let prepare_task =
        tokio::spawn(async move { prepare_client.prepare(writer).await.unwrap().unwrap() });
    delayed_commit.wait_until_entered().await;

    let first_runtime = servers_by_id[&first_server_id].current_database();
    timeout(Duration::from_secs(2), async {
        loop {
            if matches!(
                first_runtime
                    .chunks()
                    .read_cell_snapshot(&first_id, u64::MAX)
                    .unwrap(),
                crate::ram::cell::SnapshotRead::Wait
            ) {
                break;
            }
            tokio::time::sleep(Duration::from_millis(10)).await;
        }
    })
    .await
    .expect("first participant should install pending before delayed peer");

    assert_eq!(
        score_of(&accepted_cell(
            txn.read(observer, first_id).await.unwrap().unwrap()
        )),
        1,
        "an already-selected old revision remains readable"
    );
    assert_eq!(
        score_of(&accepted_cell(
            txn.read(observer, second_id).await.unwrap().unwrap()
        )),
        2,
        "an already-selected old revision remains readable on the delayed peer"
    );

    for (id, server_id) in [(first_id, first_server_id), (second_id, second_server_id)] {
        let server_index = servers
            .iter()
            .position(|server| server.server_id == server_id)
            .unwrap();
        let data_site =
            scoped_data_site_client_for_database(addresses[server_index], group, group).await;
        let reader_tid = servers_by_id[&server_id].hlc.now();
        assert!(matches!(
            data_site
                .read(server_id, reader_tid, reader_tid, id)
                .await
                .unwrap()
                .payload,
            TxnExecResult::Wait
        ));
    }

    delayed_commit.release();
    assert_eq!(prepare_task.await.unwrap(), TMPrepareResult::Success);
    assert_eq!(
        txn.commit(writer).await.unwrap().unwrap(),
        EndResult::Success
    );
    abort_txn(&txn, observer).await;

    let first_current = first_runtime
        .chunks()
        .read_cell(&first_id)
        .unwrap()
        .to_owned();
    let second_current = servers_by_id[&second_server_id]
        .current_database()
        .chunks()
        .read_cell(&second_id)
        .unwrap()
        .to_owned();
    assert_eq!(score_of(&first_current), 11);
    assert_eq!(score_of(&second_current), 12);
    assert_eq!(
        first_current.header.revision_ts,
        second_current.header.revision_ts
    );

    for server in servers {
        server.shutdown().await;
    }
}

#[tokio::test(flavor = "multi_thread")]
async fn commit_stage_failure_compensates_installed_peer_and_resolves_abort() {
    let _ = env_logger::try_init();
    let addresses = ["127.0.0.1:5406", "127.0.0.1:5407"];
    let group = "txn_occ_commit_failure_pending_barrier";
    let servers = start_occ_test_cluster(&addresses, group).await;
    let schema = install_occ_schema_on_servers(&servers);
    let ((first_id, first_server_id), (second_id, second_server_id)) =
        ids_on_distinct_servers(&servers[0]);
    let servers_by_id: HashMap<u64, Arc<NebServer>> = servers
        .iter()
        .map(|server| (server.server_id, server.clone()))
        .collect();

    let mut first_seed = counter_cell(schema.id, first_id, 1, "commit-failure-first-seed");
    let first_seed_header = servers_by_id[&first_server_id]
        .current_database()
        .chunks()
        .write_cell(&mut first_seed)
        .unwrap();
    let mut second_seed = counter_cell(schema.id, second_id, 2, "commit-failure-second-seed");
    let second_seed_header = servers_by_id[&second_server_id]
        .current_database()
        .chunks()
        .write_cell(&mut second_seed)
        .unwrap();
    observe_distributed_seed_revisions(
        &servers[0],
        [
            first_seed_header.revision_ts,
            second_seed_header.revision_ts,
        ],
    );

    let txn = scoped_txn_client_for_database(addresses[0], group, group).await;
    let manager = servers[0].current_database().txn_manager().unwrap().clone();
    let tid = txn.begin().await.unwrap().unwrap();
    let first = accepted_cell(txn.read(tid, first_id).await.unwrap().unwrap());
    let second = accepted_cell(txn.read(tid, second_id).await.unwrap().unwrap());
    assert_eq!(
        txn.update(
            tid,
            counter_cell(
                schema.id,
                first_id,
                score_of(&first) + 10,
                "commit-failure-first-pending",
            ),
        )
        .await
        .unwrap()
        .unwrap(),
        TxnExecResult::Accepted(())
    );
    assert_eq!(
        txn.update(
            tid,
            counter_cell(
                schema.id,
                second_id,
                score_of(&second) + 10,
                "commit-failure-second-pending",
            ),
        )
        .await
        .unwrap()
        .unwrap(),
        TxnExecResult::Accepted(())
    );

    let delayed_commit = transactions::data_site::install_commit_delay_for_cell(tid, second_id);
    let prepare_client = txn.clone();
    let prepare_task =
        tokio::spawn(async move { prepare_client.prepare(tid).await.unwrap().unwrap() });
    delayed_commit.wait_until_entered().await;

    let first_runtime = servers_by_id[&first_server_id].current_database();
    timeout(Duration::from_secs(2), async {
        loop {
            if matches!(
                first_runtime
                    .chunks()
                    .read_cell_snapshot(&first_id, u64::MAX)
                    .unwrap(),
                crate::ram::cell::SnapshotRead::Wait
            ) {
                break;
            }
            tokio::time::sleep(Duration::from_millis(10)).await;
        }
    })
    .await
    .expect("participant A should install pending before participant B fails");

    let mut conflicting = counter_cell(schema.id, second_id, 99, "commit-failure-second-conflict");
    servers_by_id[&second_server_id]
        .current_database()
        .chunks()
        .update_cell(&mut conflicting)
        .unwrap();
    delayed_commit.release();

    assert_eq!(
        prepare_task.await.unwrap(),
        TMPrepareResult::DMCommitError(DMCommitResult::CellChanged(second_id))
    );
    wait_for_transaction_count(&manager, 0, Duration::from_secs(2)).await;
    assert_eq!(
        transactions::data_site::participant_owner_for_test(
            first_server_id,
            group,
            group,
            &first_id,
        ),
        None,
        "the already-installed peer must be compensated and released"
    );
    assert_eq!(
        txn.resolve(tid).await.unwrap().unwrap(),
        TxnResolution::Abort
    );
    assert_eq!(
        score_of(
            &first_runtime
                .chunks()
                .read_cell(&first_id)
                .unwrap()
                .to_owned()
        ),
        1,
        "compensation must restore the pre-transaction value"
    );
    assert_eq!(
        score_of(
            &servers_by_id[&second_server_id]
                .current_database()
                .chunks()
                .read_cell(&second_id)
                .unwrap()
                .to_owned()
        ),
        99,
        "the independent conflicting write must remain authoritative"
    );

    for server in servers {
        server.shutdown().await;
    }
}

#[tokio::test(flavor = "multi_thread")]
async fn equal_commit_timestamp_is_invisible_to_snapshot() {
    let _ = env_logger::try_init();
    let address = "127.0.0.1:5396";
    let group = "txn_occ_equal_commit_boundary";
    let server = start_occ_test_server(address, group).await;
    let runtime = server.current_database();
    let schema = install_occ_schema(&runtime);
    let cell_id = Id::new(0, 90144);

    let mut old = counter_cell(schema.id, cell_id, 1, "equal-boundary-old");
    runtime
        .chunks()
        .write_cell_at_revision(&mut old, crate::ram::cell::RevisionWrite::committed(100))
        .unwrap();
    let mut current = counter_cell(schema.id, cell_id, 2, "equal-boundary-current");
    runtime
        .chunks()
        .update_cell_at_revision(
            &mut current,
            crate::ram::cell::RevisionWrite::committed(200),
        )
        .unwrap();

    let snapshot = runtime.chunks().read_cell_snapshot(&cell_id, 200).unwrap();
    match snapshot {
        crate::ram::cell::SnapshotRead::Present(cell) => {
            assert_eq!(score_of(&cell), 1);
            assert_eq!(cell.header.revision_ts, 100);
        }
        other => panic!("expected the old revision at an equal boundary, got {other:?}"),
    }

    server.shutdown().await;
}

#[tokio::test(flavor = "multi_thread")]
async fn aborted_update_restores_content_with_newer_revision() {
    let _ = env_logger::try_init();
    let address = "127.0.0.1:5410";
    let group = "txn_occ_abort_update_compensation";
    let server = start_occ_test_server(address, group).await;
    let runtime = server.current_database();
    let schema = install_occ_schema(&runtime);
    let cell_id = Id::new(0, 90150);

    let mut original = counter_cell(schema.id, cell_id, 1, "abort-update-original");
    let original_header = runtime.chunks().write_cell(&mut original).unwrap();
    let tid = server.hlc.now();
    let data_site = scoped_data_site_client_for_database(address, group, group).await;
    let prepare = data_site
        .prepare(
            server.server_id,
            tid,
            tid,
            vec![PrepareOp {
                id: cell_id,
                expectation: CellExpectation::Present(original_header.revision_ts),
                intent: PrepareIntent::Write,
            }],
        )
        .await
        .unwrap();
    assert_eq!(prepare.payload, DMPrepareResult::Success);

    let commit_hlc = server.hlc.now();
    let commit = data_site
        .commit(
            commit_hlc,
            tid,
            vec![CommitOp::Update(counter_cell(
                schema.id,
                cell_id,
                2,
                "abort-update-failed",
            ))],
        )
        .await
        .unwrap();
    assert_eq!(commit.payload, DMCommitResult::Success);

    let abort = data_site.abort(server.hlc.now(), tid).await.unwrap();
    assert_eq!(abort.payload, AbortResult::Success(None));

    let current = runtime.chunks().read_cell(&cell_id).unwrap().to_owned();
    assert_eq!(current.data, original.data);
    assert!(current.header.revision_ts > commit_hlc.ts);
    assert!(matches!(
        runtime
            .chunks()
            .read_cell_snapshot(&cell_id, current.header.revision_ts)
            .unwrap(),
        crate::ram::cell::SnapshotRead::Present(ref cell)
            if cell.header.revision_ts == original_header.revision_ts
                && cell.data == original.data
    ));

    let end = data_site.end(server.hlc.now(), tid).await.unwrap();
    assert_eq!(end.payload, EndResult::Success);
    server.shutdown().await;
}

#[tokio::test(flavor = "multi_thread")]
async fn aborted_insert_installs_a_newer_tombstone() {
    let _ = env_logger::try_init();
    let address = "127.0.0.1:5411";
    let group = "txn_occ_abort_insert_compensation";
    let server = start_occ_test_server(address, group).await;
    let runtime = server.current_database();
    let schema = install_occ_schema(&runtime);
    let cell_id = Id::new(0, 90151);
    let tid = server.hlc.now();
    let data_site = scoped_data_site_client_for_database(address, group, group).await;

    let prepare = data_site
        .prepare(
            server.server_id,
            tid,
            tid,
            vec![PrepareOp {
                id: cell_id,
                expectation: CellExpectation::Absent(None),
                intent: PrepareIntent::Write,
            }],
        )
        .await
        .unwrap();
    assert_eq!(prepare.payload, DMPrepareResult::Success);
    let commit_hlc = server.hlc.now();
    let commit = data_site
        .commit(
            commit_hlc,
            tid,
            vec![CommitOp::Write(counter_cell(
                schema.id,
                cell_id,
                1,
                "abort-insert-failed",
            ))],
        )
        .await
        .unwrap();
    assert_eq!(commit.payload, DMCommitResult::Success);

    let abort = data_site.abort(server.hlc.now(), tid).await.unwrap();
    assert_eq!(abort.payload, AbortResult::Success(None));
    assert!(runtime.chunks().read_cell(&cell_id).is_err());
    let compensation_ts = runtime
        .chunks()
        .current_revision_ts(&cell_id)
        .expect("aborted insert must leave a current tombstone");
    assert!(compensation_ts > commit_hlc.ts);
    assert!(matches!(
        runtime
            .chunks()
            .read_cell_snapshot(&cell_id, compensation_ts)
            .unwrap(),
        crate::ram::cell::SnapshotRead::Absent(None)
    ));

    assert_eq!(
        data_site.end(server.hlc.now(), tid).await.unwrap().payload,
        EndResult::Success
    );
    server.shutdown().await;
}

#[tokio::test(flavor = "multi_thread")]
async fn aborted_delete_restores_content_with_newer_revision() {
    let _ = env_logger::try_init();
    let address = "127.0.0.1:5412";
    let group = "txn_occ_abort_delete_compensation";
    let server = start_occ_test_server(address, group).await;
    let runtime = server.current_database();
    let schema = install_occ_schema(&runtime);
    let cell_id = Id::new(0, 90152);

    let mut original = counter_cell(schema.id, cell_id, 1, "abort-delete-original");
    let original_header = runtime.chunks().write_cell(&mut original).unwrap();
    let tid = server.hlc.now();
    let data_site = scoped_data_site_client_for_database(address, group, group).await;
    let prepare = data_site
        .prepare(
            server.server_id,
            tid,
            tid,
            vec![PrepareOp {
                id: cell_id,
                expectation: CellExpectation::Present(original_header.revision_ts),
                intent: PrepareIntent::Write,
            }],
        )
        .await
        .unwrap();
    assert_eq!(prepare.payload, DMPrepareResult::Success);
    let commit_hlc = server.hlc.now();
    let commit = data_site
        .commit(commit_hlc, tid, vec![CommitOp::Remove(cell_id)])
        .await
        .unwrap();
    assert_eq!(commit.payload, DMCommitResult::Success);

    let abort = data_site.abort(server.hlc.now(), tid).await.unwrap();
    assert_eq!(abort.payload, AbortResult::Success(None));
    let current = runtime.chunks().read_cell(&cell_id).unwrap().to_owned();
    assert_eq!(current.data, original.data);
    assert!(current.header.revision_ts > commit_hlc.ts);
    assert!(matches!(
        runtime
            .chunks()
            .read_cell_snapshot(&cell_id, current.header.revision_ts)
            .unwrap(),
        crate::ram::cell::SnapshotRead::Present(ref cell)
            if cell.header.revision_ts == original_header.revision_ts
                && cell.data == original.data
    ));

    assert_eq!(
        data_site.end(server.hlc.now(), tid).await.unwrap().payload,
        EndResult::Success
    );
    server.shutdown().await;
}

#[tokio::test(flavor = "multi_thread")]
async fn abort_rejects_a_later_successful_revision_without_overwriting_it() {
    let _ = env_logger::try_init();
    let address = "127.0.0.1:5416";
    let group = "txn_occ_abort_preserves_later_revision";
    let server = start_occ_test_server(address, group).await;
    let runtime = server.current_database();
    let schema = install_occ_schema(&runtime);
    let cell_id = Id::new(0, 90157);

    let mut original = counter_cell(schema.id, cell_id, 1, "later-winner-original");
    let original_header = runtime.chunks().write_cell(&mut original).unwrap();
    let tid = server.hlc.now();
    let data_site = scoped_data_site_client_for_database(address, group, group).await;
    assert_eq!(
        data_site
            .prepare(
                server.server_id,
                tid,
                tid,
                vec![PrepareOp {
                    id: cell_id,
                    expectation: CellExpectation::Present(original_header.revision_ts),
                    intent: PrepareIntent::Write,
                }],
            )
            .await
            .unwrap()
            .payload,
        DMPrepareResult::Success
    );
    assert_eq!(
        data_site
            .commit(
                server.hlc.now(),
                tid,
                vec![CommitOp::Update(counter_cell(
                    schema.id,
                    cell_id,
                    2,
                    "later-winner-failed",
                ))],
            )
            .await
            .unwrap()
            .payload,
        DMCommitResult::Success
    );

    let mut later = counter_cell(schema.id, cell_id, 3, "later-winner-success");
    let later_header = runtime.chunks().update_cell(&mut later).unwrap();
    assert_eq!(
        data_site
            .abort(server.hlc.now(), tid)
            .await
            .unwrap()
            .payload,
        AbortResult::CheckFailed(CheckError::CannotEnd)
    );
    let retained = runtime.chunks().read_cell(&cell_id).unwrap().to_owned();
    assert_eq!(retained.header.revision_ts, later_header.revision_ts);
    assert_eq!(retained.data, later.data);

    server.shutdown().await;
}

#[tokio::test(flavor = "multi_thread")]
async fn undo_is_durable_before_transactional_storage_mutation() {
    let _ = env_logger::try_init();
    let temp_dir = TempDir::new().unwrap();
    let address = "127.0.0.1:5413";
    let group = "txn_occ_undo_before_mutation";
    let server = start_occ_test_server_with_options(
        address,
        group,
        Some(temp_dir.path().join("undo").to_string_lossy().into_owned()),
        1,
    )
    .await;
    let runtime = server.current_database();
    let schema = install_occ_schema(&runtime);
    let cell_id = Id::new(0, 90153);
    let mut original = counter_cell(schema.id, cell_id, 1, "undo-durable-original");
    let original_header = runtime.chunks().write_cell(&mut original).unwrap();
    let tid = server.hlc.now();
    let data_site = scoped_data_site_client_for_database(address, group, group).await;
    assert_eq!(
        data_site
            .prepare(
                server.server_id,
                tid,
                tid,
                vec![PrepareOp {
                    id: cell_id,
                    expectation: CellExpectation::Present(original_header.revision_ts),
                    intent: PrepareIntent::Write,
                }],
            )
            .await
            .unwrap()
            .payload,
        DMPrepareResult::Success
    );

    let commit_hlc = server.hlc.now();
    let pause = transactions::data_site::install_before_storage_mutation_pause(tid, cell_id);
    let commit_site = data_site.clone();
    let commit_task = tokio::spawn(async move {
        commit_site
            .commit(
                commit_hlc,
                tid,
                vec![CommitOp::Update(counter_cell(
                    schema.id,
                    cell_id,
                    2,
                    "undo-durable-pending",
                ))],
            )
            .await
            .unwrap()
            .payload
    });
    pause.wait_until_entered().await;

    let before_mutation = runtime.chunks().read_cell(&cell_id).unwrap().to_owned();
    assert_eq!(
        before_mutation.header.revision_ts,
        original_header.revision_ts
    );
    assert_eq!(before_mutation.data, original.data);
    let recovered = runtime.undo_log().unwrap().recover().unwrap();
    let entries = recovered
        .get(&tid)
        .expect("the synced undo entry must be recoverable before mutation");
    assert_eq!(entries.len(), 1);
    assert_eq!(entries[0].installed_revision_ts, commit_hlc.ts);
    let durable_prior = entries[0]
        .prior_cell
        .as_ref()
        .expect("update undo must own the prior cell");
    assert_eq!(
        durable_prior.header.revision_ts,
        original_header.revision_ts
    );
    assert_eq!(durable_prior.data, original.data);

    pause.release();
    assert_eq!(commit_task.await.unwrap(), DMCommitResult::Success);
    assert_eq!(
        data_site
            .abort(server.hlc.now(), tid)
            .await
            .unwrap()
            .payload,
        AbortResult::Success(None)
    );
    assert_eq!(
        data_site.end(server.hlc.now(), tid).await.unwrap().payload,
        EndResult::Success
    );
    server.shutdown().await;
}

#[tokio::test(flavor = "multi_thread")]
async fn undo_write_failure_prevents_transactional_storage_mutation() {
    let _ = env_logger::try_init();
    let temp_dir = TempDir::new().unwrap();
    let address = "127.0.0.1:5414";
    let group = "txn_occ_undo_failure_stops_mutation";
    let server = start_occ_test_server_with_options(
        address,
        group,
        Some(temp_dir.path().join("undo").to_string_lossy().into_owned()),
        1,
    )
    .await;
    let runtime = server.current_database();
    let schema = install_occ_schema(&runtime);
    let cell_id = Id::new(0, 90154);
    let mut original = counter_cell(schema.id, cell_id, 1, "undo-failure-original");
    let original_header = runtime.chunks().write_cell(&mut original).unwrap();
    let tid = server.hlc.now();
    let data_site = scoped_data_site_client_for_database(address, group, group).await;
    assert_eq!(
        data_site
            .prepare(
                server.server_id,
                tid,
                tid,
                vec![PrepareOp {
                    id: cell_id,
                    expectation: CellExpectation::Present(original_header.revision_ts),
                    intent: PrepareIntent::Write,
                }],
            )
            .await
            .unwrap()
            .payload,
        DMPrepareResult::Success
    );

    runtime.undo_log().unwrap().fail_next_undo_write_for_test();
    let commit = data_site
        .commit(
            server.hlc.now(),
            tid,
            vec![CommitOp::Update(counter_cell(
                schema.id,
                cell_id,
                2,
                "undo-failure-pending",
            ))],
        )
        .await
        .unwrap()
        .payload;
    assert_ne!(commit, DMCommitResult::Success);
    let after = runtime.chunks().read_cell(&cell_id).unwrap().to_owned();
    assert_eq!(after.header.revision_ts, original_header.revision_ts);
    assert_eq!(after.data, original.data);

    assert_eq!(
        data_site
            .abort(server.hlc.now(), tid)
            .await
            .unwrap()
            .payload,
        AbortResult::Success(None)
    );
    assert_eq!(
        data_site.end(server.hlc.now(), tid).await.unwrap().payload,
        EndResult::Success
    );
    server.shutdown().await;
}

#[tokio::test(flavor = "multi_thread")]
async fn partial_compensation_retry_resumes_aborted_node_without_duplicates() {
    let _ = env_logger::try_init();
    let address = "127.0.0.1:5415";
    let group = "txn_occ_partial_compensation_retry";
    let server = start_occ_test_server_with_options(address, group, None, 2).await;
    let runtime = server.current_database();
    let schema = install_occ_schema(&runtime);
    let first_id = Id::new(0, 90155);
    let second_id = Id::new(1, 90156);
    let mut first_original = counter_cell(
        schema.id,
        first_id,
        1,
        "partial-compensation-first-original",
    );
    let first_header = runtime.chunks().write_cell(&mut first_original).unwrap();
    let mut second_original = counter_cell(
        schema.id,
        second_id,
        2,
        "partial-compensation-second-original",
    );
    let second_header = runtime.chunks().write_cell(&mut second_original).unwrap();
    let tid = server.hlc.now();
    let data_site = scoped_data_site_client_for_database(address, group, group).await;
    assert_eq!(
        data_site
            .prepare(
                server.server_id,
                tid,
                tid,
                vec![
                    PrepareOp {
                        id: first_id,
                        expectation: CellExpectation::Present(first_header.revision_ts),
                        intent: PrepareIntent::Write,
                    },
                    PrepareOp {
                        id: second_id,
                        expectation: CellExpectation::Present(second_header.revision_ts),
                        intent: PrepareIntent::Write,
                    },
                ],
            )
            .await
            .unwrap()
            .payload,
        DMPrepareResult::Success
    );
    let commit_hlc = server.hlc.now();
    assert_eq!(
        data_site
            .commit(
                commit_hlc,
                tid,
                vec![
                    CommitOp::Update(counter_cell(
                        schema.id,
                        first_id,
                        11,
                        "partial-compensation-first-failed",
                    )),
                    CommitOp::Update(counter_cell(
                        schema.id,
                        second_id,
                        12,
                        "partial-compensation-second-failed",
                    )),
                ],
            )
            .await
            .unwrap()
            .payload,
        DMCommitResult::Success
    );

    runtime.chunks().fail_next_allocation_for_test(&second_id);
    assert_eq!(
        data_site
            .abort(server.hlc.now(), tid)
            .await
            .unwrap()
            .payload,
        AbortResult::CheckFailed(CheckError::CannotEnd)
    );
    let first_compensation = runtime.chunks().read_cell(&first_id).unwrap().to_owned();
    assert_eq!(first_compensation.data, first_original.data);
    assert!(first_compensation.header.revision_ts > commit_hlc.ts);
    let first_compensation_ts = first_compensation.header.revision_ts;
    let expected_owner = Some(TxnPriority::new(tid, server.server_id));
    assert_eq!(
        transactions::data_site::participant_owner_for_test(
            server.server_id,
            group,
            group,
            &first_id,
        ),
        expected_owner
    );
    assert_eq!(
        transactions::data_site::participant_owner_for_test(
            server.server_id,
            group,
            group,
            &second_id,
        ),
        expected_owner
    );

    assert_eq!(
        data_site
            .abort(server.hlc.now(), tid)
            .await
            .unwrap()
            .payload,
        AbortResult::Success(None)
    );
    assert_eq!(
        runtime
            .chunks()
            .read_cell(&first_id)
            .unwrap()
            .header
            .revision_ts,
        first_compensation_ts,
        "retry must not install a duplicate compensation"
    );
    let second_compensation = runtime.chunks().read_cell(&second_id).unwrap().to_owned();
    assert_eq!(second_compensation.data, second_original.data);
    assert!(second_compensation.header.revision_ts > commit_hlc.ts);

    assert_eq!(
        data_site.end(server.hlc.now(), tid).await.unwrap().payload,
        EndResult::Success
    );
    server.shutdown().await;
}

#[tokio::test(flavor = "multi_thread")]
async fn prepare_failure_racing_with_slow_success_settles_before_cleanup() {
    let _ = env_logger::try_init();
    let addresses = ["127.0.0.1:5360", "127.0.0.1:5361"];
    let group = "txn_occ_prepare_settle_cleanup";
    let servers = start_occ_test_cluster(&addresses, group).await;
    let schema = install_occ_schema_on_servers(&servers);
    let ((slow_id, slow_server_id), (fail_id, fail_server_id)) =
        ids_on_distinct_servers(&servers[0]);
    let servers_by_id: HashMap<u64, Arc<NebServer>> = servers
        .iter()
        .map(|server| (server.server_id, server.clone()))
        .collect();
    let slow_server = servers_by_id
        .get(&slow_server_id)
        .expect("slow cell owner should exist");
    let fail_server = servers_by_id
        .get(&fail_server_id)
        .expect("fast-failure cell owner should exist");

    let mut slow_seed = counter_cell(schema.id, slow_id, 1, "counter_slow_seed");
    let slow_seed_header = slow_server
        .current_database()
        .chunks()
        .write_cell(&mut slow_seed)
        .unwrap();

    let mut fail_seed = counter_cell(schema.id, fail_id, 2, "counter_fail_seed");
    let fail_seed_header = fail_server
        .current_database()
        .chunks()
        .write_cell(&mut fail_seed)
        .unwrap();
    observe_distributed_seed_revisions(
        &servers[0],
        [slow_seed_header.revision_ts, fail_seed_header.revision_ts],
    );

    let txn = scoped_txn_client_for_database(addresses[0], group, group).await;
    let manager = servers[0].current_database().txn_manager().unwrap().clone();
    let tid = txn.begin().await.unwrap().unwrap();
    let slow_first = accepted_cell(txn.read(tid.clone(), slow_id).await.unwrap().unwrap());
    let fail_first = accepted_cell(txn.read(tid.clone(), fail_id).await.unwrap().unwrap());

    assert_eq!(score_of(&slow_first), 1);
    assert_eq!(score_of(&fail_first), 2);

    let fail_update = counter_cell(schema.id, fail_id, 9, "counter_fail_txn");
    assert_eq!(
        txn.update(tid.clone(), fail_update).await.unwrap().unwrap(),
        TxnExecResult::Accepted(())
    );

    let mut external_fail = counter_cell(schema.id, fail_id, 12, "counter_fail_external");
    fail_server
        .current_database()
        .chunks()
        .update_cell_at_revision(
            &mut external_fail,
            crate::ram::cell::RevisionWrite::committed(fail_first.header.revision_ts + 1_000),
        )
        .unwrap();
    assert!(external_fail.header.revision_ts > fail_first.header.revision_ts);

    let slow_prepare =
        transactions::data_site::install_prepare_delay_for_cell(tid.clone(), slow_id);
    let fast_failure = transactions::manager::install_prepare_result_observer(tid.clone(), fail_id);
    let abort_delay = transactions::manager::install_abort_entry_delay(tid);
    let prepare_client = txn.clone();
    let prepare_tid = tid.clone();
    let mut prepare_task =
        tokio::spawn(async move { prepare_client.prepare(prepare_tid).await.unwrap().unwrap() });

    slow_prepare.wait_until_entered().await;
    fast_failure.wait_until_observed().await;
    let early_prepare = timeout(Duration::from_millis(250), &mut prepare_task).await;
    slow_prepare.release();

    assert!(
        early_prepare.is_err(),
        "prepare returned before the delayed participant settled: {:?}",
        early_prepare
    );

    abort_delay.wait_until_entered().await;
    assert_eq!(
        transactions::data_site::participant_owner_for_test(slow_server_id, group, group, &slow_id,),
        Some(TxnPriority::new(tid, servers[0].server_id)),
        "the delayed successful participant must own its cell before failure cleanup"
    );
    assert_eq!(
        manager.cleanup_stale_transactions(-1),
        0,
        "stale cleanup must skip the partial-failure abort handoff"
    );
    assert!(
        matches!(
            txn.resolve(tid).await.unwrap().unwrap(),
            TxnResolution::InProgress | TxnResolution::Abort
        ),
        "resolution may advance only from dispatch intent to the explicit Abort choice"
    );
    abort_delay.release();

    let prepare_result = prepare_task.await.unwrap();
    assert_eq!(
        prepare_result,
        TMPrepareResult::DMPrepareError(DMPrepareResult::NotRealizable)
    );
    assert_eq!(
        servers[0]
            .current_database()
            .txn_manager()
            .unwrap()
            .transaction_count(),
        0
    );
    assert_eq!(
        transactions::data_site::participant_owner_for_test(slow_server_id, group, group, &slow_id,),
        None,
        "automatic abort/end must release the partially successful participant"
    );
    assert_eq!(
        txn.resolve(tid).await.unwrap().unwrap(),
        TxnResolution::Abort,
        "the completed partial-failure cleanup must resolve as explicit Abort"
    );

    let retry_tid = txn.begin().await.unwrap().unwrap();
    let retry_first = accepted_cell(txn.read(retry_tid.clone(), slow_id).await.unwrap().unwrap());
    assert_eq!(score_of(&retry_first), 1);

    let retry_update = counter_cell(schema.id, slow_id, 15, "counter_slow_retry");
    assert_eq!(
        txn.update(retry_tid.clone(), retry_update.clone())
            .await
            .unwrap()
            .unwrap(),
        TxnExecResult::Accepted(())
    );
    assert_eq!(
        txn.prepare(retry_tid.clone()).await.unwrap().unwrap(),
        TMPrepareResult::Success
    );
    assert!(
        matches!(
            txn.commit(retry_tid.clone()).await.unwrap().unwrap(),
            EndResult::Success | EndResult::SomeLocksNotReleased { .. }
        ),
        "retry transaction should commit after prepare cleanup"
    );

    let persisted_slow = slow_server
        .current_database()
        .chunks()
        .read_cell(&slow_id)
        .unwrap()
        .to_owned();
    let persisted_fail = fail_server
        .current_database()
        .chunks()
        .read_cell(&fail_id)
        .unwrap()
        .to_owned();
    assert_eq!(persisted_slow.data, retry_update.data);
    assert_eq!(persisted_fail.data, external_fail.data);

    for server in &servers {
        assert_eq!(
            server
                .current_database()
                .txn_manager()
                .unwrap()
                .transaction_count(),
            0
        );
    }

    for server in servers {
        server.shutdown().await;
    }
}

#[tokio::test(flavor = "multi_thread")]
async fn cancelled_prepare_future_still_settles_votes_and_cleans_up_in_background() {
    let _ = env_logger::try_init();
    let addresses = ["127.0.0.1:5362", "127.0.0.1:5363"];
    let group = "txn_occ_prepare_cancellation_cleanup";
    let servers = start_occ_test_cluster(&addresses, group).await;
    let schema = install_occ_schema_on_servers(&servers);
    let ((slow_id, slow_server_id), (fail_id, fail_server_id)) =
        ids_on_distinct_servers(&servers[0]);
    let servers_by_id: HashMap<u64, Arc<NebServer>> = servers
        .iter()
        .map(|server| (server.server_id, server.clone()))
        .collect();
    let slow_server = servers_by_id
        .get(&slow_server_id)
        .expect("slow cell owner should exist");
    let fail_server = servers_by_id
        .get(&fail_server_id)
        .expect("fast-failure cell owner should exist");

    let mut slow_seed = counter_cell(schema.id, slow_id, 1, "counter_cancel_slow_seed");
    let slow_seed_header = slow_server
        .current_database()
        .chunks()
        .write_cell(&mut slow_seed)
        .unwrap();

    let mut fail_seed = counter_cell(schema.id, fail_id, 2, "counter_cancel_fail_seed");
    let fail_seed_header = fail_server
        .current_database()
        .chunks()
        .write_cell(&mut fail_seed)
        .unwrap();
    observe_distributed_seed_revisions(
        &servers[0],
        [slow_seed_header.revision_ts, fail_seed_header.revision_ts],
    );

    let txn = scoped_txn_client_for_database(addresses[0], group, group).await;
    let manager = servers[0].current_database().txn_manager().unwrap().clone();
    let tid = txn.begin().await.unwrap().unwrap();
    let slow_first = accepted_cell(txn.read(tid.clone(), slow_id).await.unwrap().unwrap());
    let fail_first = accepted_cell(txn.read(tid.clone(), fail_id).await.unwrap().unwrap());

    assert_eq!(score_of(&slow_first), 1);
    assert_eq!(score_of(&fail_first), 2);

    let fail_update = counter_cell(schema.id, fail_id, 9, "counter_cancel_fail_txn");
    assert_eq!(
        txn.update(tid.clone(), fail_update).await.unwrap().unwrap(),
        TxnExecResult::Accepted(())
    );

    let mut external_fail = counter_cell(schema.id, fail_id, 12, "counter_cancel_fail_external");
    let external_header = fail_server
        .current_database()
        .chunks()
        .update_cell(&mut external_fail)
        .unwrap();
    assert!(external_header.revision_ts > fail_first.header.revision_ts);

    let slow_prepare =
        transactions::data_site::install_prepare_delay_for_cell(tid.clone(), slow_id);
    let fast_failure = transactions::manager::install_prepare_result_observer(tid.clone(), fail_id);
    let prepare_tid = tid.clone();
    let manager_for_prepare = manager.clone();
    let mut caller_prepare = tokio::spawn(async move {
        <transactions::manager::TransactionManager as transactions::manager::Service>::prepare(
            manager_for_prepare.as_ref(),
            prepare_tid,
        )
        .await
    });

    slow_prepare.wait_until_entered().await;
    fast_failure.wait_until_observed().await;
    let still_waiting = timeout(Duration::from_millis(250), &mut caller_prepare).await;
    assert!(
        still_waiting.is_err(),
        "prepare returned before the delayed participant was released: {:?}",
        still_waiting
    );

    caller_prepare.abort();
    let caller_join = caller_prepare.await;
    assert!(
        matches!(&caller_join, Err(error) if error.is_cancelled()),
        "caller-side prepare task should be cancelled, got {:?}",
        caller_join
    );

    slow_prepare.release();
    wait_for_transaction_count(&manager, 0, Duration::from_secs(2)).await;

    let retry_tid = txn.begin().await.unwrap().unwrap();
    let retry_first = accepted_cell(txn.read(retry_tid.clone(), slow_id).await.unwrap().unwrap());
    assert_eq!(score_of(&retry_first), 1);

    let retry_update = counter_cell(schema.id, slow_id, 15, "counter_cancel_slow_retry");
    assert_eq!(
        txn.update(retry_tid.clone(), retry_update.clone())
            .await
            .unwrap()
            .unwrap(),
        TxnExecResult::Accepted(())
    );
    assert_eq!(
        txn.prepare(retry_tid.clone()).await.unwrap().unwrap(),
        TMPrepareResult::Success
    );
    assert!(
        matches!(
            txn.commit(retry_tid.clone()).await.unwrap().unwrap(),
            EndResult::Success | EndResult::SomeLocksNotReleased { .. }
        ),
        "retry transaction should commit after background cleanup"
    );

    let persisted_slow = slow_server
        .current_database()
        .chunks()
        .read_cell(&slow_id)
        .unwrap()
        .to_owned();
    let persisted_fail = fail_server
        .current_database()
        .chunks()
        .read_cell(&fail_id)
        .unwrap()
        .to_owned();
    assert_eq!(persisted_slow.data, retry_update.data);
    assert_eq!(persisted_fail.data, external_fail.data);

    for server in &servers {
        assert_eq!(
            server
                .current_database()
                .txn_manager()
                .unwrap()
                .transaction_count(),
            0
        );
    }

    for server in servers {
        server.shutdown().await;
    }
}

#[tokio::test(flavor = "multi_thread")]
async fn cancelled_successful_prepare_rolls_back_when_response_is_not_delivered() {
    let _ = env_logger::try_init();
    let address = "127.0.0.1:5364";
    let group = "txn_occ_prepare_success_cancellation_cleanup";
    let server = start_occ_test_server(address, group).await;
    let runtime = server.current_database();
    let schema = install_occ_schema(&runtime);
    let cell_id = Id::new(0, 90114);

    let mut initial = counter_cell(schema.id, cell_id, 1, "counter_cancel_success_seed");
    runtime.chunks().write_cell(&mut initial).unwrap();

    let txn = scoped_txn_client_for_database(address, group, group).await;
    let manager = runtime.txn_manager().unwrap().clone();
    let tid = txn.begin().await.unwrap().unwrap();
    let first = accepted_cell(txn.read(tid.clone(), cell_id).await.unwrap().unwrap());
    assert_eq!(score_of(&first), 1);

    let pending = counter_cell(schema.id, cell_id, 9, "counter_cancel_success_pending");
    assert_eq!(
        txn.update(tid.clone(), pending).await.unwrap().unwrap(),
        TxnExecResult::Accepted(())
    );

    let delayed_prepare =
        transactions::data_site::install_prepare_delay_for_cell(tid.clone(), cell_id);
    let prepare_tid = tid.clone();
    let manager_for_prepare = manager.clone();
    let caller_prepare = tokio::spawn(async move {
        <transactions::manager::TransactionManager as transactions::manager::Service>::prepare(
            manager_for_prepare.as_ref(),
            prepare_tid,
        )
        .await
    });

    delayed_prepare.wait_until_entered().await;
    caller_prepare.abort();
    assert!(caller_prepare.await.unwrap_err().is_cancelled());
    delayed_prepare.release();

    wait_for_transaction_count(&manager, 0, Duration::from_secs(2)).await;
    let rolled_back = runtime.chunks().read_cell(&cell_id).unwrap().to_owned();
    assert_eq!(score_of(&rolled_back), 1);
    assert!(rolled_back.header.revision_ts > first.header.revision_ts);

    let retry_tid = txn.begin().await.unwrap().unwrap();
    let retry_first = accepted_cell(txn.read(retry_tid.clone(), cell_id).await.unwrap().unwrap());
    assert_eq!(score_of(&retry_first), 1);
    let retry = counter_cell(schema.id, cell_id, 15, "counter_cancel_success_retry");
    assert_eq!(
        txn.update(retry_tid.clone(), retry.clone())
            .await
            .unwrap()
            .unwrap(),
        TxnExecResult::Accepted(())
    );
    assert_eq!(
        txn.prepare(retry_tid.clone()).await.unwrap().unwrap(),
        TMPrepareResult::Success
    );
    assert!(matches!(
        txn.commit(retry_tid).await.unwrap().unwrap(),
        EndResult::Success | EndResult::SomeLocksNotReleased { .. }
    ));
    assert_eq!(
        runtime
            .chunks()
            .read_cell(&cell_id)
            .unwrap()
            .to_owned()
            .data,
        retry.data
    );

    server.shutdown().await;
}

#[tokio::test(flavor = "multi_thread")]
async fn volatile_end_response_loss_waits_for_retirement_before_starting_ttl() {
    let address = "127.0.0.1:5484";
    let group = "txn_occ_volatile_end_response_retirement";
    let server = start_occ_test_server(address, group).await;
    let runtime = server.current_database();
    let schema = install_occ_schema(&runtime);
    let cell_id = Id::new(0, 90_184);
    let mut initial = counter_cell(schema.id, cell_id, 1, "volatile-end-loss-seed");
    runtime.chunks().write_cell(&mut initial).unwrap();
    let txn = scoped_txn_client_for_database(address, group, group).await;
    let manager = runtime.txn_manager().unwrap().clone();
    let tid = txn.begin().await.unwrap().unwrap();
    let _ = accepted_cell(txn.read(tid, cell_id).await.unwrap().unwrap());
    assert_eq!(
        txn.update(
            tid,
            counter_cell(schema.id, cell_id, 2, "volatile-end-loss-update"),
        )
        .await
        .unwrap()
        .unwrap(),
        TxnExecResult::Accepted(())
    );
    assert_eq!(
        txn.prepare(tid).await.unwrap().unwrap(),
        TMPrepareResult::Success
    );

    let participant_clock = transactions::data_site::install_participant_clock_for_test(
        server.server_id,
        group,
        group,
        tid,
        bifrost::utils::time::get_time(),
    )
    .expect("volatile participant manager must be registered");
    manager.drop_next_end_response_for_test();
    assert_eq!(
        txn.abort(tid).await.unwrap(),
        Err(TMError::RPCErrorFromCellServer)
    );
    assert_eq!(
        transactions::data_site::participant_completion_for_test(
            server.server_id,
            group,
            group,
            &tid,
        ),
        Some((TxnState::Aborted, None)),
        "a lost volatile end response must leave unexpired completion evidence"
    );

    participant_clock.advance_by(300_001);
    assert_eq!(
        transactions::data_site::participant_completion_for_test(
            server.server_id,
            group,
            group,
            &tid,
        ),
        Some((TxnState::Aborted, None)),
        "advancing beyond 300s must not expire unfinalized participant evidence"
    );
    let data_site = scoped_data_site_client_for_database(address, group, group).await;
    assert!(
        matches!(
            data_site
            .prepare(server.server_id, tid, tid, vec![])
            .await
            .unwrap()
            .payload,
            DMPrepareResult::StateError(TxnState::Aborted) | DMPrepareResult::NotRealizable
        ),
        "a delayed duplicate prepare must remain rejected after more than 300s while retirement is unfinalized"
    );
    let retry_result = txn.abort(tid).await.unwrap().unwrap();
    assert!(
        matches!(
            retry_result,
            AbortResult::Success(None) | AbortResult::CheckFailed(CheckError::AlreadyCleanup)
        ),
        "abort retry after lost end response returned {retry_result:?}"
    );
    timeout(Duration::from_secs(2), async {
        loop {
            if transactions::data_site::participant_completion_for_test(
                server.server_id,
                group,
                group,
                &tid,
            )
            .is_some_and(|(outcome, expires_at_ms)| {
                outcome == TxnState::Aborted
                    && expires_at_ms
                        .is_some_and(|deadline| deadline > bifrost::utils::time::get_time())
            }) {
                break;
            }
            tokio::time::sleep(Duration::from_millis(10)).await;
        }
    })
    .await
    .expect("coordinator retirement must finalize volatile evidence asynchronously");
    let (_, expires_at_ms) = transactions::data_site::participant_completion_for_test(
        server.server_id,
        group,
        group,
        &tid,
    )
    .expect("finalized participant completion evidence");
    let expires_at_ms = expires_at_ms.expect("finalize must start the local TTL");
    assert_eq!(
        transactions::data_site::participant_completion_outcome_at_for_test(
            server.server_id,
            group,
            group,
            &tid,
            expires_at_ms - 1,
        ),
        Some(TxnState::Aborted),
        "participant evidence must remain at local acceptance + 299,999ms"
    );
    assert_eq!(
        transactions::data_site::participant_completion_outcome_at_for_test(
            server.server_id,
            group,
            group,
            &tid,
            expires_at_ms,
        ),
        None,
        "participant evidence must expire exactly at local acceptance + 300,000ms"
    );

    server.shutdown().await;
}

#[tokio::test(flavor = "multi_thread")]
async fn abort_queued_behind_commit_reports_already_cleanup() {
    let _ = env_logger::try_init();
    let address = "127.0.0.1:5365";
    let group = "txn_occ_commit_abort_cleanup_race";
    let server = start_occ_test_server(address, group).await;
    let runtime = server.current_database();
    let schema = install_occ_schema(&runtime);
    let cell_id = Id::new(0, 90115);

    let mut initial = counter_cell(schema.id, cell_id, 1, "commit-abort-seed");
    runtime.chunks().write_cell(&mut initial).unwrap();

    let txn = scoped_txn_client_for_database(address, group, group).await;
    let tid = txn.begin().await.unwrap().unwrap();
    let first = accepted_cell(txn.read(tid.clone(), cell_id).await.unwrap().unwrap());
    let committed = counter_cell(schema.id, cell_id, 9, "commit-abort-committed");
    assert_eq!(
        txn.update(tid.clone(), committed.clone())
            .await
            .unwrap()
            .unwrap(),
        TxnExecResult::Accepted(())
    );
    assert_eq!(
        txn.prepare(tid.clone()).await.unwrap().unwrap(),
        TMPrepareResult::Success
    );

    let abort_delay = transactions::manager::install_abort_entry_delay(tid.clone());
    let abort_client = txn.clone();
    let abort_tid = tid.clone();
    let abort_task =
        tokio::spawn(async move { abort_client.abort(abort_tid).await.unwrap().unwrap() });
    abort_delay.wait_until_entered().await;

    assert_eq!(txn.commit(tid).await.unwrap().unwrap(), EndResult::Success);
    abort_delay.release();
    assert_eq!(
        abort_task.await.unwrap(),
        AbortResult::CheckFailed(CheckError::AlreadyCleanup)
    );
    assert_eq!(runtime.txn_manager().unwrap().transaction_count(), 0);

    let persisted = runtime.chunks().read_cell(&cell_id).unwrap().to_owned();
    assert_eq!(persisted.data, committed.data);
    assert!(persisted.header.revision_ts > first.header.revision_ts);

    server.shutdown().await;
}

#[tokio::test(flavor = "multi_thread")]
async fn partial_commit_end_failure_retains_coordinator_state_until_retry_completes() {
    let _ = env_logger::try_init();
    let addresses = ["127.0.0.1:5421", "127.0.0.1:5422"];
    let group = "txn_occ_partial_commit_end_retry";
    let servers = start_occ_test_cluster(&addresses, group).await;
    let schema = install_occ_schema_on_servers(&servers);
    let ((success_id, success_server_id), (fail_id, fail_server_id)) =
        ids_on_distinct_servers(&servers[0]);
    let servers_by_id: HashMap<u64, Arc<NebServer>> = servers
        .iter()
        .map(|server| (server.server_id, server.clone()))
        .collect();

    let mut success_seed = counter_cell(schema.id, success_id, 1, "partial-commit-success-seed");
    let success_seed_header = servers_by_id[&success_server_id]
        .current_database()
        .chunks()
        .write_cell(&mut success_seed)
        .unwrap();
    let mut fail_seed = counter_cell(schema.id, fail_id, 2, "partial-commit-fail-seed");
    let fail_seed_header = servers_by_id[&fail_server_id]
        .current_database()
        .chunks()
        .write_cell(&mut fail_seed)
        .unwrap();
    observe_distributed_seed_revisions(
        &servers[0],
        [
            success_seed_header.revision_ts,
            fail_seed_header.revision_ts,
        ],
    );

    let txn = scoped_txn_client_for_database(addresses[0], group, group).await;
    let manager = servers[0].current_database().txn_manager().unwrap().clone();
    let tid = txn.begin().await.unwrap().unwrap();
    assert_eq!(
        txn.update(
            tid,
            counter_cell(schema.id, success_id, 11, "partial-commit-success-update"),
        )
        .await
        .unwrap()
        .unwrap(),
        TxnExecResult::Accepted(())
    );
    assert_eq!(
        txn.update(
            tid,
            counter_cell(schema.id, fail_id, 12, "partial-commit-fail-update"),
        )
        .await
        .unwrap()
        .unwrap(),
        TxnExecResult::Accepted(())
    );
    assert_eq!(
        txn.prepare(tid).await.unwrap().unwrap(),
        TMPrepareResult::Success
    );

    let failure = transactions::data_site::install_end_promotion_failure(tid, fail_id);
    assert_eq!(
        txn.commit(tid).await.unwrap().unwrap(),
        EndResult::CheckFailed(CheckError::CannotEnd)
    );
    assert_eq!(
        manager.transaction_count(),
        1,
        "a chosen commit with one unresolved participant must remain retryable"
    );
    assert_eq!(
        transactions::data_site::participant_owner_for_test(
            success_server_id,
            group,
            group,
            &success_id,
        ),
        None,
        "the participant that durably completed may release its owner"
    );
    assert_eq!(
        transactions::data_site::participant_owner_for_test(fail_server_id, group, group, &fail_id,),
        Some(TxnPriority::new(tid, servers[0].server_id)),
        "the unresolved participant must retain its owner"
    );

    drop(failure);
    assert_eq!(txn.commit(tid).await.unwrap().unwrap(), EndResult::Success);
    assert_eq!(manager.transaction_count(), 0);
    assert_eq!(
        score_of(
            &servers_by_id[&success_server_id]
                .current_database()
                .chunks()
                .read_cell(&success_id)
                .unwrap()
                .to_owned()
        ),
        11
    );
    assert_eq!(
        score_of(
            &servers_by_id[&fail_server_id]
                .current_database()
                .chunks()
                .read_cell(&fail_id)
                .unwrap()
                .to_owned()
        ),
        12
    );

    for server in servers {
        server.shutdown().await;
    }
}

#[tokio::test(flavor = "multi_thread")]
async fn coordinator_decision_marker_failure_keeps_commit_irrevocable_and_retryable() {
    let _ = env_logger::try_init();
    let storage = TempDir::new().unwrap();
    let address = "127.0.0.1:5430";
    let group = "txn_occ_coordinator_decision_retry";
    let server = start_durable_occ_test_server(address, group, &storage).await;
    let runtime = server.current_database();
    let manager = runtime.txn_manager().unwrap().clone();
    let schema = install_occ_schema(&runtime);
    let cell_id = Id::new(0, 90_170);
    let mut original = counter_cell(schema.id, cell_id, 1, "decision-retry-original");
    runtime.chunks().write_cell(&mut original).unwrap();

    let txn = scoped_txn_client_for_database(address, group, group).await;
    let tid = txn.begin().await.unwrap().unwrap();
    assert_eq!(
        txn.update(
            tid,
            counter_cell(schema.id, cell_id, 2, "decision-retry-committed"),
        )
        .await
        .unwrap()
        .unwrap(),
        TxnExecResult::Accepted(())
    );
    assert_eq!(
        txn.prepare(tid).await.unwrap().unwrap(),
        TMPrepareResult::Success
    );

    runtime
        .undo_log()
        .unwrap()
        .fail_next_coordinator_commit_decision_for_test();
    assert_eq!(
        txn.commit(tid).await.unwrap().unwrap(),
        EndResult::CheckFailed(CheckError::CannotEnd)
    );
    assert_eq!(
        txn.resolve(tid).await.unwrap().unwrap(),
        TxnResolution::InProgress,
        "participants must not expose Commit until the global record is durable"
    );
    assert_eq!(
        txn.abort(tid).await.unwrap().unwrap(),
        AbortResult::CheckFailed(CheckError::AlreadyCommitted),
        "an attempted coordinator commit record makes the choice irrevocable"
    );
    assert_eq!(
        runtime
            .undo_log()
            .unwrap()
            .coordinator_decision(&tid)
            .unwrap(),
        Some(TxnResolution::InProgress),
        "the injected final-decision failure preserves the durable pre-dispatch intent"
    );

    let end_failure = transactions::data_site::install_end_promotion_failure(tid, cell_id);
    assert_eq!(
        txn.commit(tid).await.unwrap().unwrap(),
        EndResult::CheckFailed(CheckError::CannotEnd)
    );
    assert!(matches!(
        runtime
            .undo_log()
            .unwrap()
            .coordinator_decision(&tid)
            .unwrap(),
        Some(TxnResolution::Commit(_))
    ));
    assert!(matches!(
        txn.resolve(tid).await.unwrap().unwrap(),
        TxnResolution::Commit(_)
    ));
    manager.forget_transaction_for_test(&tid);
    drop(end_failure);
    assert_eq!(
        txn.commit(tid).await.unwrap().unwrap(),
        EndResult::Success,
        "a caller retry after coordinator restart must replay unresolved participants from the durable decision"
    );
    assert_eq!(
        score_of(&runtime.chunks().read_cell(&cell_id).unwrap().to_owned()),
        2
    );

    server.shutdown().await;
}

#[tokio::test(flavor = "multi_thread")]
async fn coordinator_abort_marker_failure_defers_compensation_until_retry_is_durable() {
    let _ = env_logger::try_init();
    let storage = TempDir::new().unwrap();
    let address = "127.0.0.1:5433";
    let group = "txn_occ_coordinator_abort_decision_retry";
    let server = start_durable_occ_test_server(address, group, &storage).await;
    let runtime = server.current_database();
    let manager = runtime.txn_manager().unwrap().clone();
    let schema = install_occ_schema(&runtime);
    let cell_id = Id::new(0, 90_171);
    let mut original = counter_cell(schema.id, cell_id, 1, "abort-decision-original");
    runtime.chunks().write_cell(&mut original).unwrap();

    let txn = scoped_txn_client_for_database(address, group, group).await;
    let tid = txn.begin().await.unwrap().unwrap();
    assert_eq!(
        txn.update(
            tid,
            counter_cell(schema.id, cell_id, 2, "abort-decision-pending"),
        )
        .await
        .unwrap()
        .unwrap(),
        TxnExecResult::Accepted(())
    );
    assert_eq!(
        txn.prepare(tid).await.unwrap().unwrap(),
        TMPrepareResult::Success
    );

    runtime
        .undo_log()
        .unwrap()
        .fail_next_coordinator_abort_decision_for_test();
    let abort_failure =
        transactions::data_site::install_persistent_abort_cannot_end_for_cell(tid, cell_id);
    assert_eq!(
        txn.abort(tid).await.unwrap().unwrap(),
        AbortResult::CheckFailed(CheckError::CannotEnd)
    );
    assert!(
        matches!(
            txn.resolve(tid).await.unwrap().unwrap(),
            TxnResolution::InProgress | TxnResolution::Abort
        ),
        "resolution may advance only from dispatch intent to durable Abort"
    );
    assert_eq!(
        txn.commit(tid).await.unwrap(),
        Err(TMError::InvalidTransactionState(TxnState::Aborted)),
        "the in-memory Abort choice is irrevocable even while its record retries"
    );
    assert!(
        matches!(
            runtime
                .undo_log()
                .unwrap()
                .coordinator_decision(&tid)
                .unwrap(),
            Some(TxnResolution::InProgress | TxnResolution::Abort)
        ),
        "the pre-dispatch intent remains fail-closed until background Abort persistence succeeds"
    );
    assert!(
        runtime
            .undo_log()
            .unwrap()
            .recover()
            .unwrap()
            .contains_key(&tid),
        "failed decision persistence must leave participant undo unresolved"
    );

    timeout(Duration::from_secs(2), async {
        loop {
            if runtime
                .undo_log()
                .unwrap()
                .coordinator_decision(&tid)
                .unwrap()
                == Some(TxnResolution::Abort)
            {
                break;
            }
            tokio::time::sleep(Duration::from_millis(10)).await;
        }
    })
    .await
    .expect("background cleanup must retry the failed Abort decision");
    assert_eq!(
        runtime
            .undo_log()
            .unwrap()
            .coordinator_decision(&tid)
            .unwrap(),
        Some(TxnResolution::Abort)
    );
    manager.forget_transaction_for_test(&tid);
    drop(abort_failure);
    assert_eq!(
        txn.abort(tid).await.unwrap().unwrap(),
        AbortResult::Success(None),
        "a caller retry after coordinator restart must replay durable Abort participants"
    );
    assert_eq!(
        txn.resolve(tid).await.unwrap().unwrap(),
        TxnResolution::Abort
    );
    assert_eq!(
        score_of(&runtime.chunks().read_cell(&cell_id).unwrap().to_owned()),
        1
    );

    server.shutdown().await;
}

#[tokio::test(flavor = "multi_thread")]
async fn background_abort_retries_decision_and_participant_cleanup_after_caller_disappears() {
    let _ = env_logger::try_init();
    let storage = TempDir::new().unwrap();
    let address = "127.0.0.1:5435";
    let group = "txn_occ_background_abort_retry";
    let server = start_durable_occ_test_server(address, group, &storage).await;
    let runtime = server.current_database();
    let manager = runtime.txn_manager().unwrap().clone();
    let schema = install_occ_schema(&runtime);
    let cell_id = Id::new(0, 90_172);
    let mut original = counter_cell(schema.id, cell_id, 1, "background-abort-original");
    runtime.chunks().write_cell(&mut original).unwrap();

    let txn = scoped_txn_client_for_database(address, group, group).await;
    let tid = txn.begin().await.unwrap().unwrap();
    assert_eq!(
        txn.update(
            tid,
            counter_cell(schema.id, cell_id, 2, "background-abort-pending"),
        )
        .await
        .unwrap()
        .unwrap(),
        TxnExecResult::Accepted(())
    );
    assert_eq!(
        txn.prepare(tid).await.unwrap().unwrap(),
        TMPrepareResult::Success
    );
    assert_eq!(
        transactions::data_site::participant_owner_for_test(
            server.server_id,
            group,
            group,
            &cell_id,
        ),
        Some(TxnPriority::new(tid, server.server_id))
    );

    runtime
        .undo_log()
        .unwrap()
        .fail_next_coordinator_abort_decision_for_test();
    let participant_failure =
        transactions::data_site::install_persistent_abort_cannot_end_for_cell(tid, cell_id);
    assert_eq!(
        txn.abort(tid).await.unwrap().unwrap(),
        AbortResult::CheckFailed(CheckError::CannotEnd),
        "the caller observes the injected first decision persistence failure"
    );
    assert_eq!(
        txn.resolve(tid).await.unwrap().unwrap(),
        TxnResolution::InProgress
    );

    timeout(Duration::from_secs(3), async {
        loop {
            if runtime
                .undo_log()
                .unwrap()
                .coordinator_decision(&tid)
                .unwrap()
                == Some(TxnResolution::Abort)
            {
                break;
            }
            tokio::time::sleep(Duration::from_millis(10)).await;
        }
    })
    .await
    .expect("background cleanup must retry the transient Abort decision failure");
    assert_eq!(
        transactions::data_site::participant_owner_for_test(
            server.server_id,
            group,
            group,
            &cell_id,
        ),
        Some(TxnPriority::new(tid, server.server_id)),
        "the injected first participant cleanup failure must retain ownership"
    );

    drop(participant_failure);
    timeout(Duration::from_secs(3), async {
        loop {
            let completed = matches!(
                runtime
                    .undo_log()
                    .unwrap()
                    .coordinator_status(&tid)
                    .unwrap(),
                Some(undo_log::CoordinatorStatus::Completed(ref record))
                    if record.resolution == TxnResolution::Abort
            );
            if completed
                && manager.transaction_count() == 0
                && transactions::data_site::participant_owner_for_test(
                    server.server_id,
                    group,
                    group,
                    &cell_id,
                )
                .is_none()
            {
                break;
            }
            tokio::time::sleep(Duration::from_millis(10)).await;
        }
    })
    .await
    .expect("background cleanup must retry participant abort/end and final completion");
    assert_eq!(
        score_of(&runtime.chunks().read_cell(&cell_id).unwrap().to_owned()),
        1
    );

    server.shutdown().await;
}

#[tokio::test(flavor = "multi_thread")]
async fn durable_dispatch_intent_is_rediscovered_and_aborted_without_a_live_coordinator() {
    let _ = env_logger::try_init();
    let storage = TempDir::new().unwrap();
    let address = "127.0.0.1:5436";
    let group = "txn_occ_dispatch_intent_restart_abort";
    let server = start_durable_occ_test_server(address, group, &storage).await;
    let runtime = server.current_database();
    let manager = runtime.txn_manager().unwrap().clone();
    let schema = install_occ_schema(&runtime);
    let cell_id = Id::new(0, 90_173);
    let mut original = counter_cell(schema.id, cell_id, 1, "intent-restart-original");
    runtime.chunks().write_cell(&mut original).unwrap();

    let txn = scoped_txn_client_for_database(address, group, group).await;
    let tid = txn.begin().await.unwrap().unwrap();
    assert_eq!(
        txn.update(
            tid,
            counter_cell(schema.id, cell_id, 2, "intent-restart-pending"),
        )
        .await
        .unwrap()
        .unwrap(),
        TxnExecResult::Accepted(())
    );
    assert_eq!(
        txn.prepare(tid).await.unwrap().unwrap(),
        TMPrepareResult::Success
    );
    assert_eq!(
        runtime
            .undo_log()
            .unwrap()
            .coordinator_decision(&tid)
            .unwrap(),
        Some(TxnResolution::InProgress),
        "the exact participant target set must be durable before dispatch"
    );
    assert_eq!(
        transactions::data_site::participant_owner_for_test(
            server.server_id,
            group,
            group,
            &cell_id,
        ),
        Some(TxnPriority::new(tid, server.server_id))
    );

    manager.forget_transaction_for_test(&tid);
    timeout(Duration::from_secs(3), async {
        loop {
            let completed = matches!(
                runtime
                    .undo_log()
                    .unwrap()
                    .coordinator_status(&tid)
                    .unwrap(),
                Some(undo_log::CoordinatorStatus::Completed(ref record))
                    if record.resolution == TxnResolution::Abort
            );
            if completed
                && transactions::data_site::participant_owner_for_test(
                    server.server_id,
                    group,
                    group,
                    &cell_id,
                )
                .is_none()
            {
                break;
            }
            tokio::time::sleep(Duration::from_millis(10)).await;
        }
    })
    .await
    .expect("maintenance must rediscover and complete the abandoned durable dispatch intent");
    assert_eq!(
        score_of(&runtime.chunks().read_cell(&cell_id).unwrap().to_owned()),
        1
    );

    server.shutdown().await;
}

#[tokio::test(flavor = "multi_thread")]
async fn undispatched_intent_target_not_existed_completes_without_retirement_obligation() {
    let _ = env_logger::try_init();
    let storage = TempDir::new().unwrap();
    let address = "127.0.0.1:5437";
    let group = "txn_occ_undispatched_intent_absence";
    let server = start_durable_occ_test_server(address, group, &storage).await;
    let runtime = server.current_database();
    let tid = server.hlc.now();
    runtime
        .undo_log()
        .unwrap()
        .write_coordinator_dispatch_intent(&tid, &[server.server_id])
        .unwrap();

    let completion = timeout(Duration::from_secs(3), async {
        loop {
            if let Some(undo_log::CoordinatorStatus::Completed(record)) = runtime
                .undo_log()
                .unwrap()
                .coordinator_status(&tid)
                .unwrap()
            {
                break record;
            }
            tokio::time::sleep(Duration::from_millis(10)).await;
        }
    })
    .await
    .expect("a conclusively absent participant must not cause an infinite Abort cleanup loop");
    assert_eq!(completion.resolution, TxnResolution::Abort);
    assert!(
        completion.participants.is_empty(),
        "NotExisted proves cleanup but creates no participant retirement evidence"
    );
    assert!(runtime
        .undo_log()
        .unwrap()
        .coordinator_retirement_candidates(8)
        .unwrap()
        .is_empty());

    server.shutdown().await;
}

#[tokio::test(flavor = "multi_thread")]
async fn restarted_unended_participant_resolves_durable_commit_from_remote_coordinator() {
    let _ = env_logger::try_init();
    let storage = TempDir::new().unwrap();
    let addresses = ["127.0.0.1:5431", "127.0.0.1:5432"];
    let group = "txn_occ_remote_participant_restart_resolution";
    let mut servers = start_durable_occ_test_cluster(&addresses, group, storage.path()).await;
    let schema = install_occ_schema_on_servers(&servers);
    let coordinator = servers[0].clone();
    let coordinator_id = coordinator.server_id;
    let ((first_id, first_server_id), (second_id, second_server_id)) =
        ids_on_stably_routed_distinct_servers(&servers).await;
    let ((local_id, local_server_id), (remote_id, remote_server_id)) =
        if first_server_id == coordinator_id {
            ((first_id, first_server_id), (second_id, second_server_id))
        } else {
            ((second_id, second_server_id), (first_id, first_server_id))
        };
    assert_eq!(local_server_id, coordinator_id);
    assert_ne!(remote_server_id, coordinator_id);
    let local_server = servers
        .iter()
        .find(|server| server.server_id == local_server_id)
        .unwrap()
        .clone();
    let remote_index = servers
        .iter()
        .position(|server| server.server_id == remote_server_id)
        .unwrap();
    let remote_server = servers[remote_index].clone();

    let mut local_seed = counter_cell(schema.id, local_id, 1, "restart-local-seed");
    let local_header = local_server
        .current_database()
        .chunks()
        .write_cell(&mut local_seed)
        .unwrap();
    let mut remote_seed = counter_cell(schema.id, remote_id, 2, "restart-remote-seed");
    let remote_header = remote_server
        .current_database()
        .chunks()
        .write_cell(&mut remote_seed)
        .unwrap();
    observe_distributed_seed_revisions(
        &coordinator,
        [local_header.revision_ts, remote_header.revision_ts],
    );

    let txn = scoped_txn_client_for_database(addresses[0], group, group).await;
    let tid = txn.begin().await.unwrap().unwrap();
    assert_eq!(
        txn.update(
            tid,
            counter_cell(schema.id, local_id, 11, "restart-local-commit"),
        )
        .await
        .unwrap()
        .unwrap(),
        TxnExecResult::Accepted(())
    );
    assert_eq!(
        txn.update(
            tid,
            counter_cell(schema.id, remote_id, 12, "restart-remote-commit"),
        )
        .await
        .unwrap()
        .unwrap(),
        TxnExecResult::Accepted(())
    );
    assert_eq!(
        txn.prepare(tid).await.unwrap().unwrap(),
        TMPrepareResult::Success
    );

    remote_server
        .current_database()
        .undo_log()
        .unwrap()
        .fail_next_commit_marker_for_test();
    assert_eq!(
        txn.commit(tid).await.unwrap().unwrap(),
        EndResult::CheckFailed(CheckError::CannotEnd)
    );
    assert!(matches!(
        coordinator
            .current_database()
            .undo_log()
            .unwrap()
            .coordinator_decision(&tid)
            .unwrap(),
        Some(TxnResolution::Commit(_))
    ));
    assert!(
        remote_server
            .current_database()
            .undo_log()
            .unwrap()
            .recover()
            .unwrap()
            .contains_key(&tid),
        "the remote participant must still have unresolved undo before restart"
    );

    let remote_address_index = remote_index;
    let stopped_remote = servers.remove(remote_index);
    stopped_remote.shutdown().await;
    drop(stopped_remote);
    drop(remote_server);

    let meta_servers = addresses
        .iter()
        .map(|address| address.to_string())
        .collect::<Vec<_>>();
    let mut restart_opts = durable_occ_cluster_options(
        &storage
            .path()
            .join(format!("server-{remote_address_index}")),
        true,
    );
    // The data/undo roots are the crash image under test. Rejoin the live
    // metadata cluster with a fresh local Raft runtime so graceful shutdown of
    // the old process cannot leave a stale root-membership snapshot.
    restart_opts.raft_storage = Some(
        storage
            .path()
            .join(format!("server-{remote_address_index}/raft-restart"))
            .to_string_lossy()
            .into_owned(),
    );
    let restarted_remote = NebServer::new_cluster_from_opts(
        &restart_opts,
        addresses[remote_address_index],
        &meta_servers,
        group,
        async |_| {},
    )
    .await
    .expect("remote participant startup must resolve Commit from the live coordinator");
    restarted_remote
        .current_database()
        .meta()
        .schemas
        .debug_only_new_schema(schema.clone());
    assert!(
        !restarted_remote
            .current_database()
            .undo_log()
            .unwrap()
            .recover()
            .unwrap()
            .contains_key(&tid),
        "startup must durably complete the participant outcome before exposure"
    );

    assert_eq!(
        txn.commit(tid).await.unwrap().unwrap(),
        EndResult::Success,
        "the coordinator retry must finish only the restarted unresolved participant"
    );
    assert_eq!(
        coordinator
            .current_database()
            .txn_manager()
            .unwrap()
            .transaction_count(),
        0
    );
    assert_eq!(
        score_of(
            &restarted_remote
                .current_database()
                .chunks()
                .read_cell(&remote_id)
                .unwrap()
                .to_owned()
        ),
        12
    );

    restarted_remote.shutdown().await;
    for server in servers {
        server.shutdown().await;
    }
}

#[tokio::test(flavor = "multi_thread")]
async fn partial_abort_failure_retains_all_site_owners_until_retry_resolves_every_site() {
    let _ = env_logger::try_init();
    let addresses = ["127.0.0.1:5366", "127.0.0.1:5367"];
    let group = "txn_occ_partial_abort_retry";
    let servers = start_occ_test_cluster(&addresses, group).await;
    let schema = install_occ_schema_on_servers(&servers);
    let ((success_id, success_server_id), (fail_id, fail_server_id)) =
        ids_on_distinct_servers(&servers[0]);
    let servers_by_id: HashMap<u64, Arc<NebServer>> = servers
        .iter()
        .map(|server| (server.server_id, server.clone()))
        .collect();
    let success_server = servers_by_id
        .get(&success_server_id)
        .expect("successful abort participant should exist");
    let fail_server = servers_by_id
        .get(&fail_server_id)
        .expect("failing abort participant should exist");

    let mut success_seed = counter_cell(schema.id, success_id, 1, "partial-abort-success-seed");
    let success_seed_header = success_server
        .current_database()
        .chunks()
        .write_cell(&mut success_seed)
        .unwrap();
    let mut fail_seed = counter_cell(schema.id, fail_id, 2, "partial-abort-fail-seed");
    let fail_seed_header = fail_server
        .current_database()
        .chunks()
        .write_cell(&mut fail_seed)
        .unwrap();
    observe_distributed_seed_revisions(
        &servers[0],
        [
            success_seed_header.revision_ts,
            fail_seed_header.revision_ts,
        ],
    );

    let txn = scoped_txn_client_for_database(addresses[0], group, group).await;
    let manager = servers[0].current_database().txn_manager().unwrap().clone();
    let tid = txn.begin().await.unwrap().unwrap();
    let success_first = accepted_cell(txn.read(tid.clone(), success_id).await.unwrap().unwrap());
    let fail_first = accepted_cell(txn.read(tid.clone(), fail_id).await.unwrap().unwrap());
    assert_eq!(score_of(&success_first), 1);
    assert_eq!(score_of(&fail_first), 2);

    assert_eq!(
        txn.update(
            tid.clone(),
            counter_cell(schema.id, success_id, 11, "partial-abort-success-update"),
        )
        .await
        .unwrap()
        .unwrap(),
        TxnExecResult::Accepted(())
    );
    assert_eq!(
        txn.update(
            tid.clone(),
            counter_cell(schema.id, fail_id, 12, "partial-abort-fail-update"),
        )
        .await
        .unwrap()
        .unwrap(),
        TxnExecResult::Accepted(())
    );
    assert_eq!(
        txn.prepare(tid.clone()).await.unwrap().unwrap(),
        TMPrepareResult::Success
    );

    let forced_failure =
        transactions::data_site::install_persistent_abort_cannot_end_for_cell(tid.clone(), fail_id);
    assert_eq!(
        txn.abort(tid.clone()).await.unwrap().unwrap(),
        AbortResult::CheckFailed(CheckError::CannotEnd)
    );
    assert_eq!(
        manager.transaction_count(),
        1,
        "a partial abort failure must retain coordinator state for retry"
    );
    assert_eq!(
        txn.commit(tid.clone()).await.unwrap(),
        Err(TMError::InvalidTransactionState(TxnState::Aborted)),
        "an accepted abort decision must make commit permanently illegal"
    );

    let expected_owner = Some(TxnPriority::new(tid, servers[0].server_id));
    assert_eq!(
        transactions::data_site::participant_owner_for_test(
            success_server_id,
            group,
            group,
            &success_id,
        ),
        expected_owner,
        "a compensated participant must retain its owner until every participant aborts"
    );
    assert_eq!(
        transactions::data_site::participant_owner_for_test(fail_server_id, group, group, &fail_id,),
        expected_owner,
        "the unresolved participant must retain its owner"
    );

    let probe_tid = txn.begin().await.unwrap().unwrap();
    assert!(
        timeout(Duration::from_millis(250), txn.read(probe_tid, success_id),)
            .await
            .is_err(),
        "the compensated participant must remain behind the owner barrier"
    );
    abort_txn(&txn, probe_tid).await;

    drop(forced_failure);
    let retry_result = txn.abort(tid.clone()).await.unwrap().unwrap();
    assert!(
        matches!(
            retry_result,
            AbortResult::Success(_) | AbortResult::CheckFailed(CheckError::AlreadyCleanup)
        ),
        "caller retry may race the queued background completion, got {retry_result:?}"
    );
    assert_eq!(manager.transaction_count(), 0);
    assert_eq!(
        transactions::data_site::participant_owner_for_test(
            success_server_id,
            group,
            group,
            &success_id,
        ),
        None
    );
    assert_eq!(
        transactions::data_site::participant_owner_for_test(fail_server_id, group, group, &fail_id,),
        None
    );

    let verify_tid = txn.begin().await.unwrap().unwrap();
    assert_eq!(
        score_of(&accepted_cell(
            txn.read(verify_tid.clone(), success_id)
                .await
                .unwrap()
                .unwrap()
        )),
        1
    );
    assert_eq!(
        score_of(&accepted_cell(
            txn.read(verify_tid.clone(), fail_id)
                .await
                .unwrap()
                .unwrap()
        )),
        2
    );
    abort_txn(&txn, verify_tid).await;

    for server in servers {
        server.shutdown().await;
    }
}

#[tokio::test(flavor = "multi_thread")]
async fn repeatable_full_read_uses_first_snapshot() {
    let _ = env_logger::try_init();
    let address = "127.0.0.1:5330";
    let group = "txn_occ_repeatable_full";
    let server = start_occ_test_server(address, group).await;
    let runtime = server.current_database();
    let schema = install_occ_schema(&runtime);
    let cell_id = Id::new(0, 90101);

    let mut initial = counter_cell(schema.id, cell_id, 0, "counter_full_initial");
    runtime.chunks().write_cell(&mut initial).unwrap();

    let txn = scoped_txn_client_for_database(address, group, group).await;
    let tid = txn.begin().await.unwrap().unwrap();

    let first = accepted_cell(txn.read(tid.clone(), cell_id).await.unwrap().unwrap());
    assert_eq!(score_of(&first), 0);

    let mut updated = counter_cell(schema.id, cell_id, 9, "counter_full_updated");
    let updated_header = runtime.chunks().update_cell(&mut updated).unwrap();
    assert!(updated_header.revision_ts > first.header.revision_ts);

    let second = accepted_cell(txn.read(tid.clone(), cell_id).await.unwrap().unwrap());
    assert_eq!(second.header.revision_ts, first.header.revision_ts);
    assert_eq!(score_of(&second), 0);

    abort_txn(&txn, tid).await;
    server.shutdown().await;
}

#[tokio::test(flavor = "multi_thread")]
async fn transaction_reads_revision_older_than_current_head() {
    let _ = env_logger::try_init();
    let address = "127.0.0.1:5385";
    let group = "txn_occ_fixed_snapshot_after_current_advance";
    let server = start_occ_test_server(address, group).await;
    let runtime = server.current_database();
    let schema = install_occ_schema(&runtime);
    let cell_id = Id::new(0, 90136);

    let mut initial = counter_cell(schema.id, cell_id, 1, "fixed-snapshot-initial");
    runtime.chunks().write_cell(&mut initial).unwrap();

    let txn = scoped_txn_client_for_database(address, group, group).await;
    let tid = txn.begin().await.unwrap().unwrap();

    // Leased direct revisions may lag the transaction boundary by design, so
    // advance the head with an assigned revision strictly above the snapshot.
    let mut current = counter_cell(schema.id, cell_id, 2, "fixed-snapshot-current");
    runtime
        .chunks()
        .update_cell_at_revision(
            &mut current,
            crate::ram::cell::RevisionWrite::committed(tid.ts + 1_000),
        )
        .unwrap();
    assert!(current.header.revision_ts >= tid.ts);

    let snapshot = accepted_cell(txn.read(tid, cell_id).await.unwrap().unwrap());
    assert!(
        snapshot.header.revision_ts < tid.ts,
        "transaction read must resolve its fixed begin snapshot, got revision {} at boundary {}",
        snapshot.header.revision_ts,
        tid.ts
    );

    server.shutdown().await;
}

#[tokio::test(flavor = "multi_thread")]
async fn deleted_snapshot_carries_exact_tombstone_revision() {
    let _ = env_logger::try_init();
    let address = "127.0.0.1:5386";
    let group = "txn_occ_deleted_snapshot_expectation";
    let server = start_occ_test_server(address, group).await;
    let runtime = server.current_database();
    let schema = install_occ_schema(&runtime);
    let cell_id = Id::new(0, 90137);

    // Revision-aware absence is a transactional guarantee: only assigned
    // (transaction-domain) revisions build the chain that certifies the exact
    // tombstone. Direct removes are unindexed recovery records by design.
    let mut initial = counter_cell(schema.id, cell_id, 1, "deleted-snapshot-initial");
    runtime
        .chunks()
        .write_cell_at_revision(
            &mut initial,
            crate::ram::cell::RevisionWrite::committed(100),
        )
        .unwrap();
    runtime
        .chunks()
        .remove_cell_at_revision(&cell_id, crate::ram::cell::RevisionWrite::committed(200))
        .unwrap();
    let delete_revision_ts = match runtime
        .chunks()
        .read_cell_snapshot(&cell_id, u64::MAX)
        .unwrap()
    {
        crate::ram::cell::SnapshotRead::Absent(Some(revision_ts)) => revision_ts,
        other => panic!("expected current tombstone, got {:?}", other),
    };
    assert_eq!(delete_revision_ts, 200);

    let txn = scoped_txn_client_for_database(address, group, group).await;
    let tid = txn.begin().await.unwrap().unwrap();

    let mut recreated = counter_cell(schema.id, cell_id, 2, "deleted-snapshot-recreated");
    runtime
        .chunks()
        .write_cell_at_revision(
            &mut recreated,
            crate::ram::cell::RevisionWrite::committed(tid.ts + 1_000),
        )
        .unwrap();
    assert!(recreated.header.revision_ts >= tid.ts);

    assert_missing(txn.read(tid.clone(), cell_id).await.unwrap().unwrap());
    assert_eq!(
        runtime
            .txn_manager()
            .unwrap()
            .coordinator_expectation_for_test(&tid, &cell_id)
            .await,
        Some(CellExpectation::Absent(Some(delete_revision_ts)))
    );

    abort_txn(&txn, tid).await;
    server.shutdown().await;
}

#[tokio::test(flavor = "multi_thread")]
async fn never_existed_snapshot_carries_no_tombstone_revision() {
    let _ = env_logger::try_init();
    let address = "127.0.0.1:5392";
    let group = "txn_occ_never_existed_snapshot_expectation";
    let server = start_occ_test_server(address, group).await;
    let runtime = server.current_database();
    let cell_id = Id::new(0, 90141);

    let txn = scoped_txn_client_for_database(address, group, group).await;
    let tid = txn.begin().await.unwrap().unwrap();

    assert_missing(txn.read(tid.clone(), cell_id).await.unwrap().unwrap());
    assert_eq!(
        runtime
            .txn_manager()
            .unwrap()
            .coordinator_expectation_for_test(&tid, &cell_id)
            .await,
        Some(CellExpectation::Absent(None))
    );

    abort_txn(&txn, tid).await;
    server.shutdown().await;
}

#[tokio::test(flavor = "multi_thread")]
async fn mixed_point_read_shapes_resolve_one_fixed_snapshot_revision() {
    let _ = env_logger::try_init();
    let address = "127.0.0.1:5387";
    let group = "txn_occ_mixed_shapes_fixed_snapshot";
    let server = start_occ_test_server(address, group).await;
    let runtime = server.current_database();
    let schema = install_occ_schema(&runtime);
    let cell_id = Id::new(0, 90138);

    let mut initial = counter_cell(schema.id, cell_id, 10, "mixed-shapes-initial");
    runtime.chunks().write_cell(&mut initial).unwrap();

    let txn = scoped_txn_client_for_database(address, group, group).await;
    let tid = txn.begin().await.unwrap().unwrap();

    let mut current = counter_cell(schema.id, cell_id, 20, "mixed-shapes-current");
    runtime
        .chunks()
        .update_cell_at_revision(
            &mut current,
            crate::ram::cell::RevisionWrite::committed(tid.ts + 1_000),
        )
        .unwrap();

    let selected = accepted_cell(
        txn.read_selected(tid.clone(), cell_id, vec![hash_str("score")])
            .await
            .unwrap()
            .unwrap(),
    );
    let head = accepted_head(txn.head(tid.clone(), cell_id).await.unwrap().unwrap());
    let full = accepted_cell(txn.read(tid.clone(), cell_id).await.unwrap().unwrap());

    assert!(full.header.revision_ts < tid.ts);
    assert_eq!(selected.header.revision_ts, full.header.revision_ts);
    assert_eq!(head.revision_ts, full.header.revision_ts);
    assert_eq!(selected_score_of(&selected), 10);
    assert_eq!(score_of(&full), 10);

    abort_txn(&txn, tid).await;
    server.shutdown().await;
}

#[tokio::test(flavor = "multi_thread")]
async fn transaction_snapshot_too_old_is_not_downgraded_to_current_data() {
    let _ = env_logger::try_init();
    let address = "127.0.0.1:5388";
    let group = "txn_occ_snapshot_too_old";
    let server = start_occ_test_server(address, group).await;
    let runtime = server.current_database();
    let schema = install_occ_schema(&runtime);
    let cell_id = Id::new(0, 90139);

    let mut initial = counter_cell(schema.id, cell_id, 1, "snapshot-too-old-initial");
    runtime.chunks().write_cell(&mut initial).unwrap();

    let txn = scoped_txn_client_for_database(address, group, group).await;
    let tid = txn.begin().await.unwrap().unwrap();
    let floor = runtime.chunks().establish_recovery_floor().unwrap();
    assert!(floor > tid.ts);

    assert!(matches!(
        txn.read(tid.clone(), cell_id).await.unwrap().unwrap(),
        TxnExecResult::Error(ReadError::SnapshotTooOld)
    ));

    abort_txn(&txn, tid).await;
    server.shutdown().await;
}

#[tokio::test(flavor = "multi_thread")]
async fn buffered_update_overlays_the_fixed_point_snapshot() {
    let _ = env_logger::try_init();
    let address = "127.0.0.1:5389";
    let group = "txn_occ_snapshot_read_your_update";
    let server = start_occ_test_server(address, group).await;
    let runtime = server.current_database();
    let schema = install_occ_schema(&runtime);
    let cell_id = Id::new(0, 90140);

    let mut initial = counter_cell(schema.id, cell_id, 1, "snapshot-overlay-initial");
    runtime.chunks().write_cell(&mut initial).unwrap();

    let txn = scoped_txn_client_for_database(address, group, group).await;
    let tid = txn.begin().await.unwrap().unwrap();
    let mut buffered = accepted_cell(txn.read(tid.clone(), cell_id).await.unwrap().unwrap());
    buffered.data["score"] = OwnedValue::U64(99);

    assert_eq!(
        txn.update(tid.clone(), buffered).await.unwrap().unwrap(),
        TxnExecResult::Accepted(())
    );
    assert_eq!(
        score_of(&accepted_cell(
            txn.read(tid.clone(), cell_id).await.unwrap().unwrap()
        )),
        99
    );

    abort_txn(&txn, tid).await;
    server.shutdown().await;
}

#[tokio::test(flavor = "multi_thread")]
async fn repeatable_missing_read_caches_absence() {
    let _ = env_logger::try_init();
    let address = "127.0.0.1:5331";
    let group = "txn_occ_repeatable_missing";
    let server = start_occ_test_server(address, group).await;
    let runtime = server.current_database();
    let schema = install_occ_schema(&runtime);
    let missing_id = Id::new(0, 90102);

    let txn = scoped_txn_client_for_database(address, group, group).await;
    let tid = txn.begin().await.unwrap().unwrap();

    assert_missing(txn.read(tid.clone(), missing_id).await.unwrap().unwrap());

    let mut inserted = counter_cell(schema.id, missing_id, 9, "counter_missing_inserted");
    runtime.chunks().write_cell(&mut inserted).unwrap();

    assert_missing(txn.read(tid.clone(), missing_id).await.unwrap().unwrap());

    abort_txn(&txn, tid).await;
    server.shutdown().await;
}

#[tokio::test(flavor = "multi_thread")]
async fn repeatable_selected_and_head_share_full_snapshot() {
    let _ = env_logger::try_init();
    let address = "127.0.0.1:5332";
    let group = "txn_occ_repeatable_select_head";
    let server = start_occ_test_server(address, group).await;
    let runtime = server.current_database();
    let schema = install_occ_schema(&runtime);
    let cell_id = Id::new(0, 90103);

    let mut initial = counter_cell(schema.id, cell_id, 0, "counter_select_initial");
    runtime.chunks().write_cell(&mut initial).unwrap();

    let txn = scoped_txn_client_for_database(address, group, group).await;
    let tid = txn.begin().await.unwrap().unwrap();

    let selected = accepted_cell(
        txn.read_selected(tid.clone(), cell_id, vec![hash_str("score")])
            .await
            .unwrap()
            .unwrap(),
    );
    let head = accepted_head(txn.head(tid.clone(), cell_id).await.unwrap().unwrap());

    assert_eq!(selected.header.revision_ts, head.revision_ts);
    assert_eq!(selected_score_of(&selected), 0);

    let mut updated = counter_cell(schema.id, cell_id, 9, "counter_select_updated");
    let updated_header = runtime.chunks().update_cell(&mut updated).unwrap();
    assert!(updated_header.revision_ts > head.revision_ts);

    let full = accepted_cell(txn.read(tid.clone(), cell_id).await.unwrap().unwrap());
    assert_eq!(full.header.revision_ts, head.revision_ts);
    assert_eq!(score_of(&full), 0);

    abort_txn(&txn, tid).await;
    server.shutdown().await;
}

#[tokio::test(flavor = "multi_thread")]
async fn repeatable_selected_empty_fields_return_full_cached_snapshot() {
    let _ = env_logger::try_init();
    let address = "127.0.0.1:5333";
    let group = "txn_occ_repeatable_select_all";
    let server = start_occ_test_server(address, group).await;
    let runtime = server.current_database();
    let schema = install_occ_schema(&runtime);
    let cell_id = Id::new(0, 90104);

    let mut initial = counter_cell(schema.id, cell_id, 0, "counter_select_all_initial");
    runtime.chunks().write_cell(&mut initial).unwrap();

    let txn = scoped_txn_client_for_database(address, group, group).await;
    let tid = txn.begin().await.unwrap().unwrap();

    let selected_all = accepted_cell(
        txn.read_selected(tid.clone(), cell_id, vec![])
            .await
            .unwrap()
            .unwrap(),
    );
    assert!(matches!(&selected_all.data, OwnedValue::Map(_)));
    assert_eq!(selected_all.data, initial.data);

    let mut updated = counter_cell(schema.id, cell_id, 9, "counter_select_all_updated");
    let updated_header = runtime.chunks().update_cell(&mut updated).unwrap();
    assert!(updated_header.revision_ts > selected_all.header.revision_ts);

    let full = accepted_cell(txn.read(tid.clone(), cell_id).await.unwrap().unwrap());
    assert_eq!(full.header.revision_ts, selected_all.header.revision_ts);
    assert_eq!(full.data, selected_all.data);

    abort_txn(&txn, tid).await;
    server.shutdown().await;
}

#[tokio::test(flavor = "multi_thread")]
async fn repeatable_selected_dynamic_fields_fall_back_to_map_lookup() {
    let _ = env_logger::try_init();
    let address = "127.0.0.1:5334";
    let group = "txn_occ_repeatable_select_dynamic";
    let server = start_occ_test_server(address, group).await;
    let runtime = server.current_database();
    let schema = install_occ_schema_with_dynamic(&runtime, true);
    let cell_id = Id::new(0, 90105);
    let dynamic_field = hash_str("bonus");
    let missing_field = hash_str("bonus_missing");

    let mut initial = counter_cell(schema.id, cell_id, 0, "counter_select_dynamic_initial");
    match &mut initial.data {
        OwnedValue::Map(map) => {
            map.insert(&String::from("bonus"), OwnedValue::U64(7));
        }
        other => panic!("expected map-backed test cell, got {:?}", other),
    }
    runtime.chunks().write_cell(&mut initial).unwrap();

    let txn = scoped_txn_client_for_database(address, group, group).await;
    let tid = txn.begin().await.unwrap().unwrap();

    let selected = accepted_cell(
        txn.read_selected(tid.clone(), cell_id, vec![dynamic_field, missing_field])
            .await
            .unwrap()
            .unwrap(),
    );

    assert_eq!(selected.header.revision_ts, initial.header.revision_ts);
    assert_eq!(selected_value(&selected, 0), &OwnedValue::U64(7));
    assert_eq!(selected_value(&selected, 1), &OwnedValue::Null);

    abort_txn(&txn, tid).await;
    server.shutdown().await;
}

#[tokio::test(flavor = "multi_thread")]
async fn repeatable_absence_rejects_update_and_preserves_create_path() {
    let _ = env_logger::try_init();
    let address = "127.0.0.1:5335";
    let group = "txn_occ_repeatable_absence_update";
    let server = start_occ_test_server(address, group).await;
    let runtime = server.current_database();
    let schema = install_occ_schema(&runtime);
    let missing_id = Id::new(0, 90106);

    let txn = scoped_txn_client_for_database(address, group, group).await;
    let tid = txn.begin().await.unwrap().unwrap();

    assert_missing(txn.read(tid.clone(), missing_id).await.unwrap().unwrap());

    let missing_update = counter_cell(schema.id, missing_id, 5, "counter_absence_update");
    assert_eq!(
        txn.update(tid.clone(), missing_update.clone())
            .await
            .unwrap()
            .unwrap(),
        TxnExecResult::Error(WriteError::CellDoesNotExisted)
    );
    assert_missing(txn.read(tid.clone(), missing_id).await.unwrap().unwrap());

    assert_eq!(
        txn.write(tid.clone(), missing_update)
            .await
            .unwrap()
            .unwrap(),
        TxnExecResult::Accepted(())
    );
    let written = accepted_cell(txn.read(tid.clone(), missing_id).await.unwrap().unwrap());
    assert_eq!(score_of(&written), 5);

    abort_txn(&txn, tid).await;
    server.shutdown().await;
}

#[tokio::test(flavor = "multi_thread")]
async fn repeatable_remove_then_write_replaces_existing_cell() {
    let _ = env_logger::try_init();
    let address = "127.0.0.1:5336";
    let group = "txn_occ_repeatable_replace_after_remove";
    let server = start_occ_test_server(address, group).await;
    let runtime = server.current_database();
    let schema = install_occ_schema(&runtime);
    let cell_id = Id::new(0, 90107);

    let mut initial = counter_cell(schema.id, cell_id, 1, "counter_replace_initial");
    runtime.chunks().write_cell(&mut initial).unwrap();

    let txn = scoped_txn_client_for_database(address, group, group).await;
    let tid = txn.begin().await.unwrap().unwrap();

    let first = accepted_cell(txn.read(tid.clone(), cell_id).await.unwrap().unwrap());
    assert_eq!(score_of(&first), 1);

    assert_eq!(
        txn.remove(tid.clone(), cell_id).await.unwrap().unwrap(),
        TxnExecResult::Accepted(())
    );

    let replacement = counter_cell(schema.id, cell_id, 9, "counter_replace_updated");
    assert_eq!(
        txn.write(tid.clone(), replacement).await.unwrap().unwrap(),
        TxnExecResult::Accepted(())
    );
    assert_eq!(
        txn.prepare(tid.clone()).await.unwrap().unwrap(),
        TMPrepareResult::Success
    );
    assert_eq!(
        txn.commit(tid.clone()).await.unwrap().unwrap(),
        EndResult::Success
    );

    let replaced = runtime.chunks().read_cell(&cell_id).unwrap().to_owned();
    assert_eq!(score_of(&replaced), 9);
    assert!(replaced.header.revision_ts > first.header.revision_ts);

    server.shutdown().await;
}

#[tokio::test(flavor = "multi_thread")]
async fn repeatable_blind_remove_then_write_replaces_existing_cell() {
    let _ = env_logger::try_init();
    let address = "127.0.0.1:5337";
    let group = "txn_occ_repeatable_blind_replace_after_remove";
    let server = start_occ_test_server(address, group).await;
    let runtime = server.current_database();
    let schema = install_occ_schema(&runtime);
    let cell_id = Id::new(0, 90108);

    let mut initial = counter_cell(schema.id, cell_id, 2, "counter_blind_replace_initial");
    runtime.chunks().write_cell(&mut initial).unwrap();

    let txn = scoped_txn_client_for_database(address, group, group).await;
    let tid = txn.begin().await.unwrap().unwrap();

    assert_eq!(
        txn.remove(tid.clone(), cell_id).await.unwrap().unwrap(),
        TxnExecResult::Accepted(())
    );

    let replacement = counter_cell(schema.id, cell_id, 8, "counter_blind_replace_updated");
    assert_eq!(
        txn.write(tid.clone(), replacement).await.unwrap().unwrap(),
        TxnExecResult::Accepted(())
    );
    assert_eq!(
        txn.prepare(tid.clone()).await.unwrap().unwrap(),
        TMPrepareResult::Success
    );
    assert_eq!(
        txn.commit(tid.clone()).await.unwrap().unwrap(),
        EndResult::Success
    );

    let replaced = runtime.chunks().read_cell(&cell_id).unwrap().to_owned();
    assert_eq!(score_of(&replaced), 8);
    assert!(replaced.header.revision_ts > initial.header.revision_ts);

    server.shutdown().await;
}

#[tokio::test(flavor = "multi_thread")]
async fn blind_remove_of_missing_cell_fails_at_prepare() {
    let _ = env_logger::try_init();
    let address = "127.0.0.1:5340";
    let group = "txn_occ_repeatable_blind_remove_missing";
    let server = start_occ_test_server(address, group).await;
    let runtime = server.current_database();
    let schema = install_occ_schema(&runtime);
    let missing_id = Id::new(0, 90109);

    let txn = scoped_txn_client_for_database(address, group, group).await;
    let tid = txn.begin().await.unwrap().unwrap();

    // Blind removes defer head observation: the call is accepted and the
    // absence surfaces when prepare fails to resolve the unobserved head.
    assert_eq!(
        txn.remove(tid.clone(), missing_id).await.unwrap().unwrap(),
        TxnExecResult::Accepted(())
    );
    assert_eq!(
        txn.prepare(tid.clone()).await.unwrap().unwrap(),
        TMPrepareResult::DMPrepareError(DMPrepareResult::NotRealizable)
    );

    // The failed prepare aborts the transaction; a fresh one can still
    // create the cell.
    let create_tid = txn.begin().await.unwrap().unwrap();
    let created = counter_cell(schema.id, missing_id, 8, "counter_blind_remove_missing");
    assert_eq!(
        txn.write(create_tid.clone(), created).await.unwrap().unwrap(),
        TxnExecResult::Accepted(())
    );
    let written = accepted_cell(
        txn.read(create_tid.clone(), missing_id)
            .await
            .unwrap()
            .unwrap(),
    );
    assert_eq!(score_of(&written), 8);

    abort_txn(&txn, create_tid).await;
    server.shutdown().await;
}

#[tokio::test(flavor = "multi_thread")]
async fn transactional_blind_write_recreates_tombstoned_cell() {
    let _ = env_logger::try_init();
    let address = "127.0.0.1:5501";
    let group = "txn_occ_blind_recreate_tombstone";
    let server = start_occ_test_server(address, group).await;
    let runtime = server.current_database();
    let schema = install_occ_schema(&runtime);
    let cell_id = Id::new(0, 90201);
    let txn = scoped_txn_client_for_database(address, group, group).await;

    let create_tid = txn.begin().await.unwrap().unwrap();
    assert_eq!(
        txn.write(
            create_tid.clone(),
            counter_cell(schema.id, cell_id, 1, "blind-recreate-initial"),
        )
        .await
        .unwrap()
        .unwrap(),
        TxnExecResult::Accepted(())
    );
    assert_eq!(
        txn.prepare(create_tid.clone()).await.unwrap().unwrap(),
        TMPrepareResult::Success
    );
    assert_eq!(
        txn.commit(create_tid).await.unwrap().unwrap(),
        EndResult::Success
    );

    let remove_tid = txn.begin().await.unwrap().unwrap();
    assert_eq!(
        txn.remove(remove_tid.clone(), cell_id)
            .await
            .unwrap()
            .unwrap(),
        TxnExecResult::Accepted(())
    );
    assert_eq!(
        txn.prepare(remove_tid.clone()).await.unwrap().unwrap(),
        TMPrepareResult::Success
    );
    assert_eq!(
        txn.commit(remove_tid).await.unwrap().unwrap(),
        EndResult::Success
    );
    assert!(runtime.chunks().read_cell(&cell_id).is_err());

    let recreate_tid = txn.begin().await.unwrap().unwrap();
    assert_eq!(
        txn.write(
            recreate_tid.clone(),
            counter_cell(schema.id, cell_id, 2, "blind-recreate-current"),
        )
        .await
        .unwrap()
        .unwrap(),
        TxnExecResult::Accepted(())
    );
    assert_eq!(
        runtime
            .txn_manager()
            .unwrap()
            .coordinator_expectation_for_test(&recreate_tid, &cell_id)
            .await,
        Some(CellExpectation::UnobservedAbsent)
    );
    assert_eq!(
        txn.prepare(recreate_tid.clone()).await.unwrap().unwrap(),
        TMPrepareResult::Success
    );
    assert_eq!(
        txn.commit(recreate_tid).await.unwrap().unwrap(),
        EndResult::Success
    );

    let recreated = runtime.chunks().read_cell(&cell_id).unwrap().to_owned();
    assert_eq!(score_of(&recreated), 2);

    server.shutdown().await;
}

#[tokio::test(flavor = "multi_thread")]
async fn observed_never_absence_rejects_create_delete_aba() {
    let _ = env_logger::try_init();
    let address = "127.0.0.1:5502";
    let group = "txn_occ_observed_absence_aba";
    let server = start_occ_test_server(address, group).await;
    let runtime = server.current_database();
    let schema = install_occ_schema(&runtime);
    let cell_id = Id::new(0, 90202);
    let txn = scoped_txn_client_for_database(address, group, group).await;

    let observing_tid = txn.begin().await.unwrap().unwrap();
    assert_missing(
        txn.read(observing_tid.clone(), cell_id)
            .await
            .unwrap()
            .unwrap(),
    );

    let create_tid = txn.begin().await.unwrap().unwrap();
    assert_eq!(
        txn.write(
            create_tid.clone(),
            counter_cell(schema.id, cell_id, 1, "observed-absence-aba-created"),
        )
        .await
        .unwrap()
        .unwrap(),
        TxnExecResult::Accepted(())
    );
    assert_eq!(
        txn.prepare(create_tid.clone()).await.unwrap().unwrap(),
        TMPrepareResult::Success
    );
    assert_eq!(
        txn.commit(create_tid).await.unwrap().unwrap(),
        EndResult::Success
    );

    let remove_tid = txn.begin().await.unwrap().unwrap();
    assert_eq!(
        txn.remove(remove_tid.clone(), cell_id)
            .await
            .unwrap()
            .unwrap(),
        TxnExecResult::Accepted(())
    );
    assert_eq!(
        txn.prepare(remove_tid.clone()).await.unwrap().unwrap(),
        TMPrepareResult::Success
    );
    assert_eq!(
        txn.commit(remove_tid).await.unwrap().unwrap(),
        EndResult::Success
    );

    assert_eq!(
        txn.write(
            observing_tid.clone(),
            counter_cell(schema.id, cell_id, 2, "observed-absence-aba-recreate"),
        )
        .await
        .unwrap()
        .unwrap(),
        TxnExecResult::Accepted(())
    );
    assert_eq!(
        runtime
            .txn_manager()
            .unwrap()
            .coordinator_expectation_for_test(&observing_tid, &cell_id)
            .await,
        Some(CellExpectation::Absent(None))
    );
    assert_eq!(
        txn.prepare(observing_tid).await.unwrap().unwrap(),
        TMPrepareResult::DMPrepareError(DMPrepareResult::NotRealizable)
    );
    assert!(runtime.chunks().read_cell(&cell_id).is_err());

    server.shutdown().await;
}

#[tokio::test(flavor = "multi_thread")]
async fn blind_write_rejects_present_cell() {
    let _ = env_logger::try_init();
    let address = "127.0.0.1:5504";
    let group = "txn_occ_blind_write_present";
    let server = start_occ_test_server(address, group).await;
    let runtime = server.current_database();
    let schema = install_occ_schema(&runtime);
    let cell_id = Id::new(0, 90204);

    let mut original = counter_cell(schema.id, cell_id, 1, "blind-present-original");
    runtime.chunks().write_cell(&mut original).unwrap();

    let txn = scoped_txn_client_for_database(address, group, group).await;
    let tid = txn.begin().await.unwrap().unwrap();
    assert_eq!(
        txn.write(
            tid.clone(),
            counter_cell(schema.id, cell_id, 2, "blind-present-replacement"),
        )
        .await
        .unwrap()
        .unwrap(),
        TxnExecResult::Accepted(())
    );
    assert_eq!(
        runtime
            .txn_manager()
            .unwrap()
            .coordinator_expectation_for_test(&tid, &cell_id)
            .await,
        Some(CellExpectation::UnobservedAbsent)
    );
    assert_eq!(
        txn.prepare(tid).await.unwrap().unwrap(),
        TMPrepareResult::DMPrepareError(DMPrepareResult::NotRealizable)
    );

    let persisted = runtime.chunks().read_cell(&cell_id).unwrap().to_owned();
    assert_eq!(persisted.data, original.data);

    server.shutdown().await;
}

#[tokio::test(flavor = "multi_thread")]
async fn competing_blind_writers_cannot_both_prepare() {
    let _ = env_logger::try_init();
    let address = "127.0.0.1:5505";
    let group = "txn_occ_competing_blind_writers";
    let server = start_occ_test_server(address, group).await;
    let runtime = server.current_database();
    let schema = install_occ_schema(&runtime);
    let cell_id = Id::new(0, 90205);
    let txn = scoped_txn_client_for_database(address, group, group).await;
    let first_tid = txn.begin().await.unwrap().unwrap();
    let second_tid = txn.begin().await.unwrap().unwrap();

    assert_eq!(
        txn.write(
            first_tid.clone(),
            counter_cell(schema.id, cell_id, 1, "blind-writer-first"),
        )
        .await
        .unwrap()
        .unwrap(),
        TxnExecResult::Accepted(())
    );
    assert_eq!(
        txn.write(
            second_tid.clone(),
            counter_cell(schema.id, cell_id, 2, "blind-writer-second"),
        )
        .await
        .unwrap()
        .unwrap(),
        TxnExecResult::Accepted(())
    );

    assert_eq!(
        txn.prepare(first_tid.clone()).await.unwrap().unwrap(),
        TMPrepareResult::Success
    );
    assert_eq!(
        txn.prepare(second_tid).await.unwrap().unwrap(),
        TMPrepareResult::DMPrepareError(DMPrepareResult::NotRealizable)
    );
    assert_eq!(
        txn.commit(first_tid).await.unwrap().unwrap(),
        EndResult::Success
    );

    let persisted = runtime.chunks().read_cell(&cell_id).unwrap().to_owned();
    assert_eq!(score_of(&persisted), 1);

    server.shutdown().await;
}

#[tokio::test(flavor = "multi_thread")]
async fn occ_mixed_read_write_prepare_commit_updates_only_changed_cell() {
    let _ = env_logger::try_init();
    let address = "127.0.0.1:5341";
    let group = "txn_occ_mixed_read_write_commit";
    let server = start_occ_test_server(address, group).await;
    let runtime = server.current_database();
    let schema = install_occ_schema(&runtime);
    let read_id = Id::new(0, 90110);
    let write_id = Id::new(0, 90111);

    let mut read_seed = counter_cell(schema.id, read_id, 3, "counter_mixed_read_seed");
    runtime.chunks().write_cell(&mut read_seed).unwrap();

    let mut write_seed = counter_cell(schema.id, write_id, 5, "counter_mixed_write_seed");
    runtime.chunks().write_cell(&mut write_seed).unwrap();

    let txn = scoped_txn_client_for_database(address, group, group).await;
    let tid = txn.begin().await.unwrap().unwrap();

    let observed_read = accepted_cell(txn.read(tid.clone(), read_id).await.unwrap().unwrap());
    assert_eq!(observed_read.data, read_seed.data);

    let updated_write = counter_cell(schema.id, write_id, 12, "counter_mixed_write_updated");
    assert_eq!(
        txn.update(tid.clone(), updated_write.clone())
            .await
            .unwrap()
            .unwrap(),
        TxnExecResult::Accepted(())
    );

    assert_eq!(
        txn.prepare(tid.clone()).await.unwrap().unwrap(),
        TMPrepareResult::Success
    );
    assert_eq!(
        txn.commit(tid.clone()).await.unwrap().unwrap(),
        EndResult::Success
    );

    let persisted_read = runtime.chunks().read_cell(&read_id).unwrap().to_owned();
    let persisted_write = runtime.chunks().read_cell(&write_id).unwrap().to_owned();
    assert_eq!(persisted_read.data, read_seed.data);
    assert_eq!(score_of(&persisted_write), 12);
    assert_eq!(persisted_write.data, updated_write.data);

    server.shutdown().await;
}

#[tokio::test(flavor = "multi_thread")]
async fn repeatable_blind_update_after_clock_advance_uses_transaction_observation_clock() {
    let _ = env_logger::try_init();
    let address = "127.0.0.1:5342";
    let group = "txn_occ_repeatable_blind_update_clock";
    let server = start_occ_test_server(address, group).await;
    let runtime = server.current_database();
    let schema = install_occ_schema(&runtime);
    let cell_id = Id::new(0, 90112);

    let mut initial = counter_cell(schema.id, cell_id, 4, "counter_blind_clock_initial");
    runtime.chunks().write_cell(&mut initial).unwrap();

    let txn = scoped_txn_client_for_database(address, group, group).await;
    let tid = txn.begin().await.unwrap().unwrap();
    let later_tid = txn.begin().await.unwrap().unwrap();

    let updated = counter_cell(schema.id, cell_id, 11, "counter_blind_clock_updated");
    assert_eq!(
        txn.update(tid.clone(), updated.clone())
            .await
            .unwrap()
            .unwrap(),
        TxnExecResult::Accepted(())
    );

    assert_eq!(
        txn.prepare(tid.clone()).await.unwrap().unwrap(),
        TMPrepareResult::Success
    );
    assert_eq!(
        txn.commit(tid.clone()).await.unwrap().unwrap(),
        EndResult::Success
    );

    let persisted = runtime.chunks().read_cell(&cell_id).unwrap().to_owned();
    assert_eq!(persisted.data, updated.data);

    abort_txn(&txn, later_tid).await;
    server.shutdown().await;
}

#[tokio::test(flavor = "multi_thread")]
async fn lost_update_prepare_rejects_stale_retry_and_fresh_retry_succeeds() {
    let _ = env_logger::try_init();
    let address = "127.0.0.1:5350";
    let group = "txn_occ_lost_update_retry";
    let server = start_occ_test_server(address, group).await;
    let runtime = server.current_database();
    let schema = install_occ_schema(&runtime);
    let cell_id = Id::new(0, 90113);

    let mut initial = counter_cell(schema.id, cell_id, 0, "counter_lost_update_initial");
    runtime.chunks().write_cell(&mut initial).unwrap();

    let txn = scoped_txn_client_for_database(address, group, group).await;
    let t1 = txn.begin().await.unwrap().unwrap();
    let t2 = txn.begin().await.unwrap().unwrap();

    let first = accepted_cell(txn.read(t1.clone(), cell_id).await.unwrap().unwrap());
    let second = accepted_cell(txn.read(t2.clone(), cell_id).await.unwrap().unwrap());
    assert_eq!(score_of(&first), 0);
    assert_eq!(score_of(&second), 0);
    assert_eq!(first.header.revision_ts, second.header.revision_ts);

    let t1_update = counter_cell(schema.id, cell_id, 1, "counter_lost_update_t1");
    let t2_update = counter_cell(schema.id, cell_id, 1, "counter_lost_update_t2");
    assert_eq!(
        txn.update(t1.clone(), t1_update).await.unwrap().unwrap(),
        TxnExecResult::Accepted(())
    );
    assert_eq!(
        txn.update(t2.clone(), t2_update).await.unwrap().unwrap(),
        TxnExecResult::Accepted(())
    );

    assert_eq!(
        txn.prepare(t1.clone()).await.unwrap().unwrap(),
        TMPrepareResult::Success
    );
    assert_eq!(
        txn.commit(t1.clone()).await.unwrap().unwrap(),
        EndResult::Success
    );

    let after_t1 = runtime.chunks().read_cell(&cell_id).unwrap().to_owned();
    assert_eq!(score_of(&after_t1), 1);
    assert!(after_t1.header.revision_ts > first.header.revision_ts);

    assert_eq!(
        txn.prepare(t2.clone()).await.unwrap().unwrap(),
        TMPrepareResult::DMPrepareError(DMPrepareResult::NotRealizable)
    );
    let after_t2 = runtime.chunks().read_cell(&cell_id).unwrap().to_owned();
    assert_eq!(score_of(&after_t2), 1);
    assert_eq!(after_t2.data, after_t1.data);

    let retry = txn.begin().await.unwrap().unwrap();
    let retry_read = accepted_cell(txn.read(retry.clone(), cell_id).await.unwrap().unwrap());
    assert_eq!(score_of(&retry_read), 1);

    let retry_update = counter_cell(schema.id, cell_id, 2, "counter_lost_update_retry");
    assert_eq!(
        txn.update(retry.clone(), retry_update)
            .await
            .unwrap()
            .unwrap(),
        TxnExecResult::Accepted(())
    );
    assert_eq!(
        txn.prepare(retry.clone()).await.unwrap().unwrap(),
        TMPrepareResult::Success
    );
    assert_eq!(
        txn.commit(retry.clone()).await.unwrap().unwrap(),
        EndResult::Success
    );

    let final_cell = runtime.chunks().read_cell(&cell_id).unwrap().to_owned();
    assert_eq!(score_of(&final_cell), 2);

    server.shutdown().await;
}

#[tokio::test(flavor = "multi_thread")]
async fn shape_gated_reads_defer_full_cell_fetch() {
    let _ = env_logger::try_init();
    let address = "127.0.0.1:5380";
    let group = "txn_occ_shape_gated_defer";
    let server = start_occ_test_server(address, group).await;
    let runtime = server.current_database();
    let schema = install_occ_schema(&runtime);
    let cell_id = Id::new(0, 90130);

    let mut initial = counter_cell(schema.id, cell_id, 0, "counter_shape_gated_initial");
    runtime.chunks().write_cell(&mut initial).unwrap();
    let seeded_revision = initial.header.revision_ts;

    let txn = scoped_txn_client_for_database(address, group, group).await;
    let tid = txn.begin().await.unwrap().unwrap();

    // Step 1: a header read and a projected read must NOT transfer the whole cell.
    let before_partial = transactions::data_site::full_read_rpc_count();

    let head = accepted_head(txn.head(tid.clone(), cell_id).await.unwrap().unwrap());
    assert_eq!(head.revision_ts, seeded_revision);

    let selected = accepted_cell(
        txn.read_selected(tid.clone(), cell_id, vec![hash_str("score")])
            .await
            .unwrap()
            .unwrap(),
    );
    assert_eq!(selected_score_of(&selected), 0);
    assert_eq!(selected.header.revision_ts, head.revision_ts);

    assert_eq!(
        transactions::data_site::full_read_rpc_count(),
        before_partial,
        "head/read_selected must not issue a full-cell participant read"
    );

    // A concurrent update advances the current revision_ts; the fixed snapshot
    // must keep the later full read on the transaction's original revision.
    let mut updated = counter_cell(schema.id, cell_id, 9, "counter_shape_gated_updated");
    let updated_header = runtime.chunks().update_cell(&mut updated).unwrap();
    assert!(updated_header.revision_ts > head.revision_ts);

    // Step 2: the full read fetches the whole cell exactly once, served from the
    // snapshot revision so it is consistent with the earlier partial reads.
    let before_full = transactions::data_site::full_read_rpc_count();
    let full = accepted_cell(txn.read(tid.clone(), cell_id).await.unwrap().unwrap());
    assert_eq!(
        transactions::data_site::full_read_rpc_count(),
        before_full + 1,
        "the full read must fetch the whole cell exactly once"
    );
    assert_eq!(full.header.revision_ts, head.revision_ts);
    assert_eq!(score_of(&full), 0);

    // Repeated reads of every shape are consistent and transfer nothing further
    // (served from the now-materialized full cell).
    let before_repeat = transactions::data_site::full_read_rpc_count();
    let head_again = accepted_head(txn.head(tid.clone(), cell_id).await.unwrap().unwrap());
    let full_again = accepted_cell(txn.read(tid.clone(), cell_id).await.unwrap().unwrap());
    let selected_again = accepted_cell(
        txn.read_selected(tid.clone(), cell_id, vec![hash_str("score")])
            .await
            .unwrap()
            .unwrap(),
    );
    assert_eq!(head_again.revision_ts, head.revision_ts);
    assert_eq!(full_again.header.revision_ts, head.revision_ts);
    assert_eq!(score_of(&full_again), 0);
    assert_eq!(selected_again.header.revision_ts, head.revision_ts);
    assert_eq!(selected_score_of(&selected_again), 0);
    assert_eq!(
        transactions::data_site::full_read_rpc_count(),
        before_repeat,
        "repeated reads must be served from cache without another transfer"
    );

    abort_txn(&txn, tid).await;
    server.shutdown().await;
}

#[tokio::test(flavor = "multi_thread")]
async fn head_read_certifies_snapshot_revision_and_aborts_on_conflict() {
    let _ = env_logger::try_init();
    let address = "127.0.0.1:5381";
    let group = "txn_occ_shape_gated_certify";
    let server = start_occ_test_server(address, group).await;
    let runtime = server.current_database();
    let schema = install_occ_schema(&runtime);
    let read_id = Id::new(0, 90131);
    let write_id = Id::new(0, 90132);

    let mut read_seed = counter_cell(schema.id, read_id, 0, "counter_certify_read_seed");
    runtime.chunks().write_cell(&mut read_seed).unwrap();
    let read_revision = read_seed.header.revision_ts;
    let mut write_seed = counter_cell(schema.id, write_id, 0, "counter_certify_write_seed");
    runtime.chunks().write_cell(&mut write_seed).unwrap();

    let txn = scoped_txn_client_for_database(address, group, group).await;
    let tid = txn.begin().await.unwrap().unwrap();

    // A header-only read must still enter the certified read set as Present(revision_ts)
    // without transferring the whole cell.
    let before_partial = transactions::data_site::full_read_rpc_count();
    let head = accepted_head(txn.head(tid.clone(), read_id).await.unwrap().unwrap());
    assert_eq!(head.revision_ts, read_revision);
    assert_eq!(
        transactions::data_site::full_read_rpc_count(),
        before_partial,
        "head must not transfer the whole cell"
    );

    // A write on a different cell makes this a read-write transaction so prepare runs.
    let write_update = counter_cell(schema.id, write_id, 5, "counter_certify_write_update");
    assert_eq!(
        txn.update(tid.clone(), write_update)
            .await
            .unwrap()
            .unwrap(),
        TxnExecResult::Accepted(())
    );

    // A concurrent transactional writer advances the header-read cell past
    // the observed revision_ts (direct writes carry no certification
    // guarantee under the isolation contract).
    let mut conflicting = counter_cell(schema.id, read_id, 7, "counter_certify_conflict");
    runtime
        .chunks()
        .update_cell_at_revision(
            &mut conflicting,
            crate::ram::cell::RevisionWrite::committed(head.revision_ts + 1_000),
        )
        .unwrap();
    assert!(conflicting.header.revision_ts > head.revision_ts);

    // Certification must abort the transaction: the header-only read's recorded
    // revision_ts no longer matches the current stored revision_ts.
    assert_eq!(
        txn.prepare(tid.clone()).await.unwrap().unwrap(),
        TMPrepareResult::DMPrepareError(DMPrepareResult::NotRealizable)
    );

    // The conflicting write survived; the aborted txn wrote nothing.
    let after_read = runtime.chunks().read_cell(&read_id).unwrap().to_owned();
    assert_eq!(score_of(&after_read), 7);
    let after_write = runtime.chunks().read_cell(&write_id).unwrap().to_owned();
    assert_eq!(score_of(&after_write), 0);

    abort_txn(&txn, tid).await;
    server.shutdown().await;
}

#[tokio::test(flavor = "multi_thread")]
async fn selected_read_certifies_snapshot_revision_and_aborts_on_conflict() {
    let _ = env_logger::try_init();
    let address = "127.0.0.1:5400";
    let group = "txn_occ_selected_read_certificate";
    let server = start_occ_test_server(address, group).await;
    let runtime = server.current_database();
    let schema = install_occ_schema(&runtime);
    let read_id = Id::new(0, 90145);
    let write_id = Id::new(0, 90146);

    let mut read_seed = counter_cell(schema.id, read_id, 3, "selected-cert-read");
    runtime.chunks().write_cell(&mut read_seed).unwrap();
    let mut write_seed = counter_cell(schema.id, write_id, 4, "selected-cert-write");
    runtime.chunks().write_cell(&mut write_seed).unwrap();

    let txn = scoped_txn_client_for_database(address, group, group).await;
    let tid = txn.begin().await.unwrap().unwrap();
    let selected = accepted_cell(
        txn.read_selected(tid, read_id, vec![hash_str("score")])
            .await
            .unwrap()
            .unwrap(),
    );
    assert_eq!(selected_score_of(&selected), 3);
    assert_eq!(
        txn.update(
            tid,
            counter_cell(schema.id, write_id, 9, "selected-cert-write-new"),
        )
        .await
        .unwrap()
        .unwrap(),
        TxnExecResult::Accepted(())
    );

    let mut external = counter_cell(schema.id, read_id, 7, "selected-cert-external");
    runtime
        .chunks()
        .update_cell_at_revision(
            &mut external,
            crate::ram::cell::RevisionWrite::committed(selected.header.revision_ts + 1_000),
        )
        .unwrap();
    assert_eq!(
        txn.prepare(tid).await.unwrap().unwrap(),
        TMPrepareResult::DMPrepareError(DMPrepareResult::NotRealizable)
    );
    assert_eq!(
        score_of(&runtime.chunks().read_cell(&write_id).unwrap().to_owned()),
        4
    );

    server.shutdown().await;
}

#[tokio::test(flavor = "multi_thread")]
async fn snapshot_read_survives_concurrent_assigned_overwrite() {
    let _ = env_logger::try_init();
    let address = "127.0.0.1:5382";
    let group = "txn_occ_history_survives_overwrite";
    let server = start_occ_test_server(address, group).await;
    let runtime = server.current_database();
    let schema = install_occ_schema(&runtime);
    let cell_id = Id::new(0, 90133);

    // Seed revision_ts A.
    let mut revision_a = counter_cell(schema.id, cell_id, 100, "counter_overwrite_a");
    runtime.chunks().write_cell(&mut revision_a).unwrap();
    let revision_a_ts = revision_a.header.revision_ts;

    let txn = scoped_txn_client_for_database(address, group, group).await;
    let tid = txn.begin().await.unwrap().unwrap();

    // head resolves revision_ts A at the transaction's fixed snapshot.
    let head_a = accepted_head(txn.head(tid.clone(), cell_id).await.unwrap().unwrap());
    assert_eq!(head_a.revision_ts, revision_a_ts);

    // Overwrite the cell with an assigned revision above the first
    // transaction's snapshot: revision A is retained in the chain
    // (copy-on-write) while B becomes current. Direct overwrites make no
    // snapshot promise under the isolation contract.
    let mut revision_b = counter_cell(schema.id, cell_id, 200, "counter_overwrite_b");
    runtime
        .chunks()
        .update_cell_at_revision(
            &mut revision_b,
            crate::ram::cell::RevisionWrite::committed(tid.ts + 1),
        )
        .unwrap();
    let revision_b_header = revision_b.header;
    assert!(revision_b_header.revision_ts > head_a.revision_ts);

    // Force a deterministic storage cleaner pass between the overwrite and the
    // snapshot reads below. `Cleaner::clean` (src/ram/cleaner/mod.rs) is the real
    // GC entry point reachable off `chunks()` (it is used the same way in
    // src/ram/tiered/tests.rs and src/ram/tests/chunk.rs); calling it with
    // full=true, wait=true makes any reclamation work run synchronously instead
    // of racing the background cleaner thread.
    //
    // NOTE: occ_tests servers are started via `start_occ_test_server` with
    // chunk_size == db_size == SEGMENT_SIZE, so this database has exactly one
    // chunk holding exactly one segment (Chunk::new divides `size` into
    // `size / SEGMENT_SIZE` segments). The combine-cleaner only ever considers
    // combining when it has >= 2 segment candidates, so in this fixture it can
    // never find a second segment to combine revision_ts A's dead bytes into. The
    // call below is therefore a genuine, real cleaner pass, but it cannot itself
    // reclaim anything under this particular layout. What actually protects the
    // assertions below rely on the retained MVCC history and copy-on-write
    // immutability of already-written cell bytes.
    let _ = crate::ram::cleaner::Cleaner::clean(&runtime.chunks().list[0], true, true);

    // In the SAME tid: both a full read and another head must still observe
    // revision_ts A, not revision_ts B.
    let full_a = accepted_cell(txn.read(tid.clone(), cell_id).await.unwrap().unwrap());
    assert_eq!(full_a.header.revision_ts, head_a.revision_ts);
    assert_eq!(score_of(&full_a), 100);

    let head_a_again = accepted_head(txn.head(tid.clone(), cell_id).await.unwrap().unwrap());
    assert_eq!(head_a_again.revision_ts, head_a.revision_ts);

    // A different, fresh transaction sees the current revision_ts B.
    let tid2 = txn.begin().await.unwrap().unwrap();
    assert!(tid2.ts > revision_b_header.revision_ts);
    let full_b = accepted_cell(txn.read(tid2.clone(), cell_id).await.unwrap().unwrap());
    assert_eq!(full_b.header.revision_ts, revision_b_header.revision_ts);
    assert_eq!(score_of(&full_b), 200);

    abort_txn(&txn, tid).await;
    abort_txn(&txn, tid2).await;
    server.shutdown().await;
}

#[tokio::test(flavor = "multi_thread")]
async fn snapshot_read_survives_concurrent_transactional_remove() {
    let _ = env_logger::try_init();
    let address = "127.0.0.1:5383";
    let group = "txn_occ_history_survives_remove";
    let server = start_occ_test_server(address, group).await;
    let runtime = server.current_database();
    let schema = install_occ_schema(&runtime);
    let cell_id = Id::new(0, 90134);

    // Seed revision_ts A.
    let mut revision_a = counter_cell(schema.id, cell_id, 42, "counter_remove_history_seed");
    runtime.chunks().write_cell(&mut revision_a).unwrap();
    let revision_a_ts = revision_a.header.revision_ts;

    let txn = scoped_txn_client_for_database(address, group, group).await;
    let tid = txn.begin().await.unwrap().unwrap();

    // head records revision_ts A for tid.
    let head_a = accepted_head(txn.head(tid.clone(), cell_id).await.unwrap().unwrap());
    assert_eq!(head_a.revision_ts, revision_a_ts);

    // A concurrent transaction removes the cell and commits.
    let tid2 = txn.begin().await.unwrap().unwrap();
    assert_eq!(
        txn.remove(tid2.clone(), cell_id).await.unwrap().unwrap(),
        TxnExecResult::Accepted(())
    );
    assert_eq!(
        txn.prepare(tid2.clone()).await.unwrap().unwrap(),
        TMPrepareResult::Success
    );
    assert_eq!(
        txn.commit(tid2.clone()).await.unwrap().unwrap(),
        EndResult::Success
    );

    // The cell is now genuinely removed/tombstoned in raw storage.
    assert!(
        runtime.chunks().read_cell(&cell_id).is_err(),
        "the cell must be gone from storage after the concurrent remove commits"
    );

    // The ORIGINAL tid still returns revision_ts A on a full read: the snapshot
    // revision remains visible across the remove.
    let full_a = accepted_cell(txn.read(tid.clone(), cell_id).await.unwrap().unwrap());
    assert_eq!(full_a.header.revision_ts, head_a.revision_ts);
    assert_eq!(score_of(&full_a), 42);

    // ...and on a repeated head, too.
    let head_a_again = accepted_head(txn.head(tid.clone(), cell_id).await.unwrap().unwrap());
    assert_eq!(head_a_again.revision_ts, head_a.revision_ts);

    abort_txn(&txn, tid).await;
    server.shutdown().await;
}

#[tokio::test(flavor = "multi_thread")]
async fn snapshot_read_caches_absence_across_concurrent_transactional_insert() {
    let _ = env_logger::try_init();
    let address = "127.0.0.1:5384";
    let group = "txn_occ_history_absence_insert";
    let server = start_occ_test_server(address, group).await;
    let runtime = server.current_database();
    let schema = install_occ_schema(&runtime);
    let missing_id = Id::new(0, 90135);

    let txn = scoped_txn_client_for_database(address, group, group).await;
    let tid = txn.begin().await.unwrap().unwrap();

    // The cell is absent. head observes CellDoesNotExisted and the coordinator
    // caches the logical absence expectation for tid.
    assert_missing_head(txn.head(tid.clone(), missing_id).await.unwrap().unwrap());

    // Concurrently, a different transaction inserts the cell transactionally
    // (write, then prepare/commit) and it becomes visible in storage.
    let tid2 = txn.begin().await.unwrap().unwrap();
    let inserted = counter_cell(schema.id, missing_id, 77, "counter_absence_insert");
    assert_eq!(
        txn.write(tid2.clone(), inserted.clone())
            .await
            .unwrap()
            .unwrap(),
        TxnExecResult::Accepted(())
    );
    assert_eq!(
        txn.prepare(tid2.clone()).await.unwrap().unwrap(),
        TMPrepareResult::Success
    );
    assert_eq!(
        txn.commit(tid2.clone()).await.unwrap().unwrap(),
        EndResult::Success
    );

    // The insert is really there in raw storage.
    let persisted = runtime.chunks().read_cell(&missing_id).unwrap().to_owned();
    assert_eq!(score_of(&persisted), 77);

    // The ORIGINAL tid still observes repeatable absence on both head and a
    // full read: the first observation of absence is stable within tid, even
    // though the cell now exists.
    assert_missing_head(txn.head(tid.clone(), missing_id).await.unwrap().unwrap());
    assert_missing(txn.read(tid.clone(), missing_id).await.unwrap().unwrap());

    abort_txn(&txn, tid).await;
    server.shutdown().await;
}
