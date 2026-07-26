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
use std::sync::Arc;
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

async fn start_occ_test_server(address: &str, group: &str) -> Arc<NebServer> {
    NebServer::new_from_opts(
        &ServerOptions {
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
    tokio::time::sleep(Duration::from_millis(500)).await;
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
    let mut first = None;
    for partition in 1..8192u64 {
        let id = Id::new(partition, 90_000 + partition);
        let server_id = server
            .get_server_id_by_id(&id)
            .expect("cluster should route every partition");
        if let Some((first_id, first_server_id)) = first {
            if server_id != first_server_id {
                return ((first_id, first_server_id), (id, server_id));
            }
        } else {
            first = Some((id, server_id));
        }
    }
    panic!("expected at least two routed servers in the test cluster");
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
    slow_server
        .current_database()
        .chunks()
        .write_cell(&mut slow_seed)
        .unwrap();

    let mut fail_seed = counter_cell(schema.id, fail_id, 2, "counter_fail_seed");
    fail_server
        .current_database()
        .chunks()
        .write_cell(&mut fail_seed)
        .unwrap();

    let txn = scoped_txn_client_for_database(addresses[0], group, group).await;
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
    let external_header = fail_server
        .current_database()
        .chunks()
        .update_cell(&mut external_fail)
        .unwrap();
    assert!(external_header.revision_ts > fail_first.header.revision_ts);

    let slow_prepare =
        transactions::data_site::install_prepare_delay_for_cell(tid.clone(), slow_id);
    let fast_failure = transactions::manager::install_prepare_result_observer(tid.clone(), fail_id);
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
    slow_server
        .current_database()
        .chunks()
        .write_cell(&mut slow_seed)
        .unwrap();

    let mut fail_seed = counter_cell(schema.id, fail_id, 2, "counter_cancel_fail_seed");
    fail_server
        .current_database()
        .chunks()
        .write_cell(&mut fail_seed)
        .unwrap();

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
async fn partial_abort_failure_ends_successful_sites_and_remains_retryable() {
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
    success_server
        .current_database()
        .chunks()
        .write_cell(&mut success_seed)
        .unwrap();
    let mut fail_seed = counter_cell(schema.id, fail_id, 2, "partial-abort-fail-seed");
    fail_server
        .current_database()
        .chunks()
        .write_cell(&mut fail_seed)
        .unwrap();

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

    let _forced_failure =
        transactions::data_site::install_abort_cannot_end_for_cell(tid.clone(), fail_id);
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

    let probe_tid = txn.begin().await.unwrap().unwrap();
    let released = timeout(
        Duration::from_secs(1),
        txn.read(probe_tid.clone(), success_id),
    )
    .await
    .expect("the successful abort participant should release its lock")
    .unwrap()
    .unwrap();
    assert_eq!(score_of(&accepted_cell(released)), 1);
    abort_txn(&txn, probe_tid).await;

    assert!(matches!(
        txn.abort(tid.clone()).await.unwrap().unwrap(),
        AbortResult::Success(_)
    ));
    assert_eq!(manager.transaction_count(), 0);

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
async fn repeatable_blind_remove_missing_errors_immediately() {
    let _ = env_logger::try_init();
    let address = "127.0.0.1:5340";
    let group = "txn_occ_repeatable_blind_remove_missing";
    let server = start_occ_test_server(address, group).await;
    let runtime = server.current_database();
    let schema = install_occ_schema(&runtime);
    let missing_id = Id::new(0, 90109);

    let txn = scoped_txn_client_for_database(address, group, group).await;
    let tid = txn.begin().await.unwrap().unwrap();

    assert_eq!(
        txn.remove(tid.clone(), missing_id).await.unwrap().unwrap(),
        TxnExecResult::Error(WriteError::CellDoesNotExisted)
    );

    let created = counter_cell(schema.id, missing_id, 8, "counter_blind_remove_missing");
    assert_eq!(
        txn.write(tid.clone(), created).await.unwrap().unwrap(),
        TxnExecResult::Accepted(())
    );
    let written = accepted_cell(txn.read(tid.clone(), missing_id).await.unwrap().unwrap());
    assert_eq!(score_of(&written), 8);

    abort_txn(&txn, tid).await;
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
    let seeded_version = initial.header.revision_ts;

    let txn = scoped_txn_client_for_database(address, group, group).await;
    let tid = txn.begin().await.unwrap().unwrap();

    // Step 1: a header read and a projected read must NOT transfer the whole cell.
    let before_partial = transactions::data_site::full_read_rpc_count();

    let head = accepted_head(txn.head(tid.clone(), cell_id).await.unwrap().unwrap());
    assert_eq!(head.revision_ts, seeded_version);

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

    // A concurrent update advances the current revision_ts; the pin must keep the txn
    // observing its snapshot for the later full read.
    let mut updated = counter_cell(schema.id, cell_id, 9, "counter_shape_gated_updated");
    let updated_header = runtime.chunks().update_cell(&mut updated).unwrap();
    assert!(updated_header.revision_ts > head.revision_ts);

    // Step 2: the full read fetches the whole cell exactly once, served from the
    // pinned revision_ts so it is consistent with the earlier partial reads.
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
async fn head_read_certifies_pinned_version_and_aborts_on_conflict() {
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
    let read_version = read_seed.header.revision_ts;
    let mut write_seed = counter_cell(schema.id, write_id, 0, "counter_certify_write_seed");
    runtime.chunks().write_cell(&mut write_seed).unwrap();

    let txn = scoped_txn_client_for_database(address, group, group).await;
    let tid = txn.begin().await.unwrap().unwrap();

    // A header-only read must still enter the certified read set as Present(revision_ts)
    // without transferring the whole cell.
    let before_partial = transactions::data_site::full_read_rpc_count();
    let head = accepted_head(txn.head(tid.clone(), read_id).await.unwrap().unwrap());
    assert_eq!(head.revision_ts, read_version);
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

    // A concurrent writer advances the header-read cell past the pinned revision_ts.
    let mut conflicting = counter_cell(schema.id, read_id, 7, "counter_certify_conflict");
    let conflicting_header = runtime.chunks().update_cell(&mut conflicting).unwrap();
    assert!(conflicting_header.revision_ts > head.revision_ts);

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
async fn head_pin_survives_concurrent_non_transactional_overwrite() {
    let _ = env_logger::try_init();
    let address = "127.0.0.1:5382";
    let group = "txn_occ_pin_survives_overwrite";
    let server = start_occ_test_server(address, group).await;
    let runtime = server.current_database();
    let schema = install_occ_schema(&runtime);
    let cell_id = Id::new(0, 90133);

    // Seed revision_ts A.
    let mut version_a = counter_cell(schema.id, cell_id, 100, "counter_overwrite_a");
    runtime.chunks().write_cell(&mut version_a).unwrap();
    let version_a_version = version_a.header.revision_ts;

    let txn = scoped_txn_client_for_database(address, group, group).await;
    let tid = txn.begin().await.unwrap().unwrap();

    // head pins revision_ts A at the participant for this transaction.
    let head_a = accepted_head(txn.head(tid.clone(), cell_id).await.unwrap().unwrap());
    assert_eq!(head_a.revision_ts, version_a_version);

    // Overwrite the cell NON-transactionally: this writes a NEW revision_ts B and
    // only marks revision_ts A dead (copy-on-write) rather than mutating it in place.
    let mut version_b = counter_cell(schema.id, cell_id, 200, "counter_overwrite_b");
    let version_b_header = runtime.chunks().update_cell(&mut version_b).unwrap();
    assert!(version_b_header.revision_ts > head_a.revision_ts);

    // Force a deterministic storage cleaner pass between the overwrite and the
    // pinned reads below. `Cleaner::clean` (src/ram/cleaner/mod.rs) is the real
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
    // pinned bytes from reclamation (once there is something to reclaim) is the
    // `SegmentReferenceGuard` held by the pinning transaction's participant
    // entry (see `PinnedReadSet` / `ensure_read_pin` in data_site.rs); the
    // assertions below rely on that guard together with copy-on-write
    // immutability of already-written cell bytes.
    let _ = crate::ram::cleaner::Cleaner::clean(&runtime.chunks().list[0], true, true);

    // In the SAME tid: both a full read and another head must still observe
    // revision_ts A, not revision_ts B.
    let full_a = accepted_cell(txn.read(tid.clone(), cell_id).await.unwrap().unwrap());
    assert_eq!(full_a.header.revision_ts, head_a.revision_ts);
    assert_eq!(score_of(&full_a), 100);

    let head_a_again = accepted_head(txn.head(tid.clone(), cell_id).await.unwrap().unwrap());
    assert_eq!(head_a_again.revision_ts, head_a.revision_ts);

    // A different, fresh transaction that never pinned sees the current revision_ts B.
    let tid2 = txn.begin().await.unwrap().unwrap();
    let full_b = accepted_cell(txn.read(tid2.clone(), cell_id).await.unwrap().unwrap());
    assert_eq!(full_b.header.revision_ts, version_b_header.revision_ts);
    assert_eq!(score_of(&full_b), 200);

    abort_txn(&txn, tid).await;
    abort_txn(&txn, tid2).await;
    server.shutdown().await;
}

#[tokio::test(flavor = "multi_thread")]
async fn head_pin_survives_concurrent_transactional_remove() {
    let _ = env_logger::try_init();
    let address = "127.0.0.1:5383";
    let group = "txn_occ_pin_survives_remove";
    let server = start_occ_test_server(address, group).await;
    let runtime = server.current_database();
    let schema = install_occ_schema(&runtime);
    let cell_id = Id::new(0, 90134);

    // Seed revision_ts A.
    let mut version_a = counter_cell(schema.id, cell_id, 42, "counter_remove_pin_seed");
    runtime.chunks().write_cell(&mut version_a).unwrap();
    let version_a_version = version_a.header.revision_ts;

    let txn = scoped_txn_client_for_database(address, group, group).await;
    let tid = txn.begin().await.unwrap().unwrap();

    // head pins revision_ts A for tid.
    let head_a = accepted_head(txn.head(tid.clone(), cell_id).await.unwrap().unwrap());
    assert_eq!(head_a.revision_ts, version_a_version);

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

    // The ORIGINAL tid still returns revision_ts A on a full read: the pinned
    // revision_ts survives the remove.
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
async fn head_pin_caches_absence_across_concurrent_transactional_insert() {
    let _ = env_logger::try_init();
    let address = "127.0.0.1:5384";
    let group = "txn_occ_pin_absence_insert";
    let server = start_occ_test_server(address, group).await;
    let runtime = server.current_database();
    let schema = install_occ_schema(&runtime);
    let missing_id = Id::new(0, 90135);

    let txn = scoped_txn_client_for_database(address, group, group).await;
    let tid = txn.begin().await.unwrap().unwrap();

    // The cell is absent. head observes CellDoesNotExisted and the coordinator
    // caches the absence (CellExpectation::Absent) for tid.
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
