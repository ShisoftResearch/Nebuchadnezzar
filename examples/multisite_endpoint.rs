//! Process endpoint for tools/multisite. The controller owns all fault actions.
use bifrost::rpc::ServiceClient;
use bifrost::{membership::client::ObserverClient, raft, rpc};
use dovahkiin::types::Type;
use neb::client::{transaction::Transaction, AsyncClient};
use neb::ram::{
    cell::OwnedCell,
    schema::{Field, Schema, SchemaVid},
    types::{Id, OwnedValue},
};
use neb::server::{
    transactions::{self, TxnState},
    NebServer, ServerOptions, Service,
};
use serde_json::{json, Value};
use std::{cell::Cell, io::Write, path::Path, sync::Arc};
use tokio::io::{AsyncBufReadExt, BufReader};

fn emit(value: Value) {
    println!("{}", value);
    std::io::stdout().flush().unwrap();
}

#[tokio::main(flavor = "multi_thread", worker_threads = 4)]
async fn main() {
    env_logger::init();
    assert!(
        bifrost::DISABLE_SHORTCUT,
        "build with --cfg disable_shortcut"
    );
    let args: Vec<String> = std::env::args().collect();
    let mode = &args[1];
    let address = &args[2];
    let peers: Vec<String> = args[3].split(',').map(str::to_owned).collect();
    let group = &args[4];
    emit(json!({"event":"process", "pid":std::process::id(), "mode":mode, "tcp":true}));
    if mode == "server" {
        let dir = Path::new(&args[5]);
        let opts = ServerOptions {
            chunk_size: 64 * 1024 * 1024,
            db_size: 256 * 1024 * 1024,
            tiered_config: None,
            backup_storage: Some(dir.join("backup").display().to_string()),
            wal_storage: Some(dir.join("wal").display().to_string()),
            raft_storage: Some(dir.join("raft").display().to_string()),
            index_enabled: false,
            services: vec![Service::Cell, Service::Transaction],
            enable_recovery: true,
            disable_storage_locks: false,
        };
        let server = NebServer::new_cluster_from_opts(&opts, address, &peers, group, async |_| {})
            .await
            .unwrap();
        emit(json!({"event":"ready", "server":server.server_id}));
        let mut term =
            tokio::signal::unix::signal(tokio::signal::unix::SignalKind::terminate()).unwrap();
        let control = std::env::var("MULTISITE_CONTROL")
            .map(std::path::PathBuf::from)
            .unwrap();
        let mut interval = tokio::time::interval(std::time::Duration::from_millis(100));
        loop {
            tokio::select! {
                _ = term.recv() => break,
                _ = interval.tick() => {
                    if control.join("refresh").exists() {
                        let refreshed = server.refresh_slot_placement().await;
                        std::fs::write(control.join("refreshed"), refreshed.to_string()).unwrap();
                        std::fs::remove_file(control.join("refresh")).unwrap();
                    }
                    if let Ok(data) = std::fs::read(control.join("inspect")) {
                        let tids: Vec<transactions::TxnId> = serde_json::from_slice(&data).unwrap();
                        let stats: Vec<Value> = tids.iter().map(|tid| json!({
                            "tid":tid,
                            "leases":neb::ram::chunk::transaction_lease_count(tid, &server.chunks().address_range()),
                            "bracket_status":format!("{:?}",server.chunks().durable_txn_status(tid)),
                        })).collect();
                        let result = json!({"transactions":stats,"unresolved":server.chunks().unresolved_transactions().len(),
                                            "pinned_segments":server.chunks().pinned_segments()});
                        std::fs::write(control.join("stats.tmp"), result.to_string()).unwrap();
                        std::fs::rename(control.join("stats.tmp"), control.join("stats")).unwrap();
                        std::fs::remove_file(control.join("inspect")).unwrap();
                    }
                }
            }
        }
        server.shutdown().await;
        emit(json!({"event":"shutdown"}));
        return;
    }
    assert_eq!(mode, "client");
    let callback = rpc::Server::new(address);
    rpc::Server::listen_and_resume(&callback).await;
    let raft_client = raft::client::RaftClient::new(&peers, raft::DEFAULT_SERVICE_ID)
        .await
        .unwrap();
    let membership = Arc::new(ObserverClient::new(&raft_client));
    let client = Arc::new(
        AsyncClient::new(&callback, &membership, &peers, group)
            .await
            .unwrap(),
    );
    emit(json!({"event":"ready"}));
    let mut input = BufReader::new(tokio::io::stdin()).lines();
    while let Some(line) = input.next_line().await.unwrap() {
        let request: Value = serde_json::from_str(&line).unwrap();
        let result = command(&client, &peers, group, &request).await;
        emit(json!({"event":"result", "request":request, "result":result}));
    }
}

async fn command(client: &Arc<AsyncClient>, peers: &[String], group: &str, req: &Value) -> Value {
    match req["op"].as_str().unwrap() {
        "setup" => {
            let schema = Schema::new_with_id(
                4343,
                "multisite",
                None,
                Field::new_unindexed("*", Type::U64),
                false,
                false,
            );
            if client.schema_by_id(4343).await.unwrap().is_none() {
                client.new_schema_with_id(schema).await.unwrap().unwrap();
            }
            for (index, addr) in peers.iter().enumerate() {
                let slot = 91 + index as u64;
                let id = Id::from_parts(slot, 1);
                let from = client.locate_server_id(&id).unwrap();
                let to = bifrost_hasher::hash_str(addr);
                if from != to {
                    let plan = neb::migration::MigrationPlan {
                        batch_cells: 8,
                        ..Default::default()
                    };
                    neb::migration::migrate_slot(client, slot as u32, from, to, &plan)
                        .await
                        .unwrap();
                }
                client.reload_slot_owners().await;
                assert_eq!(client.locate_server_id(&id).unwrap(), to);
            }
            json!({"ok":true})
        }
        "owners" => {
            client.reload_slot_owners().await;
            let owners: Vec<Value> = (0..peers.len() as u64)
                .map(|index| {
                    let id = Id::from_parts(91 + index, 1);
                    json!({"slot":91 + index,"owner":client.locate_server_id(&id).ok(),
                           "expected":bifrost_hasher::hash_str(&peers[index as usize])})
                })
                .collect();
            json!({"owners":owners})
        }
        "status" => {
            let index = req["site"].as_u64().unwrap() as usize;
            let tid: transactions::TxnId = serde_json::from_value(req["tid"].clone()).unwrap();
            let rpc = rpc::DEFAULT_CLIENT_POOL.get(&peers[index]).await.unwrap();
            let site = transactions::data_site::AsyncServiceClient::new_with_service_id(
                transactions::data_site::generate_scoped_service_id(group, group),
                &rpc,
            );
            json!({"status":format!("{:?}",site.txn_status(tid).await.unwrap().payload)})
        }
        "read" => {
            client.reload_slot_owners().await;
            let mut values = Vec::new();
            for key in req["keys"].as_array().unwrap() {
                let id = Id::from_parts(key[0].as_u64().unwrap(), key[1].as_u64().unwrap());
                values.push(match client.read_cell(id).await {
                    Ok(Ok(cell)) => json!({"value":cell.data.u64()}),
                    Ok(Err(neb::ram::cell::ReadError::CellDoesNotExisted)) => {
                        json!({"absent":true})
                    }
                    other => json!({"error":format!("{:?}", other)}),
                });
            }
            json!({"values":values})
        }
        "txn" => run_txn(peers, group, req).await,
        "batch" => {
            let results = futures::future::join_all(
                req["requests"]
                    .as_array()
                    .unwrap()
                    .iter()
                    .map(|request| run_txn(peers, group, request)),
            )
            .await;
            json!({"results":results})
        }
        _ => panic!("unknown command"),
    }
}

async fn run_txn(peers: &[String], group: &str, req: &Value) -> Value {
    let coordinator = req["coordinator"].as_u64().unwrap_or(0) as usize;
    let svc = transactions::new_async_client_for_database(&peers[coordinator], group, group)
        .await
        .unwrap();
    let tid = match svc.begin().await {
        Ok(Ok(tid)) => tid,
        other => return json!({"outcome":"begin_error", "error":format!("{:?}",other)}),
    };
    emit(json!({"event":"begin", "id":req["id"], "tid":tid}));
    let txn = Transaction {
        tid,
        state: Cell::new(TxnState::Started),
        client: svc,
    };
    if let Some(reads) = req["reads"].as_array() {
        for key in reads {
            let id = Id::from_parts(key[0].as_u64().unwrap(), key[1].as_u64().unwrap());
            if let Err(error) = txn.read(id).await {
                let _ = txn.abort().await;
                return json!({"outcome":"read_error", "error":format!("{error:?}")});
            }
        }
    }
    for key in req["keys"].as_array().unwrap() {
        let id = Id::from_parts(key[0].as_u64().unwrap(), key[1].as_u64().unwrap());
        let cell = OwnedCell::new_with_id(
            SchemaVid(4343),
            &id,
            OwnedValue::U64(req["value"].as_u64().unwrap()),
        );
        if let Err(error) = txn.write(cell).await {
            let _ = txn.abort().await;
            return json!({"outcome":"write_error","error":format!("{:?}",error)});
        }
    }
    if req["abort"].as_bool().unwrap_or(false) {
        return json!({"outcome":"abort", "result":format!("{:?}",txn.abort().await)});
    }
    if let Err(error) = txn.prepare().await {
        let _ = txn.abort().await;
        return json!({"outcome":"prepare_error","error":format!("{:?}",error)});
    }
    emit(json!({"event":"prepared", "id":req["id"], "tid":txn.tid}));
    if let Some(index) = req["end_site"].as_u64() {
        let rpc = rpc::DEFAULT_CLIENT_POOL
            .get(&peers[index as usize])
            .await
            .unwrap();
        let site = transactions::data_site::AsyncServiceClient::new_with_service_id(
            transactions::data_site::generate_scoped_service_id(group, group),
            &rpc,
        );
        let end = site.end(txn.tid, txn.tid, false).await.unwrap().payload;
        let abort = site.abort(txn.tid, txn.tid).await.unwrap().payload;
        return json!({"outcome":"end_probe", "tid":txn.tid, "end":format!("{end:?}"), "abort":format!("{abort:?}")});
    }
    let outcome = if req["durable"].as_bool().unwrap_or(false) {
        txn.commit_durable().await
    } else {
        txn.commit().await
    };
    match outcome {
        Ok(()) => json!({"outcome":"ack"}),
        Err(neb::client::transaction::TxnError::OutcomeUnknown) => {
            json!({"outcome":"unknown"})
        }
        Err(error) => json!({"outcome":"commit_error","error":format!("{:?}",error)}),
    }
}
