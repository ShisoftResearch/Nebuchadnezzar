//! Phase 6b step 0: make a torn transaction observable.
//!
//! The hole 2PC cannot close on its own: the coordinator decides COMMIT, tells
//! one participant, and dies before telling the rest. The participants it
//! reached have durable COMMITs; the ones it did not are holding prepared
//! entries with no way to tell "the coordinator decided and I missed it" from
//! "the coordinator died before deciding". Presuming abort -- which is what
//! the lease sweeper does today -- tears the transaction across nodes.
//!
//! It is invisible from any single node, which is why it needed an injection
//! point before anything could be built against it. `stop_end_fanout_after`
//! is that point.
//!
//! These tests are the gate for the rest of Phase 6b. The first one asserts
//! the bug EXISTS: if it ever starts passing without the termination protocol,
//! the injection has stopped injecting and the others prove nothing.

use super::*;
use crate::client::AsyncClient;
use crate::ram::cell::OwnedCell;
use crate::ram::schema::Schema;
use crate::ram::tests::default_fields;
use crate::ram::types::{Id, Map, OwnedMap, OwnedValue};
use crate::ram::chunk::DurableTxnStatus;
use crate::server::{NebServer, Service, ServerOptions};
use std::sync::Arc;
use std::time::Duration;

const SCHEMA_ID: u32 = 4242;

/// A two-server cluster with durable storage, because the evidence
/// cooperative termination runs on lives in the segments.
async fn start_txn_cluster(
    group: &str,
) -> (
    Vec<Arc<NebServer>>,
    Arc<AsyncClient>,
    Vec<String>,
    Vec<tempfile::TempDir>,
) {
    let _ = env_logger::try_init();
    let temp: Vec<tempfile::TempDir> = (0..2).map(|_| tempfile::TempDir::new().unwrap()).collect();
    let addresses: Vec<String> = (0..2)
        .map(|_| crate::utils::test_port::unique_localhost_addr())
        .collect();
    let mut servers = Vec::with_capacity(addresses.len());
    for (address, dir) in addresses.iter().zip(temp.iter()) {
        let opts = ServerOptions {
            // Big enough that a transaction can be GIVEN A HEAD LEASE. A
            // chunk that cannot spare one writes unbracketed, and then there
            // is no BEGIN or COMMIT for anyone to reason about -- both
            // participants read as `Aborted` and the test proves nothing.
            chunk_size: 64 * 1024 * 1024,
            db_size: 256 * 1024 * 1024,
            tiered_config: None,
            backup_storage: Some(dir.path().join("backup").to_string_lossy().to_string()),
            wal_storage: Some(dir.path().join("wal").to_string_lossy().to_string()),
            raft_storage: None,
            index_enabled: false,
            services: vec![Service::Cell, Service::Transaction],
            enable_recovery: false,
            disable_storage_locks: true,
        };
        servers.push(
            NebServer::new_cluster_from_opts(&opts, address, &addresses, group, async |_| {})
                .await
                .unwrap(),
        );
    }
    tokio::time::sleep(Duration::from_millis(500)).await;
    let client = Arc::new(
        AsyncClient::new(&servers[0].rpc, &servers[0].membership, &addresses, group)
            .await
            .unwrap(),
    );
    client.reload_slot_owners().await;
    let schema = Schema::new_with_id(
        SCHEMA_ID,
        &String::from("termination_schema"),
        None,
        default_fields(),
        false,
        false,
    );
    for server in &servers {
        server.meta().schemas.debug_only_new_schema(schema.clone());
    }
    (servers, client, addresses, temp)
}

/// Two ids on DIFFERENT servers, by moving a slot rather than hoping the hash
/// spreads.
///
/// Slot placement is STORED, not hashed: a fresh cluster leaves every slot on
/// the first node and nothing spreads them automatically (the auto-fill
/// watcher is off by default). 199,999 probes all routed to one server before
/// this was added, with the ring reporting two members the whole time.
async fn two_ids_on_distinct_servers(
    client: &Arc<AsyncClient>,
    servers: &[Arc<NebServer>],
) -> Vec<Id> {
    const HERE: u16 = 91;
    const THERE: u16 = 92;
    let plan = crate::migration::MigrationPlan {
        batch_cells: 8,
        ..Default::default()
    };
    crate::migration::migrate_slot(
        client,
        THERE as u32,
        servers[0].server_id,
        servers[1].server_id,
        &plan,
    )
    .await
    .expect("slot should move to the second server");
    client.reload_slot_owners().await;

    let ids = vec![
        Id::from_parts(HERE as u64, 1),
        Id::from_parts(THERE as u64, 1),
    ];
    assert_ne!(
        client.locate_server_id(&ids[0]).unwrap(),
        client.locate_server_id(&ids[1]).unwrap(),
        "the two cells must land on different servers, or there is one \
         participant and no tear is possible"
    );
    ids
}

fn cell_of(id: Id, score: u64) -> OwnedCell {
    let mut data = OwnedMap::new();
    data.insert(&String::from("id"), OwnedValue::I64((id.bits() & ((1u64 << 48) - 1)) as i64));
    data.insert(&String::from("score"), OwnedValue::U64(score));
    data.insert(&String::from("name"), OwnedValue::String(String::from("t")));
    OwnedCell::new_with_id(SCHEMA_ID, &id, OwnedValue::Map(data))
}

/// Put one slot on the second server, so a transaction over both slots has
/// two participants.
///
/// Necessary because slot placement is STORED, not hashed: a fresh cluster
/// leaves every slot on the first node and nothing spreads them automatically
/// (the auto-fill watcher is off by default). 199,999 probes all routed to one
/// server before this was added -- the ring had two members and the placement
/// table had one owner.
async fn place_slot_on(
    client: &Arc<AsyncClient>,
    slot: u16,
    from: u64,
    to: u64,
) {
    let plan = crate::migration::MigrationPlan {
        batch_cells: 8,
        ..Default::default()
    };
    crate::migration::migrate_slot(client, slot as u32, from, to, &plan)
        .await
        .expect("slot should move to the second server");
    client.reload_slot_owners().await;
}

fn id_in_slot(slot: u16, seq: u64) -> Id {
    Id::from_parts(slot as u64, seq)
}

/// THE WINDOW, demonstrated: what a torn transaction looks like BEFORE
/// termination runs.
///
/// This is not a bug report any more -- termination resolves it, and
/// `an_in_doubt_participant_commits_when_a_peer_was_told_to_commit` proves
/// that. What it pins down is that the tear is real and reachable, so the
/// resolution test cannot quietly start proving nothing. If this ever fails,
/// the injection has stopped injecting.
///
/// Asserted on DURABLE state rather than on reads, because a read cannot see
/// it yet. Both participants wrote their cells during prepare and both serve
/// reads happily -- which is precisely why this is invisible in production.
/// The difference is the COMMIT bracket: the participant that heard the
/// decision has one and will keep its half through any restart, the one that
/// did not has an open BEGIN and loses its half at the next recovery.
#[tokio::test(flavor = "multi_thread")]
async fn a_coordinator_dying_mid_decision_tears_the_transaction() {
    let (servers, client, addresses, _temp) = start_txn_cluster("tear_demo").await;
    let ids = two_ids_on_distinct_servers(&client, &servers).await;

    let txn = super::new_async_client_for_database(&addresses[0], "tear_demo", "tear_demo")
        .await
        .unwrap();
    let tid = txn.begin().await.unwrap().unwrap();
    for (n, id) in ids.iter().enumerate() {
        txn.write(tid.clone(), cell_of(*id, 100 + n as u64))
            .await
            .unwrap()
            .unwrap();
    }
    // Every participant votes yes and makes its entries durable.
    txn.prepare(tid.clone()).await.unwrap().unwrap();

    // Then the coordinator tells exactly one participant and stops.
    let limit = super::manager::stop_end_fanout_after(tid.clone(), 1);
    let _ = txn.commit(tid.clone()).await;
    drop(limit);
    tokio::time::sleep(Duration::from_millis(300)).await;

    // Both cells read fine RIGHT NOW. That is the trap.
    for id in &ids {
        assert!(
            matches!(client.read_cell(*id).await, Ok(Ok(_))),
            "both halves are readable before any restart; {:?} is not",
            id
        );
    }

    let status: Vec<DurableTxnStatus> = servers
        .iter()
        .map(|s| s.chunks().durable_txn_status(&tid))
        .collect();

    assert!(
        status.contains(&DurableTxnStatus::Committed)
            && status.contains(&DurableTxnStatus::Prepared),
        "expected a TORN transaction -- one participant durably Committed and \
         one still Prepared -- but the participants agree: {:?}. Either the \
         injection stopped injecting, or termination is resolving it, in which \
         case this test has done its job and its assertion should be inverted.",
        status
    );

    for server in servers {
        server.shutdown().await;
    }
}

/// The control: an undisturbed transaction leaves EVERY participant durably
/// committed, so the split above is the coordinator's failure and not
/// something the two-participant setup does on its own.
#[tokio::test(flavor = "multi_thread")]
async fn an_undisturbed_transaction_commits_durably_everywhere() {
    let (servers, client, addresses, _temp) = start_txn_cluster("tear_control").await;
    let ids = two_ids_on_distinct_servers(&client, &servers).await;

    let txn = super::new_async_client_for_database(&addresses[0], "tear_control", "tear_control")
        .await
        .unwrap();
    let tid = txn.begin().await.unwrap().unwrap();
    for (n, id) in ids.iter().enumerate() {
        txn.write(tid.clone(), cell_of(*id, 200 + n as u64))
            .await
            .unwrap()
            .unwrap();
    }
    txn.prepare(tid.clone()).await.unwrap().unwrap();
    txn.commit(tid.clone()).await.unwrap().unwrap();
    tokio::time::sleep(Duration::from_millis(300)).await;

    for server in &servers {
        assert_eq!(
            server.chunks().durable_txn_status(&tid),
            DurableTxnStatus::Committed,
            "every participant of an undisturbed transaction must hold a \
             durable COMMIT"
        );
    }

    for server in servers {
        server.shutdown().await;
    }
}

/// THE FIX. The same torn transaction, resolved by asking.
///
/// The coordinator dies after telling one participant. The one it did not
/// reach is in doubt: its entries are durable, its bracket is open, and
/// nothing local can tell it whether the decision was commit or abort.
/// Before cooperative termination it presumed abort and lost its half.
///
/// Now it asks the participant set it learned at prepare, hears `Committed`
/// from a peer that was told, and closes its own bracket.
///
/// Driven directly rather than by waiting out the lease timeout: the sweeper
/// that calls this runs every second with a 120s idle window, and a test that
/// waits for it would be a two-minute test that fails as a timeout when the
/// mechanism breaks.
#[tokio::test(flavor = "multi_thread")]
async fn an_in_doubt_participant_commits_when_a_peer_was_told_to_commit() {
    let (servers, client, addresses, _temp) = start_txn_cluster("tear_resolved").await;
    let ids = two_ids_on_distinct_servers(&client, &servers).await;

    let txn = super::new_async_client_for_database(&addresses[0], "tear_resolved", "tear_resolved")
        .await
        .unwrap();
    let tid = txn.begin().await.unwrap().unwrap();
    for (n, id) in ids.iter().enumerate() {
        txn.write(tid.clone(), cell_of(*id, 300 + n as u64))
            .await
            .unwrap()
            .unwrap();
    }
    txn.prepare(tid.clone()).await.unwrap().unwrap();

    let limit = super::manager::stop_end_fanout_after(tid.clone(), 1);
    let _ = txn.commit(tid.clone()).await;
    drop(limit);
    tokio::time::sleep(Duration::from_millis(300)).await;

    let torn: Vec<DurableTxnStatus> = servers
        .iter()
        .map(|s| s.chunks().durable_txn_status(&tid))
        .collect();
    assert!(
        torn.contains(&DurableTxnStatus::Committed) && torn.contains(&DurableTxnStatus::Prepared),
        "the transaction must be torn before termination runs, or this test \
         proves nothing about termination: {:?}",
        torn
    );

    // Every site sweeps. Only the one in doubt has anything to do.
    for server in &servers {
        server
            .database_runtime()
            .data_site()
            .expect("every server in this cluster runs a data site")
            .terminate_in_doubt_transactions(Duration::ZERO, Duration::ZERO)
            .await;
    }

    for server in &servers {
        assert_eq!(
            server.chunks().durable_txn_status(&tid),
            DurableTxnStatus::Committed,
            "after termination every participant must hold a durable COMMIT; \
             the one that never heard the decision asked and was told"
        );
    }

    for server in servers {
        server.shutdown().await;
    }
}

/// The other half of the rule: a transaction NO participant was told to
/// commit still aborts. Termination must not turn presume-abort into
/// presume-commit.
#[tokio::test(flavor = "multi_thread")]
async fn an_in_doubt_participant_still_aborts_when_no_peer_committed() {
    let (servers, client, addresses, _temp) = start_txn_cluster("tear_abort").await;
    let ids = two_ids_on_distinct_servers(&client, &servers).await;

    let txn = super::new_async_client_for_database(&addresses[0], "tear_abort", "tear_abort")
        .await
        .unwrap();
    let tid = txn.begin().await.unwrap().unwrap();
    for (n, id) in ids.iter().enumerate() {
        txn.write(tid.clone(), cell_of(*id, 400 + n as u64))
            .await
            .unwrap()
            .unwrap();
    }
    txn.prepare(tid.clone()).await.unwrap().unwrap();

    // Nobody is told: the coordinator decides and dies before the first
    // participant hears it.
    let limit = super::manager::stop_end_fanout_after(tid.clone(), 0);
    let _ = txn.commit(tid.clone()).await;
    drop(limit);
    tokio::time::sleep(Duration::from_millis(300)).await;

    for server in &servers {
        assert_eq!(
            server.chunks().durable_txn_status(&tid),
            DurableTxnStatus::Prepared,
            "no participant may hold a COMMIT when none was told"
        );
    }

    for server in &servers {
        server
            .database_runtime()
            .data_site()
            .expect("every server in this cluster runs a data site")
            .terminate_in_doubt_transactions(Duration::ZERO, Duration::ZERO)
            .await;
    }

    for server in &servers {
        assert_ne!(
            server.chunks().durable_txn_status(&tid),
            DurableTxnStatus::Committed,
            "a transaction no participant was told to commit must not become \
             committed by asking around"
        );
    }

    for server in servers {
        server.shutdown().await;
    }
}

/// The restart case: a participant that lost its memory still resolves.
///
/// This is the half the lease sweeper cannot reach. A site that restarts in
/// doubt has no transaction object and no participant set -- both lived in
/// memory -- so it cannot ask the participants. It asks the whole group
/// instead, and the peer that was told answers from its own record.
///
/// Uses `enable_recovery: true` for both servers, unlike the other tests
/// here: the resolution runs inside recovery, before the cleaner is allowed
/// to touch the evidence.
#[tokio::test(flavor = "multi_thread")]
async fn a_restarted_participant_asks_the_group_and_commits() {
    let _ = env_logger::try_init();
    let group = "tear_restart";
    let temp: Vec<tempfile::TempDir> = (0..2).map(|_| tempfile::TempDir::new().unwrap()).collect();
    let addresses: Vec<String> = (0..2)
        .map(|_| crate::utils::test_port::unique_localhost_addr())
        .collect();
    let opts_for = |dir: &tempfile::TempDir| ServerOptions {
        chunk_size: 64 * 1024 * 1024,
        db_size: 256 * 1024 * 1024,
        tiered_config: None,
        backup_storage: Some(dir.path().join("backup").to_string_lossy().to_string()),
        wal_storage: Some(dir.path().join("wal").to_string_lossy().to_string()),
        // Required once recovery is on: schemas live in raft storage, and
        // recovering cells without them restores data nothing can read.
        raft_storage: Some(dir.path().join("raft").to_string_lossy().to_string()),
        index_enabled: false,
        services: vec![Service::Cell, Service::Transaction],
        enable_recovery: true,
        disable_storage_locks: true,
    };
    let schema = Schema::new_with_id(
        SCHEMA_ID,
        &String::from("termination_schema"),
        None,
        default_fields(),
        false,
        false,
    );

    let mut servers = Vec::new();
    for (address, dir) in addresses.iter().zip(temp.iter()) {
        servers.push(
            NebServer::new_cluster_from_opts(&opts_for(dir), address, &addresses, group, async |_| {})
                .await
                .unwrap(),
        );
    }
    tokio::time::sleep(Duration::from_millis(500)).await;
    let client = Arc::new(
        AsyncClient::new(&servers[0].rpc, &servers[0].membership, &addresses, group)
            .await
            .unwrap(),
    );
    client.reload_slot_owners().await;
    for server in &servers {
        server.meta().schemas.debug_only_new_schema(schema.clone());
    }
    let ids = two_ids_on_distinct_servers(&client, &servers).await;

    let txn = super::new_async_client_for_database(&addresses[0], group, group)
        .await
        .unwrap();
    let tid = txn.begin().await.unwrap().unwrap();
    for (n, id) in ids.iter().enumerate() {
        txn.write(tid.clone(), cell_of(*id, 500 + n as u64))
            .await
            .unwrap()
            .unwrap();
    }
    txn.prepare(tid.clone()).await.unwrap().unwrap();

    // Tell the SECOND server only. The first is left in doubt, and it is the
    // one that restarts.
    let told = client.locate_server_id(&ids[1]).unwrap();
    let limit = super::manager::stop_end_fanout_after_server(tid.clone(), told);
    let _ = txn.commit(tid.clone()).await;
    drop(limit);
    tokio::time::sleep(Duration::from_millis(300)).await;

    assert_eq!(
        servers[0].chunks().durable_txn_status(&tid),
        DurableTxnStatus::Prepared,
        "the server that was not told must be in doubt before it restarts"
    );
    assert_eq!(
        servers[1].chunks().durable_txn_status(&tid),
        DurableTxnStatus::Committed,
        "the server that was told must hold a durable COMMIT"
    );

    // Restart the in-doubt one. Its transaction object and participant set go
    // with the process; only the segments survive.
    let restarting = servers.remove(0);
    restarting.shutdown().await;
    drop(restarting);
    tokio::time::sleep(Duration::from_millis(300)).await;

    let restarted = NebServer::new_cluster_from_opts(
        &opts_for(&temp[0]),
        &addresses[0],
        &addresses,
        group,
        async |_| {},
    )
    .await
    .unwrap();

    assert_eq!(
        restarted.chunks().durable_txn_status(&tid),
        DurableTxnStatus::Committed,
        "a restarted participant must ask the group and commit what a peer \
         was told to commit, rather than discarding its half"
    );

    restarted.shutdown().await;
    for server in servers {
        server.shutdown().await;
    }
}

/// A silent peer must not be counted as a vote to abort.
///
/// The participant is in doubt and the peer that knows the answer is DOWN.
/// Concluding abort here is the tear: the peer holds a durable COMMIT and
/// will still hold it when it comes back. So the heads stay held and the
/// question stays open -- but not forever, because a held head is a writer
/// slot gone from its chunk with no refill path.
///
/// Both halves are asserted: it holds while inside the grace window, and it
/// gives up once past it.
#[tokio::test(flavor = "multi_thread")]
async fn a_silent_peer_holds_the_transaction_open_but_not_forever() {
    let (servers, client, addresses, _temp) = start_txn_cluster("tear_silent").await;
    let ids = two_ids_on_distinct_servers(&client, &servers).await;

    let txn = super::new_async_client_for_database(&addresses[0], "tear_silent", "tear_silent")
        .await
        .unwrap();
    let tid = txn.begin().await.unwrap().unwrap();
    for (n, id) in ids.iter().enumerate() {
        txn.write(tid.clone(), cell_of(*id, 600 + n as u64))
            .await
            .unwrap()
            .unwrap();
    }
    txn.prepare(tid.clone()).await.unwrap().unwrap();

    let told = client.locate_server_id(&ids[1]).unwrap();
    let limit = super::manager::stop_end_fanout_after_server(tid.clone(), told);
    let _ = txn.commit(tid.clone()).await;
    drop(limit);
    tokio::time::sleep(Duration::from_millis(300)).await;

    let in_doubt = &servers[0];
    assert_eq!(
        in_doubt.chunks().durable_txn_status(&tid),
        DurableTxnStatus::Prepared,
        "the untold server must be in doubt"
    );
    let leases_before = crate::ram::chunk::transaction_lease_count(
        &tid,
        &in_doubt.chunks().address_range(),
    );
    assert!(
        leases_before > 0,
        "an in-doubt participant must still hold the heads its bracket needs"
    );

    // The peer that knows goes away.
    let knows = servers[1].clone();
    knows.shutdown().await;
    tokio::time::sleep(Duration::from_millis(300)).await;

    // Inside the grace window: hold, do not decide.
    let site = in_doubt
        .database_runtime()
        .data_site()
        .expect("the in-doubt server runs a data site");
    let hold = site
        .terminate_in_doubt_transactions(Duration::ZERO, Duration::from_secs(300))
        .await;
    assert!(
        hold.contains(&tid),
        "a transaction whose peer never answered must be held, not decided"
    );
    assert_eq!(
        crate::ram::chunk::transaction_lease_count(&tid, &in_doubt.chunks().address_range()),
        leases_before,
        "holding means KEEPING the leases; a commit still needs them"
    );
    assert_ne!(
        in_doubt.chunks().durable_txn_status(&tid),
        DurableTxnStatus::Committed,
        "holding is not deciding"
    );

    // Past it: give up, so a permanently silent peer cannot cost a writer
    // slot for the life of the process.
    let hold = site
        .terminate_in_doubt_transactions(Duration::ZERO, Duration::ZERO)
        .await;
    assert!(
        !hold.contains(&tid),
        "past the grace window the transaction must be released to the sweeper"
    );

    for server in servers {
        server.shutdown().await;
    }
}
