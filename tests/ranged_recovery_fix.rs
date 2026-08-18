use std::sync::Arc;
use tokio::time::{timeout, Duration};

use neb::client;
use neb::index::ranged::sm::{self, client::SMClient as PlacementClient};
use neb::index::ranged::tree::btree::{page_schema, storage, Ordering};
use neb::index::ranged::tree::service::{OpResult, Range, Service as TreeRpcService, TreeService};
use neb::index::ranged::tree::tree::{RangedTree, RANGED_TREE_HEAD_HASH, RANGED_TREE_SCHEMA};
use neb::index::EntryKey;
use neb::ram::types::Id;
use neb::server::{database_meta_plane_id, NebServer, ServerOptions, Service as NebService};

#[tokio::test(flavor = "multi_thread")]
async fn lazy_hydration_recovers_missing_active_tree() {
    let _ = env_logger::try_init();

    let server_addr = String::from("127.0.0.1:6831");
    let server_group = "ranged_recovery_fix_lazy";
    let server = NebServer::new_from_opts(
        &ServerOptions {
            chunk_size: 64 * 1024 * 1024,
            db_size: 64 * 1024 * 1024,
            tiered_config: None,
            backup_storage: None,
            wal_storage: None,
            undo_log_storage: None,
            raft_storage: None,
            index_enabled: false,
            services: vec![NebService::Cell, NebService::RangedIndexer],
            enable_recovery: false,
            disable_storage_locks: true,
        },
        &server_addr,
        server_group,
        async |_| {},
    )
    .await
    .unwrap();

    let client = Arc::new(
        client::AsyncClient::new(
            &server.rpc,
            &server.membership,
            &vec![server_addr.clone()],
            server_group,
        )
        .await
        .unwrap(),
    );
    let meta_plane_client = server
        .raft_client
        .plane(database_meta_plane_id(server_group, server_group));
    let sm_client = Arc::new(PlacementClient::new(
        sm::generate_scoped_sm_id(server_group, server_group),
        &meta_plane_client,
    ));
    let local_service = TreeService::new(&client, &sm_client);

    let entry = EntryKey::for_scannable(&Id::from_parts(7, 7), 0x12_34_56_78);
    let (_lower, placement, _upper) = sm_client
        .locate_key(&entry)
        .await
        .unwrap()
        .expect("placement map should cover the key after recovery");

    let insert_result =
        TreeRpcService::insert(&local_service, placement.id, entry.clone(), placement.epoch).await;
    assert!(matches!(insert_result, OpResult::Successful(true)));

    let seek_result = TreeRpcService::seek(
        &local_service,
        placement.id,
        Range::new_inclusive_opened(entry.clone(), Ordering::Forward),
        &None::<Vec<u8>>,
        8,
        placement.epoch,
    )
    .await;
    match seek_result {
        OpResult::Successful(block) => assert_eq!(block.buffer, vec![entry.id()]),
        _ => panic!("expected recovered tree to serve seek successfully"),
    }

    timeout(Duration::from_secs(10), async {
        let _ = server.raft_service.shutdown().await;
        let _ = server.rpc.shutdown().await;
    })
    .await
    .expect("lazy hydration test teardown should not hang");
}

#[tokio::test(flavor = "multi_thread")]
async fn mark_migration_recreates_missing_metadata_cell() {
    let _ = env_logger::try_init();

    let server_addr = String::from("127.0.0.1:6832");
    let server_group = "ranged_recovery_fix_metadata";
    let server = NebServer::new_from_opts(
        &ServerOptions {
            chunk_size: 64 * 1024 * 1024,
            db_size: 64 * 1024 * 1024,
            tiered_config: None,
            backup_storage: None,
            wal_storage: None,
            undo_log_storage: None,
            raft_storage: None,
            index_enabled: false,
            services: vec![NebService::Cell],
            enable_recovery: false,
            disable_storage_locks: true,
        },
        &server_addr,
        server_group,
        async |_| {},
    )
    .await
    .unwrap();

    let client = Arc::new(
        client::AsyncClient::new(
            &server.rpc,
            &server.membership,
            &vec![server_addr],
            server_group,
        )
        .await
        .unwrap(),
    );

    client
        .new_schema_with_id(page_schema())
        .await
        .unwrap()
        .unwrap();
    client
        .new_schema_with_id(RANGED_TREE_SCHEMA.clone())
        .await
        .unwrap()
        .unwrap();

    storage::start_external_nodes_write_back(&client);

    let tree_id = Id::from_parts(902, 902);
    let tree = RangedTree::create(&client, &tree_id).await;
    let head_id = tree.head_id();

    client.remove_cell(tree_id).await.unwrap().unwrap();
    tree.mark_migration(&tree_id, None, &client)
        .await
        .expect("mark_migration should recreate a missing tree metadata cell");

    let restored = client.read_cell(tree_id).await.unwrap().unwrap();
    assert_eq!(restored.data[*RANGED_TREE_HEAD_HASH].id(), Some(&head_id));

    server.shutdown().await;
}
