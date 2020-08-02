use super::*;
use bifrost::raft;
use bifrost::raft::client::RaftClient;
use crate::client;

use crate::index::btree;
use crate::index::btree::{max_entry_key, min_entry_key};
use crate::index::btree::BPlusTree;
use crate::index::trees::key_with_id;
use crate::index::lsmtree::placement::sm::Placement;
use crate::index::lsmtree::split::{check_and_split, tree_client};
pub use crate::index::lsmtree::tree::*;
use crate::index::trees::Cursor;
use crate::index::trees::EntryKey;
use crate::index::trees::Ordering;
use crate::index::trees::Ordering::Forward;
use crate::index::*;
use bifrost::membership::client::ObserverClient;
use itertools::Itertools;
use crate::ram::segs::MAX_SEGMENT_SIZE;
use crate::ram::types::Id;
use crate::ram::types::RandValue;
use rand::thread_rng;
use rayon::iter::IntoParallelRefIterator;
use crate::server;
use crate::server::NebServer;
use crate::server::ServerOptions;
use std::{mem, ptr};
use rand::seq::SliceRandom;
use rayon::prelude::*;

const BLOCK_SIZE: u32 = 32;

with_levels! {
    lm, 8;
    l1, 10;
}

#[tokio::test(threaded_scheduler)]
pub async fn split() {
    let _ = env_logger::try_init();
    let server_group = "lsm_service_split";
    let server_addr = String::from("127.0.0.1:5700");
    let server = NebServer::new_from_opts(
        &ServerOptions {
            chunk_count: 1,
            memory_size: 3 * 1024 * 1024 * 1024,
            backup_storage: None,
            wal_storage: None,
            services: vec![server::Service::Cell, server::Service::LSMTreeIndex],
        },
        &server_addr,
        &server_group,
    ).await;
    let meta_servers = vec![server_addr];
    let raft_client = RaftClient::new(&meta_servers, raft::DEFAULT_SERVICE_ID).await.unwrap();
    let membership = Arc::new(ObserverClient::new(&raft_client));
    let client =
        Arc::new(client::AsyncClient::new(&server.rpc, &membership, &meta_servers, server_group).await.unwrap());
    client.new_schema_with_id(btree::page_schema()).await.unwrap().unwrap();
    let sm_client = Arc::new(lsmtree::placement::sm::client::SMClient::new(
        lsmtree::placement::sm::SM_ID,
        &raft_client,
    ));
    let lsm_tree = LSMTree::new_with_levels(
        init_lsm_level_trees(),
        (min_entry_key(), max_entry_key()),
        Id::rand(),
    );

    let tree_capacity = lsm_tree.full_size() as u64;
    let test_volume = (tree_capacity as f32 * 1.1) as u64;
    let mut nums = (0..test_volume).collect_vec();
    let mut rng = thread_rng();
    nums.as_mut_slice().shuffle(&mut rng);
    nums.par_iter().for_each(|n| {
        let id = Id::new(0, *n);
        let mut entry_key: EntryKey = min_entry_key();
        key_with_id(&mut entry_key, &id);
        lsm_tree.insert(entry_key);
    });
    nums.iter().for_each(|n| {
        let id = Id::new(0, *n);
        let mut entry_key: EntryKey = min_entry_key();
        key_with_id(&mut entry_key, &id);
        let cursor = lsm_tree.seek(&entry_key, Forward);
        assert_eq!(cursor.current(), Some(&entry_key));
    });
    debug!("Inserted {} elements", test_volume);
    assert!(lsm_tree.is_full());
    debug!("Before split there are {} entries", lsm_tree.count());

    sm_client.upsert(&lsm_tree.to_placement()).await.unwrap().unwrap();
    lsm_tree.bump_epoch();
    check_and_split(&lsm_tree, &sm_client, &server).await;
    debug!("After split there are {} entries", lsm_tree.count());
    assert!(!lsm_tree.is_full());

    let first = sm_client.get(&lsm_tree.id).await.unwrap().unwrap();
    debug!("First placement now end with {:?}", first.ends);
    assert!(first.ends < max_entry_key().into_iter().collect_vec());

    let pivot: EntryKey = SmallVec::from(first.ends.clone());
    for n in 0..test_volume {
        let id = Id::new(0, n);
        let mut entry_key: EntryKey = min_entry_key();
        key_with_id(&mut entry_key, &id);
        let cursor = lsm_tree.seek(&entry_key, Forward);
        if entry_key < pivot {
            // should exists in source tree
            assert_eq!(cursor.current(), Some(&entry_key));
        } else {
            assert_ne!(cursor.current(), Some(&entry_key));
            // should exists in split tree
            let vec_entry_key: Vec<_> = entry_key.iter().cloned().collect();
            let placement: Placement = sm_client.locate(&vec_entry_key).await.unwrap().unwrap();
            assert_ne!(placement.id, lsm_tree.id);
            let client = tree_client(&placement.id, &server).await.unwrap();
            let block = client
                .seek(
                    placement.id,
                    vec_entry_key,
                    Ordering::Forward,
                    placement.epoch,
                    BLOCK_SIZE
                )
                .await
                .unwrap()
                .unwrap()
                .unwrap()
                .unwrap();
            let remote_key_vec = client
                .current(placement.id, block.cursor_id)
                .await
                .unwrap()
                .unwrap()
                .unwrap()
                .unwrap();
            let remote_key: EntryKey = SmallVec::from(remote_key_vec);
            assert_eq!(remote_key, entry_key);
        }
    }
}
