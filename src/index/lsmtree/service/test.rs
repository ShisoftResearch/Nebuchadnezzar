use super::*;
use bifrost::raft;
use bifrost::raft::client::RaftClient;
use client;
use client::AsyncClient;
use index::btree;
use index::btree::max_entry_key;
use index::btree::test::u64_to_slice;
use index::btree::LevelTree;
use index::btree::NodeCellRef;
use index::btree::{BPlusTree, RTCursor as BPlusTreeCursor};
use index::key_with_id;
use index::lsmtree::cursor::LSMTreeCursor;
use index::lsmtree::placement::sm::Placement;
use index::lsmtree::split::SplitStatus;
use index::lsmtree::split::{check_and_split, placement_client};
pub use index::lsmtree::tree::*;
use index::Cursor;
use index::EntryKey;
use index::Ordering;
use index::Ordering::Forward;
use index::*;
use itertools::Itertools;
use parking_lot::Mutex;
use parking_lot::RwLock;
use ram::segs::MAX_SEGMENT_SIZE;
use ram::types::Id;
use ram::types::RandValue;
use rand::thread_rng;
use rand::Rng;
use rayon::iter::IntoParallelRefIterator;
use server;
use server::NebServer;
use server::ServerOptions;
use std::collections::btree_map::Entry;
use std::collections::BTreeSet;
use std::sync::atomic::AtomicU64;
use std::sync::atomic::Ordering::Relaxed;
use std::sync::Arc;
use std::thread;
use std::time::Duration;
use std::{mem, ptr};

with_levels! {
    lm, 8;
    l1, 10;
}

#[test]
pub fn split() {
    env_logger::init();
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
    );
    let meta_servers = vec![server_addr];
    let client =
        Arc::new(client::AsyncClient::new(&server.rpc, &meta_servers, server_group).unwrap());
    client.new_schema_with_id(btree::page_schema()).wait();
    let raft_client = RaftClient::new(&meta_servers, raft::DEFAULT_SERVICE_ID).unwrap();
    let sm_client = Arc::new(lsmtree::placement::sm::client::SMClient::new(
        lsmtree::placement::sm::SM_ID,
        &raft_client,
    ));
    let lsm_tree = LSMTree::new_with_levels(
        init_lsm_level_trees(),
        (smallvec!(), max_entry_key()),
        Id::rand(),
    );

    let tree_capacity = lsm_tree.full_size() as u64;
    let test_volume = (tree_capacity as f32 * 1.1) as u64;
    let mut nums = (0..test_volume).collect_vec();
    thread_rng().shuffle(nums.as_mut_slice());
    nums.par_iter().for_each(|n| {
        let id = Id::new(0, *n);
        let mut entry_key: EntryKey = smallvec!();
        key_with_id(&mut entry_key, &id);
        lsm_tree.insert(entry_key);
    });
    nums.iter().for_each(|n| {
        let id = Id::new(0, *n);
        let mut entry_key: EntryKey = smallvec!();
        key_with_id(&mut entry_key, &id);
        let cursor = lsm_tree.seek(&entry_key, Forward);
        assert_eq!(cursor.current(), Some(&entry_key));
    });
    debug!("Inserted {} elements", test_volume);
    assert!(lsm_tree.is_full());
    debug!("Before split there are {} entries", lsm_tree.count());

    sm_client.upsert(&lsm_tree.to_placement()).wait().unwrap();
    lsm_tree.bump_epoch();
    check_and_split(&lsm_tree, &sm_client, &server);
    debug!("After split there are {} entries", lsm_tree.count());
    assert!(!lsm_tree.is_full());

    let first = sm_client.get(&lsm_tree.id).wait().unwrap().unwrap();
    debug!("First placement now end with {:?}", first.ends);
    assert!(first.ends < max_entry_key().into_iter().collect_vec());

    let pivot: EntryKey = SmallVec::from(first.ends.clone());
    (0..test_volume).collect_vec().iter().for_each(|n| {
        let id = Id::new(0, *n);
        let mut entry_key: EntryKey = smallvec!();
        key_with_id(&mut entry_key, &id);
        let cursor = lsm_tree.seek(&entry_key, Forward);
        if entry_key < pivot {
            // should exists in source tree
            assert_eq!(cursor.current(), Some(&entry_key));
        } else {
            assert_ne!(cursor.current(), Some(&entry_key));
            // should exists in split tree
            let vec_entry_key: Vec<_> = entry_key.iter().cloned().collect();
            let placement: Placement = sm_client.locate(&vec_entry_key).wait().unwrap().unwrap();
            assert_ne!(placement.id, lsm_tree.id);
            let client = placement_client(&placement.id, &server).wait().unwrap();
            let cursor_id = client
                .seek(
                    placement.id,
                    vec_entry_key,
                    Ordering::Forward,
                    placement.epoch,
                )
                .wait()
                .unwrap()
                .unwrap()
                .unwrap();
            let remote_key_vec = client
                .current(placement.id, cursor_id)
                .wait()
                .unwrap()
                .unwrap()
                .unwrap()
                .unwrap();
            let remote_key: EntryKey = SmallVec::from(remote_key_vec);
            assert_eq!(remote_key, entry_key);
        }
    });
}
