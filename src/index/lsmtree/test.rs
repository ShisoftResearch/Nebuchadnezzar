use super::*;
use byteorder::BigEndian;
use byteorder::WriteBytesExt;
use client;
use futures::prelude::*;
use index::btree;
use index::key_with_id;
use index::lsmtree::tree::KeyRange;
use index::lsmtree::tree::LSMTree;
use index::Cursor;
use index::Ordering;
use itertools::Itertools;
use ram::types::Id;
use rand::distributions::Uniform;
use rand::thread_rng;
use rand::Rng;
use rayon::prelude::*;
use server::NebServer;
use server::ServerOptions;
use smallvec::SmallVec;
use std::env;
use std::io::Cursor as StdCursor;
use std::sync::Arc;
use std::thread;
use std::time::Duration;

fn u64_to_slice(n: u64) -> [u8; 8] {
    let mut key_slice = [0u8; 8];
    {
        let mut cursor = StdCursor::new(&mut key_slice[..]);
        cursor.write_u64::<BigEndian>(n);
    };
    key_slice
}

fn dump_trees(lsm_tree: &LSMTree, name: &str) {
    for i in 0..lsm_tree.trees.len() {
        lsm_tree.trees[i].dump(&format!("{}_lsm_{}_dump.json", name, i));
    }
}

fn default_key_range() -> KeyRange {
    (smallvec!(), smallvec!())
}

#[test]
pub fn insertions() {
    env_logger::init();
    let server_group = "lsm_insertions";
    let server_addr = String::from("127.0.0.1:5700");
    let server = NebServer::new_from_opts(
        &ServerOptions {
            chunk_count: 1,
            memory_size: 3 * 1024 * 1024 * 1024,
            backup_storage: None,
            wal_storage: None,
        },
        &server_addr,
        &server_group,
    );
    let client =
        Arc::new(client::AsyncClient::new(&server.rpc, &vec![server_addr], server_group).unwrap());
    client.new_schema_with_id(btree::page_schema()).wait();
    let tree = Arc::new(LSMTree::new(&client, default_key_range()));
    let num = env::var("LSM_TREE_TEST_ITEMS")
        // this value cannot do anything useful to the test
        // must arrange a long-term test to cover every levels
        .unwrap_or("331776".to_string())
        .parse::<u64>()
        .unwrap();

    let tree_clone = tree.clone();
    debug!("Testing LSM-tree");
    thread::spawn(move || loop {
        thread::sleep(Duration::from_secs(10));
        let tree_len = tree_clone.len();
        debug!(
            "LSM-Tree now have {}/{} elements, total {:.2}%",
            tree_len,
            num,
            tree_len as f32 / num as f32 * 100.0
        );
    });

    (0..num).collect::<Vec<_>>().par_iter().for_each(|i| {
        let i = *i;
        let id = Id::new(0, i);
        let key_slice = u64_to_slice(i);
        let key = SmallVec::from_slice(&key_slice);
        tree.insert(key, &id, 0);
    });

    dump_trees(&*tree, "stage_1_before_insertion");
    for _ in 0..20 {
        tree.check_and_merge();
    }
    dump_trees(&*tree, "stage_1_after_insertion");

    debug!("Start point search validations");
    (0..num).collect::<Vec<_>>().par_iter().for_each(|i| {
        let i = *i;
        let id = Id::new(0, i);
        let key_slice = u64_to_slice(i);
        let mut key = SmallVec::from_slice(&key_slice);
        debug!("checking: {}", i);
        let cursor = tree.seek(key.clone(), Ordering::Forward);
        key_with_id(&mut key, &id);
        assert_eq!(cursor.current(), Some(&key), "{}", i);
    });

    LSMTree::start_sentinel(&tree);

    let mut test_data = (num..num * 2).collect_vec();
    thread_rng().shuffle(test_data.as_mut_slice());
    test_data.par_iter().for_each(|i| {
        let i = *i;
        let id = Id::new(0, i);
        let key_slice = u64_to_slice(i);
        let key = SmallVec::from_slice(&key_slice);
        tree.insert(key, &id, 0);
    });

    dump_trees(&*tree, "stage_2_after_insertion");
    thread::sleep(Duration::new(30, 0));
    dump_trees(&*tree, "stage_2_waited_insertion");

    (0..num * 2).collect::<Vec<_>>().par_iter().for_each(|i| {
        let i = *i;
        let id = Id::new(0, i);
        let key_slice = u64_to_slice(i);
        let mut key = SmallVec::from_slice(&key_slice);
        debug!("checking: {}", i);
        let cursor = tree.seek(key.clone(), Ordering::Forward);
        key_with_id(&mut key, &id);
        assert_eq!(cursor.current(), Some(&key), "{}", i);
    });
}

#[test]
pub fn hybrid() {
    env_logger::init();
    let server_group = "lsm_hybird";
    let server_addr = String::from("127.0.0.1:5701");
    let server = NebServer::new_from_opts(
        &ServerOptions {
            chunk_count: 1,
            memory_size: 3 * 1024 * 1024 * 1024,
            backup_storage: None,
            wal_storage: None,
        },
        &server_addr,
        &server_group,
    );
    let client =
        Arc::new(client::AsyncClient::new(&server.rpc, &vec![server_addr], server_group).unwrap());
    client.new_schema_with_id(btree::page_schema()).wait();
    let tree = Arc::new(LSMTree::new(&client, default_key_range()));
    let num = env::var("LSM_TREE_TEST_ITEMS")
        // this value cannot do anything useful to the test
        // must arrange a long-term test to cover every levels
        .unwrap_or("663552".to_string())
        .parse::<u64>()
        .unwrap();

    LSMTree::start_sentinel(&tree);
    let tree_clone = tree.clone();
    debug!("Testing LSM-tree");
    thread::spawn(move || loop {
        thread::sleep(Duration::from_secs(10));
        let tree_len = tree_clone.len();
        debug!(
            "LSM-Tree now have {}/{} elements, total {:.2}%",
            tree_len,
            num,
            tree_len as f32 / num as f32 * 100.0
        );
    });

    let mut test_data = (0..num).collect_vec();
    thread_rng().shuffle(test_data.as_mut_slice());
    test_data.par_iter().for_each(|i| {
        let i = *i;
        let id = Id::new(0, i);
        let key_slice = u64_to_slice(i);
        let key = SmallVec::from_slice(&key_slice);
        tree.insert(key, &id, 0);
    });

    dump_trees(&*tree, "hybird_after_insertion");
    thread::sleep(Duration::new(30, 0));
    dump_trees(&*tree, "hybird_after_sleep");

    debug!("Start point search validations");
    (0..num).collect::<Vec<_>>().par_iter().for_each(|i| {
        let i = *i;
        let id = Id::new(0, i);
        let key_slice = u64_to_slice(i);
        let mut key = SmallVec::from_slice(&key_slice);
        debug!("checking: {}", i);
        let cursor = tree.seek(key.clone(), Ordering::Forward);
        key_with_id(&mut key, &id);
        assert_eq!(cursor.current(), Some(&key), "{}", i);
    });
}
