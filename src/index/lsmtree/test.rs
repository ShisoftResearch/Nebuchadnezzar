use super::*;
use byteorder::BigEndian;
use byteorder::WriteBytesExt;
use crate::client;
use crate::index::btree;
use crate::index::btree::min_entry_key;
use crate::index::btree::test::u64_to_slice;
use crate::index::trees::*;
use crate::index::lsmtree::tree::KeyRange;
use crate::index::lsmtree::tree::LSMTree;
use itertools::Itertools;
use crate::ram::types::Id;
use rand::distributions::Uniform;
use rand::thread_rng;
use rand::Rng;
use crate::server::NebServer;
use crate::server::ServerOptions;
use smallvec::SmallVec;
use std::env;
use std::io::Cursor as StdCursor;
use std::sync::Arc;
use std::thread;
use std::time::Duration;
use rayon::prelude::*;
use rand::seq::SliceRandom;

fn dump_trees(lsm_tree: &LSMTree, name: &str) {
    for i in 0..lsm_tree.trees.len() {
        lsm_tree.trees[i].dump(&format!("{}_lsm_{}_dump.json", name, i));
    }
}

fn default_key_range() -> KeyRange {
    (min_entry_key(), min_entry_key())
}

#[test]
pub fn insertions() {
    env_logger::init();
    let tree = Arc::new(LSMTree::new(default_key_range(), Id::unit_id()));
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
        let mut key = SmallVec::from_slice(&key_slice);
        key_with_id(&mut key, &id);
        tree.insert(key);
    });

    debug!("Start point search validations");
    tree.ensure_trees_in_order();
    (0..num).collect::<Vec<_>>().par_iter().for_each(|i| {
        let i = *i;
        let id = Id::new(0, i);
        let key_slice = u64_to_slice(i);
        let mut key = SmallVec::from_slice(&key_slice);
        debug!("checking: {}", i);
        let cursor = tree.seek(&key, Ordering::Forward);
        key_with_id(&mut key, &id);
        assert_eq!(cursor.current(), Some(&key), "{}", i);
    });

    dump_trees(&*tree, "insertions_before_merge");
    for _ in 0..50 {
        tree.check_and_merge();
    }
    dump_trees(&*tree, "insertions_after_merge");
    tree.ensure_trees_in_order();

    debug!("Start point search validations");
    (0..num).collect::<Vec<_>>().iter().for_each(|i| {
        let i = *i;
        let id = Id::new(0, i);
        let key_slice = u64_to_slice(i);
        let mut key = SmallVec::from_slice(&key_slice);
        debug!("checking: {}", i);
        let cursor = tree.seek(&key, Ordering::Forward);
        key_with_id(&mut key, &id);
        assert_eq!(cursor.current(), Some(&key), "{}, looking for {:?}", i, key);
    });
}

#[test]
pub fn hybrid() {
    env_logger::init();
    let tree = Arc::new(LSMTree::new(default_key_range(), Id::unit_id()));
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
    let mut rng = thread_rng();
    test_data.as_mut_slice().shuffle(&mut rng);
    test_data.par_iter().for_each(|i| {
        let i = *i;
        let id = Id::new(0, i);
        let key_slice = u64_to_slice(i);
        let mut key = SmallVec::from_slice(&key_slice);
        key_with_id(&mut key, &id);
        tree.insert(key);
    });

    tree.ensure_trees_in_order();
    dump_trees(&*tree, "hybird_after_insertion");
    thread::sleep(Duration::new(30, 0));
    dump_trees(&*tree, "hybird_after_sleep");
    tree.ensure_trees_in_order();

    debug!("Start point search validations");
    (0..num).collect::<Vec<_>>().iter().for_each(|i| {
        let i = *i;
        let id = Id::new(0, i);
        let key_slice = u64_to_slice(i);
        let mut key = SmallVec::from_slice(&key_slice);
        debug!("checking: {}", i);
        let cursor = tree.seek(&key, Ordering::Forward);
        key_with_id(&mut key, &id);
        assert_eq!(cursor.current(), Some(&key), "{}", i);
    });
}
