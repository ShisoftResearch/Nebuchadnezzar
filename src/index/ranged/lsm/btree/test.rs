use super::dump::dump_tree;
use super::reconstruct::TreeConstructor;
use super::*;
use crate::ram::types::RandValue;
use byteorder::BigEndian;
use byteorder::WriteBytesExt;
use dovahkiin::types::custom_types::id::Id;
use itertools::Itertools;
use lightning::map::HashSet;
use rand::distributions::Uniform;
use rand::prelude::*;
use rand::seq::SliceRandom;
use rayon::prelude::*;
use std::collections::HashSet as StdHashSet;
use std::env;
use std::fs::File;
use std::io::Cursor as StdCursor;
use std::io::Write;
use std::mem::size_of;
use std::sync::Arc;
use std::thread;
use std::time::Duration;

extern crate env_logger;

#[derive(Serialize, Deserialize)]
struct DebugNode {
    keys: Vec<String>,
    nodes: Vec<DebugNode>,
    id: Option<String>,
    next: Option<String>,
    prev: Option<String>,
    len: usize,
    is_external: bool,
}

pub const PAGE_SIZE: usize = 26;
pub type LevelBPlusTree = BPlusTree<PAGE_SIZE>;

pub fn u64_to_slice(n: u64) -> [u8; 8] {
    let mut key_slice = [0u8; 8];
    {
        let mut cursor = StdCursor::new(&mut key_slice[..]);
        cursor.write_u64::<BigEndian>(n).unwrap();
    };
    key_slice
}

#[test]
fn node_size() {
    // expecting the node size to be an on-heap pointer plus node type tag, aligned, and one for concurrency control.
    assert_eq!(
        size_of::<Node<KeySlice<PAGE_SIZE>, RefSlice<PAGE_SIZE>>>(),
        size_of::<usize>() * 3
    );
}

fn deletion_set() -> Arc<DeletionSet> {
    Arc::new(HashSet::with_capacity(16))
}

#[test]
fn init() {
    let _ = env_logger::try_init();
    let tree = LevelBPlusTree::new(&deletion_set());
    let id = Id::new(1, 2);
    let key = EntryKey::from_id(&id);
    info!("test insertion");
    let entry_key = key.clone();
    tree.insert(&entry_key);
    let cursor = tree.seek(&key, Ordering::Forward);
    assert_eq!(cursor.current().unwrap().id(), id);
}

fn check_ordering(tree: &LevelBPlusTree, key: &EntryKey) {
    let mut cursor = tree.seek(&*MIN_ENTRY_KEY, Ordering::Forward);
    let mut last_key = cursor.current().unwrap().clone();
    while cursor.next().is_some() {
        let current = cursor.current().unwrap();
        if &last_key > current {
            dump_tree(tree, "error_insert_dump.json");
            panic!(
                "error on ordering check {:?} > {:?}, key {:?}",
                last_key, current, key
            );
        }
        last_key = current.clone();
    }
}

#[test]
fn crd() {
    let _ = env_logger::try_init();
    let tree = LevelBPlusTree::new(&deletion_set());
    std::fs::remove_dir_all("dumps").unwrap();
    std::fs::create_dir_all("dumps").unwrap();
    let num = env::var("BTREE_TEST_ITEMS")
        .unwrap_or("1000".to_string())
        .parse::<u64>()
        .unwrap();
    // die-rolling
    let rng = thread_rng();
    let die_range = Uniform::new_inclusive(1, 6);
    let mut roll_die = rng.sample_iter(&die_range);
    {
        info!("test insertion");
        let mut nums = (0..num).collect_vec();
        let mut rng = thread_rng();
        nums.as_mut_slice().shuffle(&mut rng);
        let json = serde_json::to_string(&nums).unwrap();
        let mut file = File::create("nums_dump.json").unwrap();
        file.write_all(json.as_bytes()).unwrap();
        let mut i = 0;
        for n in nums {
            let id = Id::new(1, n);
            let key = EntryKey::from_id(&id);
            debug!("{}. insert id: {}", i, n);
            tree.insert(&key);
            if roll_die.next().unwrap() == 6 {
                check_ordering(&tree, &key);
            }
            i += 1;
        }
        dump_tree(&tree, "tree_dump.json");
        assert_eq!(tree.len(), num as usize);
        assert!(verification::is_tree_in_order(&tree, 0));
    }

    {
        debug!("Scanning for sequence");
        let mut cursor = tree.seek(&*MIN_ENTRY_KEY, Ordering::Forward);
        for i in 0..num {
            let id = cursor.current().unwrap().id();
            let unmatched = i != id.lower;
            let check_msg = if unmatched {
                "=-=-=-=-=-=-=-= NO =-=-=-=-=-=-="
            } else {
                "YES"
            };
            debug!("Index {} have id {:?} check: {}", i, id, check_msg);
            if unmatched {
                debug!(
                    "Expecting index {} encoded {:?}",
                    i,
                    Id::new(1, i).to_binary()
                );
            }
            assert_eq!(cursor.next().is_some(), i + 1 < num);
        }
        debug!("Forward scanning for sequence verification");
        let mut cursor = tree.seek(&*MIN_ENTRY_KEY, Ordering::Forward);
        for i in 0..num {
            let expected = Id::new(1, i);
            debug!("Expecting id {:?}", expected);
            let id = cursor.current().unwrap().id();
            assert_eq!(id, expected);
            assert_eq!(cursor.next().is_some(), i + 1 < num);
        }
        assert!(cursor.current().is_none());
    }

    {
        debug!("Backward scanning for sequence verification");
        // search backward required max possible id
        let entry_key = EntryKey::from_id(&Id::new(::std::u64::MAX, ::std::u64::MAX));
        let mut cursor = tree.seek(&entry_key, Ordering::Backward);
        for i in (0..num).rev() {
            let expected = Id::new(1, i);
            debug!("Expecting id {:?}", expected);
            let id = cursor.current().unwrap().id();
            assert_eq!(id, expected, "{}", i);
            assert_eq!(cursor.next().is_some(), i > 0);
        }
        assert!(cursor.current().is_none());
    }

    {
        debug!("point search");
        for i in 0..num {
            let id = Id::new(1, i);
            let key = EntryKey::from_id(&id);
            assert_eq!(
                tree.seek(&key, Ordering::default()).current().unwrap().id(),
                id,
                "{}",
                i
            );
        }
    }
}

#[test]
pub fn alternative_insertion_pattern() {
    let _ = env_logger::try_init();
    let tree = LevelBPlusTree::new(&deletion_set());
    let num = env::var("BTREE_TEST_ITEMS")
        // this value cannot do anything useful to the test
        // must arrange a long-term test to cover every levels
        .unwrap_or("1000".to_string())
        .parse::<u64>()
        .unwrap();

    for i in 0..num {
        let id = Id::new(1, i);
        let key = EntryKey::from_id(&id);
        debug!("insert {:?}", key);
        tree.insert(&key);
    }

    assert!(verification::is_tree_in_order(&tree, 0));

    let rng = thread_rng();
    let die_range = Uniform::new_inclusive(1, 6);
    let mut roll_die = rng.sample_iter(&die_range);
    for i in 0..num {
        let id = Id::new(1, i);
        let key = EntryKey::from_id(&id);
        if roll_die.next().unwrap() != 6 {
            continue;
        }
        debug!("checking {:?}", &key);
        let mut cursor = tree.seek(&key, Ordering::Forward);
        for j in i..num {
            let id = Id::new(1, j);
            let key = EntryKey::from_id(&id);
            assert_eq!(cursor.current(), Some(&key));
            assert_eq!(cursor.next().is_some(), j != num - 1);
        }
    }
}

#[test]
fn parallel() {
    let _ = env_logger::try_init();
    let tree = Arc::new(LevelBPlusTree::new(&deletion_set()));
    let num = env::var("BTREE_TEST_ITEMS")
        // this value cannot do anything useful to the test
        // must arrange a long-term test to cover every levels
        .unwrap_or("1000".to_string())
        .parse::<u64>()
        .unwrap();

    let tree_clone = tree.clone();
    thread::spawn(move || loop {
        thread::sleep(Duration::from_secs(10));
        let tree_len = tree_clone.len();
        debug!(
            "B+ Tree now have {}/{} elements, total {:.2}%",
            tree_len,
            num,
            tree_len as f32 / num as f32 * 100.0
        );
    });
    let mut rng = rand::thread_rng();
    let mut nums = (0..num).collect_vec();
    nums.as_mut_slice().shuffle(&mut rng);
    nums.par_iter().for_each(|i| {
        let i = *i;
        let id = Id::new(1, i);
        let key = EntryKey::from_id(&id);
        tree.insert(&key);
    });

    dump_tree(&*tree, "btree_parallel_insertion_dump.json");
    debug!("Start validation");

    assert!(verification::is_tree_in_order(&*tree, 0));

    nums.as_mut_slice().shuffle(&mut rng);
    let die_range = Uniform::new_inclusive(1, 6);
    let roll_die = RwLock::new(rng.sample_iter(&die_range));
    (0..num).collect::<Vec<_>>().iter().for_each(|i| {
        let i = *i;
        let id = Id::new(1, i);
        let key = EntryKey::from_id(&id);
        debug!("checking: {}", i);
        {
            let mut cursor = tree.seek(&key, Ordering::Forward);
            assert_eq!(cursor.current(), Some(&key), "{}", i);
            if roll_die.write().next().unwrap() == 6 {
                debug!("Scanning {}", num);
                for j in i..num {
                    let id = Id::new(1, j);
                    let key = EntryKey::from_id(&id);
                    assert_eq!(cursor.current(), Some(&key), "{}/{}", i, j);
                    assert_eq!(cursor.next().is_some(), j != num - 1, "{}/{}", i, j);
                }
            }
        }
        {
            let mut cursor = tree.seek(&key, Ordering::Backward);
            assert_eq!(cursor.current(), Some(&key), "{}", i);
            if roll_die.write().next().unwrap() == 6 {
                debug!("Scanning {}", num);
                for j in (0..=i).rev() {
                    let id = Id::new(1, j);
                    let key = EntryKey::from_id(&id);
                    assert_eq!(cursor.current(), Some(&key), "{}/{}", i, j);
                    assert_eq!(cursor.next().is_some(), j != 0, "{}/{}", i, j);
                }
            }
        }
    });

    //    debug!("Start parallel deleting");
    //    let mut nums = (num / 2..num).collect_vec();
    //    thread_rng().shuffle(nums.as_mut_slice());
    //    nums.par_iter().for_each(|i| {
    //        debug!("Deleting {}", i);
    //        let i = *i;
    //        let id = Id::new(0, i);
    //        let key_slice = u64_to_slice(i);
    //        let mut key = SmallVec::from_slice(&key_slice);
    //        key_with_id(&mut key, &id);
    //        assert!(tree.remove(&key), "Cannot find item to remove {}, {:?}", i, &key);
    //    });
    //    dump_tree(&*tree, "btree_parallel_deletion_dump.json");
}

const TINY_PAGE_SIZE: usize = 5;
type TinyLevelBPlusTree = BPlusTree<TINY_PAGE_SIZE>;
type TinyKeySlice = KeySlice<TINY_PAGE_SIZE>;
type TinyPtrSlice = RefSlice<TINY_PAGE_SIZE>;

#[tokio::test(flavor = "multi_thread")]
async fn level_merge() {
    let _ = env_logger::try_init();
    let range = 1000;
    let deletion = deletion_set();
    let tree_1 = Arc::new(TinyLevelBPlusTree::new(&deletion));
    let tree_2 = Arc::new(LevelBPlusTree::new(&deletion));
    for i in 0..range {
        let n = i * 2;
        let id = Id::new(1, n);
        let key = EntryKey::from_id(&id);
        debug!("insert id: {}", n);
        let entry_key = key.clone();
        tree_1.insert(&entry_key);
    }

    for i in 0..range {
        let n = i * 2 + 1;
        let id = Id::new(1, n);
        let key = EntryKey::from_id(&id);
        debug!("insert id: {}", n);
        tree_2.insert(&key);
    }

    dump_tree(&tree_1, "lsm-tree_level_merge_1_before_dump.json");
    dump_tree(&tree_2, "lsm-tree_level_merge_2_before_dump.json");

    assert!(verification::is_tree_in_order(&*tree_1, 0));
    assert!(verification::is_tree_in_order(&*tree_2, 0));

    debug!("MERGING...");
    let merged = tree_1.merge_to(999, &*tree_2, &mut StdHashSet::new(), true);
    assert!(merged > 0);

    dump_tree(&tree_1, "lsm-tree_level_merge_1_after_dump.json");
    dump_tree(&tree_2, "lsm-tree_level_merge_2_after_dump.json");

    assert!(verification::is_tree_in_order(&*tree_1, 0));
    assert!(verification::is_tree_in_order(&*tree_2, 0));

    for i in 0..range {
        let n2 = i * 2 + 1;
        let id2 = Id::new(1, n2);
        let key2 = EntryKey::from_id(&id2);
        let key2_cur = tree_2.seek(&key2, Ordering::Forward);
        if (i as usize) < merged {
            let n1 = i * 2;
            let id1 = Id::new(1, n1);
            let key1 = EntryKey::from_id(&id1);
            let mut key1_cur = tree_2.seek(&key1, Ordering::Forward);
            assert_eq!(key1_cur.current().unwrap(), &key1);
            assert_eq!(key1_cur.next().is_some(), i as usize != merged);
            assert_eq!(key1_cur.current().unwrap(), &key2);
            assert_ne!(tree_1.seek(&key1, Ordering::Forward).current(), Some(&key1));
        }
        assert_eq!(key2_cur.current().unwrap(), &key2);

        tree_2.seek(&key2, Default::default());
    }
}

#[test]
fn level_merge_insertion() {
    let _ = env_logger::try_init();
    let range = 10000;
    let nums = range * 3;
    let tree = Arc::new(TinyLevelBPlusTree::new(&deletion_set()));
    let mut numbers = (0..nums).collect_vec();
    let mut rng = thread_rng();
    numbers.as_mut_slice().shuffle(&mut rng);
    let tree_nums = numbers.as_slice()[0..range].iter().cloned().collect_vec();
    let merge_nums = numbers.as_slice()[range..range * 2]
        .iter()
        .cloned()
        .collect_vec();
    let insert_nums = numbers.as_slice()[range * 2..]
        .iter()
        .cloned()
        .collect_vec();
    for i in tree_nums {
        let n = i as u64;
        let id = Id::new(1, n);
        let key = EntryKey::from_id(&id);
        assert!(tree.insert(&key));
    }

    dump_tree(&tree, "lsm-tree_level_merge_insert_orig_dump.json");
    assert!(verification::is_tree_in_order(&*tree, 0));

    let tree_2 = tree.clone();
    let th1 = thread::spawn(move || {
        insert_nums.into_par_iter().for_each(|i| {
            let n = i as u64;
            let id = Id::new(1, n);
            let key = EntryKey::from_id(&id);
            assert!(tree_2.insert(&key));
        });
    });

    let tree_3 = tree.clone();
    let merge_keys = merge_nums
        .into_iter()
        .map(|i| {
            let n = i as u64;
            let id = Id::new(1, n);
            let entry_key = EntryKey::from_id(&id);
            entry_key
        })
        .sorted()
        .collect();
    let th2 = thread::spawn(move || {
        tree_3.merge_with_keys(merge_keys);
    });
    th1.join().unwrap();
    th2.join().unwrap();

    dump_tree(&tree, "lsm-tree_level_merge_insert_ins_dump.json");
    assert!(verification::is_tree_in_order(&*tree, 0));

    numbers.sort();
    for num in numbers {
        let n = num as u64;
        let id = Id::new(1, n);
        let key = EntryKey::from_id(&id);
        let cursor = tree.seek(&key, Ordering::Forward);
        assert_eq!(&key, cursor.current().unwrap());
    }
}

#[test]
fn reconstruct() {
    let _ = env_logger::try_init();
    let num = env::var("BTREE_RECONSTRUCT_TEST")
        // this value cannot do anything useful to the test
        // must arrange a long-term test to cover every levels
        .unwrap_or("1000".to_string())
        .parse::<u64>()
        .unwrap();

    let tree = {
        let mut reconstructor = TreeConstructor::<TINY_PAGE_SIZE>::new();
        let new_node = || ExtNode::<TinyKeySlice, TinyPtrSlice>::new(Id::rand(), max_entry_key());
        let mut nodes = vec![];
        let mut last_node = new_node();
        for i in 0..num {
            let id = Id::new(1, i);
            let key = EntryKey::from_id(&id);
            if last_node.len == TINY_PAGE_SIZE {
                last_node.right_bound = key.clone();
                nodes.push(last_node);
                last_node = new_node();
            }
            last_node.keys[last_node.len] = key;
            last_node.len += 1;
        }
        nodes.push(last_node);
        let nodes = nodes
            .into_iter()
            .map(|n| NodeCellRef::new(Node::with_external(n)))
            .collect_vec();
        nodes.iter().enumerate().for_each(|(i, _nr)| {
            if i == 0 {
                return;
            }
            let mut node = write_node::<TinyKeySlice, TinyPtrSlice>(&nodes[i]);
            let mut prev = write_node::<TinyKeySlice, TinyPtrSlice>(&nodes[i - 1]);
            let node_ref = node.node_ref().clone();
            let prev_ref = prev.node_ref().clone();
            let mut node_extnode = node.extnode_mut_no_persist();
            let mut prev_extnode = prev.extnode_mut_no_persist();
            node_extnode.prev = prev_ref;
            prev_extnode.next = node_ref;
        });
        debug!("Total {} external nodes", nodes.len());
        nodes.iter().for_each(|n| {
            let first_node = read_unchecked::<TinyKeySlice, TinyPtrSlice>(n)
                .first_key()
                .clone();
            reconstructor.push_extnode(n, first_node);
        });
        BPlusTree::<TINY_PAGE_SIZE>::from_root(
            reconstructor.root(),
            Id::rand(),
            num as usize,
            &deletion_set(),
        )
    };
    dump_tree(&tree, "reconstruct_first_run_dump.json");
    for n in 0..num {
        let id = Id::new(1, n);
        let key = EntryKey::from_id(&id);
        let cursor = tree.seek(&key, Ordering::Forward);
        assert_eq!(&key, cursor.current().unwrap());
    }
}
