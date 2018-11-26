use super::btree::{BPlusTree, RTCursor as BPlusTreeCursor};
use super::sstable::*;
use super::*;
use client::AsyncClient;
use itertools::Itertools;
use parking_lot::RwLock;
use ram::segs::MAX_SEGMENT_SIZE;
use std::collections::BTreeSet;
use std::sync::Arc;
use std::thread;
use std::time::Duration;
use std::{mem, ptr};

const LEVEL_M_MAX_ELEMENTS_COUNT: usize = LEVEL_M * LEVEL_M * LEVEL_M;
const LEVEL_ELEMENTS_MULTIPLIER: usize = 10;
const LEVEL_DIFF_MULTIPLIER: usize = 10;
const LEVEL_M: usize = super::btree::NUM_KEYS;
const LEVEL_1: usize = LEVEL_M * LEVEL_DIFF_MULTIPLIER;
const LEVEL_2: usize = LEVEL_1 * LEVEL_DIFF_MULTIPLIER;
const LEVEL_3: usize = LEVEL_2 * LEVEL_DIFF_MULTIPLIER;
const LEVEL_4: usize = LEVEL_3 * LEVEL_DIFF_MULTIPLIER;

type LevelTrees = Vec<Box<SSLevelTree>>;

macro_rules! with_levels {
    ($($sym:ident, $level_size:ident;)*) => {
        $(
            type $sym = [EntryKey; $level_size];
            impl_sspage_slice!($sym, EntryKey, $level_size);
        )*

        fn init_lsm_level_trees(neb_client: &Arc<AsyncClient>) -> (LevelTrees, Vec<usize>, Vec<usize>) {
            let mut trees = LevelTrees::new();
            let mut max_elements_list = Vec::new();
            let mut max_elements = LEVEL_M_MAX_ELEMENTS_COUNT;
            let mut page_sizes = Vec::new();
            $(
                max_elements *= LEVEL_ELEMENTS_MULTIPLIER;
                trees.push(box LevelTree::<$sym>::new(neb_client));
                max_elements_list.push(max_elements);
                page_sizes.push($level_size);
                const_assert!($level_size * KEY_SIZE <= MAX_SEGMENT_SIZE);
            )*
            return (trees, max_elements_list, page_sizes);
        }
    };
}

with_levels! {
    L1, LEVEL_1;
    L2, LEVEL_2;
    L3, LEVEL_3;
    L4, LEVEL_4;
}

pub struct LSMTree {
    level_m: BPlusTree,
    trees: LevelTrees,
    // use Vec here for convenience
    max_sizes: Vec<usize>,
    page_sizes: Vec<usize>,
}

unsafe impl Send for LSMTree {}
unsafe impl Sync for LSMTree {}

impl LSMTree {
    pub fn new(neb_client: &Arc<AsyncClient>) -> Arc<Self> {
        let (trees, max_sizes, page_sizes) = init_lsm_level_trees(neb_client);
        let lsm_tree = Arc::new(LSMTree {
            level_m: BPlusTree::new(neb_client),
            trees,
            max_sizes,
            page_sizes,
        });
        let tree_clone = lsm_tree.clone();
        thread::spawn(move || {
            tree_clone.sentinel();
        });
        lsm_tree
    }

    pub fn insert(&self, mut key: EntryKey, id: &Id) {
        key_with_id(&mut key, id);
        self.level_m.insert(&key)
    }

    pub fn remove(&self, mut key: EntryKey, id: &Id) -> bool {
        key_with_id(&mut key, id);
        let m_deleted = self.level_m.remove(&key);
        let levels_deleted = self
            .trees
            .iter()
            .map(|tree| tree.mark_deleted(&key))
            .collect_vec() // collect here to prevent short circuit
            .into_iter()
            .any(|d| d);
        m_deleted || levels_deleted
    }

    pub fn seek(&self, mut key: EntryKey, ordering: Ordering) -> LSMTreeCursor {
        match ordering {
            Ordering::Forward => key_with_id(&mut key, &Id::unit_id()),
            Ordering::Backward => key_with_id(&mut key, &Id::new(::std::u64::MAX, ::std::u64::MAX)),
        };
        let mut cursors: Vec<Box<Cursor>> = vec![box self.level_m.seek(&key, ordering)];
        for tree in &self.trees {
            cursors.push(tree.seek(&key, ordering));
        }
        return LSMTreeCursor::new(
            cursors,
            self.trees.iter().map(|t| t.tombstones()).collect_vec(),
        );
    }

    fn sentinel(&self) {
        self.check_and_merge(
            LEVEL_M_MAX_ELEMENTS_COUNT,
            self.page_sizes[0],
            &mut BTreeSet::new(),
            &self.level_m,
            &*self.trees[0],
        );
        for i in 0..self.trees.len() - 1 {
            let upper = &*self.trees[i];
            let lower = &*self.trees[i + 1];
            let upper_tombstons_ref = upper.tombstones();
            let mut upper_tombstones = upper_tombstons_ref.write();
            self.check_and_merge(
                self.max_sizes[i],
                self.page_sizes[i + 1],
                &mut *upper_tombstones,
                upper,
                lower,
            );
        }
        thread::sleep(Duration::from_millis(100));
    }

    fn check_and_merge<U, L>(
        &self,
        max_elements: usize,
        upper_page_size: usize,
        upper_tombstones: &mut TombstonesInner,
        upper_level: &U,
        lower_level: &L,
    ) where
        U: MergeableTree + ?Sized,
        L: SSLevelTree + ?Sized,
    {
        let elements = upper_level.elements();
        if elements > max_elements {
            debug!(
                "upper level have elements {} exceeds {}, start level merging",
                elements, max_elements
            );
            let entries_to_merge = upper_page_size;
            let guard = upper_level.prepare_level_merge();
            let mut pages = vec![guard.last_page()];
            let mut collected_entries = pages.last().unwrap().keys().len();
            while collected_entries < entries_to_merge {
                let next_page = pages.last().unwrap().next();
                collected_entries += next_page.keys().len();
                pages.push(next_page);
            }
            let mut entries = vec![];
            for page in &pages {
                entries.append(&mut Vec::from(page.keys()));
            }
            debug!("going to merge {} entries", entries.len());
            lower_level.merge(entries.as_mut_slice(), upper_tombstones);
            guard.remove_pages(pages.iter().map(|p| p.keys()).collect_vec().as_slice())
        }
    }

    pub fn len(&self) -> usize {
        let mem_len = self.level_m.len();
        let levels_len_sum = self.trees.iter().map(|tree| tree.len()).sum::<usize>();
        return mem_len + levels_len_sum;
    }
}

pub struct LSMTreeCursor {
    level_cursors: Vec<Box<Cursor>>,
    tombstones: Vec<Arc<Tombstones>>,
}

impl LSMTreeCursor {
    fn new(cursors: Vec<Box<Cursor>>, tombstones: Vec<Arc<Tombstones>>) -> Self {
        LSMTreeCursor {
            tombstones,
            level_cursors: cursors,
        }
    }

    fn next_candidate(&mut self) -> (bool, usize) {
        // TODO: redo this part
        let min_tree = self
            .level_cursors
            .iter()
            .enumerate()
            .map(|(i, cursor)| (i, cursor.current()))
            .filter_map(|(i, current)| current.map(|current_val| (i, current_val)))
            .min_by_key(|(i, val)| val.clone())
            .map(|(id, _)| id);
        if let Some(id) = min_tree {
            let min_has_next = self.level_cursors[id].next();
            if !min_has_next {
                let next = self
                    .level_cursors
                    .iter()
                    .enumerate()
                    .filter_map(|(level, cursor)| cursor.current().map(|key| (id, key)))
                    .min_by_key(|(level, key)| key.clone());
                if let Some((id, _)) = next {
                    return (true, id);
                }
            } else {
                return (true, id);
            }
        }
        return (false, 0);
    }
}

impl Cursor for LSMTreeCursor {
    fn next(&mut self) -> bool {
        loop {
            let (has_next, candidate_level) = self.next_candidate();
            if !has_next {
                break;
            }
            if candidate_level == 0 {
                return has_next;
            }
            let candidate_key = self.level_cursors[candidate_level].current().unwrap();
            if self.tombstones[candidate_level + 1]
                .read()
                .contains(&candidate_key)
            {
                // marked in tombstone, skip
                continue;
            }
            return true;
        }
        return false;
    }

    fn current(&self) -> Option<EntryKey> {
        self.level_cursors.iter().filter_map(|c| c.current()).min()
    }
}

#[cfg(test)]
mod test {
    use super::*;
    use byteorder::BigEndian;
    use byteorder::WriteBytesExt;
    use client;
    use futures::prelude::*;
    use rand::distributions::Uniform;
    use rand::thread_rng;
    use rand::Rng;
    use rayon::prelude::*;
    use server::NebServer;
    use server::ServerOptions;
    use std::env;
    use std::io::Cursor as StdCursor;

    fn u64_to_slice(n: u64) -> [u8; 8] {
        let mut key_slice = [0u8; 8];
        {
            let mut cursor = StdCursor::new(&mut key_slice[..]);
            cursor.write_u64::<BigEndian>(n);
        };
        key_slice
    }

    #[test]
    #[ignore]
    pub fn insertions() {
        env_logger::init();
        let server_group = "sstable_index_init";
        let server_addr = String::from("127.0.0.1:5600");
        let server = NebServer::new_from_opts(
            &ServerOptions {
                chunk_count: 1,
                memory_size: 4 * 1024 * 1024 * 1024,
                backup_storage: None,
                wal_storage: None,
            },
            &server_addr,
            &server_group,
        );
        let client = Arc::new(
            client::AsyncClient::new(&server.rpc, &vec![server_addr], server_group).unwrap(),
        );
        client.new_schema_with_id(super::get_schema()).wait();
        let tree = LSMTree::new(&client);
        let num = env::var("LSM_TREE_TEST_ITEMS")
            // this value cannot do anything useful to the test
            // must arrange a long-term test to cover every levels
            .unwrap_or("1000".to_string())
            .parse::<u64>()
            .unwrap();

        let tree_clone = tree.clone();
        thread::spawn(move || {
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
            tree.insert(key, &id);
        });

        if tree.len() > LEVEL_M_MAX_ELEMENTS_COUNT {
            debug!("Sleep 5 minute for level merge");
            thread::sleep(Duration::from_secs(5 * 60));
        }

        debug!("Start validations");
        (0..num).collect::<Vec<_>>().par_iter().for_each(|i| {
            let mut rng = rand::rngs::OsRng::new().unwrap();
            let die_range = Uniform::new_inclusive(1, 6);
            let mut roll_die = rng.sample_iter(&die_range);
            let i = *i;
            let id = Id::new(0, i);
            let key_slice = u64_to_slice(i);
            let mut key = SmallVec::from_slice(&key_slice);
            debug!("checking: {}", i);
            let mut cursor = tree.seek(key.clone(), Ordering::Forward);
            key_with_id(&mut key, &id);
            assert_eq!(cursor.current(), Some(&key), "{}", i);
            if roll_die.next().unwrap() == 6 {
                for j in i..num {
                    let id = Id::new(0, j);
                    let key_slice = u64_to_slice(j);
                    let mut key = SmallVec::from_slice(&key_slice);
                    key_with_id(&mut key, &id);
                    assert_eq!(cursor.current(), Some(&key), "{}/{}", i, j);
                    assert_eq!(cursor.next(), j != num - 1, "{}/{}", i, j);
                }
            }
        });
    }
}
