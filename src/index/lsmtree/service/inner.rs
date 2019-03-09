use client::AsyncClient;
use dovahkiin::types::custom_types::id::Id;
use index;
use index::lsmtree::cursor::LSMTreeCursor;
use index::lsmtree::tree::LSMTree;
use index::lsmtree::tree::{KeyRange, LSMTreeResult};
use index::Cursor;
use index::EntryKey;
use linked_hash_map::LinkedHashMap;
use parking_lot::Mutex;
use parking_lot::MutexGuard;
use ram::clock;
use std::cell::RefCell;
use std::cell::RefMut;
use std::collections::btree_map::BTreeMap;
use std::collections::hash_map::Entry;
use std::rc::Rc;
use std::sync::atomic::AtomicU64;
use std::sync::atomic::Ordering;
use std::sync::Arc;
use index::lsmtree::split::check_and_split;
use index::lsmtree::placement::sm::client::SMClient;

const CURSOR_DEFAULT_TTL: u32 = 5 * 60 * 1000;

struct DelegatedCursor {
    cursor: MutCursorRef,
    timestamp: u32,
}

type CursorMap = LinkedHashMap<u64, DelegatedCursor>;
type MutCursorRef = Rc<RefCell<LSMTreeCursor>>;

pub struct LSMTreeIns {
    pub tree: LSMTree,
    counter: AtomicU64,
    cursors: Mutex<CursorMap>,
}

impl DelegatedCursor {
    fn new(cursor: LSMTreeCursor) -> Self {
        let cursor = Rc::new(RefCell::new(cursor));
        let timestamp = clock::now();
        Self { cursor, timestamp }
    }
}

impl LSMTreeIns {
    pub fn new(neb_client: &Arc<AsyncClient>, range: KeyRange, id: Id) -> Self {
        Self {
            tree: LSMTree::new(neb_client, range, id),
            counter: AtomicU64::new(0),
            cursors: Mutex::new(CursorMap::new()),
        }
    }

    fn get(&self, id: &u64) -> Option<MutCursorRef> {
        self.cursors.lock().get_refresh(id).map(|c| {
            c.timestamp = clock::now();
            c.cursor.clone()
        })
    }

    fn pop_expired(map: &mut MutexGuard<CursorMap>) {
        let mut expired_cursors = 0;
        while let Some((_, c)) = map.iter().next() {
            if c.timestamp + CURSOR_DEFAULT_TTL < clock::now() {
                expired_cursors += 1;
            } else {
                break;
            }
        }
        for _ in 0..expired_cursors {
            map.pop_front();
        }
    }

    pub fn seek(&self, key: EntryKey, ordering: index::Ordering) -> u64 {
        let cursor = self.tree.seek(key, ordering);
        let mut map = self.cursors.lock();
        Self::pop_expired(&mut map);
        let id = self.counter.fetch_and(1, Ordering::Relaxed);
        map.insert(id, DelegatedCursor::new(cursor));
        return id;
    }

    pub fn next(&self, id: &u64) -> Option<bool> {
        self.get(id).map(|mut c| c.borrow_mut().next())
    }

    pub fn current(&self, id: &u64) -> Option<Option<Vec<u8>>> {
        self.get(id)
            .map(|c| c.borrow().current().map(|k| k.as_slice().to_vec()))
    }

    pub fn complete(&self, id: &u64) -> bool {
        self.cursors.lock().remove(id).is_some()
    }

    pub fn count(&self) -> u64 {
        self.tree.count() as u64
    }

    pub fn range(&self) -> (Vec<u8>, Vec<u8>) {
        let range = self.tree.range.lock();
        (range.0.clone().into_vec(), range.1.clone().into_vec())
    }

    pub fn check_and_merge(&self) {
        self.tree.check_and_merge()
    }

    pub fn insert(&self, key: EntryKey) -> bool {
        self.tree.insert(key)
    }

    pub fn epoch_mismatch(&self, epoch: u64) -> bool {
        self.tree.epoch_mismatch(epoch)
    }

    pub fn epoch(&self) -> u64 {
        self.tree.epoch()
    }

    pub fn merge(&self, keys: Box<Vec<EntryKey>>) {
        self.tree.merge(keys)
    }

    pub fn remove_to_right(&self, start_key: &EntryKey) {
        self.tree.remove_to_right(start_key);
    }

    pub fn set_epoch(&self, epoch: u64) {
        self.tree.set_epoch(epoch);
    }

    pub fn check_and_split(&self, tree: &LSMTree, sm: &Arc<SMClient>, client: &Arc<AsyncClient>) -> bool {
        check_and_split(&self.tree, sm, client)
    }
}

unsafe impl Send for LSMTreeIns {}
unsafe impl Sync for LSMTreeIns {}
