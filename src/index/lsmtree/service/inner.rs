use std::collections::btree_map::BTreeMap;
use index::lsmtree::cursor::LSMTreeCursor;
use parking_lot::Mutex;
use std::rc::Rc;
use ram::clock;
use parking_lot::MutexGuard;
use index::Cursor;
use std::cell::RefMut;
use std::cell::RefCell;
use linked_hash_map::LinkedHashMap;
use std::sync::atomic::AtomicU64;
use std::sync::atomic::Ordering;
use index::lsmtree::tree::LSMTree;
use index::EntryKey;
use index;
use std::sync::Arc;
use client::AsyncClient;

const CURSOR_DEFAULT_TTL: u32 = 5 * 60 * 1000;

struct DelegatedCursor {
    cursor: MutCursorRef,
    timestamp: u32
}

type CursorMap = LinkedHashMap<u64, DelegatedCursor>;
type MutCursorRef = Rc<RefCell<LSMTreeCursor>>;

pub struct TreeServiceInner {
    tree: LSMTree,
    counter: AtomicU64,
    cursors: Mutex<CursorMap>
}

impl DelegatedCursor {
    fn new(cursor: LSMTreeCursor) -> Self {
        let cursor = Rc::new(RefCell::new(cursor));
        let timestamp = clock::now();
        Self {
            cursor, timestamp
        }
    }
}

impl TreeServiceInner {

    pub fn new(neb_client: &Arc<AsyncClient>) -> Self {
        Self {
            tree: LSMTree::new(neb_client),
            counter: AtomicU64::new(0),
            cursors: Mutex::new(CursorMap::new())
        }
    }

    fn get(&self , id: &u64) -> Option<MutCursorRef> {
        self.cursors.lock().get_refresh(id).map(|c| {
            c.timestamp = clock::now();
            c.cursor.clone()
        })
    }

    fn pop_expired(map: &mut MutexGuard<CursorMap>) {
        if let Some((_, c)) = map.iter().next() {
            if c.timestamp + CURSOR_DEFAULT_TTL > clock::now() {
                return;
            }
        } else {
            return;
        }
        map.pop_front();
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
        self.get(id).map(|c| c.borrow().current().map(|k| k.as_slice().to_vec()))
    }

    pub fn complete(&self, id: &u64) -> bool {
        self.cursors.lock().remove(id).is_some()
    }
}

unsafe impl Send for TreeServiceInner {}
unsafe impl Sync for TreeServiceInner {}