use smallvec::SmallVec;
use dovahkiin::types::custom_types::id::Id;
use utils::lru_cache::LRUCache;
use std::io::Cursor;
use std::borrow::{Borrow, BorrowMut};
use parking_lot::RwLock;
use std::borrow::Cow;
use parking_lot::Mutex;
use std::rc::Rc;
use core::mem;
use std::cell::Cell;
use std::cell::RefCell;
use std::ops::Deref;
use std::cell::Ref;

const ID_SIZE: usize = 16;
const NUM_NODES: usize = 2048;

type EntryKey = SmallVec<[u8; 32]>;
type ExtNodeCacheMap = Mutex<LRUCache<Id, ExtNodeCached>>;

struct InNode {
    keys: [EntryKey; NUM_NODES],
    pointers: [Box<Node>; NUM_NODES + 1],
    len: usize
}

#[derive(Clone)]
struct ExtNodeCached {
    id: Id,
    keys: [EntryKey; NUM_NODES],
    next: Id,
    len: usize,
    version: u64
}

enum ExtNodeInner {
    Pointer(Id),
    Cached(ExtNodeCached)
}

struct ExtNode {
    inner: RefCell<ExtNodeInner>
}

pub enum Node {
    External(Box<ExtNode>),
    Internal(Box<InNode>)
}

pub struct RTCursor<'a> {
    index: usize,
    node: &'a ExtNodeCached
}

pub struct BPlusTree {
    root: Node,
    num_nodes: usize,
    height: usize,
    ext_node_cache: ExtNodeCacheMap
}

impl BPlusTree {
    fn search(&self, node: &Node, key: &EntryKey) -> RTCursor {
        if node.is_ext() {
            // is external node
            let pos = node.search(key, self);
        }
        unimplemented!()
    }
    fn get_ext_node_cached(&self, id: &Id) -> ExtNodeCached {
        let mut map = self.ext_node_cache.lock();
        return map.get_or_fetch(id).unwrap().clone();
    }
}

impl Node {
    fn search(&self, key: &EntryKey, tree: &BPlusTree) -> usize {
        let mut cached_ref = unsafe { mem::uninitialized() };
        match self {
            &Node::External(ref n) => {
                cached_ref = n.get_cached(tree);
                &cached_ref.keys
            },
            &Node::Internal(ref n) => &n.keys
        }.binary_search(key).unwrap_or_else(|i| i + 1)
    }
    fn is_ext(&self) -> bool {
        match self {
            &Node::Internal(_) => false,
            &Node::External(_) => true
        }
    }
}

impl ExtNode {
    fn get_cached(&self, tree: &BPlusTree) -> Ref<'_, ExtNodeCached> {
        let id = {
            let inner = self.inner.borrow();
            if inner.is_cached() {
                return Ref::map(inner, |r| if let ExtNodeInner::Cached(c) = r {
                    return c
                } else { unreachable!() })
            } else {
                inner.get_id()
            }
        };
        *self.inner.borrow_mut() = ExtNodeInner::Cached(tree.get_ext_node_cached(&id));
        return self.get_cached(tree);
    }
}

impl ExtNodeInner {
    fn is_cached(&self) -> bool {
        if let &ExtNodeInner::Cached(_) = self {
            return true;
        } else {
            return false;
        }
    }
    fn get_id(&self) -> Id {
        match self {
            &ExtNodeInner::Cached(ref c) => c.id,
            &ExtNodeInner::Pointer(ref id) => *id
        }
    }
}

fn id_from_key(key: &EntryKey) -> Id {
    let mut id_cursor = Cursor::new(&key[key.len() - ID_SIZE ..]);
    return Id::from_binary(&mut id_cursor).unwrap(); // read id from tailing 128 bits
}

fn key_prefixed(prefix: &EntryKey, x: &EntryKey) -> bool {
    return prefix.as_slice() == &x[.. x.len() - ID_SIZE];
}

mod test {
    use std::mem::size_of;
    use super::Node;

    #[test]
    fn node_size() {
        // expecting the node size to be an on-heap pointer plus node type tag, aligned.
        assert_eq!(size_of::<Node>(), size_of::<usize>() * 2);
    }
}