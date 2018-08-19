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
use ram::types::RandValue;
use std::cell::Cell;
use std::cell::RefCell;
use std::ops::Deref;
use std::cell::Ref;
use std::cell::RefMut;

const ID_SIZE: usize = 16;
const NUM_NODES: usize = 2048;

type EntryKey = SmallVec<[u8; 32]>;
type ExtNodeCacheMap = Mutex<LRUCache<Id, ExtNodeCached>>;
type EntryKeySlice = [EntryKey; NUM_NODES];
type NodePtr = Box<Node>;
type NodePointerSlice = [NodePtr; NUM_NODES + 1];

struct InNode {
    keys: EntryKeySlice,
    pointers: NodePointerSlice,
    len: usize
}

#[derive(Clone)]
struct ExtNodeCached {
    id: Id,
    keys: EntryKeySlice,
    next: Id,
    len: usize,
    version: u64,
    removed: bool,
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

pub struct RTCursor {
    index: usize,
    version: u64,
    id: Id
}

pub struct BPlusTree {
    root: RefCell<Node>,
    num_nodes: usize,
    height: usize,
    ext_node_cache: ExtNodeCacheMap
}

struct ExtNodeSplit {
    node_2: ExtNodeCached,
    keys_1_len: usize
}

struct InNodeKeysSplit {
    keys_2: EntryKeySlice,
    keys_1_len: usize,
    keys_2_len: usize,
    pivot_key: EntryKey
}

struct InNodePtrSplit {
    ptrs_2: NodePointerSlice
}

impl BPlusTree {
    pub fn seek(&self, key: &EntryKey) -> RTCursor {
        return self.search(&self.root.borrow(), key);
    }
    fn search(&self, node: &Node, key: &EntryKey) -> RTCursor {
        let pos = node.search(key, self);
        match node {
            &Node::External(ref n) => {
                // preload page from storage
                let cached = n.get_cached(self);
                RTCursor {
                    index: pos,
                    version: cached.version,
                    id: cached.id
                }
            },
            &Node::Internal(ref n) => {
                let next_node = &n.pointers[pos];
                self.search(next_node.borrow(), key)
            }
        }
    }
    pub fn insert(&mut self, mut key: EntryKey, id: &Id) {
        key_with_id(&mut key, id);
        if let Some((new_node, pivotKey)) = self.insert_to_node(&mut *self.root.borrow_mut(), key) {
            // split root
            let pivot = pivotKey.unwrap_or_else(|| new_node.first_key(self));
            let new_root = InNode {
                keys: unsafe{ mem::uninitialized() },
                pointers: unsafe{ mem::uninitialized() },
                len: 1
            };
            let old_root = mem::replace(&mut *self.root.borrow_mut(), Node::Internal(box new_root));
            if let &mut Node::Internal(ref mut new_root) = &mut *self.root.borrow_mut() {
                new_root.keys[0] = pivot;
                new_root.pointers[0] = box old_root;
                new_root.pointers[1] = box new_node;
            } else { unreachable!() }
        }
    }
    fn insert_to_node(&self, node: &mut Node, key: EntryKey) -> Option<(Node, Option<EntryKey>)> {
        let pos = node.search(&key, self);
        let split_node = match node {
            &mut Node::External(_) => {
                return node.insert(key, None, pos, self)
            },
            &mut Node::Internal(ref mut n) => {
                let next_node = &mut n.pointers[pos];
                self.insert_to_node(next_node.as_mut(), key)
            }
        };
        match split_node {
            Some((new_node, pivotKey)) => {
                assert!(!(!new_node.is_ext() && pivotKey.is_none()));
                let pivot = pivotKey.unwrap_or_else(|| new_node.first_key(self));
                return node.insert(pivot, Some(box new_node), pos + 1, self)
            },
            None => return None
        }
    }
    fn remove(&self, key: &EntryKey, id: &Id) {
        unimplemented!()
    }
    fn remove_from_node(&self, node: &mut Node, key: &EntryKey) {
        let pos = node.search(key, self);
        match node {
            &mut Node::External(_) => return node.remove(key, pos, self),
            &mut Node::Internal(ref mut n) => {
                let mut child_node = &mut n.pointers[pos];
                self.remove_from_node(&mut child_node.borrow_mut(), key);
//                if child_node.len < NUM_NODES / 2 {
//                    unimplemented!()
//                }
            }
        }
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
    fn insert(&mut self, key: EntryKey, ptr: Option<NodePtr>, pos: usize, tree: &BPlusTree) -> Option<(Node, Option<EntryKey>)> {
        match self {
            &mut Node::External(ref n) => {
                n.insert(key, pos, tree)
            },
            &mut Node::Internal(ref mut n) => {
                n.insert(key, ptr, pos)
            }
        }
    }
    fn remove(&mut self, key: &EntryKey, pos: usize, tree: &BPlusTree) {
        match self {
            &mut Node::Internal(ref mut n) => {
                n.remove(pos)
            },
            &mut Node::External(ref n) => {
                n.remove(pos, tree)
            }
        }
    }
    fn is_ext(&self) -> bool {
        match self {
            &Node::Internal(_) => false,
            &Node::External(_) => true
        }
    }
    fn first_key(&self, tree: &BPlusTree) -> EntryKey {
        match self {
            &Node::Internal(ref n) => n.keys[0].to_owned(),
            &Node::External(ref n) => n.get_cached(tree).keys[0].to_owned()
        }
    }
}

impl ExtNode {
    fn from_id(id: Id) -> Self {
        Self {
            inner: RefCell::new(ExtNodeInner::Pointer(id))
        }
    }
    fn from_cached(cached: ExtNodeCached) -> Self {
        Self {
            inner: RefCell::new(ExtNodeInner::Cached(cached))
        }
    }
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
    fn get_cached_mut(&self, tree: &BPlusTree) -> RefMut<'_, ExtNodeCached> {
        let id = {
            let inner = self.inner.borrow_mut();
            if inner.is_cached() {
                return RefMut::map(inner, |r| if let ExtNodeInner::Cached(c) = r {
                    return c
                } else { unreachable!() })
            } else {
                inner.get_id()
            }
        };
        *self.inner.borrow_mut() = ExtNodeInner::Cached(tree.get_ext_node_cached(&id));
        return self.get_cached_mut(tree);
    }
    fn remove(&self, pos: usize, tree: &BPlusTree) {
        let mut cached = self.get_cached_mut(tree);
        let cached_len = cached.len;
        cached.keys.remove_at(pos, cached_len);
        cached.len -= 1;
    }
    fn insert(&self, key: EntryKey, pos: usize, tree: &BPlusTree) -> Option<(Node, Option<EntryKey>)> {
        let mut cached = self.get_cached_mut(tree);
        let cached_len = cached.len;
        if cached_len + 1 >= NUM_NODES {
            // need to split
            let pivot = cached_len / 2;
            let split = {
                let cached_next = *&cached.next;
                let mut keys_1 = &mut cached.keys;
                let mut keys_2 = keys_1.split_at_pivot(pivot, cached_len);
                let mut keys_1_len = pivot;
                let mut keys_2_len = cached_len - pivot;
                insert_into_split(
                    key,
                    keys_1, &mut keys_2,
                    &mut keys_1_len, &mut keys_2_len,
                    pos, pivot);
                ExtNodeSplit {
                    keys_1_len,
                    node_2: ExtNodeCached {
                        id: Id::rand(),
                        keys: keys_2,
                        next: cached_next,
                        len: keys_2_len,
                        version: 0,
                        removed: false
                    }
                }
            };
            cached.next = split.node_2.id;
            cached.len = split.keys_1_len;
            return Some((Node::External(box ExtNode::from_cached(split.node_2)), None));

        } else {
            cached.keys.insert_at(key, pos, cached_len);
            return None;
        }
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

impl ExtNodeCached {
    fn persist(&self) {

    }
    fn update(&self) {

    }
}

impl InNode {
    fn remove(&mut self, pos: usize) {
        let n_len = self.len;
        self.keys.remove_at(pos, n_len);
        self.pointers.remove_at(pos + 1, n_len + 1);
        self.len -= 1;
    }
    fn insert(&mut self, key: EntryKey, ptr: Option<NodePtr>, pos: usize) -> Option<(Node, Option<EntryKey>)> {
        let node_len = self.len;
        let ptr_len = self.len + 1;
        if node_len + 1 >= NUM_NODES {
            let keys_split = {
                let pivot = node_len / 2 + 1;
                let mut keys_1 = &mut self.keys;
                let mut keys_2 = keys_1.split_at_pivot(pivot, node_len);
                let mut keys_1_len = pivot - 1; // will not count the pivot
                let mut keys_2_len = node_len - pivot;
                let pivot_key = keys_1[pivot - 1].to_owned();
                insert_into_split(
                    key,
                    keys_1, &mut keys_2,
                    &mut keys_1_len, &mut keys_2_len,
                    pos, pivot);
                InNodeKeysSplit {
                    keys_2, keys_1_len, keys_2_len, pivot_key
                }
            };
            let ptr_split = {
                let pivot = ptr_len / 2;
                let mut ptrs_1 = &mut self.pointers;
                let mut ptrs_2 = ptrs_1.split_at_pivot(pivot, ptr_len);
                let mut ptrs_1_len = pivot;
                let mut ptrs_2_len = ptr_len - pivot;
                insert_into_split(
                    ptr.unwrap(),
                    ptrs_1, &mut ptrs_2,
                    &mut ptrs_1_len, &mut ptrs_2_len,
                    pos, pivot);
                assert_eq!(ptrs_1_len, keys_split.keys_1_len + 1);
                assert_eq!(ptrs_2_len, keys_split.keys_2_len + 1);
                InNodePtrSplit { ptrs_2 }
            };
            let node_2 = InNode {
                len: keys_split.keys_2_len,
                keys: keys_split.keys_2,
                pointers: ptr_split.ptrs_2
            };
            self.len = keys_split.keys_1_len;
            return Some((Node::Internal(box node_2), Some(keys_split.pivot_key)));
        } else {
            self.keys.insert_at(key, pos, node_len);
            self.pointers.insert_at(ptr.unwrap(), pos + 1, node_len + 1);
            self.len += 1;
            return None;
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

fn insert_into_split<T, S>(
    item: T,
    x: &mut S, y: &mut S,
    xlen: &mut usize, ylen: &mut usize,
    pos: usize, pivot: usize
)
    where S: Slice<T>
{
    if pos <= pivot {
        x.insert_at(item, pos, *xlen);
        *xlen += 1;
    } else {
        y.insert_at(item, pos - pivot, *ylen);
        *ylen += 1;
    }
}

fn key_with_id(key: &mut EntryKey, id: &Id) {
    let id_bytes = id.to_binary();
    key.extend_from_slice(&id_bytes);
}

trait Slice<T> : Sized {
    fn as_slice(&mut self) -> &mut [T];
    fn init() -> Self;
    fn split_at_pivot(&mut self, pivot: usize, len: usize) -> Self {
        let mut right_slice = Self::init();
        {
            let mut slice1: &mut[T] = self.as_slice();
            let mut slice2: &mut[T] = right_slice.as_slice();
            for i in pivot .. len { // leave pivot to the left slice
                slice2[i - pivot] = mem::replace(
                    &mut slice1[i],
                    unsafe { mem::uninitialized() });
            }
        }
        return right_slice;
    }
    fn insert_at(&mut self, item: T, pos: usize, len: usize) {
        assert!(pos < len);
        let slice = self.as_slice();
        for i in len .. pos {
            slice[i] = mem::replace(&mut slice[i - 1], unsafe { mem::uninitialized() });
        }
        slice[pos] = item;
    }
    fn remove_at(&mut self, pos: usize, len: usize) {
        if pos >= len - 1 { return; }
        let slice  = self.as_slice();
        for i in pos .. len - 1 {
            slice[i] = mem::replace(&mut slice[i + 1], unsafe { mem::uninitialized() });
        }
    }
}

macro_rules! impl_slice_ops {
    ($t: ty, $et: ty) => {
        impl Slice<$et> for $t {
            fn as_slice(&mut self) -> &mut [$et] { self }
            fn init() -> Self { unsafe { mem::uninitialized() } }
        }
    };
}

impl_slice_ops!(EntryKeySlice, EntryKey);
impl_slice_ops!(NodePointerSlice, NodePtr);

mod test {
    use std::mem::size_of;
    use super::Node;

    #[test]
    fn node_size() {
        // expecting the node size to be an on-heap pointer plus node type tag, aligned.
        assert_eq!(size_of::<Node>(), size_of::<usize>() * 2);
    }
}