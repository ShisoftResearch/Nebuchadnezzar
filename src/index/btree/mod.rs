use smallvec::SmallVec;
use dovahkiin::types::custom_types::id::Id;
use utils::lru_cache::LRUCache;
use std::io::Cursor;
use std::borrow::{Borrow, BorrowMut};
use std::borrow::Cow;
use std::rc::Rc;
use ram::types::RandValue;
use client::AsyncClient;
use std::cell::RefCell;
use std::ops::Deref;
use std::cell::Ref;
use std::cell::RefMut;
use std::cmp::{max, min};
use std::ptr;
use std::mem;
use std::sync::Arc;
use itertools::{Itertools, chain};
use ram::cell::Cell;
use futures::Future;
use dovahkiin::types;
use dovahkiin::types::{Value, Map, PrimitiveArray, ToValue, key_hash};
use index::btree::external::*;
use index::btree::internal::*;
use parking_lot::RwLock;

mod internal;
mod external;

const ID_SIZE: usize = 16;
const NUM_KEYS: usize = 2048;
const NUM_PTRS: usize = NUM_KEYS + 1;
const CACHE_SIZE: usize = 2048;

type EntryKey = SmallVec<[u8; 32]>;
type EntryKeySlice = [EntryKey; NUM_KEYS];
type NodePtr = RefCell<Node>;
type NodePointerSlice = [NodePtr; NUM_PTRS];


enum Node {
    External(Box<Id>),
    Internal(Box<RwLock<InNode>>),
    None
}

enum AcqNode<'a> {
    ExternalMut(ExtNodeCachedMut<'a>),
    ExternalImmut(ExtNodeCachedImmute<'a>),
    InternalMut(&'a mut InNode),
    InternalImmut(&'a InNode)
}

pub struct RTCursor {
    index: usize,
    version: u64,
    id: Id
}

pub struct BPlusTree {
    root: RwLock<Node>,
    num_nodes: usize,
    height: usize,
    ext_node_cache: ExtNodeCacheMap
}

macro_rules! make_array {
    ($n: expr, $constructor:expr) => {
        unsafe {
            let mut items: [_; $n] = mem::uninitialized();
            for place in items.iter_mut() {
                ptr::write(place, $constructor);
            }
            items
        }
    };
}

impl BPlusTree {
    pub fn new(neb_client: &Arc<AsyncClient>) -> BPlusTree {
        let neb_client_1 = neb_client.clone();
        let neb_client_2 = neb_client.clone();
        let mut tree = BPlusTree {
            root: RwLock::new(Node::None),
            num_nodes: 0,
            height: 0,
            ext_node_cache:
            RwLock::new(
                LRUCache::new(
                    CACHE_SIZE,
                    move |id|{
                        neb_client_1.read_cell(*id).wait().unwrap().map(|cell| {
                            RwLock::new(ExtNodeCached::from_cell(cell))
                        }).ok()
                    },
                    move |_, value| {
                        let cache = value.read();
                        let cell = cache.to_cell();
                        neb_client_2.transaction(move |txn| {
                            let cell_owned = (&cell).to_owned();
                            txn.upsert(cell_owned)
                        }).wait().unwrap()
                    }))
        };
        {
            let mut tree_root = tree.root.write();
            *tree_root = tree.new_ext_cached_node();
        }
        return tree;
    }
    pub fn seek(&self, key: &EntryKey) -> RTCursor {
        return self.search(&self.root.borrow().acquire(self), key);
    }
    fn search(&self, node: &AcqNode, key: &EntryKey) -> RTCursor {
        let pos = node.search(key);
        if node.is_ext() {
            let extnode = node.extnode();
            RTCursor {
                index: pos,
                version: extnode.version,
                id: extnode.id
            }
        } else {
            let next_node = &n.pointers[pos];
            self.search(&next_node.borrow(), key)
        }
    }
    pub fn insert(&mut self, mut key: EntryKey, id: &Id) {
        key_with_id(&mut key, id);
        if let Some((new_node, pivotKey)) = self.insert_to_node(&mut *self.root.borrow_mut(), key) {
            // split root
            let acq_new_node = new_node.acquire(self);
            let pivot = pivotKey.unwrap_or_else(|| acq_new_node.first_key());
            let new_root = InNode {
                keys: make_array!(NUM_KEYS, EntryKey::default()),
                pointers: make_array!(NUM_PTRS, NodePtr::default()),
                len: 1
            };
            let old_root = mem::replace(&mut *self.root.borrow_mut(), Node::Internal(box new_root));
            if let &mut Node::Internal(ref mut new_root) = &mut *self.root.borrow_mut() {
                new_root.keys[0] = pivot;
                new_root.pointers[0] = RefCell::new(old_root);
                new_root.pointers[1] = RefCell::new(new_node);
            } else { unreachable!() }
        }
    }
    fn insert_to_node(&self, node: &mut Node, key: EntryKey) -> Option<(Node, Option<EntryKey>)> {
        let mut acq_node = node.acquire_mut(self);
        let pos = acq_node.search(&key);
        let split_node = match node {
            &mut Node::External(_) => {
                return acq_node.insert(key, None, pos, self)
            },
            &mut Node::Internal(ref mut n) => {
                let mut next_node = n.pointers[pos].acquire_mut();
                self.insert_to_node(next_node.borrow_mut(), key)
            },
            &mut Node::None => unreachable!()
        };
        match split_node {
            Some((new_node, pivot_key)) => {
                assert!(!(!new_node.is_ext() && pivot_key.is_none()));
                let pivot = pivot_key.unwrap_or_else(|| new_node.first_key(self));
                return node.insert(pivot, Some(RefCell::new(new_node)), pos + 1, self)
            },
            None => return None
        }
    }
    fn remove(&self, key: &EntryKey, id: &Id) {
        let mut key = key.clone();
        key_with_id(&mut key, id);
        let mut root = self.root.borrow_mut();
        self.remove_from_node(&mut *root, &key);
        if !root.is_ext() && root.len(self) == 0 {
            remove_empty_innode_node(root);
        }
    }
    fn remove_from_node(&self, node: &mut Node, key: &EntryKey) -> Option<()> {
        let key_pos = node.search(key, self);
        if let &mut Node::Internal(ref mut n) = node {
            let pointer_pos = key_pos + 1;
            let result = self.remove_from_node(&mut *n.pointers[pointer_pos].borrow_mut(), key);
            if result.is_none() { return result }
            let mut acq_node = n.pointers[pointer_pos].acquire_mut();
            if acq_node.borrow().len(self) == 0 {
                // need to remove empty child node
                if acq_node.is_ext() {
                    // empty external node should be removed and rearrange 'next' and 'prev' pointer for neighbourhoods
                    rearrange_empty_extnode(acq_node.borrow_mut(), self);
                    n.remove(pointer_pos);
                } else {
                    // empty internal nodes should be replaced with it's only remaining child pointer
                    // there must be at least one child pointer exists
                    remove_empty_innode_node(acq_node.borrow_mut());
                }
            } else if !acq_node.is_half_full(self) {
                // need to rebalance
                let cand_key_pos = n.rebalance_candidate(key_pos, self);
                let cand_ptr_pos = cand_key_pos + 1;
                let left_ptr_pos = min(pointer_pos, cand_ptr_pos);
                let right_ptr_pos = max(pointer_pos, cand_ptr_pos);
                if acq_node.cannot_merge(self) {
                    // relocate
                    n.relocate_children(left_ptr_pos, right_ptr_pos);
                } else {
                    // merge
                    n.merge_children(left_ptr_pos, right_ptr_pos);
                    n.remove(right_ptr_pos - 1);
                }
            }
            return result;
        } else if let &mut Node::External(ref n) = node {
            let mut cached_node = &self.get_mut_ext_node_cached(n);
            if &cached_node.keys[key_pos] == key {
                cached_node.remove(key_pos, self);
                return Some(());
            } else {
                return None;
            }
        } else { unreachable!() }
    }
    fn get_mut_ext_node_cached(&self, id: &Id) -> ExtNodeCachedMut {
        let mut map = self.ext_node_cache.read();
        return map.get_or_fetch(id).unwrap().write();
    }
    fn get_ext_node_cached(&self, id: &Id) -> ExtNodeCachedImmute {
        let mut map = self.ext_node_cache.read();
        return map.get_or_fetch(id).unwrap().read();
    }
    fn new_page_id(&self) -> Id {
        // TODO: achieve locality
        Id::rand()
    }
    fn new_ext_cached_node(&self) -> Node {
        let id = self.new_page_id();
        let node = ExtNodeCached::new(&id);
        self.ext_node_cache.write().insert(id, RwLock::new(node));
        return Node::External(box id);
    }
}

impl Node {
    fn acquire(&self, tree: &BPlusTree) -> AcqNode {
        match self {
            &Node::Internal(ref n) => AcqNode::InternalImmut(n),
            &Node::External(ref n) => AcqNode::ExternalImmut(tree.get_ext_node_cached(n)),
            _ => unreachable!()
        }
    }
    fn acquire_mut(&mut self, tree: &BPlusTree) -> AcqNode {
        match self {
            &mut Node::Internal(ref mut n) => AcqNode::InternalMut(n),
            &mut Node::External(ref n) => AcqNode::ExternalMut(tree.get_mut_ext_node_cached(n)),
            _ => unreachable!()
        }
    }
}

impl <'a> AcqNode<'a> {
    fn keys(&self) -> &EntryKeySlice {
        &match self {
            &AcqNode::ExternalImmut(ref n) => n.keys,
            &AcqNode::ExternalMut(ref n) => n.keys,
            &AcqNode::InternalImmut(ref n) => n.keys,
            &AcqNode::InternalMut(ref n) => n.keys,
        }
    }
    fn search(&self, key: &EntryKey) -> usize {
        self.keys().binary_search(key).unwrap_or_else(|i| i)
    }
    fn insert(&mut self, key: EntryKey, ptr: Option<NodePtr>, pos: usize, tree: &BPlusTree) -> Option<(Node, Option<EntryKey>)> {
        match self {
            &mut AcqNode::ExternalMut(ref mut n) => n.insert(key, pos, tree),
            &mut AcqNode::InternalMut(ref mut n) => n.insert(key, ptr, pos),
            _ => unreachable!()
        }
    }
    fn remove(&mut self, pos: usize, tree: &BPlusTree) {
        match self {
            &mut AcqNode::ExternalMut(ref mut n) => n.remove(pos, tree),
            &mut AcqNode::InternalMut(ref mut n) => n.remove(pos),
            _ => unreachable!()
        }
    }
    fn is_ext(&self) -> bool {
        match self {
            &AcqNode::ExternalImmut(_) | &AcqNode::ExternalMut(_) => true,
            &AcqNode::InternalImmut(_) | &AcqNode::InternalMut(ref n) => false
        }
    }
    fn first_key(&self) -> EntryKey {
        self.keys()[0].to_owned()
    }
    fn len(&self) -> usize {
        match self {
            &AcqNode::ExternalImmut(ref n) => n.len,
            &AcqNode::ExternalMut(ref n) => n.len,
            &AcqNode::InternalImmut(ref n) => n.len,
            &AcqNode::InternalMut(ref n) => n.len,
        }
    }
    fn is_half_full(&self) -> bool {
        self.len() >= NUM_KEYS / 2
    }
    fn cannot_merge(&self, tree: &BPlusTree) -> bool {
        self.len() >= NUM_KEYS/ 2 - 1
    }
    fn extnode_mut(&mut self) -> &mut ExtNode {
        match self {
            &mut AcqNode::ExternalMut (ref mut n) => n.borrow_mut(),
            _ => unreachable!()
        }
    }
    fn innode_mut(&mut self) -> &mut InNode {
        match self {
            &mut AcqNode::InternalMut (ref mut n) => n.borrow_mut(),
            _ => unreachable!()
        }
    }
    fn extnode(&self) -> &ExtNode {
        match self {
            &AcqNode::ExternalImmut (ref n) => n.borrow(),
            _ => unreachable!()
        }
    }
    fn innode(&self) -> &InNode {
        match self {
            &AcqNode::InternalImmut (ref n) => n.borrow(),
            _ => unreachable!()
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
    where S: Slice<T>, T: Default
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

fn remove_empty_innode_node(mut sub_level_pointer: RefMut<Node>) {
    let new_ptr = sub_level_pointer.innode().pointers[0].replace(Default::default());
    mem::replace(&mut *sub_level_pointer, new_ptr);
}

trait Slice<T> : Sized where T: Default{
    fn as_slice(&mut self) -> &mut [T];
    fn init() -> Self;
    fn item_default() -> T {
        T::default()
    }
    fn split_at_pivot(&mut self, pivot: usize, len: usize) -> Self {
        let mut right_slice = Self::init();
        {
            let mut slice1: &mut[T] = self.as_slice();
            let mut slice2: &mut[T] = right_slice.as_slice();
            for i in pivot .. len { // leave pivot to the left slice
                slice2[i - pivot] = mem::replace(
                    &mut slice1[i],
                    T::default());
            }
        }
        return right_slice;
    }
    fn insert_at(&mut self, item: T, pos: usize, len: usize) {
        assert!(pos < len);
        let slice = self.as_slice();
        for i in len .. pos {
            slice[i] = mem::replace(&mut slice[i - 1], T::default());
        }
        slice[pos] = item;
    }
    fn remove_at(&mut self, pos: usize, len: usize) {
        if pos >= len - 1 { return; }
        let slice  = self.as_slice();
        for i in pos .. len - 1 {
            slice[i] = mem::replace(&mut slice[i + 1], T::default());
        }
    }
}

macro_rules! impl_slice_ops {
    ($t: ty, $et: ty, $n: expr) => {
        impl Slice<$et> for $t {
            fn as_slice(&mut self) -> &mut [$et] { self }
            fn init() -> Self { make_array!($n, Self::item_default()) }
        }
    };
}

impl_slice_ops!(EntryKeySlice, EntryKey, NUM_KEYS);
impl_slice_ops!(NodePointerSlice, NodePtr, NUM_PTRS);

impl Default for Node {
    fn default() -> Self {
        Node::None
    }
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