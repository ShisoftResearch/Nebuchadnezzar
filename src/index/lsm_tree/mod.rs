use std::rc::{Rc, Weak};
use std::cell::{RefCell, RefMut};
use std::borrow::Borrow;
use dovahkiin::types::custom_types::id::Id;
use smallvec::SmallVec;
use std::sync::Arc;
use ram::chunk::Chunks;
use ram::cell::Cell;
use ram::types::RandValue;
use dovahkiin::types;
use dovahkiin::types::custom_types::map::key_hash;
use dovahkiin::types::value::{ValueIter};
use dovahkiin::types::{Value, PrimitiveArray};
use utils::lru_cache::LRUCache;
use parking_lot::{Mutex, MutexGuard};
use std::io::Cursor;
use std::ops::Index;
use std::cmp::Ordering;
use std::mem;
use std::borrow::BorrowMut;

lazy_static! {
    pub static ref ENTRIES_KEY_HASH : u64 = key_hash("entries");
    pub static ref KEYS_KEY_HASH : u64 = key_hash("keys");
    pub static ref PREV_PAGE_KEY_HASH : u64 = key_hash("prev");
    pub static ref NEXT_PAGE_KEY_HASH : u64 = key_hash("next");
}

type EntryKey = SmallVec<[u8; 32]>;
type CachedExtNodeRef<N> = Arc<Mutex<CachedExtNode<N>>>;

pub struct LSMTree {
    num_levels: u8,
}

trait BPlusTree<N> where N: Node {
    fn root(&self) -> &N;
    fn root_mut(&self) -> &mut N;
    fn get_height(&self) -> u32;
    fn set_height(&mut self) -> u32;
    fn get_num_nodes(&self) -> u32;
    fn set_num_nodes(&self) -> u32;
    fn page_size(&self) -> u32;
    fn chunks(&self) -> &Arc<Chunks>;
    fn page_cache(&self) -> MutexGuard<LRUCache<Id, CachedExtNodeRef<N>>>;
    fn get_page_direct(&self, id: &Id) -> CachedExtNodeRef<N> {
        let cell = self.chunks().read_cell(id).unwrap(); // should crash if not exists
        let keys = &cell.data[*KEYS_KEY_HASH];
        let keys_len = keys.len().unwrap();
        let mut v_node = CachedExtNode {
            keys: Vec::new(),
            ids: Vec::new(),
            cap: self.page_size(),
            persist_id: Id::rand()
        };
        for key in keys.iter_value().unwrap() {
            let key = if let Value::PrimArray(PrimitiveArray::U8(ref array)) = key {
                EntryKey::from_slice(array.as_slice())
            } else { panic!("invalid entry") };
            let id = id_from_key(&key);
            v_node.keys.push(EntryKey::from_slice(key.as_slice()));
            v_node.ids.push(Delimiter::External(id));
        }
        return Arc::new(Mutex::new(v_node));
    }
    fn get_page(&self, id: &Id) -> CachedExtNodeRef<N> {
        self.page_cache().get_or_fetch(id).unwrap().clone()
    }
    fn get(&self, key: &EntryKey) -> Option<Id> {
        self.search(self.root(), key, self.get_height())
    }

    fn search_pos(node: &N, key: &EntryKey) -> u32 {
        node.keys()
            .search(key)
            .map(|i| i + 1)
            .unwrap_or_else(|i| i) as u32
    }

    fn search<'a>(&self, node: &N, key: &EntryKey, ht: u32) -> Option<Id> {
        if ht == 0 { // persist page
            unreachable!();
        } else {
            let index = Self::search_pos(node, key);
            match &node.delimiters().get(index as usize) {
                Some(Delimiter::External(ref id)) => {
                    assert_eq!(ht, 1);
                    // search in leaf page
                    let page = self.get_page(id);
                    let search_result = page
                        .lock()
                        .keys()
                        .search(key);
                    let index = if let Ok(i) = search_result { i } else { return None };
                    match &node.delimiters().get(index) {
                        Some(Delimiter::External(id)) => return Some(*id),
                        Some(Delimiter::Internal(_)) => panic!("Error on leaf delimiter type"),
                        None => panic!("Error on leaf delimiter is none"),
                        _ => unreachable!()
                    };
                },
                Some(Delimiter::Internal(node)) => {
                    let node = node.borrow();
                    return self.search(node, key, ht - 1);
                },
                None => return None,
                _ => unreachable!()
            }
        }
    }
    fn insert(&mut self, key: &EntryKey, value: &Id) {
        let mut root = self.root_mut();
        let height = self.get_height();
        if let Some(new_node) = self.put(root, key, height) {
            let seed_key = new_node.keys().index_of(0).clone();
            let original_root = mem::replace(root, unsafe { mem::uninitialized() });
            let deli1 = Delimiter::Internal(box original_root);
            let deli2 = Delimiter::Internal(box new_node);
            let mut new_root = N::construct(seed_key, deli1, deli2);
            *root = new_root;
        }
    }
    fn put<'a>(&self, node: &mut N, key: &EntryKey, ht: u32) -> Option<N> {
        let index = Self::search_pos(node, key);
        if ht == 0 {
            unreachable!()
        } else if ht == 1 {
            let id = external_id(node, index);
            let page_ref = self.get_page(&id);
            let mut page = page_ref.lock();
            let page_pos = match page.keys().search(key) {
                Ok(_) => return None,
                Err(i) => i
            } as u32;
            if let Some(new_page) = page.add(page_pos, key.clone(), Delimiter::None) {
                return node.add(
                    index + 1,
                    page.keys().first().clone(),
                    Delimiter::External(page.persist_id));
            }
        } else {
            let new_node = {
                let mut next_node = match node.mut_delimiter(index) {
                    Delimiter::Internal(ref mut n) => n.borrow_mut(),
                    _ => unreachable!()
                };
                match self.put(next_node, key, ht + 1) {
                    None => return None,
                    Some(n) => n
                }
            };
            return node.add(
                index + 1,
                new_node.keys().first().clone(),
                Delimiter::Internal(box new_node));
        }
        return None;
    }
    fn remove(&mut self, key: &EntryKey) -> Option<()> {
        unimplemented!()
    }
    fn delete(&mut self, node: &mut N, key: &EntryKey, ht: u32) -> Option<()> {
        let index = Self::search_pos(node, key);
        if ht == 0 {
            unreachable!()
        } else if ht == 1 {
            let id = external_id(node, index);
            let page_ref = self.get_page(&id);
            let mut page = page_ref.lock();
            let page_pos = match page.keys().search(key) {
                Ok(i) => i,
                Err(i) => return None
            } as u32;
            page.del(page_pos);
            self.balance_nodes(node, index);
            return Some(())
        } else {
            {
                let mut innode = internal_node(node, index);
                if self.delete(innode.borrow_mut(), key, ht - 1).is_none() {
                    return None;
                }
            }
            self.balance_nodes(node, index);
            return Some(());
        }
        return None;
    }
    fn balance_nodes(&self, node: &mut N, index: u32) {
        if node.len() <= self.page_size() / 2 {
            // need to merge / relocate with page spouse
            // we will merge the node with right or left spouse
//            let left_node: CachedExtNode<> = node.
//            if index >= self.page_size() - 1 {
//                // the page is at right most, merge with left one
//
//            }
        }
    }
}

enum Delimiter<N> {
    External(Id), // to leaf
    Internal(Box<N>), // to higher level node
    None
}

trait Array<T> where T: Ord {
    #[inline]
    fn index_of(&self, index: usize) -> &T;
    #[inline]
    fn size(&self) -> usize;
    #[inline]
    fn search(&self, item: &T) -> Result<usize, usize>;
    #[inline]
    fn get(&self, index: usize) -> Option<&T>;
    #[inline]
    fn as_slice_mut<'a>(&'a mut self) -> &'a mut [T];
    #[inline]
    fn first(&self) -> &T {
        self.index_of(0)
    }
}

trait Keys : Array<EntryKey> {}
trait Delimiters<N> : Array<Delimiter<N>> {}
impl Keys for [EntryKey] {}
impl <N> Delimiters<N> for [Delimiter<N>] {}
impl <T> Array<T> for [T] where T: Ord {
    #[inline]
    fn index_of(&self, index: usize) -> &T { &self[index] }

    #[inline]
    fn size(&self) -> usize { self.len() }

    #[inline]
    fn search(&self, item: &T) -> Result<usize, usize> { self.binary_search(item) }

    #[inline]
    fn get(&self, index: usize) -> Option<&T> {
        if index >= self.size() { None } else { Some(&self[index]) }
    }

    fn as_slice_mut<'a>(&'a mut self) -> &'a mut [T] {
        self
    }
}
impl <T> Array<T> for Vec<T> where T: Ord {
    #[inline]
    fn index_of(&self, index: usize) -> &T {
        self.get(index).unwrap()
    }

    #[inline]
    fn size(&self) -> usize {
        self.len()
    }

    #[inline]
    fn search(&self, item: &T) -> Result<usize, usize> {
        self.as_slice().binary_search(item)
    }

    #[inline]
    fn get(&self, index: usize) -> Option<&T> {
        if index >= self.len() { None } else { Some(&self[index]) }
    }

    fn as_slice_mut<'a>(&'a mut self) -> &'a mut [T] {
        self.as_mut_slice()
    }
}
impl <N> Delimiters<N> for Vec<Delimiter<N>> {}
impl Keys for Vec<EntryKey> {}

trait Node : Sized {
    #[inline]
    fn len(&self) -> u32;
    #[inline]
    fn keys(&self) -> &Keys;
    #[inline]
    fn delimiters(&self) -> &Delimiters<Self>;
    #[inline]
    fn mut_delimiter(&mut self, pos: u32) -> &mut Delimiter<Self>;
    #[inline]
    fn add(&mut self, pos: u32, key: EntryKey, delimiter: Delimiter<Self>) -> Option<Self> ;
    #[inline]
    fn del(&mut self, pos: u32);
    #[inline]
    fn merge(mut x: Self, mut y: Self) -> Self;
    #[inline]
    fn construct(seed_key: EntryKey, seed_delimiter_1: Delimiter<Self>, seed_delimiter_2: Delimiter<Self>) -> Self;
}

struct CachedExtNode<N> {
    keys: Vec<EntryKey>,
    ids: Vec<Delimiter<N>>,
    cap: u32,
    persist_id: Id
}

impl <N> Node for CachedExtNode<N> {
    #[inline]
    fn len(&self) -> u32 { self.keys.len() as u32 }
    #[inline]
    fn keys(&self) -> &Keys {
        &self.keys
    }

    #[inline]
    fn delimiters(&self) -> &Delimiters<Self> {
        &self.ids
    }

    fn mut_delimiter(&mut self, pos: u32) -> &mut Delimiter<Self> {
        unreachable!()
    }

    fn add(&mut self, pos: u32, key: EntryKey, _: Delimiter<Self>) -> Option<Self> {
        let id = id_from_key(&key);
        self.keys.insert(pos as usize, key);
        self.ids.insert(pos as usize, Delimiter::External(id));
        if self.keys.len() > self.cap as usize {
            // need to split
            let mid = self.keys.len() / 2;
            let keys_2: Vec<_> = self.keys.drain(mid..).collect();
            let ids_2: Vec<_> = self.ids.drain(mid..).collect();
            return Some(CachedExtNode {
                keys: keys_2,
                ids: ids_2,
                cap: self.cap,
                persist_id: Id::rand()
            });
        } else {
            return None;
        }
    }

    fn del(&mut self, pos: u32) {
        self.keys.remove(pos as usize);
        self.ids.remove(pos as usize);
    }

    fn merge(mut x: Self, mut y: Self) -> Self {
        x.keys.append(&mut y.keys);
        x.ids.append(&mut y.ids);
        return x;
    }

    fn construct(seed_key: EntryKey, seed_delimiter_1: Delimiter<Self>, seed_delimiter_2: Delimiter<Self>) -> Self {
        unreachable!() // should always construct from cell
    }
}

impl <N> Delimiters<CachedExtNode<N>> for Vec<Delimiter<N>> {}
impl <N> Array<Delimiter<CachedExtNode<N>>> for Vec<Delimiter<N>> {
    fn index_of(&self, index: usize) -> & Delimiter<CachedExtNode<N>> {
        unimplemented!()
    }

    fn size(&self) -> usize {
        unimplemented!()
    }

    fn search(&self, item: & Delimiter<CachedExtNode<N>>) -> Result<usize, usize> {
        unimplemented!()
    }

    fn get(&self, index: usize) -> Option<& Delimiter<CachedExtNode<N>>> {
        unimplemented!()
    }

    fn as_slice_mut<'a>(&'a mut self) -> &'a mut [Delimiter<CachedExtNode<N>>] {
        unimplemented!()
    }
}

trait SliceNode : Node + Sized {

    type K: Keys;
    type D: Delimiters<Self>;

    #[inline]
    fn keys_mut(&mut self) -> &mut [EntryKey];

    #[inline]
    fn delimiters_mut(&mut self) -> &mut [Delimiter<Self>];

    #[inline]
    fn get_len(&self) -> u32;

    #[inline]
    fn set_len(&mut self, len: u32);

    #[inline]
    fn capacity() -> u32;

    #[inline]
    fn moving_delimiters(slice: &mut [Delimiter<Self>]) -> Self::D;

    #[inline]
    fn moving_keys(slice: &mut [EntryKey]) -> Self::K;

    fn construct_slice(keys: Self::K, delimiters: Self::D, len: u32) -> Self;

    fn insert_to_slice<V>(slice: &mut [V], val: V, pos: u32, len: u32) {
        for i in (pos..len).rev() {
            let i = i as usize;
            slice[i + 1] = mem::replace(&mut slice[i], unsafe { mem::uninitialized() });
        }
        slice[pos as usize] = val;
    }

    fn del_from_slice<V>(slice: &mut[V], pos: u32, len: u32) {
        for i in pos..len - 1 {
            let i = i as usize;
            slice[i] = mem::replace(&mut slice[i + 1], unsafe { mem::uninitialized() });
        }
    }

    fn insert_split<V>(value: V, s1: &mut [V], s2: &mut [V], mid: u32, insert_pos: u32, len1: u32, len2: u32)
    {
        if insert_pos <= mid {
            Self::insert_to_slice(s1, value, insert_pos, len1);
        } else {
            Self::insert_to_slice(s2, value, insert_pos - mid, len2);
        }
    }

    fn slice_add(&mut self, pos: u32, key: EntryKey, delimiter: Delimiter<Self>) -> Option<Self> {
        let len = self.get_len();
        let capacity = Self::capacity();
        if len + 1 >= capacity {
            // need to split
            let mid = len / 2;
            let len1 = mid - 1;
            let len2 = len - mid;
            let keys_2 = {
                let mut keys_1 = self.keys_mut();
                let mut keys_2 =
                    Self::moving_keys(keys_1.split_at_mut(mid as usize).1);
                Self::insert_split(
                    key,
                    &mut keys_1,
                    &mut keys_2.as_slice_mut(),
                    mid, pos,
                    len1, len2);
                keys_2
            };
            let delis_2 = {
                let mut delis_1 = self.delimiters_mut();
                let mut delis_2 =
                    Self::moving_delimiters(delis_1.split_at_mut(mid as usize).1);
                Self::insert_split(
                    delimiter,
                    &mut delis_1,
                    &mut delis_2.as_slice_mut(),
                    mid, pos + 1,
                    len1 + 1, len2 + 1);
                delis_2
            };
            self.set_len(len1);

            return Some(Self::construct_slice(keys_2, delis_2, len2))
        } else {
            Self::insert_to_slice(self.keys_mut(), key, pos, len);
            Self::insert_to_slice(self.delimiters_mut(), delimiter, pos + 1, len);
            self.set_len(len + 1);
            return None;
        }
    }

    fn slice_del(&mut self, pos: u32) {
        let len = self.get_len();
        Self::del_from_slice(self.keys_mut(), pos, len);
        Self::del_from_slice(self.delimiters_mut(), pos, len + 1);
        self.set_len(len - 1);
    }

    fn slice_merge(&mut self, mut x: Self) {
        assert!(self.get_len() + x.get_len() <= Self::capacity());
        for i in 0..x.get_len() {
            let src_i = i as usize;
            let target_i = (i + self.get_len()) as usize;
            self.keys_mut()[target_i] =
                mem::replace(&mut x.keys_mut()[src_i], unsafe { mem::uninitialized() });
        }
        for i in 0..x.get_len() + 1 {
            let src_i = i as usize;
            let target_i = (i + self.get_len() + 1) as usize;
            self.delimiters_mut()[target_i] =
                mem::replace(&mut x.delimiters_mut()[src_i], unsafe { mem::uninitialized() });
        }
    }
}

impl <T> Index<usize> for Array<T> where T: Ord {
    type Output = T;

    fn index(&self, index: usize) -> &<Self as Index<usize>>::Output {
        self.index_of(index)
    }
}

macro_rules! impl_nodes {
    ($(($level: ident, $entry_size: expr, $delimiter_size: expr)),+) => {
        $(
            mod $level {
                use super::*;

                type LEntryKeys = [EntryKey; $entry_size];

                impl Array<EntryKey> for LEntryKeys {
                    #[inline]
                    fn size(&self) -> usize { $entry_size }
                    #[inline]
                    fn index_of(&self, index: usize) -> &EntryKey { &self[index] }
                    #[inline]
                    fn search(&self, item: &EntryKey) -> Result<usize, usize> {
                        self.binary_search(item)
                    }
                    #[inline]
                    fn get(&self, index: usize) -> Option<&EntryKey> {
                        if index >= $entry_size { None } else { Some(&self[index]) }
                    }
                    #[inline]
                    fn as_slice_mut<'a>(&'a mut self) -> &'a mut [EntryKey] {
                        self
                    }
                }

                type LDelimiters = [Delimiter<LNode>; $delimiter_size];

                impl Array<Delimiter<LNode>> for LDelimiters {
                    #[inline]
                    fn size(&self) -> usize { $delimiter_size }
                    #[inline]
                    fn index_of(&self, index: usize) -> &Delimiter<LNode> { &self[index] }
                    #[inline]
                    fn search(&self, item: &Delimiter<LNode>) -> Result<usize, usize> {
                        unreachable!()
                    }
                    #[inline]
                    fn get(&self, index: usize) -> Option<&Delimiter<LNode>> {
                        if index >= $entry_size { None } else { Some(&self[index]) }
                    }
                    #[inline]
                    fn as_slice_mut<'a>(&'a mut self) -> &'a mut [Delimiter<LNode>] {
                        self
                    }
                }

                impl Delimiters<LNode> for LDelimiters {}
                impl Keys for LEntryKeys {}

                struct LNode {
                    keys: LEntryKeys,
                    delimiters: LDelimiters,
                    len: u32
                }

                impl Node for LNode {
                    #[inline]
                    fn len(&self) -> u32 {
                        self.len
                    }
                    #[inline]
                    fn keys(&self) -> &Keys {
                        &self.keys
                    }
                    #[inline]
                    fn delimiters(&self) -> &Delimiters<Self> {
                        &self.delimiters
                    }
                    #[inline]
                    fn mut_delimiter(&mut self, pos: u32) -> &mut Delimiter<Self> {
                        &mut self.delimiters[pos as usize]
                    }
                    #[inline]
                    fn add(&mut self, pos: u32, key: EntryKey, delimiter: Delimiter<Self>) -> Option<Self> {
                        self.slice_add(pos, key, delimiter)
                    }
                    #[inline]
                    fn del(&mut self, pos: u32) {
                        self.slice_del(pos)
                    }
                    #[inline]
                    fn merge(mut x: Self, mut y: Self) -> Self {
                        x.slice_merge(y);
                        return x;
                    }
                    fn construct(seed_key: EntryKey, seed_delimiter_1: Delimiter<Self>, seed_delimiter_2: Delimiter<Self>) -> Self {
                        let mut keys: LEntryKeys = unsafe { mem::uninitialized() };
                        let mut delimiters: LDelimiters = unsafe { mem::uninitialized() };

                        keys[0] = seed_key;
                        delimiters[0] = seed_delimiter_1;
                        delimiters[1] = seed_delimiter_2;

                        Self::construct_slice(keys, delimiters, 1)
                    }
                }

                impl SliceNode for LNode {
                    type K = LEntryKeys;
                    type D = LDelimiters;

                    #[inline]
                    fn keys_mut(&mut self) -> &mut [EntryKey] {
                        &mut self.keys
                    }

                    #[inline]
                    fn delimiters_mut(&mut self) -> &mut [Delimiter<Self>] {
                        &mut self.delimiters
                    }

                    #[inline]
                    fn get_len(&self) -> u32 {
                        self.len
                    }

                    #[inline]
                    fn set_len(&mut self, len: u32) {
                        self.len = len;
                    }

                    #[inline]
                    fn capacity() -> u32 {
                        $entry_size
                    }

                    fn moving_delimiters(slice: &mut [Delimiter<Self>]) -> Self::D {
                        let mut d: LDelimiters = unsafe { mem::uninitialized() };
                        for i in 0..slice.len() {
                            d[i] = mem::replace(&mut slice[i], unsafe { mem::uninitialized() });
                        }
                        return d;
                    }

                    fn moving_keys(slice: &mut [EntryKey]) -> Self::K {
                        let mut k: LEntryKeys = unsafe { mem::uninitialized() };
                        for i in 0..slice.len() {
                            k[i] = mem::replace(&mut slice[i], unsafe { mem::uninitialized() });
                        }
                        return k;
                    }

                    fn construct_slice(keys: Self::K, delimiters: Self::D, len: u32) -> Self {
                        LNode { keys, delimiters, len }
                    }
                }
            }
        )+
    };
}

impl_nodes!((level_0, 2, 3), (level_1, 20, 21), (level_2, 200, 201), (level_3, 2000, 2001), (level_4, 20000, 20001));

impl <N> Ord for Delimiter<N> {
    fn cmp(&self, other: &Self) -> Ordering {
        panic!()
    }
}
impl <N> PartialOrd for Delimiter<N> {
    fn partial_cmp(&self, other: &Delimiter<N>) -> Option<Ordering> {
        panic!()
    }
}
impl <N> Eq for Delimiter<N> {}
impl <N> PartialEq for Delimiter<N> {
    fn eq(&self, other: &Delimiter<N>) -> bool {
        panic!()
    }
}

fn id_from_key(key: &EntryKey) -> Id {
    let mut id_cursor = Cursor::new(&key[key.len() - 16 ..]);
    return Id::from_binary(&mut id_cursor).unwrap(); // read id from tailing 128 bits
}

fn external_id<N>(node: &N, index: u32) -> Id where N: Node {
    match node.delimiters().get(index as usize) {
        Some(Delimiter::External(id)) => *id,
        _ => unreachable!()
    }
}

fn internal_node<N>(node: &mut N, index: u32) -> &mut N where N: Node {
    match node.mut_delimiter(index) {
        Delimiter::Internal(ref mut n) => n.borrow_mut(),
        _ => unreachable!()
    }
}