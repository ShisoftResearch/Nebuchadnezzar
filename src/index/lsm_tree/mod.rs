use std::rc::{Rc, Weak};
use std::cell::RefCell;
use std::borrow::Borrow;
use dovahkiin::types::custom_types::id::Id;
use smallvec::SmallVec;
use std::sync::Arc;
use ram::chunk::Chunks;
use ram::cell::Cell;
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

lazy_static! {
    pub static ref ENTRIES_KEY_HASH : u64 = key_hash("entries");
    pub static ref KEYS_KEY_HASH : u64 = key_hash("keys");
    pub static ref PREV_PAGE_KEY_HASH : u64 = key_hash("prev");
    pub static ref NEXT_PAGE_KEY_HASH : u64 = key_hash("next");
}

type EntryKey = SmallVec<[u8; 32]>;
type CachedExtNodeRef<N> = Arc<CachedExtNode<N>>;

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
              cap: self.page_size()
          };
          for key in keys.iter_value().unwrap() {
              let key = if let Value::PrimArray(PrimitiveArray::U8(ref array)) = key {
                  EntryKey::from_slice(array.as_slice())
              } else { panic!("invalid entry") };
              let id = id_from_key(&key);
              v_node.keys.push(EntryKey::from_slice(key.as_slice()));
              v_node.ids.push(Delimiter::External(id));
          }
          return Arc::new(v_node);
      }
      fn get_page(&self, id: &Id) -> CachedExtNodeRef<N> {
          self.page_cache().get_or_fetch(id).unwrap().clone()
      }
      fn get(&self, key: &EntryKey) -> Option<Id> {
          self.search(self.root(), key, self.get_height())
      }
      fn search<'a>(&self, node: &N, key: &EntryKey, ht: u32) -> Option<Id> {
          if ht == 0 { // persist page
              unreachable!();
          } else {
              let index = node.keys()
                  .search(key)
                  .map(|i| i + 1)
                  .unwrap_or_else(|i| i);
              match &node.delimiters().get(index) {
                  Some(Delimiter::External(ref id)) => {
                      assert_eq!(ht, 1);
                      // search in leaf page
                      let page = self.get_page(id);
                      let search_result = page.keys()
                          .search(key);
                      let index = if let Ok(i) = search_result { i } else { return None };
                      match &node.delimiters().get(index) {
                          Some(Delimiter::External(id)) => return Some(*id),
                          Some(Delimiter::Internal(_)) => panic!("Error on leaf delimiter type"),
                          None => panic!("Error on leaf delimiter is none")
                      };
                  },
                  Some(Delimiter::Internal(node)) => {
                      let node: &N = node.borrow();
                      return self.search(node, key, ht - 1);
                  },
                  None => return None
              }
          }
      }
     fn insert(&self, key: &EntryKey, value: &Id) {
         unimplemented!()
     }
     fn put<'a>(&self, node: &N, key: &EntryKey, ht: u32) {
         let keys = node.keys();
         let index = keys
             .search(key)
             .map(|i| i + 1)
             .unwrap_or_else(|i| i);
         if ht == 0 {

         }
     }
 }

enum Delimiter<N> {
    External(Id), // to leaf
    Internal(Box<N>) // to higher level node
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
    fn keys(&self) -> &Keys;
    #[inline]
    fn delimiters(&self) -> &Delimiters<Self>;
    #[inline]
    fn add(&mut self, key: EntryKey, delimiter: Delimiter<Self>) -> Option<Self> ;
    #[inline]
    fn del(&mut self, key: &EntryKey);
    #[inline]
    fn merge(&mut self, x: Self);
}

struct CachedExtNode<N> {
    keys: Vec<EntryKey>,
    ids: Vec<Delimiter<N>>,
    cap: u32,
}

impl <N> Node for CachedExtNode<N> {
    #[inline]
    fn keys(&self) -> &Keys {
        &self.keys
    }

    #[inline]
    fn delimiters(&self) -> &Delimiters<Self> {
        &self.ids
    }

    fn add(&mut self, key: EntryKey, _: Delimiter<Self>) -> Option<Self> {
        let insert_pos = match self.keys.binary_search(&key) {
            Ok(_) => return None, // existed
            Err(i) => i
        };
        let id = id_from_key(&key);
        self.keys.insert(insert_pos, key);
        self.ids.insert(insert_pos, Delimiter::External(id));
        if self.keys.len() > self.cap as usize {
            // need to split
            let mid = self.keys.len() / 2;
            let keys_2: Vec<_> = self.keys.drain(mid..).collect();
            let ids_2: Vec<_> = self.ids.drain(mid..).collect();
            return Some(CachedExtNode {
                keys: keys_2,
                ids: ids_2,
                cap: self.cap
            });
        } else {
            return None;
        }
    }

    fn del(&mut self, key: &EntryKey) {
        let pos = match self.keys.binary_search(&key) {
            Ok(i) => i, // existed
            Err(i) => return
        };
        self.keys.remove(pos);
        self.ids.remove(pos);
    }

    fn merge(&mut self, mut x: Self) {
        self.keys.append(&mut x.keys);
        self.ids.append(&mut x.ids);
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
    fn get_pos(&self) -> u32;

    #[inline]
    fn set_pos(&mut self, pos: u32);

    #[inline]
    fn capacity() -> u32;

    #[inline]
    fn moving_delimiters(slice: &mut [Delimiter<Self>]) -> Self::D;

    #[inline]
    fn moving_keys(slice: &mut [EntryKey]) -> Self::K;

    fn construct(keys: Self::K, delimiters: Self::D, pos: u32) -> Self;

    fn insert_to_slice<V>(slice: &mut [V], val: V, pos: u32, len: u32) {
        for i in (pos..len).rev() {
            let i = i as usize;
            slice[i + 1] = mem::replace(&mut slice[i], unsafe { mem::uninitialized() });
        }
        slice[pos as usize] = val;
    }

    fn insert_split<V>(value: V, s1: &mut [V], s2: &mut [V], mid: u32, insert_pos: u32, len: u32)
    {
        if insert_pos <= mid {
            Self::insert_to_slice(s1, value, insert_pos, len);
        } else {
            Self::insert_to_slice(s2, value, insert_pos - mid, len);
        }
    }

    fn slice_add(&mut self, key: EntryKey, delimiter: Delimiter<Self>) -> Option<Self> {
        let pos = self.get_pos();
        let capacity = Self::capacity();
        let insert_pos = match self.keys().search(&key) {
            Ok(pos) => pos, // point to insert to
            _ => return None // already exists, exit
        } as u32;
        if pos + 1 >= capacity {
            // need to split
            let mid = self.get_pos() / 2;
            let pos2 = self.get_pos() - mid;
            let pos1 = mid - 1;
            let keys_2 = {
                let mut keys_1 = self.keys_mut();
                let mut keys_2 =
                    Self::moving_keys(keys_1.split_at_mut(mid as usize).1);
                Self::insert_split(
                    key,
                    &mut keys_1,
                    &mut keys_2.as_slice_mut(),
                    mid, insert_pos, capacity);
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
                    mid, insert_pos + 1, capacity + 1);
                delis_2
            };
            self.set_pos(pos1);

            return Some(Self::construct(keys_2, delis_2, pos2))
        } else {
            let pos = self.get_pos();
            Self::insert_to_slice(self.keys_mut(), key, pos, capacity);
            Self::insert_to_slice(self.delimiters_mut(), delimiter, pos + 1, capacity + 1);
            self.set_pos(pos + 1);
            return None;
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

                type LDelimiter = [Delimiter<LNode>; $delimiter_size];

                impl Array<Delimiter<LNode>> for LDelimiter {
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

                impl Delimiters<LNode> for LDelimiter {}
                impl Keys for LEntryKeys {}

                struct LNode {
                    keys: LEntryKeys,
                    delimeters: LDelimiter,
                    pos: u32
                }

                impl Node for LNode {
                    #[inline]
                    fn keys(&self) -> &Keys {
                        &self.keys
                    }
                    #[inline]
                    fn delimiters(&self) -> &Delimiters<Self> {
                        &self.delimeters
                    }
                    #[inline]
                    fn add(&mut self, key: EntryKey, delimiter: Delimiter<Self>) -> Option<Self> {
                        unimplemented!()
                    }
                    #[inline]
                    fn del(&mut self, key: &EntryKey) {
                        unimplemented!()
                    }
                    #[inline]
                    fn merge(&mut self, x: Self) {
                        unimplemented!()
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