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

lazy_static! {
    pub static ref ENTRIES_KEY_HASH : u64 = key_hash("entries");
    pub static ref KEYS_KEY_HASH : u64 = key_hash("keys");
    pub static ref PREV_PAGE_KEY_HASH : u64 = key_hash("prev");
    pub static ref NEXT_PAGE_KEY_HASH : u64 = key_hash("next");
}

type EntryKey = SmallVec<[u8; 32]>;
type CachedExtNodeRef = Arc<CachedExtNode>;

pub struct LSMTree {
    num_levels: u8,
    levels: Vec<Rc<BPlusTree>>,
}

 trait BPlusTree {
      fn root(&self) -> &Node;
      fn root_mut(&self) -> &mut Node;
      fn get_height(&self) -> u32;
      fn set_height(&mut self) -> u32;
      fn get_num_nodes(&self) -> u32;
      fn set_num_nodes(&self) -> u32;
      fn page_size(&self) -> u32;
      fn chunks(&self) -> &Arc<Chunks>;
      fn page_cache(&self) -> MutexGuard<LRUCache<Id, CachedExtNodeRef>>;
      fn get_page_direct(&self, id: &Id) -> CachedExtNodeRef {
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
      fn get_page(&self, id: &Id) -> CachedExtNodeRef {
          self.page_cache().get_or_fetch(id).unwrap().clone()
      }
      fn get(&self, key: &EntryKey) -> Option<Id> {
          self.search(self.root(), key, self.get_height())
      }
      fn search<'a>(&self, node: &'a Node, key: &EntryKey, ht: u32) -> Option<Id> {
          if ht == 0 { // persist page
              let search_result = node.keys()
                  .search(key);
              let index = if let Ok(i) = search_result { i } else { return None };
              match &node.delimiters().get(index) {
                  Some(Delimiter::External(id)) => Some(*id),
                  Some(Delimiter::Internal(_)) => panic!("Error on leaf delimiter type"),
                  None => panic!("Error on leaf delimiter is none")
              }
          } else {
              let index = node.keys()
                  .search(key)
                  .map(|i| i + 1)
                  .unwrap_or_else(|i| i);
              match &node.delimiters().get(index) {
                  Some(Delimiter::External(ref id)) => {
                      assert_eq!(ht, 0);
                      // search in leaf page
                      let page = self.get_page(id);
                      return self.search(&*page, key, ht - 1);
                  },
                  Some(Delimiter::Internal(node)) => return self.search(node.borrow(), key, ht - 1),
                  None => return None
              }
          }
      }
     fn insert(&self, key: &EntryKey, value: &Id) {
         unimplemented!()
     }
     fn put<'a>(&self, node: &'a Node, key: &EntryKey, ht: u32) {
         let keys = node.keys();
         let index = keys
             .search(key)
             .map(|i| i + 1)
             .unwrap_or_else(|i| i);
         if ht == 0 {

         }
     }
 }

enum Delimiter {
    External(Id), // to leaf
    Internal(Box<Node>) // to higher level node
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
}

trait Keys : Array<EntryKey> {}
trait Delimiters : Array<Delimiter> {}
impl Keys for [EntryKey] {}
impl Delimiters for [Delimiter] {}
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
}
impl Delimiters for Vec<Delimiter> {}
impl Keys for Vec<EntryKey> {}

trait Node {
    #[inline]
    fn keys(&self) -> &Keys;
    #[inline]
    fn delimiters(&self) -> &Delimiters;
    #[inline]
    fn add(&mut self, key: EntryKey) -> Option<Node>;
    #[inline]
    fn del(&mut self, key: &EntryKey);
    #[inline]
    fn merge(&mut self, x: Node);
}

struct CachedExtNode {
    keys: Vec<EntryKey>,
    ids: Vec<Delimiter>,
    cap: u32,
}

impl Node for CachedExtNode {
    #[inline]
    fn keys(&self) -> &Keys {
        &self.keys
    }

    #[inline]
    fn delimiters(&self) -> &Delimiters {
        &self.ids
    }

    fn add(&mut self, key: EntryKey) -> Option<Self> {
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

trait InterNode {

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
                }

                type LDelimiter = [Delimiter; $delimiter_size];

                impl Array<Delimiter> for LDelimiter {
                    #[inline]
                    fn size(&self) -> usize { $delimiter_size }
                    #[inline]
                    fn index_of(&self, index: usize) -> &Delimiter { &self[index] }
                    #[inline]
                    fn search(&self, item: &Delimiter) -> Result<usize, usize> {
                        self.binary_search(item)
                    }
                    #[inline]
                    fn get(&self, index: usize) -> Option<&Delimiter> {
                        if index >= $entry_size { None } else { Some(&self[index]) }
                    }
                }

                impl Delimiters for LDelimiter {}
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
                    fn delimiters(&self) -> &Delimiters {
                        &self.delimeters
                    }
                    #[inline]
                    fn add(&mut self, key: EntryKey) -> Option<Node> {
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

impl Ord for Delimiter {
    fn cmp(&self, other: &Self) -> Ordering {
        panic!()
    }
}
impl PartialOrd for Delimiter {
    fn partial_cmp(&self, other: &Delimiter) -> Option<Ordering> {
        panic!()
    }
}
impl Eq for Delimiter {}
impl PartialEq for Delimiter {
    fn eq(&self, other: &Delimiter) -> bool {
        panic!()
    }
}

fn id_from_key(key: &EntryKey) -> Id {
    let mut id_cursor = Cursor::new(&key[key.len() - 16 ..]);
    return Id::from_binary(&mut id_cursor).unwrap(); // read id from tailing 128 bits
}