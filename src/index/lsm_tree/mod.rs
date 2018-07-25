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
      fn page_size(&self) -> usize;
      fn chunks(&self) -> &Arc<Chunks>;
      fn page_cache(&self) -> MutexGuard<LRUCache<Id, CachedExtNodeRef>>;
      fn get_page_direct(&self, id: &Id) -> CachedExtNodeRef {
          let cell = self.chunks().read_cell(id).unwrap(); // should crash if not exists
          let keys = &cell.data[*KEYS_KEY_HASH];
          let keys_len = keys.len().unwrap();
          let mut v_node = CachedExtNode {
              keys: Vec::new(),
              delimiters: Vec::new(),
          };
          for key in keys.iter_value().unwrap() {
              let key = if let Value::PrimArray(PrimitiveArray::U8(ref array)) = key {
                  array
              } else { panic!("invalid entry") };
              let mut id_cursor = Cursor::new(&key[key.len() - 16 ..]);
              let id = Id::from_binary(&mut id_cursor).unwrap(); // read id from tailing 128 bits
              v_node.keys.push(EntryKey::from_slice(key.as_slice()));
              v_node.delimiters.push(Delimiter::External(id));
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
          let keys = node.keys();
          let index = keys
              .search(key)
              .map(|i| i + 1)
              .unwrap_or_else(|i| i);
          if ht == 0 { // persist page
              match &node.delimiters().get(index) {
                  Some(Delimiter::External(id)) => Some(*id),
                  Some(Delimiter::Internal(_)) => panic!("Error on leaf delimiter type"),
                  None => panic!("Error on leaf delimiter is none")
              }
          } else {
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

trait Node {
    fn keys(&self) -> &Keys;
    fn delimiters(&self) -> &Delimiters;
    fn keys_mut(&mut self) -> &mut Keys;
    fn delimiters_mut(&mut self) -> &mut Delimiters;
}

struct CachedExtNode {
    keys: Vec<EntryKey>,
    delimiters: Vec<Delimiter>
}

impl Node for CachedExtNode {
    fn keys(&self) -> &Keys {
        return self.keys.as_slice()
    }

    fn delimiters(&self) -> &Delimiters {
        return self.delimiters.as_slice();
    }

    fn keys_mut(&mut self) -> &mut Keys {
        panic!()
    }

    fn delimiters_mut(&mut self) -> &mut Delimiters {
        panic!()
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
                    fn search(&self, item: &T) -> Result<usize, usize> {
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
                    id: Id,
                    keys: LEntryKeys,
                    delimeters: LDelimiter,
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
                    fn keys_mut(&mut self) -> &mut Keys {
                        &mut self.keys
                    }
                    #[inline]
                    fn delimiters_mut(&mut self) -> &mut Delimiters {
                        &mut self.delimeters
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
    fn partial_cmp(&self, other: &Rhs) -> Option<Ordering> {
        panic!()
    }
}
impl Eq for Delimiter {}
impl PartialEq for Delimiter {
    fn eq(&self, other: &Rhs) -> bool {
        panic!()
    }
}