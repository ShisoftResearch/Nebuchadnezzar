use std::rc::{Rc, Weak};
use std::cell::RefCell;
use std::borrow::Borrow;
use dovahkiin::types::custom_types::id::Id;
use smallvec::SmallVec;
use std::sync::Arc;
use ram::chunk::Chunks;
use ram::cell::Cell;
use dovahkiin::types::custom_types::map::key_hash;
use dovahkiin::types::value::{ValueIter};
use dovahkiin::types::{Value, PrimitiveArray};
use utils::lru_cache::LRUCache;
use parking_lot::{Mutex, MutexGuard};
use std::io::Cursor;

lazy_static! {
    pub static ref ENTRIES_KEY_HASH : u64 = key_hash("entries");
    pub static ref KEY_KEY_HASH : u64 = key_hash("id");
    pub static ref PREV_PAGE : u64 = key_hash("prev");
    pub static ref NEXT_PAGE : u64 = key_hash("next");
}

type EntryKey = SmallVec<[u8; 32]>;

pub struct LSMTree {
    num_levels: u8,
    levels: Vec<Rc<BPlusTree>>,
}

 trait BPlusTree {
    //  fn root(&self) -> &Node;
    //  fn root_mut(&self) -> &mut Node;
    //  fn get_height(&self) -> u32;
    //  fn set_height(&mut self) -> u32;
    //  fn get_num_nodes(&self) -> u32;
    //  fn set_num_nodes(&self) -> u32;
    //  fn page_size(&self) -> usize;
    //  fn chunks(&self) -> &Arc<Chunks>;
    //  fn page_cache(&self) -> MutexGuard<LRUCache<Id, EntryPage>>;
    //  fn get_page_direct(&self, id: &Id) -> Node {
    //      let cell = self.chunks().read_cell(id).unwrap(); // should crash if not exists
    //      let entries = &cell.data[*ENTRIES_KEY_HASH];
    //      let mut entry_result = Vec::with_capacity(entries.len().unwrap());
    //      for entry in entries.iter_value().unwrap() {
    //          let key = if let Value::PrimArray(PrimitiveArray::U8(ref array)) = entry[*VALUE_KEY_HASH] {
    //              array.clone()
    //          } else { panic!("invalid entry") };
    //          let mut id_cursor = Cursor::new(key[key.len() - 17..]);
    //      }
    //      return Arc::new(entry_result);
    //  }
    //  fn get_page(&self, id: &Id) -> EntryPage {
    //      self.page_cache().get_or_fetch(id).unwrap().clone()
    //  }
    //  fn get(&self, key: &EntryKey) -> Option<Id> {
    //      self.search(self.root(), key, self.get_height())
    //  }
    //  fn search<'a>(&self, node: &'a Node, key: &EntryKey, ht: u32) -> Option<Id> {
    //      let keys = node.keys();
    //      let index = keys
    //          .binary_search(key)
    //          .map(|i| i + 1)
    //          .unwrap_or_else(|i| i);
    //      match &node.delimiters().get(index) {
    //          Some(Delimiter::External(ref id)) => {
    //              assert_eq!(ht, 0);
    //              // search in leaf page
    //              let mut page = self.get_page(id);
    //              return page
    //                  .binary_search_by_key(&id, |entry| &entry.id)
    //                  .ok()
    //                  .map(move |index| page[index].id);
    //          },
    //          Some(Delimiter::Internal(node)) => return self.search(node.borrow(), key, ht - 1),
    //          None => return None
    //      }
    //  }
 }

enum Delimiter {
    External(Id), // to leaf
    Internal(Box<Node>) // to higher level node
}

trait Array<T> {
    #[inline]
    fn index_of(&self, index: usize) -> &T;
    #[inline]
    fn size(&self) -> usize;
}

trait Keys : Array<EntryKey> {}

trait Delimiters : Array<Delimiter> {}

trait Node {
    fn keys(&self) -> &[EntryKey];
    fn delimiters(&self) -> &[Delimiter];
    fn keys_mut(&mut self) -> &mut [EntryKey];
    fn delimiters_mut(&mut self) -> &mut [Delimiter];
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
                }

                type LDelimiter = [Delimiter; $delimiter_size];

                impl Array<Delimiter> for LDelimiter {
                    #[inline]
                    fn size(&self) -> usize { $delimiter_size }
                    #[inline]
                    fn index_of(&self, index: usize) -> &Delimiter { &self[index] }
                }

                impl Delimiters for LDelimiter {}

                struct LNode {
                    id: Id,
                    keys: LEntryKeys,
                    delimeters: LDelimiter,
                }

                impl Node for LNode {
                    #[inline]
                    fn keys(&self) -> &[EntryKey] {
                        &self.keys
                    }
                    #[inline]
                    fn delimiters(&self) -> &[Delimiter] {
                        &self.delimeters
                    }
                    #[inline]
                    fn keys_mut(&mut self) -> &mut [EntryKey] {
                        &mut self.keys
                    }
                    #[inline]
                    fn delimiters_mut(&mut self) -> &mut [Delimiter] {
                        &mut self.delimeters
                    }
                }
            }
        )+
    };
}

impl_nodes!((level_0, 2, 3), (level_1, 20, 21), (level_2, 200, 201), (level_3, 2000, 2001), (level_4, 20000, 20001));

//impl BTree {
//    pub fn new(max_entries: u64) -> BTree {
//        BTree {
//            node_size: 0,
//            height: 0,
//            max_entries,
//            root: Node::new(node_size)
//        }
//    }
//}
//
//impl Node {
//    pub fn new(node_size: u16) -> Rc<Node> {
//        Rc::new(Node {
//            id: Id::unit_id(),
//            children: [u8; 16]
//        })
//    }
//}