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

lazy_static! {
    pub static ref ENTRIES_KEY_HASH : u64 = key_hash("entries");
    pub static ref KEY_KEY_HASH : u64 = key_hash("id");
    pub static ref VALUE_KEY_HASH : u64 = key_hash("value");
}

type EntryKey = SmallVec<[u8; 16]>;

pub struct LSMTree {
    num_levels: u8,
    levels: Vec<Rc<BPlusTree>>
}

trait BPlusTree {
    fn root(&self) -> &Node;
    fn root_mut(&self) -> &mut Node;
    fn get_height(&self) -> u32;
    fn set_height(&mut self) -> u32;
    fn get_num_nodes(&self) -> u32;
    fn set_num_nodes(&self) -> u32;
    fn chunks(&self) -> &Arc<Chunks>;
    fn get_page(&self, id: &Id) -> Vec<Entry> {
        let cell = self.chunks().read_cell(id).unwrap(); // should crash if not exists
        let entries = &cell.data[*ENTRIES_KEY_HASH];
        let mut entry_result = Vec::with_capacity(entries.len().unwrap());
        for entry in entries.iter_value().unwrap() {
            let value = entry[*KEY_KEY_HASH].Id().unwrap();
            let key = if let Value::PrimArray(PrimitiveArray::U8(ref array)) = entry[*VALUE_KEY_HASH] {
                array.clone()
            } else { panic!("invalid entry") };
            entry_result.push(Entry { key: EntryKey::from(key), id: *value });
        }
        return entry_result;
    }
    fn get(&self, key: &EntryKey) -> Option<&Id> {
        self.search(self.root(), key, self.get_height())
    }
    fn search<'a>(&self, node: &'a Node, key: &EntryKey, ht: u32) -> Option<&'a Id> {
        let keys = node.keys();
        let index = keys
            .binary_search(key)
            .map(|i| i + 1)
            .unwrap_or_else(|i| i);
        match &node.delimiters().get(index) {
            Some(Delimiter::External(id)) => {
                // search in leaf
                unimplemented!()
            },
            Some(Delimiter::Internal(node)) => return self.search(node.borrow(), key, ht - 1),
            None => return None
        }
    }
}

enum Delimiter {
    External(Id), // to leaf
    Internal(Box<Node>) // to higher level node
}

struct Entry {
    key: EntryKey,
    id: Id
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

trait Leaf {
    fn entries(&self) -> &[Entry];
    fn entries_mut(&self) -> &mut [Entry];
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