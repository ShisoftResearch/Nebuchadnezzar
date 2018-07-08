use std::rc::{Rc, Weak};
use std::cell::RefCell;
use std::borrow::Borrow;
use dovahkiin::types::custom_types::id::Id;
use smallvec::SmallVec;

type EntryKey = SmallVec<[u8; 16]>;

pub struct LSMTree {
    num_levels: u8,
    levels: Vec<Rc<BTree>>
}

trait BTree {
    fn root(&self) -> &Node;
    fn root_mut(&self) -> &mut Node;
    fn get_height(&self) -> u32;
    fn set_height(&mut self) -> u32;
    fn get_num_nodes(&self) -> u32;
    fn set_num_nodes(&self) -> u32;
    fn get(&self, key: &EntryKey) -> Option<&Id> {
        self.search(self.root(), key, self.get_height())
    }
    fn search<'a>(&self, node: &'a Node, key: &EntryKey, ht: u32) -> Option<&'a Id> {
        let children = &node.entries();
        match node.entries().binary_search_by(|e| e.key.cmp(key)) {
            Ok(pos) => return Some(&node.entries()[pos].val),
            Err(pos) => {
                if ht == 0 {
                    return None
                }
                else {
                    match &node.delimiters().get(pos) {
                        Some(Delimiter::External(id)) => unimplemented!(), // TODO: read page from remote
                        Some(Delimiter::Internal(node)) => return self.search(node.borrow(), key, ht - 1),
                        None => return None
                    }
                }
            }
        }
    }
}


struct Entry {
    key: EntryKey,
    val: Id
}

enum Delimiter {
    External(Id),
    Internal(Box<Node>)
}

trait Array<T> {
    #[inline]
    fn index_of(&self, index: usize) -> &T;
    #[inline]
    fn size(&self) -> usize;
}

trait Entries : Array<Entry> {}

trait Delimiters : Array<Delimiter> {}

trait Node {
    fn id(&self) -> &Id;
    fn entries(&self) -> &[Entry];
    fn delimiters(&self) -> &[Delimiter];
    fn entries_mut(&mut self) -> &mut [Entry];
    fn delimiters_mut(&mut self) -> &mut [Delimiter];
}

macro_rules! impl_nodes {
    ($(($level: ident, $entry_size: expr, $delimiter_size: expr)),+) => {
        $(
            mod $level {
                use super::*;

                type LEntries = [Entry; $entry_size];

                impl Array<Entry> for LEntries {
                    #[inline]
                    fn size(&self) -> usize { $entry_size }
                    #[inline]
                    fn index_of(&self, index: usize) -> &Entry { &self[index] }
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
                    entries: LEntries,
                    delimeters: LDelimiter,
                }

                impl Node for LNode {
                    #[inline]
                    fn id(&self) -> &Id {
                        &self.id
                    }
                    #[inline]
                    fn entries(&self) -> &[Entry] {
                        &self.entries
                    }
                    #[inline]
                    fn delimiters(&self) -> &[Delimiter] {
                        &self.delimeters
                    }
                    #[inline]
                    fn entries_mut(&mut self) -> &mut [Entry] {
                        &mut self.entries
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