use dovahkiin::types::custom_types::id::Id;
use smallvec::SmallVec;
use std::fmt::Debug;
use std::io::{Cursor as StdCursor};
use std::mem;

#[macro_use]
mod macros;
pub mod btree;
pub mod lsmtree;
pub mod placement;
pub mod sstable;

const ID_SIZE: usize = 16;
const KEY_SIZE: usize = ID_SIZE + 16; // 16 is the estimate length of: schema id u32 (4) + field id u32(4) and value u64(8)+
type EntryKey = SmallVec<[u8; KEY_SIZE]>;

fn id_from_key(key: &EntryKey) -> Id {
    debug!("Decoding key to id {:?}", key);
    let mut id_cursor = StdCursor::new(&key[key.len() - ID_SIZE..]);
    return Id::from_binary(&mut id_cursor).unwrap(); // read id from tailing 128 bits
}

fn key_with_id(key: &mut EntryKey, id: &Id) {
    let id_bytes = id.to_binary();
    key.extend_from_slice(&id_bytes);
}

pub fn key_prefixed(prefix: &EntryKey, x: &EntryKey) -> bool {
    return prefix.as_slice() == &x[..x.len() - ID_SIZE];
}

pub trait Slice: Sized {
    type Item: Default + Debug;

    fn as_slice(&mut self) -> &mut [Self::Item];
    fn as_slice_immute(&self) -> &[Self::Item] {
        unsafe {
            let raw = self as *const Self as *mut Self;
            (*raw).as_slice()
        }
    }
    fn len(&self) -> usize {
        self.as_slice_immute().len()
    }
    fn init() -> Self;
    fn item_default() -> Self::Item {
        Self::Item::default()
    }
}

pub trait Cursor {
    fn next(&mut self) -> bool;
    fn current(&self) -> Option<&EntryKey>;
}