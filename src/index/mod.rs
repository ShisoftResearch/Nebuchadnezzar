use dovahkiin::types::custom_types::id::Id;
use smallvec::SmallVec;
use std::fmt::Debug;
use std::io::Cursor;
use std::mem;

pub mod btree;
pub mod placement;
pub mod sstable;

const ID_SIZE: usize = 16;
type EntryKey = SmallVec<[u8; 32]>;

fn id_from_key(key: &EntryKey) -> Id {
    debug!("Decoding key to id {:?}", key);
    let mut id_cursor = Cursor::new(&key[key.len() - ID_SIZE..]);
    return Id::from_binary(&mut id_cursor).unwrap(); // read id from tailing 128 bits
}

pub fn key_prefixed(prefix: &EntryKey, x: &EntryKey) -> bool {
    return prefix.as_slice() == &x[..x.len() - ID_SIZE];
}

pub trait Slice<T>: Sized
where
    T: Default + Debug,
{
    fn as_slice(&mut self) -> &mut [T];
    fn init() -> Self;
    fn item_default() -> T {
        T::default()
    }
}
