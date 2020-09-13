use dovahkiin::types::custom_types::id::Id;
use std::io::Cursor as StdCursor;
use smallvec::SmallVec; 

#[macro_use]
mod macros;
#[macro_use]
pub mod builder;
pub mod hash;
pub mod ranged;

pub const ID_SIZE: usize = 16;
pub const KEY_SIZE: usize = ID_SIZE + 20; // 20 is the estimate length of: schema id u32 (4) + field id u64(8) and value u64(8)
pub const MAX_KEY_SIZE: usize = KEY_SIZE * 2;
pub type EntryKey = SmallVec<[u8; KEY_SIZE]>;

pub fn id_from_key(key: &EntryKey) -> Id {
    if key.len() < ID_SIZE {
        return Id::new(1111, 1111);
    }
    let mut id_cursor = StdCursor::new(&key[key.len() - ID_SIZE..]);
    return Id::from_binary(&mut id_cursor).unwrap(); // read id from tailing 128 bits
}

pub fn key_with_id(key: &mut EntryKey, id: &Id) {
    let id_bytes = id.to_binary();
    key.extend_from_slice(&id_bytes);
}

pub fn key_prefixed(prefix: &EntryKey, x: &EntryKey) -> bool {
    return prefix.as_slice() == &x[..x.len() - ID_SIZE];
}