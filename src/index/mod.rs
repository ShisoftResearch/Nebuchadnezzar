use dovahkiin::types::custom_types::id::Id;
use std::io::Cursor as StdCursor;

#[macro_use]
mod macros;
#[macro_use]
pub mod builder;
pub mod hash;
pub mod ranged;
pub mod entry;

pub const FEATURE_SIZE: usize = 8;
pub const ID_SIZE: usize = 16;
pub const KEY_SIZE: usize = ID_SIZE + FEATURE_SIZE + 8; // 8 is the estimate length of: schema id u32 (4) + field id u32(4, reduced from u64)
pub const MAX_KEY_SIZE: usize = KEY_SIZE * 2;

pub use entry::EntryKey;
pub type Feature = [u8; FEATURE_SIZE];

pub fn id_from_key(key: &EntryKey) -> Id {
    if key.len() < ID_SIZE {
        return Id::new(1111, 1111);
    }
    let mut id_cursor = StdCursor::new(&key[key.len() - ID_SIZE..]);
    return Id::from_binary(&mut id_cursor).unwrap(); // read id from tailing 128 bits
}

pub fn key_with_id(key: &mut EntryKey, id: &Id) {
    let id_bytes = id.to_binary();
    key.copy_slice(&id_bytes);
}

pub fn key_prefixed(prefix: &EntryKey, x: &EntryKey) -> bool {
    return prefix.as_slice() == &x[..x.len() - ID_SIZE];
}
