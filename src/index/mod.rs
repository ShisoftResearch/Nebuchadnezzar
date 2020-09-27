#[macro_use]
mod macros;
#[macro_use]
pub mod builder;
pub mod hash;
pub mod ranged;
pub mod entry;

pub const FEATURE_SIZE: usize = 8;
pub const KEY_SIZE: usize = ID_SIZE + FEATURE_SIZE + 8; // 8 is the estimate length of: schema id u32 (4) + field id u32(4, reduced from u64)
pub const MAX_KEY_SIZE: usize = KEY_SIZE * 2;

pub use entry::EntryKey;
pub use entry::ID_SIZE;
pub type Feature = [u8; FEATURE_SIZE];