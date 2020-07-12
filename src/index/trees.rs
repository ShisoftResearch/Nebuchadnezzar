use dovahkiin::types::custom_types::id::Id;
use smallvec::SmallVec;
use std::fmt::Debug;
use std::io::Cursor as StdCursor;
use std::mem;
use std::ops::Deref;
use std::ops::DerefMut;
use std::ops::Index;
use std::ops::IndexMut;
use std::slice::SliceIndex;

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

pub trait Slice<T: Default>: Send + Sync {
    #[inline]
    fn as_slice(&mut self) -> &mut [T];
    #[inline]
    fn as_slice_immute(&self) -> &[T] {
        unsafe {
            let raw = self as *const Self as *mut Self;
            (*raw).as_slice()
        }
    }
    #[inline]
    fn slice_len() -> usize;
    #[inline]
    fn init() -> Self;
    #[inline]
    fn item_default() -> T {
        T::default()
    }

    fn split_at_pivot(&mut self, pivot: usize, len: usize) -> Self
    where
        Self: Sized,
    {
        let mut right_slice = Self::init();
        {
            let slice1: &mut [T] = self.as_slice();
            let slice2: &mut [T] = right_slice.as_slice();
            for i in pivot..len {
                // leave pivot to the right slice
                let right_pos = i - pivot;
                mem::swap(&mut slice1[i], &mut slice2[right_pos]);
            }
        }
        return right_slice;
    }
    fn insert_at(&mut self, item: T, pos: usize, len: &mut usize) {
        debug_assert!(pos <= *len, "pos {} larger or equals to len {}", pos, len);
        debug!("insert into slice, pos: {}, len {}", pos, len);
        let slice = self.as_slice();
        if *len > 0 {
            slice[*len] = T::default();
            for i in (pos..=*len - 1).rev() {
                slice.swap(i, i + 1);
            }
        }
        *len += 1;
        slice[pos] = item;
    }
    fn remove_at(&mut self, pos: usize, len: &mut usize) {
        debug!("remove at {} len {}", pos, len);
        debug_assert!(pos < *len, "remove overflow, pos {}, len {}", pos, len);
        let slice = self.as_slice();
        let bound = *len - 1;
        if pos < bound {
            for i in pos..bound {
                slice.swap(i, i + 1);
            }
        }
        slice[bound] = T::default();
        *len -= 1;
    }
}

pub trait Cursor {
    fn next(&mut self) -> bool;
    fn current(&self) -> Option<&EntryKey>;
}

#[derive(Debug, Clone, Copy, Eq, PartialEq, Serialize, Deserialize)]
pub enum Ordering {
    Forward,
    Backward,
}
