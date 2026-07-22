use std::fmt::Debug;
use std::mem;

pub use crate::index::*;

pub trait Slice<T: Default>: Send + Sync {
    const SLICE_LEN: usize;
    fn as_slice(&mut self) -> &mut [T];
    fn as_slice_immute(&self) -> &[T];
    #[inline]
    fn slice_len() -> usize {
        Self::SLICE_LEN
    }
    fn init() -> Self;
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
        trace!("insert into slice, pos: {}, len {}", pos, len);
        let slice = self.as_slice();
        // Shift [pos..len] right by one in a single rotation (compiles to a
        // memmove-style loop) and drop whatever occupied the vacated slot.
        slice[pos..=*len].rotate_right(1);
        slice[pos] = item;
        *len += 1;
    }
    fn remove_at(&mut self, pos: usize, len: &mut usize) {
        trace!("remove at {} len {}", pos, len);
        debug_assert!(pos < *len, "remove overflow, pos {}, len {}", pos, len);
        let slice = self.as_slice();
        slice[pos..*len].rotate_left(1);
        slice[*len - 1] = T::default();
        *len -= 1;
    }
}

pub trait Cursor: Send {
    fn next(&mut self) -> Option<EntryKey>;
    fn current(&self) -> Option<&EntryKey>;
}

#[derive(Debug, Clone, Copy, Eq, PartialEq, Serialize, Deserialize)]
pub enum Ordering {
    Forward,
    Backward,
}

lazy_static! {
    pub static ref MAX_ENTRY_KEY: EntryKey = raw_max_entry_key();
    pub static ref MIN_ENTRY_KEY: EntryKey = raw_min_entry_key();
}

#[inline]
fn raw_max_entry_key() -> EntryKey {
    EntryKey::max()
}

#[inline(always)]
fn raw_min_entry_key() -> EntryKey {
    Default::default()
}

#[inline(always)]
pub fn max_entry_key() -> EntryKey {
    (*MAX_ENTRY_KEY).clone()
}

#[inline(always)]
pub fn min_entry_key() -> EntryKey {
    raw_min_entry_key()
}
