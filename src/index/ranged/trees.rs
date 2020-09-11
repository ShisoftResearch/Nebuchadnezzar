use itertools::*;
use std::fmt::Debug;
use std::iter;
use std::mem;

pub use crate::index::*;

pub trait Slice<T: Default>: Send + Sync {
    fn as_slice(&mut self) -> &mut [T];
    #[inline]
    fn as_slice_immute(&self) -> &[T] {
        unsafe {
            let raw = self as *const Self as *mut Self;
            (*raw).as_slice()
        }
    }
    fn slice_len() -> usize;
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
    EntryKey::from(iter::repeat(255u8).take(MAX_KEY_SIZE).collect_vec())
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
