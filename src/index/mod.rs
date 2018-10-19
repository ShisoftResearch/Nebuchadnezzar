use smallvec::SmallVec;
use std::io::Cursor;
use dovahkiin::types::custom_types::id::Id;
use std::fmt::Debug;
use std::mem;

pub mod placement;
pub mod btree;
pub mod lsmtree;

const ID_SIZE: usize = 16;
type EntryKey = SmallVec<[u8; 32]>;

fn id_from_key(key: &EntryKey) -> Id {
    debug!("Decoding key to id {:?}", key);
    let mut id_cursor = Cursor::new(&key[key.len() - ID_SIZE ..]);
    return Id::from_binary(&mut id_cursor).unwrap(); // read id from tailing 128 bits
}

pub fn key_prefixed(prefix: &EntryKey, x: &EntryKey) -> bool {
    return prefix.as_slice() == &x[.. x.len() - ID_SIZE];
}

pub trait Slice<T> : Sized where T: Default + Debug {
    fn as_slice(&mut self) -> &mut [T];
    fn init() -> Self;
    fn item_default() -> T {
        T::default()
    }
    fn split_at_pivot(&mut self, pivot: usize, len: usize) -> Self {
        let mut right_slice = Self::init();
        {
            let mut slice1: &mut[T] = self.as_slice();
            let mut slice2: &mut[T] = right_slice.as_slice();
            for i in pivot .. len { // leave pivot to the right slice
                let right_pos = i - pivot;
                let item = mem::replace(&mut slice1[i], T::default());
                debug!("Moving from left pos {} to right {}, item {:?} for split", i, right_pos, &item);
                slice2[right_pos] = item;
            }
        }
        return right_slice;
    }
    fn insert_at(&mut self, item: T, pos: usize, len: &mut usize) {
        debug_assert!(pos <= *len, "pos {:?} larger or equals to len {:?}, item: {:?}", pos, len, item);
        debug!("insert into slice, pos: {}, len {}, item {:?}", pos, len, item);
        let mut slice = self.as_slice();
        if *len > 0 {
            slice[*len] = T::default();
            for i in (pos ..= *len - 1).rev() {
                debug!("swap {} with {} for insertion", i, i + 1);
                slice.swap(i, i + 1);
            }
        }
        debug!("setting item {:?} at {} for insertion", item, pos);
        *len += 1;
        slice[pos] = item;
    }
    fn remove_at(&mut self, pos: usize, len: &mut usize) {
        debug!("remove at {} len {}", pos, len);
        debug_assert!(pos < *len, "remove overflow, pos {}, len {}", pos, len);
        if pos < *len - 1 {
            let slice  = self.as_slice();
            for i in pos .. *len - 1 {
                slice.swap(i, i + 1);
            }
        }
        *len -= 1;
    }
}