use super::{Feature, KEY_SIZE, SCAN_SIZE};
use crate::ram::types::Id;
use byteorder::{BigEndian, WriteBytesExt};
use serde::de::{SeqAccess, Visitor};
use serde::ser::SerializeSeq;
use serde::{Deserialize, Deserializer, Serialize, Serializer};
use std::cmp;
use std::fmt;
use std::io::Cursor;
use std::io::Write;
use std::ops::{Index, IndexMut};
use std::ptr;
use std::slice::Iter;
use std::slice::SliceIndex;

pub const ID_SIZE: usize = 16;

pub type EntryKey = GenericKey<KEY_SIZE>;
pub type ScanKey = GenericKey<SCAN_SIZE>;

#[derive(Clone, Eq, PartialEq, Debug, Hash)]
pub struct GenericKey<const N: usize> {
    slice: [u8; N],
}

impl EntryKey {
    pub fn from_props(id: &Id, feature: &Feature, field: u64, schema_id: u32) -> Self {
        let mut key = Self::new();
        let mut cursor = Cursor::new(&mut key.slice[..]);
        cursor.write_u32::<BigEndian>(schema_id).unwrap();
        cursor.write_u32::<BigEndian>(field as u32).unwrap();
        cursor.write(feature).unwrap();
        cursor.write_u64::<BigEndian>(id.higher).unwrap();
        cursor.write_u64::<BigEndian>(id.lower).unwrap();
        key
    }
}

impl ScanKey {
    pub fn for_scannable(id: &Id, schema_id: u32) -> Self {
        let mut key = Self::new();
        let mut cursor = Cursor::new(&mut key.slice[..]);
        cursor.write_u32::<BigEndian>(schema_id).unwrap();
        cursor.write_u64::<BigEndian>(id.higher).unwrap();
        cursor.write_u64::<BigEndian>(id.lower).unwrap();
        key
    }
}

impl <const N: usize> GenericKey<N> {

    #[inline(always)]
    pub fn new() -> Self {
        Self::default()
    }

    #[inline(always)]
    pub fn len(&self) -> usize {
        self.slice.len()
    }

    #[inline(always)]
    pub fn as_slice(&self) -> &[u8] {
        &self.slice
    }

    #[inline(always)]
    pub fn as_mut_slice(&mut self) -> &mut [u8] {
        &mut self.slice
    }

    #[inline(always)]
    pub fn max() -> Self {
        Self {
            slice: [u8::MAX; N],
        }
    }
    pub fn from_slice(s: &[u8]) -> Self {
        let mut key = Self::new();
        key.copy_slice(s);
        key
    }
    pub fn copy_slice(&mut self, slice: &[u8]) {
        let len = cmp::min(slice.len(), N);
        unsafe {
            ptr::copy_nonoverlapping(slice.as_ptr(), self.slice.as_mut_ptr(), len);
        }
    }
    pub fn id(&self) -> Id {
        let mut id_cursor = Cursor::new(&self.slice[N - ID_SIZE..]);
        let id = Id::from_binary(&mut id_cursor).unwrap(); // read id from tailing 128 bits
        if cfg!(debug_assertions) && id.is_unit_id() {
            warn!("id is unit id from key {:?}", self.slice)
        }
        id
    }
    pub fn set_id(&mut self, id: &Id) {
        let id_data = id.to_binary();
        unsafe {
            ptr::copy_nonoverlapping(
                id_data.as_ptr(),
                self.slice[N - ID_SIZE..].as_mut_ptr(),
                ID_SIZE,
            );
        }
    }
    pub fn from_id(id: &Id) -> Self {
        let mut key = Self::new();
        key.set_id(id);
        key
    }
}

impl <const N: usize> Default for GenericKey<N> {
    fn default() -> Self {
        Self {
            slice: [0u8; N],
        }
    }
}

impl<I: SliceIndex<[u8]>, const N: usize> Index<I> for GenericKey<N> {
    type Output = I::Output;
    fn index(&self, index: I) -> &I::Output {
        &(self.slice)[index]
    }
}

impl<I: SliceIndex<[u8]>, const N: usize> IndexMut<I> for GenericKey<N> {
    fn index_mut(&mut self, index: I) -> &mut I::Output {
        &mut (self.slice)[index]
    }
}

impl<'a, const N: usize> IntoIterator for &'a GenericKey<N> {
    type Item = &'a u8;
    type IntoIter = Iter<'a, u8>;
    fn into_iter(self) -> Self::IntoIter {
        self.slice.iter()
    }
}

impl <const N: usize> Serialize for GenericKey<N> {
    fn serialize<S: Serializer>(&self, serializer: S) -> Result<S::Ok, S::Error> {
        let mut state = serializer.serialize_seq(Some(self.len()))?;
        for item in self {
            state.serialize_element(&item)?;
        }
        state.end()
    }
}

impl<'de, const N: usize> Deserialize<'de> for GenericKey<N> {
    fn deserialize<D: Deserializer<'de>>(deserializer: D) -> Result<Self, D::Error> {
        deserializer.deserialize_seq(GenericEntryKeyVisitor::<N>)
    }
}

struct GenericEntryKeyVisitor<const N: usize>;

impl<'de, const N: usize> Visitor<'de> for GenericEntryKeyVisitor<N> {
    type Value = GenericKey::<N>;

    fn expecting(&self, formatter: &mut fmt::Formatter<'_>) -> fmt::Result {
        formatter.write_str("a sequence")
    }

    fn visit_seq<B>(self, mut seq: B) -> Result<Self::Value, B::Error>
    where
        B: SeqAccess<'de>,
    {
        let mut values = GenericKey::<N>::new();
        let mut counter = 0;
        while let Some(value) = seq.next_element()? {
            values.as_mut_slice()[counter] = value;
            counter += 1;
        }

        Ok(values)
    }
}

impl <const N: usize> PartialOrd for GenericKey<N> {
    fn partial_cmp(&self, other: &Self) -> Option<cmp::Ordering> {
        Some(self.slice.cmp(&other.slice))
    }
}

impl <const N: usize> Ord for GenericKey<N> {
    fn cmp(&self, other: &Self) -> cmp::Ordering {
        self.slice.cmp(&other.slice)
    }
}
