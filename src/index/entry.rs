use super::{KEY_SIZE, Feature};
use std::ops::{Index, IndexMut};
use std::slice::SliceIndex;
use std::slice::Iter;
use std::cmp;
use std::ptr;
use std::io::Cursor;
use crate::ram::types::Id;
use serde::{Serialize, Deserialize, Serializer, Deserializer};
use serde::ser::SerializeSeq;
use serde::de::{Visitor, SeqAccess};
use byteorder::{BigEndian, ReadBytesExt, WriteBytesExt};
use std::fmt;
use std::io::Write;

type InnerSlice = [u8; KEY_SIZE];

#[derive(Clone, Eq, PartialEq, Debug, Hash)]
pub struct EntryKey {
    slice: InnerSlice,
}


impl EntryKey {
    pub fn from_props(id: &Id, feature: &Feature, field: u64, schema_id: u32) -> Self {
        let mut key = Self::new();
        let mut cursor = Cursor::new(&mut key.slice[..]);
        cursor.write_u64::<BigEndian>(id.higher).unwrap();
        cursor.write_u64::<BigEndian>(id.lower).unwrap();
        cursor.write_u32::<BigEndian>(field as u32).unwrap();
        cursor.write_u32::<BigEndian>(schema_id).unwrap();
        cursor.write(feature).unwrap();
        key
    }

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
            slice: [u8::MAX; KEY_SIZE]
        }
    }
    pub fn from_slice(s: &[u8]) -> Self {
        let mut key = EntryKey::new();
        key.copy_slice(s);
        key
    }
    pub fn copy_slice(&mut self, slice: &[u8]) {
        let len = cmp::min(slice.len(), KEY_SIZE);
        unsafe {
            ptr::copy_nonoverlapping(slice.as_ptr(), self.slice.as_mut_ptr(), len);
        }
    }
}

impl Default for EntryKey {
    fn default() -> Self {
        Self {
             slice: [0u8; KEY_SIZE],
        }
    }
}

impl<I: SliceIndex<[u8]>> Index<I> for EntryKey {
    type Output = I::Output;
    fn index(&self, index: I) -> &I::Output {
        &(self.slice)[index]
    }
}

impl<I: SliceIndex<[u8]>> IndexMut<I> for EntryKey {
    fn index_mut(&mut self, index: I) -> &mut I::Output {
        &mut (self.slice)[index]
    }
}

impl <'a> IntoIterator for &'a EntryKey {
    type Item = &'a u8;
    type IntoIter = Iter<'a, u8>;
    fn into_iter(self) -> Self::IntoIter {
        self.slice.iter()
    }
}

impl Serialize for EntryKey {
    fn serialize<S: Serializer>(&self, serializer: S) -> Result<S::Ok, S::Error> {
        let mut state = serializer.serialize_seq(Some(self.len()))?;
        for item in self {
            state.serialize_element(&item)?;
        }
        state.end()
    }
}

impl<'de> Deserialize<'de> for EntryKey {
    fn deserialize<D: Deserializer<'de>>(deserializer: D) -> Result<Self, D::Error> {
        deserializer.deserialize_seq(EntryKeyVisitor)
    }
}

struct EntryKeyVisitor;

impl<'de> Visitor<'de> for EntryKeyVisitor {
    type Value = EntryKey;

    fn expecting(&self, formatter: &mut fmt::Formatter<'_>) -> fmt::Result {
        formatter.write_str("a sequence")
    }

    fn visit_seq<B>(self, mut seq: B) -> Result<Self::Value, B::Error>
    where
        B: SeqAccess<'de>,
    {
        let len = seq.size_hint().unwrap_or(0);
        let mut values = EntryKey::new();
        let mut counter = 0;
        while let Some(value) = seq.next_element()? {
            values.as_mut_slice()[counter] = value;
            counter += 1;
        }

        Ok(values)
    }
}

impl PartialOrd for EntryKey {
    fn partial_cmp(&self, other: &EntryKey) -> Option<cmp::Ordering> {
        Some(self.slice.cmp(&other.slice))
    }
}

impl Ord for EntryKey {
    fn cmp(&self, other: &EntryKey) -> cmp::Ordering {
        self.slice.cmp(&other.slice)
    }
}