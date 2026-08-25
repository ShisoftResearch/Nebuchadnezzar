use super::{Feature, FEATURE_SIZE, KEY_SIZE};
use crate::ram::schema::SchemaUid;
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

type InnerSlice = [u8; KEY_SIZE];
pub const ID_SIZE: usize = 8;
pub const MIN_FEATURE: Feature = [0; FEATURE_SIZE];
pub const MAX_FEATURE: Feature = [!0; FEATURE_SIZE];

#[derive(Clone, Eq, PartialEq, Debug, Hash)]
pub struct EntryKey {
    slice: InnerSlice,
}

impl EntryKey {
    /// Build a ranged-index key.
    ///
    /// The leading four bytes are the schema **family**, not the generation
    /// that encoded the cell. That is what lets `for_schema` remain a single
    /// prefix covering a whole schema: were the prefix a generation, a scan
    /// would have to be repeated once per generation and the results merged,
    /// and every cell the cleaner migrated would need its index entries
    /// deleted and reinserted under a new prefix. Neither happens -- migrating
    /// a cell rewrites its bytes and leaves its keys alone.
    ///
    /// The prefix keeps its width and position; only its meaning is pinned.
    pub fn from_props(id: &Id, feature: &Feature, field: u64, schema_uid: SchemaUid) -> Self {
        let mut key = Self::new();
        let mut cursor = Cursor::new(&mut key.slice[..]);
        cursor.write_u32::<BigEndian>(schema_uid.get()).unwrap();
        cursor.write_u32::<BigEndian>(field as u32).unwrap();
        cursor.write(feature).unwrap();
        cursor.write_u64::<BigEndian>(id.bits()).unwrap();
        key
    }

    pub fn for_scannable(id: &Id, schema_uid: SchemaUid) -> Self {
        Self::from_props(id, &Default::default(), 0, schema_uid)
    }

    /// The prefix that covers every cell of one schema family, in every
    /// generation it has ever had.
    pub fn for_schema(schema_uid: SchemaUid) -> Self {
        Self::from_props(&Id::unit_id(), &Default::default(), 0, schema_uid)
    }

    pub fn for_schema_field_feature(schema_uid: SchemaUid, field: u64, feature: &Feature) -> Self {
        Self::from_props(&Id::unit_id(), feature, field, schema_uid)
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
            slice: [u8::MAX; KEY_SIZE],
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
    pub fn id(&self) -> Id {
        let mut id_cursor = Cursor::new(&self.slice[KEY_SIZE - ID_SIZE..]);
        let id = Id::from_binary(&mut id_cursor).unwrap(); // read id from tailing 64 bits
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
                self.slice[KEY_SIZE - ID_SIZE..].as_mut_ptr(),
                ID_SIZE,
            );
        }
    }
    pub fn from_id(id: &Id) -> Self {
        let mut key = EntryKey::new();
        key.set_id(id);
        key
    }

    /// Compare two EntryKeys by their prefix (schema + field + feature), ignoring the ID part.
    /// This is useful for range queries where we want to compare by feature value regardless of ID.
    pub fn cmp_prefix(&self, other: &EntryKey) -> cmp::Ordering {
        // Compare schema (4 bytes) + field (4 bytes) + feature (8 bytes) = 16 bytes
        // ID is at the end (16 bytes), so we compare slice[0..16] vs slice[0..16]
        let prefix_len = KEY_SIZE - ID_SIZE;
        self.slice[..prefix_len].cmp(&other.slice[..prefix_len])
    }

    /// Check if this key's prefix (schema + field + feature) is greater than the other's.
    pub fn prefix_gt(&self, other: &EntryKey) -> bool {
        self.cmp_prefix(other) == cmp::Ordering::Greater
    }

    /// Check if this key's prefix (schema + field + feature) is greater than or equal to the other's.
    pub fn prefix_ge(&self, other: &EntryKey) -> bool {
        matches!(
            self.cmp_prefix(other),
            cmp::Ordering::Greater | cmp::Ordering::Equal
        )
    }

    /// Check if this key's prefix (schema + field + feature) is less than the other's.
    pub fn prefix_lt(&self, other: &EntryKey) -> bool {
        self.cmp_prefix(other) == cmp::Ordering::Less
    }

    /// Check if this key's prefix (schema + field + feature) is less than or equal to the other's.
    pub fn prefix_le(&self, other: &EntryKey) -> bool {
        matches!(
            self.cmp_prefix(other),
            cmp::Ordering::Less | cmp::Ordering::Equal
        )
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

impl<'a> IntoIterator for &'a EntryKey {
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

#[cfg(test)]
mod tests {
    use super::*;
    use crate::ram::schema::SchemaUid;

    /// One prefix must cover a schema family's cells in *every* generation.
    ///
    /// This is what keeps the query layer generation-agnostic. A ranged scan
    /// of a schema seeks `for_schema(uid)` and walks while the 4-byte prefix
    /// matches; if that prefix were the generation, the scan would see only
    /// the cells written under whichever generation it happened to name, and
    /// correctness would depend on fanning out across generations and merging.
    /// It would also mean the cleaner could not migrate a cell without
    /// deleting and reinserting every index entry the cell owns.
    #[test]
    fn one_prefix_covers_every_generation_of_a_family() {
        let family = SchemaUid(42);
        let field = 7u64;
        let feature: Feature = [1, 2, 3, 4, 5, 6, 7, 8];

        // Two cells of the same family. Nothing here names a generation --
        // that is the point: the key does not carry one.
        let older = EntryKey::from_props(&Id::from_parts(1, 100), &feature, field, family);
        let newer = EntryKey::from_props(&Id::from_parts(1, 200), &feature, field, family);

        let prefix = EntryKey::for_schema(family);
        assert_eq!(&older.as_slice()[..4], &prefix.as_slice()[..4]);
        assert_eq!(&newer.as_slice()[..4], &prefix.as_slice()[..4]);

        // And a different family must not fall inside that prefix, or a scan
        // would leak another schema's rows into this one's results.
        let stranger =
            EntryKey::from_props(&Id::from_parts(1, 100), &feature, field, SchemaUid(43));
        assert_ne!(&stranger.as_slice()[..4], &prefix.as_slice()[..4]);
    }

    /// The prefix is the family, big-endian, in the first four bytes -- the
    /// same width and position it has always occupied. The ranged client
    /// rebuilds this pattern by hand in `schema_pattern`, so the two encodings
    /// have to agree byte for byte or a schema scan matches nothing.
    #[test]
    fn the_family_is_the_first_four_bytes_big_endian() {
        let key = EntryKey::for_schema(SchemaUid(0x0A0B0C0D));
        assert_eq!(&key.as_slice()[..4], &[0x0A, 0x0B, 0x0C, 0x0D]);
    }
}
