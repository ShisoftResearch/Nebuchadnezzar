use ram::cell::Cell;
use ram::schema::Schema;
use index::EntryKey;
use ram::types::Id;

pub struct RangedIndexMeta {
    old_key: EntryKey
}

pub struct HashedIndexMeta {
    old_id: Id
}

pub struct VectorizedIndexMeta {
    cell_id: Id,
    index: u32
}

pub enum IndexMeta {
    Ranged(RangedIndexMeta),
    Hashed(HashedIndexMeta),
    Vectorization(VectorizedIndexMeta),
}

pub fn make_index(cell: &Cell, schema: &Schema, update: bool) {

}