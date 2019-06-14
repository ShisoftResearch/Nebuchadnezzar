use index::EntryKey;
use ram::cell::Cell;
use ram::schema::{Field, IndexType, Schema};
use ram::types::{Id, Value};
use smallvec::SmallVec;

// cascading field hashes
type IndexId = SmallVec<[u64; 5]>;

// Define index rules
// Index can be applied on scala value and scala arrays for both Ranged and Hashed
// Only scala can be use vectorization index
// String for ranged will take first 64-bit, hash will hash the string
// Index on nested fields are allowed

pub struct RangedIndexMeta {
    old_key: EntryKey,
}

pub struct HashedIndexMeta {
    old_id: Id,
}

pub struct VectorizedIndexMeta {
    old_cell_id: Id,
    old_index: u32,
}

pub enum IndexMeta {
    Ranged(RangedIndexMeta),
    Hashed(HashedIndexMeta),
    Vectorization(VectorizedIndexMeta),
}

pub struct IndexRes {
    id: IndexId,
    meta: Vec<IndexMeta>,
}

pub fn make_indices(cell: &Cell, schema: &Schema) {}

pub fn remove_indies(indices: Vec<IndexMeta>) {}

pub fn ensure_indices(cell: &Cell, schema: &Schema, old_indices: Vec<IndexRes>) {}

pub fn probe_cell_indices(cell: &Cell, schema: &Schema) -> Vec<IndexRes> {
    let mut res = vec![];
    probe_field_indices(&smallvec!(), &schema.fields, &cell.data, &mut res);
    res
}

fn probe_field_indices(prefix: &IndexId, field: &Field, value: &Value, metas: &mut Vec<IndexRes>) {
    let mut id = prefix.clone();
    id.push(field.name_id);
    if let &Some(ref fields) = &field.sub_fields {
        for field in fields {
            let value = &value[field.name_id];
            probe_field_indices(&id, field, value, metas);
        }
    } else {
        let mut components = vec![];
        if let &Value::Array(ref array) = value {
            for val in array {
                probe_field_indices(&id, field, value, metas);
            }
        } else if let &Value::PrimArray(ref array) = value {
            for index in &field.indices {
                match index {
                    &IndexType::Ranged => components.append(&mut array.features()),
                    &IndexType::Hashed => components.append(&mut array.hashes()),
                    &IndexType::Vectorized => components.append(&mut array.features()),
                }
            }
        } else {
            for index in &field.indices {
                match index {
                    &IndexType::Ranged => components.push(value.feature()),
                    &IndexType::Hashed => components.push(value.hash()),
                    &IndexType::Vectorized => components.push(value.feature()),
                }
            }
        }
    }
}
