use itertools::Itertools;
use ram::cell::Cell;
use ram::schema::{Field, Schema};
use ram::types::*;

const LSM_TREE_SCHEMA: &'static str = "LSM_TREE_SCHEMA";

lazy_static! {
    pub static ref LSM_TREE_SCHEMA_ID: u32 = key_hash(LSM_TREE_SCHEMA) as u32;
}

pub fn hash_index_schema() -> Schema {
    Schema {
        id: *LSM_TREE_SCHEMA_ID,
        name: LSM_TREE_SCHEMA.to_string(),
        key_field: None,
        str_key_field: None,
        fields: Field::new("*", type_id_of(Type::Id), false, true, None, vec![]),
        is_dynamic: false,
    }
}

pub fn level_trees_from_cell(cell: Cell) -> Vec<Id> {
    if let Value::PrimArray(PrimitiveArray::Id(ids)) = cell.data {
        ids
    } else {
        unreachable!()
    }
}
