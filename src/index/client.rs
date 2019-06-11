use ram::cell::Cell;
use ram::schema::Schema;

pub enum IndexMeta {
    Ranged(),
    ByValue,
    Vectorization,
}

pub fn make_index(cell: &Cell, schema: &Schema, update: bool) {

}