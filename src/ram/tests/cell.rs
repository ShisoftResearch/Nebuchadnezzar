use crate::ram::cell::*;
use crate::ram::chunk::Chunks;
use crate::ram::schema::*;
use crate::ram::types;
use crate::ram::types::*;

use super::*;

pub const CHUNK_SIZE: usize = 8 * 1024 * 1024;

#[test]
pub fn cell_rw() {
    let fields = default_fields();
    let id1 = Id::allocated(1, 0, 1);
    let id2 = Id::allocated(1, 0, 2);
    let schema = Schema::new_with_id(1, &String::from("dummy"), None, fields, false, false);
    let mut data = data_map_value! {
        id: 100 as i64,
        score: 70 as u64,
        name: String::from("Jack")
    };
    let chunk = &Chunks::new_dummy(1, CHUNK_SIZE).list[0];
    chunk.meta.schemas.debug_only_new_schema(schema.clone());
    let mut cell = OwnedCell {
        header: CellHeader::new(schema.vid, &id1),
        data,
    };
    let write_plan = cell.plan_write(chunk).unwrap();
    let pending_entry = write_plan.allocate(chunk, true).unwrap();
    let write_result = chunk
        .write_cell_to_chunk(&cell, &write_plan, &pending_entry, cell.header.version)
        .unwrap();
    let cell_1_ptr = write_result.addr;
    // Under the head pool a PendingEntry OWNS its head until dropped, and
    // this dummy store has room for exactly one segment -- holding the
    // first entry across the second allocation would force a second head
    // the store cannot provide. Production paths never overlap entries;
    // this test only did so by keeping them in scope for convenience.
    drop(pending_entry);
    {
        let (stored_cell, _) =
            SharedCellData::from_chunk_raw(id1.bits(), cell_1_ptr, &chunk).unwrap();
        assert_eq!(stored_cell.data["id"].i64().unwrap(), &100);
        assert_eq!(stored_cell.data["name"].string().unwrap(), "Jack");
        assert_eq!(stored_cell.data["score"].u64().unwrap(), &70);
    }
    data = data_map_value! {
        id: 2 as i64,
        score: 80 as u64,
        name: "John"
    };
    cell = OwnedCell {
        header: CellHeader::new(schema.vid, &id2),
        data,
    };
    let write_plan = cell.plan_write(chunk).unwrap();
    let pending_entry = write_plan.allocate(chunk, true).unwrap();
    let write_result = chunk
        .write_cell_to_chunk(&cell, &write_plan, &pending_entry, cell.header.version)
        .unwrap();
    let cell_2_ptr = write_result.addr;
    {
        let stored_cell = SharedCellData::from_chunk_raw(id2.bits(), cell_2_ptr, &chunk)
            .unwrap()
            .0;
        assert_eq!(stored_cell.data["id"].i64().unwrap(), &2);
        assert_eq!(stored_cell.data["score"].u64().unwrap(), &80);
        assert_eq!(stored_cell.data["name"].string().unwrap(), "John");
    }
    {
        let stored_cell = SharedCellData::from_chunk_raw(id1.bits(), cell_1_ptr, &chunk)
            .unwrap()
            .0;
        assert_eq!(stored_cell.data["id"].i64().unwrap(), &100);
        assert_eq!(stored_cell.data["name"].string().unwrap(), "Jack");
        assert_eq!(stored_cell.data["score"].u64().unwrap(), &70);
    }
}

#[cfg(feature = "fast_map")]
#[test]
pub fn dynamic() {
    let fields = default_fields();
    let id1 = Id::allocated(1, 0, 1);
    let id2 = Id::allocated(1, 0, 2);
    let schema = Schema::new_with_id(1, &String::from("dummy"), None, fields, true, true);
    let mut data_map = types::OwnedMap::new();
    data_map.insert("id", OwnedValue::I64(100));
    data_map.insert("score", OwnedValue::U64(70));
    data_map.insert("name", OwnedValue::String(String::from("Jack")));
    data_map.insert("year", OwnedValue::U16(2010));
    data_map.insert("major", OwnedValue::String(String::from("CS")));
    let mut data = OwnedValue::Map(data_map);
    let chunk = &Chunks::new_dummy(1, CHUNK_SIZE).list[0];
    chunk.meta.schemas.new_schema(schema.clone());
    let mut cell = OwnedCell {
        header: CellHeader::new(schema.vid, &id1),
        data,
    };
    let mut loc = chunk.write_cell_to_chunk(&mut cell);
    let cell_1_ptr = loc.unwrap().0;
    {
        let stored_cell = SharedCellData::from_chunk_raw(cell_1_ptr, &chunk)
            .unwrap()
            .0;
        assert_eq!(stored_cell.data["id"].i64().unwrap(), &100);
        assert_eq!(stored_cell.data["name"].string().unwrap(), "Jack");
        assert_eq!(stored_cell.data["score"].u64().unwrap(), &70);
        assert_eq!(stored_cell.data["year"].u16().unwrap(), &2010);
        assert_eq!(stored_cell.data["major"].string().unwrap(), "CS");
    }

    data_map = types::OwnedMap::new();
    data_map.insert("id", OwnedValue::I64(2));
    data_map.insert("score", OwnedValue::U64(80));
    data_map.insert("name", OwnedValue::String(String::from("John")));
    data = OwnedValue::Map(data_map);
    cell = OwnedCell {
        header: CellHeader::new(schema.vid, &id2),
        data,
    };
    loc = chunk.write_cell_to_chunk(&mut cell);
    let cell_2_ptr = loc.unwrap().0;
    {
        let stored_cell = SharedCellData::from_chunk_raw(cell_2_ptr, &chunk)
            .unwrap()
            .0;
        assert_eq!(stored_cell.data["id"].i64().unwrap(), &2);
        assert_eq!(stored_cell.data["score"].u64().unwrap(), &80);
        assert_eq!(stored_cell.data["name"].string().unwrap(), "John");
        assert!(stored_cell.data["major"].string().is_none());
    }
}

/// A keyed cell's identity must follow its schema FAMILY, not the generation
/// that happens to encode it.
///
/// This is the contract the whole evolution campaign rests on. If a cell id
/// were derived from the generation, then the first time a schema changed
/// shape every existing keyed cell would be orphaned -- unreachable by the
/// only id a caller can compute -- and every subsequent write would address a
/// different cell than the one already stored. Nothing would report an error;
/// the data would simply stop being findable.
#[test]
pub fn a_keyed_cell_keeps_its_id_when_its_schema_is_evolved() {
    let key_fields = Some(vec![String::from("id")]);
    let mut generation_0 = Schema::new_with_id(
        1,
        &String::from("keyed"),
        key_fields.clone(),
        default_fields(),
        false,
        false,
    );

    // The same family, one evolution later: a new generation, a new vid, and
    // the old one superseded. Exactly what `evolve_schema` will produce.
    let mut generation_1 = Schema::new_with_id(
        1,
        &String::from("keyed"),
        key_fields,
        default_fields(),
        false,
        false,
    );
    generation_1.vid = SchemaVid(9001);
    generation_1.generation = 1;
    generation_0.status = SchemaVersionStatus::Stale {
        superseded_by: generation_1.vid,
    };

    assert_eq!(
        generation_0.uid, generation_1.uid,
        "an evolution keeps the family it belongs to"
    );
    assert_ne!(
        generation_0.vid, generation_1.vid,
        "an evolution is a new generation"
    );

    let data = data_map_value! {
        id: 100 as i64,
        score: 70 as u64,
        name: String::from("Jack")
    };

    let before = OwnedCell::new(&generation_0, data.clone()).id();
    let after = OwnedCell::new(&generation_1, data).id();
    assert_eq!(
        before, after,
        "the same key in the same family must name the same cell across an evolution"
    );
}
