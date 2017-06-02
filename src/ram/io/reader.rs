use ram::schema::{Schema, Field};
use ram::cell::*;
use ram::types;
use ram::types::{u16_io, u8_io, Value};

fn read_field(ptr: usize, field: &Field) -> (DataValue, usize) {
    let mut ptr = ptr;
    if field.nullable {
        let null_byte = u8_io::read(ptr);
        ptr += 1;
        if null_byte == 1 {
            return (Value::Null, ptr);
        }
    }
    if field.is_array {
        let len = u16_io::read(ptr);
        let mut sub_field = field.clone();
        sub_field.is_array = false;
        ptr += u16_io::size(ptr);
        let mut vals = Vec::<DataValue>::new();
        for _ in 0..len {
            let (nxt_val, nxt_ptr) = read_field(ptr, &sub_field);
            ptr = nxt_ptr;
            vals.push(nxt_val);
        }
        (Value::Array(vals), ptr)
    } else if let Some(ref subs) = field.sub_fields {
        let mut map = DataMap::new();
        for sub in subs {
            let (cval, cptr) = read_field(ptr, &sub);
            map.insert_key_id(sub.name_id, cval);
            ptr = cptr;
        }
        map.fields = subs.iter().map(|sub| &sub.name).cloned().collect();
        (Value::Map(map), ptr)
    } else {
        (types::get_val(field.type_id, ptr), ptr + types::get_size(field.type_id, ptr))
    }
}

pub fn read_by_schema(ptr: usize, schema: &Schema) -> DataValue {
    let (val, _) = read_field(ptr, &schema.fields);
    val
}