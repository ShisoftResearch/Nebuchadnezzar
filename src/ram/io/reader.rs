use ram::schema::{Schema, Field};
use ram::cell::*;
use ram::types;
use ram::types::{u16_io, u8_io, Map, Value};

fn read_field(ptr: usize, field: &Field) -> (DataValue, usize) {
    let mut ptr = ptr;
    if field.is_array {
        let len = u16_io::read(ptr);
        let mut sub_field = field.clone();
        sub_field.is_array = false;
        ptr += u16_io::size(ptr);
        let mut val = Vec::<DataValue>::new();
        for i in 0..len {
            let (nxt_val, nxt_ptr) = read_field(ptr, &sub_field);
            ptr = nxt_ptr;
            val.push(nxt_val);
        }
        (Value::Array(val), ptr)
    } else {
        if field.nullable {
            let null_byte = u8_io::read(ptr);
            ptr += 1;
            if null_byte == 1 {
                return (Value::Null, ptr);
            }
        }
        if let Some(ref subs) = field.sub {
            let mut map = DataMap::new();
            for sub in subs {
                let field_name = sub.name.clone();
                let (cval, cptr) = read_field(ptr, &sub);
                map.insert(field_name, cval);
                ptr = cptr;
            }
            (Value::Map(map), ptr)
        } else {
            (types::get_val(field.type_id, ptr), ptr + types::get_size(field.type_id, ptr))
        }
    }

}

pub fn read_by_schema(ptr: usize, schema: Schema) -> DataValue {
    let (val, _) = read_field(ptr, &schema.fields);
    val
}