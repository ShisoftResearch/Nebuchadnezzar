use itertools::Itertools;

use crate::ram::schema::{Field, Schema};
use crate::ram::types;
use crate::ram::types::{type_id_of, u32_io, u8_io, SharedMap, Type, SharedValue};

use super::writer::{ARRAY_TYPE_MASK, NULL_PLACEHOLDER};
use std::collections::HashMap;
use dovahkiin::types::key_hash;

fn read_field(ptr: usize, field: &Field, selected: Option<&[u64]>) -> (SharedValue, usize) {
    let mut ptr = ptr;
    if field.nullable {
        let null_byte = *u8_io::read(ptr);
        ptr += 1;
        if null_byte == 1 {
            return (SharedValue::Null, ptr);
        }
    }
    if field.is_array {
        let len = *u32_io::read(ptr);
        trace!("Got array length {}", len);
        let mut sub_field = field.clone();
        sub_field.is_array = false;
        ptr += u32_io::size(ptr);
        if field.sub_fields.is_none() {
            // maybe primitive array
            let mut ptr = ptr;
            let val = types::get_shared_prim_array_val(field.type_id, len as usize, &mut ptr);
            if let Some(prim_arr) = val {
                return (SharedValue::PrimArray(prim_arr), ptr);
            } else {
                panic!("type cannot been convert to prim array: {}", field.type_id)
            }
        } else {
            let mut vals = Vec::<SharedValue>::new();
            for _ in 0..len {
                let (nxt_val, nxt_ptr) = read_field(ptr, &sub_field, None);
                ptr = nxt_ptr;
                vals.push(nxt_val);
            }
            (SharedValue::Array(vals), ptr)
        }
    } else if let Some(ref subs) = field.sub_fields {
        let mut map = SharedMap::new();
        let mut selected_pos = 0;
        for sub in subs {
            let (cval, cptr) = read_field(ptr, &sub, selected);
            map.insert_key_id(sub.name_id, cval);
            ptr = cptr;
            match selected {
                None => {}
                Some(field_ids) => {
                    if field_ids[selected_pos] == sub.name_id {
                        selected_pos += 1;
                        if field_ids.len() <= selected_pos {
                            return (SharedValue::Map(map), ptr);
                        }
                    }
                }
            }
        }
        map.fields = subs.iter().map(|sub| &sub.name).cloned().collect();
        (SharedValue::Map(map), ptr)
    } else {
        (
            types::get_shared_val(field.type_id, ptr),
            ptr + types::get_size(field.type_id, ptr),
        )
    }
}

pub fn read_attach_dynamic_part(mut tail_ptr: usize, dest: &mut SharedValue) {
    let src = read_dynamic_value(&mut tail_ptr);
    if let &mut SharedValue::Map(ref mut map_dest) = dest {
        if let SharedValue::Map(mut map_src) = src {
            map_dest.fields.append(&mut map_src.fields);
            for (k, v) in map_src.map.into_iter() {
                map_dest.insert_key_id(k, v);
            }
        }
    }
}

fn read_dynamic_value(ptr: &mut usize) -> SharedValue {
    let type_id = types::get_shared_val(type_id_of(Type::U32), *ptr).u32().unwrap();
    let is_array = type_id & ARRAY_TYPE_MASK == ARRAY_TYPE_MASK;
    *ptr += types::u32_io::size(*ptr);
    if is_array {
        let base_type = type_id & (!ARRAY_TYPE_MASK);
        let len = types::get_shared_val(type_id_of(Type::U32), *ptr).u32().unwrap();
        *ptr += types::u32_io::size(*ptr);
        if base_type != 0 {
            // Primitive array
            if let Some(prim_arr) = types::get_shared_prim_array_val(base_type, *len as usize, ptr) {
                // ptr have been moved by `get_shared_prim_array_val`
                return SharedValue::PrimArray(prim_arr);
            } else {
                panic!("Cannot read prim array for dynamic field");
            }
        } else {
            let array = (0..*len).map(|_| read_dynamic_value(ptr)).collect();
            // ptr have been moved by recursion
            return SharedValue::Array(array);
        }
    } else if *type_id == 0{
        // Map
        let len = types::get_shared_val(type_id_of(Type::U32), *ptr).u32().unwrap();
        *ptr += types::u32_io::size(*ptr);
        let field_value_pair = (0..*len).map(|_| {
            let name = types::get_shared_val(type_id_of(Type::String), *ptr).string().unwrap();
            *ptr += types::string_io::size(*ptr);
            let value = read_dynamic_value(ptr);
            (name, value)
        })
        .collect_vec();
        let mut fields = Vec::with_capacity(field_value_pair.len());
        let mut map = HashMap::with_capacity(field_value_pair.len());
        for (name, value) in field_value_pair {
            let id = key_hash(&name);
            fields.push(name.to_owned());
            map.insert(id, value);
        }
        return SharedValue::Map(SharedMap { fields, map});
    } else if *type_id == NULL_PLACEHOLDER {
        return SharedValue::Null;
    } else {
        let value = types::get_shared_val(*type_id, *ptr);
        *ptr += types::get_size(*type_id, *ptr);
        return value;
    }
}

pub fn read_by_schema(ptr: usize, schema: &Schema) -> SharedValue {
    let (mut schema_value, tail_ptr) = read_field(ptr, &schema.fields, None);
    if schema.is_dynamic {
        read_attach_dynamic_part(tail_ptr, &mut schema_value)
    }
    schema_value
}

pub fn read_by_schema_selected(ptr: usize, schema: &Schema, fields: &[u64]) -> SharedValue {
    read_field(ptr, &schema.fields, Some(fields)).0
}
