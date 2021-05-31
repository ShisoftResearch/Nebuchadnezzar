use itertools::Itertools;

use crate::ram::schema::{Field, Schema};
use crate::ram::types;
use crate::ram::types::{bool_io, u32_io, SharedMap, SharedValue, Type};

use super::writer::{ARRAY_TYPE_MASK, NULL_PLACEHOLDER};
use dovahkiin::types::key_hash;
use std::collections::HashMap;

fn read_field(
    base_ptr: usize,
    field: &Field,
    selected: Option<&[u64]>,
    is_var: bool,
    tail_offset: &mut usize,
) -> SharedValue {
    let mut rec_field_offset = field.offset.unwrap_or(0);
    let field_offset = if is_var {
        // Is inside size variable field, read directly from the address
        tail_offset
    } else if !field.is_var() {
        // Is not inside var and field not var, read from the offset in field
        &mut rec_field_offset
    } else {
        // Is not inside var and field is var, read the pointer and direct to it
        *tail_offset = *u32_io::read(base_ptr + field.offset.unwrap()) as usize;
        tail_offset
    };
    if field.nullable {
        let null_byte = *bool_io::read(base_ptr + *field_offset);
        *field_offset += 1;
        if null_byte {
            return SharedValue::Null;
        }
    }
    if field.is_array {
        let len = *u32_io::read(base_ptr + *field_offset);
        trace!("Got array length {}", len);
        let mut sub_field = field.clone();
        sub_field.is_array = false;
        *field_offset += u32_io::type_size();
        if field.sub_fields.is_none() {
            // maybe primitive array
            let mut ptr = base_ptr + *field_offset;
            let val = types::get_shared_prim_array_val(field.data_type, len as usize, &mut ptr);
            *field_offset = ptr - base_ptr;
            if let Some(prim_arr) = val {
                SharedValue::PrimArray(prim_arr)
            } else {
                panic!(
                    "type cannot been convert to prim array: {:?}",
                    field.data_type
                )
            }
        } else {
            let mut vals = Vec::<SharedValue>::new();
            for _ in 0..len {
                let nxt_val = read_field(base_ptr, &sub_field, None, true, field_offset);
                vals.push(nxt_val);
            }
            SharedValue::Array(vals)
        }
    } else if let Some(ref subs) = field.sub_fields {
        let mut map = SharedMap::new();
        let mut read_sub = |sub: &Field| {
            map.insert_key_id(sub.name_id, read_field(base_ptr, &sub, selected, is_var, field_offset));
        };
        if let Some(field_ids) = selected {
            for sub in subs {
                let mut selected_pos = 0;
                if field_ids[selected_pos] == sub.name_id {
                    read_sub(sub);
                    selected_pos += 1;
                    if field_ids.len() <= selected_pos {
                        return SharedValue::Map(map);
                    }
                }
            }
        } else {
            for sub in subs {
                read_sub(sub);
            }
        }
        map.fields = subs.iter().map(|sub| &sub.name).cloned().collect();
        SharedValue::Map(map)
    } else {
        let field_ptr = base_ptr + *field_offset;
        *field_offset += types::get_size(field.data_type, field_ptr);
        types::get_shared_val(field.data_type, field_ptr)
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

const MAP_TYPE_ID: u8 = Type::Map.id();

fn read_dynamic_value(ptr: &mut usize) -> SharedValue {
    let type_id = types::get_shared_val(Type::U8, *ptr).u8().unwrap();
    let is_array = type_id & ARRAY_TYPE_MASK == ARRAY_TYPE_MASK;
    *ptr += types::u8_io::type_size();
    if is_array {
        let base_type = type_id & (!ARRAY_TYPE_MASK);
        let len = types::get_shared_val(Type::U32, *ptr).u32().unwrap();
        *ptr += types::u32_io::type_size();
        if base_type != MAP_TYPE_ID {
            // Primitive array
            if let Some(prim_arr) =
                types::get_shared_prim_array_val(Type::from_id(base_type), *len as usize, ptr)
            {
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
    } else if *type_id == MAP_TYPE_ID {
        // Map
        let len = types::get_shared_val(Type::U32, *ptr).u32().unwrap();
        *ptr += types::u32_io::type_size();
        let field_value_pair = (0..*len)
            .map(|_| {
                let name = types::get_shared_val(Type::String, *ptr).string().unwrap();
                *ptr += types::string_io::size_at(*ptr);
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
        return SharedValue::Map(SharedMap { fields, map });
    } else if *type_id == NULL_PLACEHOLDER {
        return SharedValue::Null;
    } else {
        let ty = Type::from_id(*type_id);
        let value = types::get_shared_val(ty, *ptr);
        *ptr += types::get_size(ty, *ptr);
        return value;
    }
}

pub fn read_by_schema(ptr: usize, schema: &Schema) -> SharedValue {
    let mut tail_offset = schema.static_bound;
    let mut schema_value = read_field(ptr, &schema.fields, None, false, &mut tail_offset);
    if schema.is_dynamic {
        read_attach_dynamic_part(ptr + tail_offset, &mut schema_value)
    }
    schema_value
}

pub fn read_by_schema_selected(ptr: usize, schema: &Schema, fields: &[u64]) -> SharedValue {
    let mut tail_offset = schema.static_bound;
    read_field(ptr, &schema.fields, Some(fields), false, &mut tail_offset)
}
