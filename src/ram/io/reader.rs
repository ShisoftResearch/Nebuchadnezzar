use itertools::Itertools;

use crate::ram::schema::{Field, Schema};
use crate::ram::types;
use crate::ram::types::{bool_io, u32_io, SharedMap, SharedValue, Type};

use super::writer::{ARRAY_TYPE_MASK, NULL_PLACEHOLDER};
use dovahkiin::types::{key_hash, Map};
use std::collections::HashMap;
use std::mem;

fn read_field<'v>(
    base_ptr: usize,
    field: &Field,
    is_var: bool,
    tail_offset: &mut usize,
) -> SharedValue<'v> {
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
    trace!("Reading {} at offset {}", field.name, field_offset);
    if field.nullable {
        let null_byte = *bool_io::read(base_ptr + *field_offset);
        *field_offset += 1;
        if null_byte {
            return SharedValue::Null;
        }
    }
    if field.is_array {
        let len = *u32_io::read(base_ptr + *field_offset);
        trace!("Field {} is array, length {}", field.name, len);
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
                let nxt_val = read_field(base_ptr, &sub_field, true, field_offset);
                vals.push(nxt_val);
            }
            SharedValue::Array(vals)
        }
    } else if let Some(ref subs) = field.sub_fields {
        trace!("Field {} is map", field.name);
        let mut map = SharedMap::new();
        for sub in subs {
            map.insert_key_id(
                sub.name_id,
                read_field(base_ptr, &sub, is_var, field_offset),
            );
        }
        map.fields = subs.iter().map(|sub| &sub.name).cloned().collect();
        SharedValue::Map(map)
    } else {
        let field_ptr = base_ptr + *field_offset;
        *field_offset += types::get_size(field.data_type, field_ptr);
        let val = types::get_shared_val(field.data_type, field_ptr);
        trace!("Field {} is value: {:?}", field.name, val);
        val
    }
}

pub fn read_attach_dynamic_part<'v>(mut tail_ptr: usize, dest: &mut SharedValue<'v>) {
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

fn read_dynamic_value<'a, 'v>(ptr: &'a mut usize) -> SharedValue<'v> {
    let type_id_val = types::get_shared_val(Type::U8, *ptr);
    let type_id = type_id_val.u8().unwrap();
    let is_array = type_id & ARRAY_TYPE_MASK == ARRAY_TYPE_MASK;
    *ptr += types::u8_io::type_size();
    if is_array {
        let base_type = type_id & (!ARRAY_TYPE_MASK);
        let len_val = types::get_shared_val(Type::U32, *ptr);
        let len = len_val.u32().unwrap();
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
        let len_val = types::get_shared_val(Type::U32, *ptr);
        let len = len_val.u32().unwrap();
        *ptr += types::u32_io::type_size();
        let field_value_pair = (0..*len)
            .map(|_| {
                let name = types::get_shared_val(Type::String, *ptr)
                    .string()
                    .unwrap()
                    .to_owned();
                *ptr += types::string_io::size_at(*ptr);
                let value = read_dynamic_value(ptr);
                (name, value)
            })
            .collect_vec();
        let mut fields = Vec::with_capacity(field_value_pair.len());
        let mut map = HashMap::with_capacity(field_value_pair.len());
        for (name, value) in field_value_pair {
            let id = key_hash(&name);
            fields.push(name);
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

pub fn read_by_schema<'v>(ptr: usize, schema: &Schema) -> SharedValue<'v> {
    let mut tail_offset = schema.static_bound;
    let mut schema_value = read_field(ptr, &schema.fields, false, &mut tail_offset);
    if schema.is_dynamic {
        read_attach_dynamic_part(ptr + tail_offset, &mut schema_value)
    }
    schema_value
}

pub fn read_by_schema_selected<'v>(ptr: usize, schema: &Schema, fields: &[u64]) -> SharedValue<'v> {
    let mut tail_offset = schema.static_bound;
    if fields.is_empty() {
        return read_by_schema(ptr, schema);
    }
    if let Some(schema_fields) = &schema.fields.sub_fields {
        let mut res = Vec::with_capacity(fields.len()); // SharedMap::new();
                                                        // iterate all selected field ids
        for field in fields {
            // Get cached index path from ids
            if let Some(index_path) = schema.field_index.get(field) {
                // Make sure the index path is valid
                if !index_path.is_empty() {
                    // Get the first level field from the index path
                    let num_levels = index_path.len();
                    if let Some(mut field) = schema_fields.get(index_path[0]) {
                        // Iterate fields in each level, construct the result map and reach the selected field and value
                        // The map is initialized as res and woulf be updated when going to the next level
                        // ;;; let mut inserting_map = &mut res;
                        // Skip the first level field since we already have the first level
                        for (l, fid) in index_path.iter().enumerate().skip(1) {
                            if let Some(Some(sub_field)) =
                                field.sub_fields.as_ref().map(|sub| sub.get(*fid))
                            {
                                // Go to next level field and get the previous level field
                                let _prev_field = mem::replace(&mut field, sub_field);
                                // let prev_id = prev_field.name_id;
                                // let next_level_value = inserting_map
                                //     .map
                                //     .entry(prev_id)
                                //     .or_insert(SharedValue::Map(SharedMap::new()));
                                // inserting_map.fields.push(prev_field.name.clone());
                                // inserting_map = match next_level_value {
                                //     SharedValue::Map(m) => m,
                                //     _ => unreachable!("Got unexpected value instead of map, got {:?}, at level {} of {}", next_level_value, l, num_levels),
                                // };
                            } else {
                                // Reached the last level of fields
                                break;
                            }
                        }
                        // Insert the last level of field to the map
                        let field_data = read_field(ptr, field, false, &mut tail_offset);
                        // inserting_map.map.insert(field.name_id, field_data);
                        // inserting_map.fields.push(field.name.clone());
                        res.push(field_data);
                    }
                }
            }
        }
        return SharedValue::Array(res);
    }
    SharedValue::Null
}
