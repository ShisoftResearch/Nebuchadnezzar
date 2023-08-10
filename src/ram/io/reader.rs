use itertools::Itertools;

use crate::ram::io::{align_address, align_ptr_addr};
use crate::ram::schema::{Field, Schema};
use crate::ram::types;
use crate::ram::types::{bool_io, u32_io, SharedMap, SharedValue, Type};

use super::align_address_with_ty;
use super::writer::{ARRAY_TYPE_MASK, NULL_PLACEHOLDER};
use dovahkiin::types::{key_hash, Map, ARRAY_LEN_TYPE};
use std::collections::HashMap;
use std::mem;

fn read_field<'v>(
    base_ptr: usize,
    field: &Field,
    is_var: bool,
    tail_offset: &mut usize,
    force_mono: bool,
) -> SharedValue<'v> {
    let orig_tail_offset = *tail_offset;
    let field_nullable = field.nullable;
    let field_is_array = field.is_array && (!force_mono);
    let field_var_base_ty  = field.data_type.size().is_none();
    let field_is_var = field_var_base_ty || field.is_array;
    let (target_offset, tailing) = match (field.offset, field_is_var, field_nullable, is_var) {
        (Some(schema_field_offset), false, false, false) => {
            trace!("Using schema field offset for {}, offset {}", field.name, schema_field_offset);
            (schema_field_offset, false)
        },
        (Some(schema_field_offset), true, _, false)
        | (Some(schema_field_offset), _, true, false) => {
            // Var or nullable schema field
            let rel_offset = *u32_io::read(base_ptr + schema_field_offset) as usize;
            if rel_offset == 0 {
                return SharedValue::Null;
            } else {
                *tail_offset = rel_offset;
                trace!("Using schema field recorded offset for {}, offset {}", field.name, tail_offset);
                (*tail_offset, true)
            }
        }
        (_, _, false, true) => {
            // In-var fields, not nullable
            *tail_offset = align_address_with_ty(field.data_type, *tail_offset);
            trace!("Using non-nullable aligned tail offset for {}, offset {}", field.name, tail_offset);
            (*tail_offset, true)
        }
        (_, _, true, true) => {
            // In-var fields, nullable
            let is_null = *bool_io::read(base_ptr + *tail_offset) as bool;
            if is_null {
                return SharedValue::Null;
            }
            *tail_offset = align_address_with_ty(field.data_type, *tail_offset + 1);
            trace!("Using nullable aligned tail offset for {}, offset {}", field.name, tail_offset);
            (*tail_offset, true)
        }
        p => unreachable!("Do not accept target offset pattern: {:?}", p),
    };
    let (val, size) = match (field_var_base_ty, field_is_array, is_var, &field.sub_fields) {
        (_, false, _, None) => {
            // Simple typed fields
            let val = types::get_shared_val(field.data_type, base_ptr + target_offset);
            let size = if field_var_base_ty {
                types::get_rsize(field.data_type, &val)
            } else {
                types::size_of_type(field.data_type)
            };
            // Simple typed fields
            let val = types::get_shared_val(field.data_type, base_ptr + target_offset);
            let size = if field_var_base_ty {
                types::get_rsize(field.data_type, &val)
            } else {
                types::size_of_type(field.data_type)
            };
            trace!(
                "Reading schema field {} shared value {:?}, type {:?}, size {}, offset {}, base {}",
                field.name,
                val,
                field.data_type,
                size,
                target_offset,
                base_ptr
            );
            (val, size)
        }
        (false, true, _, None) => {
            // Array of primitives
            let array_ptr = base_ptr + target_offset;
            let array_len = *types::get_shared_val(Type::U32, array_ptr).u32().unwrap();
            let slice_offset =
                align_address_with_ty(field.data_type, target_offset + types::u32_io::type_size());
            let mut slice_ptr = base_ptr + slice_offset;
            let prim_arr = types::get_shared_prim_array_val(
                field.data_type,
                array_len as usize,
                &mut slice_ptr,
            )
            .unwrap_or_else(|| {
                panic!(
                    "type cannot been convert to prim array: {:?}",
                    field.data_type
                )
            });
            let val = SharedValue::PrimArray(prim_arr);
            let size = slice_ptr - array_ptr;
            trace!(
                "Reading schema prim array field {} shared value {:?}, type {:?}, size {}, offset {}, base {}",
                field.name,
                val,
                field.data_type,
                size,
                target_offset,
                base_ptr
            );
            (val, size)
        }
        (true, true, _, _) => {
            // Array of non-primitives (including maps)
            let array_len = *types::get_shared_val(Type::U32, base_ptr + target_offset)
                .u32()
                .unwrap();
            let slice_offset =
                align_address_with_ty(field.data_type, target_offset + types::u32_io::type_size());
            let mut vals = Vec::<SharedValue>::new();
            trace!(
                "Reading array body for {} from offset {}, array len {}, target offset {}, used to have tailing {}", 
                field.name, slice_offset, array_len, target_offset, tail_offset,
            );
            *tail_offset = slice_offset;
            for _ in 0..array_len {
                let nxt_val = read_field(base_ptr, field, true, tail_offset, true);
                vals.push(nxt_val);
            }
            let val = SharedValue::Array(vals);
            trace!(
                "Reading schema non-prim field {} shared value {:?}, type {:?}, size {}, offset {}, base {}",
                field.name,
                val,
                field.data_type,
                0,
                target_offset,
                base_ptr
            );
            (val, 0) // Non-promitive array size will be reflected in `tail_offset`
        }
        (_, false, _, Some(sub_fields)) => {
            // Maps
            let mut map = SharedMap::new();
            trace!(
                "Reading map body for {} from offset {}, used to have tailing {}", 
                field.name, target_offset, tail_offset,
            );
            for sub in sub_fields {
                map.insert_key_id(
                    sub.name_id,
                    read_field(base_ptr, &sub, is_var, tail_offset, false),
                );
            }
            map.fields = sub_fields.iter().map(|sub| &sub.name).cloned().collect();
            let val = SharedValue::Map(map);
            (val, 0) // Map size will be reflected in `tail_offset`
        }
        p => unreachable!("Do not accept schema pattern {:?}", p),
    };
    if tailing {
        *tail_offset += size
    }
    return val;
    // let field_offset = if field.nullable {
    //     if is_var {
    //         // read from tail var part
    //         let val_null = *bool_io::read(base_ptr + *tail_offset);
    //         *tail_offset += 1;
    //         if val_null {
    //             trace!("Skip field {} at {} for it is null. Now tail {}, base {}", field.name, orig_tail_offset, tail_offset, base_ptr);
    //             return SharedValue::Null;
    //         }
    //         *tail_offset = align_address_with_ty(field.data_type, *tail_offset + 1);
    //         trace!("Reading nullable var {} from tail_offset {}", field.name, tail_offset);
    //         tail_offset
    //     } else {
    //         // read the data pointer
    //         let rel_ptr = *u32_io::read(base_ptr + rec_field_offset) as usize;
    //         if rel_ptr == 0 {
    //             trace!("Reading nullable nonvar {} found null at base {}", field.name, base_ptr);
    //             return SharedValue::Null;
    //         } else {
    //             trace!("Reading nullable nonvar {} from rel_ptr {}, tail was {}, base {}", field.name, rel_ptr, tail_offset, base_ptr);
    //             *tail_offset = rel_ptr;
    //             tail_offset
    //         }
    //     }
    // } else if is_var {
    //     // Is inside size variable field, read directly from the address
    //     trace!("Reading var {} from tail_offset {}", field.name, tail_offset);
    //     tail_offset
    // } else if field.is_array || field_is_var {
    //     let rel_ptr = *u32_io::read(base_ptr + rec_field_offset) as usize;
    //     trace!("Reading array or var {} from rel_ptr {}, tail was {}, base {}", field.name, rel_ptr, tail_offset, base_ptr);
    //     *tail_offset = rel_ptr;
    //     tail_offset
    // } else {
    //     trace!("Reading {} from static offset {}, tail {}", field.name, rec_field_offset, tail_offset);
    //     &mut rec_field_offset
    // };
    // trace!("Reading {} at offset {}, tail {}", field.name, field_offset, orig_tail_offset);
    // let res = if field.is_array {
    //     *field_offset = align_address_with_ty(ARRAY_LEN_TYPE, *field_offset);
    //     let len = *u32_io::read(base_ptr + *field_offset);
    //     let mut sub_field = field.clone();
    //     sub_field.is_array = false;
    //     *field_offset += u32_io::type_size();
    //     trace!("Field {} is array, length {}, now at {}", field.name, len, field_offset);
    //     let mut ptr = base_ptr + *field_offset;
    //     if field.sub_fields.is_none() {
    //         // maybe primitive array
    //         *field_offset = align_address_with_ty(field.data_type, *field_offset);
    //         let val = types::get_shared_prim_array_val(field.data_type, len as usize, &mut ptr);
    //         let array_size = ptr - base_ptr;
    //         trace!("Array size of {} is {}", field.name, array_size);
    //         *field_offset += array_size;
    //         if let Some(prim_arr) = val {
    //             trace!("Read prim array {} now at {}, was at {}, value {:?}", field.name, field_offset, orig_tail_offset, prim_arr);
    //             SharedValue::PrimArray(prim_arr)
    //         } else {
    //             panic!(
    //                 "type cannot been convert to prim array: {:?}",
    //                 field.data_type
    //             )
    //         }
    //     } else {
    //         let mut vals = Vec::<SharedValue>::new();
    //         trace!("Reading array of maps for {} with num maps {}", field.name, len);
    //         for _ in 0..len {
    //             let nxt_val = read_field(base_ptr, &sub_field, true, field_offset);
    //             vals.push(nxt_val);
    //         }
    //         SharedValue::Array(vals)
    //     }
    // } else if let Some(ref subs) = field.sub_fields {
    //     trace!("Field {} is map", field.name);
    //     let mut map = SharedMap::new();
    //     for sub in subs {
    //         map.insert_key_id(
    //             sub.name_id,
    //             read_field(base_ptr, &sub, is_var, field_offset),
    //         );
    //     }
    //     map.fields = subs.iter().map(|sub| &sub.name).cloned().collect();
    //     SharedValue::Map(map)
    // } else {
    //     let ty_align = types::align_of_type(field.data_type);
    //     *field_offset = align_address(
    //         ty_align,
    //         types::get_size(field.data_type, *field_offset),
    //     );
    //     let field_ptr = base_ptr + *field_offset;
    //     trace!("Reading field shared value {}, type {:?}, offset {}, base {}", field.name, field.data_type, field_offset, base_ptr);
    //     let val = types::get_shared_val(field.data_type, field_ptr);
    //     debug_assert!(types::fixed_size(field.data_type));
    //     *field_offset += types::size_of_type(field.data_type);
    //     val
    // };
    // trace!("Read {} with value {:?}, new offset {}, tail was {}, base {}", field.name, res, field_offset, orig_tail_offset, base_ptr);
    // return res;
}

pub fn read_attach_dynamic_part<'v>(mut tail_ptr: usize, dest: &mut SharedValue<'v>) {
    tail_ptr = align_ptr_addr(tail_ptr);
    let src = read_dynamic_value(&mut tail_ptr, Type::Map.id());
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

fn read_dynamic_value<'a, 'v>(ptr: &'a mut usize, type_id: u8) -> SharedValue<'v> {
    let is_array = type_id & ARRAY_TYPE_MASK == ARRAY_TYPE_MASK;
    if is_array {
        let base_type = type_id & (!ARRAY_TYPE_MASK);
        *ptr = align_address_with_ty(ARRAY_LEN_TYPE, *ptr);
        let len_val = types::get_shared_val(Type::U32, *ptr);
        let len = len_val.u32().unwrap();
        *ptr += types::u32_io::type_size();
        if type_id == ARRAY_TYPE_MASK {
            // Dynamic typed array
            let mut data_block_ptr = *ptr + *len as usize;
            let arr = (0..*len)
                .map(|i| {
                    types::get_shared_val(Type::U8, *ptr + i as usize)
                        .u8()
                        .unwrap()
                        .to_owned()
                })
                .map(|ty_id| read_dynamic_value(&mut data_block_ptr, ty_id))
                .collect_vec();
            *ptr = data_block_ptr;
            return SharedValue::Array(arr);
        } else {
            // Primitive array
            *ptr = align_address_with_ty(Type::from_id(base_type), *ptr);
            if let Some(prim_arr) =
                types::get_shared_prim_array_val(Type::from_id(base_type), *len as usize, ptr)
            {
                // ptr have been moved by `get_shared_prim_array_val`
                return SharedValue::PrimArray(prim_arr);
            } else {
                panic!("Cannot read prim array for dynamic field");
            }
        }
    } else if type_id == MAP_TYPE_ID {
        // Map
        *ptr = align_address_with_ty(ARRAY_LEN_TYPE, *ptr);
        let len_val = types::get_shared_val(Type::U32, *ptr);
        let len = len_val.u32().unwrap();
        *ptr += types::u32_io::type_size();
        let field_types = (0..*len)
            .map(|_| {
                let ty = types::get_shared_val(Type::U8, *ptr)
                    .u8()
                    .unwrap()
                    .to_owned();
                *ptr += 1;
                ty
            })
            .collect_vec();
        let field_names = (0..*len)
            .map(|_| {
                *ptr = align_address_with_ty(Type::String, *ptr);
                let name = types::get_shared_val(Type::String, *ptr)
                    .string()
                    .unwrap()
                    .to_owned();
                *ptr += types::string_io::size_at(*ptr);
                name
            })
            .collect_vec();
        let fields = field_types
            .iter()
            .map(|type_id| read_dynamic_value(ptr, *type_id));
        let map = field_names
            .iter()
            .map(|n| key_hash(n))
            .zip(fields)
            .collect::<HashMap<_, _>>();
        return SharedValue::Map(SharedMap {
            fields: field_names,
            map,
        });
    } else if type_id == NULL_PLACEHOLDER {
        return SharedValue::Null;
    } else {
        let ty = Type::from_id(type_id);
        *ptr = align_address_with_ty(ty, *ptr);
        let value = types::get_shared_val(ty, *ptr);
        *ptr += types::get_size(ty, *ptr);
        return value;
    }
}

pub fn read_by_schema<'v>(ptr: usize, schema: &Schema) -> SharedValue<'v> {
    let mut tail_offset = schema.static_bound;
    let mut schema_value = read_field(ptr, &schema.fields, false, &mut tail_offset, false);
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
                        let field_data = read_field(ptr, field, false, &mut tail_offset, false);
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
