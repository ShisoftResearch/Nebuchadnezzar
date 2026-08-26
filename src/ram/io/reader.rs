use itertools::Itertools;

use crate::ram::io::align_ptr_addr;
use crate::ram::schema::{Field, FieldCompression, Schema};
use crate::ram::types;
use crate::ram::types::{bool_io, u32_io, SharedMap, SharedValue, Type};

use super::align_address_with_ty;
use super::writer::{ARRAY_TYPE_MASK, NULL_PLACEHOLDER};
use dovahkiin::types::{key_hash, Map, ARRAY_LEN_TYPE};
use std::mem;

/// Reads a field from memory at the given base pointer according to the field's schema.
///
/// # Arguments
/// * `base_ptr` - Base memory address to read from
/// * `field` - Schema field definition containing type and layout information
/// * `is_var` - Whether this field is part of a variable-sized region
/// * `tail_offset` - Current offset into variable-sized region, updated as fields are read
/// * `force_mono` - If true, treats array fields as single values
///
/// # Returns
/// A SharedValue containing the field's data read from memory
fn read_field<'v>(
    base_ptr: usize,
    field: &Field,
    is_var: bool,
    tail_offset: &mut usize,
    force_mono: bool,
) -> SharedValue<'v> {
    // Determine key properties of the field that affect how we read it
    let field_nullable = field.nullable; // Can the field be null?
    let field_is_array_or_vec = (field.is_array || field.vector_size.is_some()) && (!force_mono); // Is it an array/vector and not forced to be treated as single value?
    let field_var_base_ty = field.data_type.size().is_none() && field.sub_fields.is_none(); // Does the field have variable size but no sub-fields?
    let field_is_var = field_var_base_ty || field.is_array || field.vector_size.is_some(); // Is the field variable-sized in any way?

    // Calculate where to read from and whether we need to update tail_offset after reading
    // This complex match handles all combinations of:
    // - Schema-defined vs variable region offsets
    // - Nullable vs non-nullable fields
    // - Variable vs fixed-size fields
    let (target_offset, tailing) = match (field.offset, field_is_var, field_nullable, is_var) {
        // Case 1: Fixed-size, non-nullable field in schema region - use direct offset
        (Some(schema_field_offset), false, false, false) => {
            trace!(
                "Using schema field offset for {}, offset {}",
                field.name,
                schema_field_offset
            );
            (schema_field_offset, false)
        }
        // Case 2: Variable-sized or nullable field in schema region - read indirect offset
        (Some(schema_field_offset), true, _, false)
        | (Some(schema_field_offset), _, true, false) => {
            // Read the offset value stored at the schema offset
            let rel_offset = *u32_io::read(base_ptr + schema_field_offset) as usize;
            if rel_offset == 0 {
                // Zero offset indicates null for nullable fields
                trace!(
                    "Returing Null for nullable schema field {}, type {:?}, offset {}",
                    field.name,
                    field.data_type,
                    schema_field_offset
                );
                return SharedValue::Null;
            } else {
                *tail_offset = rel_offset; // Update tail_offset to the indirect location
                trace!(
                    "Using schema field recorded offset for {}, offset {}",
                    field.name,
                    tail_offset
                );
                (*tail_offset, true)
            }
        }
        // Case 3: Non-nullable field in variable region - align and use tail_offset
        (_, _, false, true) => {
            *tail_offset = align_address_with_ty(field.data_type, *tail_offset); // Ensure proper alignment
            trace!(
                "Using non-nullable aligned tail offset for {}, offset {}",
                field.name,
                tail_offset
            );
            (*tail_offset, true)
        }
        // Case 4: Nullable field in variable region - check null flag then align
        (_, _, true, true) => {
            let is_null = *bool_io::read(base_ptr + *tail_offset) as bool; // Read null flag
            if is_null {
                trace!(
                    "Returing Null for in-var nullable schema field {}, type {:?}, offset {}",
                    field.name,
                    field.data_type,
                    tail_offset
                );
                // Consume the flag byte before returning. `plan_write_field`
                // emits it for null and non-null alike and advances past it in
                // both cases, so returning without advancing leaves the cursor
                // parked on the flag and shifts every later field in this
                // variable region back by one byte -- silently, since nothing
                // downstream can tell a desynced read from a valid one. It
                // surfaces as the *second* element of an array of maps reading
                // garbage when the element schema has a nullable field.
                *tail_offset += 1;
                return SharedValue::Null;
            }
            // Skip null flag and align for data
            *tail_offset = align_address_with_ty(field.data_type, *tail_offset + 1);
            trace!(
                "Using nullable aligned tail offset for {}, offset {}",
                field.name,
                tail_offset
            );
            (*tail_offset, true)
        }
        p => unreachable!("Do not accept target offset pattern: {:?}", p),
    };

    // Read the actual field value based on its type and structure
    let (val, size) = match (
        field_var_base_ty,
        field_is_array_or_vec,
        is_var,
        &field.sub_fields,
    ) {
        // Case 1: Simple typed fields (primitives) - direct read
        (_, false, _, None) => {
            let compressed = matches!(field.compression, Some(FieldCompression::Lz4))
                && (field.data_type == Type::String || field.data_type == Type::Bytes);
            let read_type = if compressed {
                Type::Bytes
            } else {
                field.data_type
            };
            let val = types::get_shared_val(read_type, base_ptr + target_offset);
            let size = if field_var_base_ty {
                types::get_rsize(read_type, &val) // Calculate size for variable-sized types
            } else {
                types::size_of_type(read_type) // Use fixed size for fixed types
            };
            trace!(
                "Reading schema field {} shared value {:?}, type {:?}, size {}, offset {}, base {}",
                field.name,
                val,
                read_type,
                size,
                target_offset,
                base_ptr
            );
            (val, size)
        }
        // Case 2: Array of primitives - read length then contiguous data block
        (_, true, _, None) => {
            let array_ptr = base_ptr + target_offset;
            // If vector_size is set, use it as the array length, otherwise read from memory
            let (array_len, raw_slice_offset) = field
                .vector_size
                .map(|v| (v as u32, target_offset))
                .unwrap_or_else(|| {
                    let array_len = *types::get_shared_val(Type::U32, base_ptr + target_offset)
                        .u32()
                        .unwrap();
                    let raw_slice_offset = target_offset + types::u32_io::type_size();
                    (array_len, raw_slice_offset)
                });
            let slice_offset = align_address_with_ty(field.data_type, raw_slice_offset);
            let mut slice_ptr = base_ptr + slice_offset;
            // Read the entire primitive array at once
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
            let size = slice_ptr - array_ptr; // Calculate total size including length and data
            trace!(
                "Reading schema prim array field {} shared value {:?}, type {:?}, size {}, offset {}, base {}",
                field.name, val, field.data_type, size, target_offset, base_ptr
            );
            (val, size)
        }
        // Case 3: Array of non-primitives - read length then each element recursively
        (_, true, _, _) => {
            let (array_len, raw_slice_offset) = field
                .vector_size
                .map(|v| (v as u32, target_offset))
                .unwrap_or_else(|| {
                    let array_len = *types::get_shared_val(Type::U32, base_ptr + target_offset)
                        .u32()
                        .unwrap();
                    let raw_slice_offset = target_offset + types::u32_io::type_size();
                    (array_len, raw_slice_offset)
                });
            let slice_offset = align_address_with_ty(field.data_type, raw_slice_offset);
            let mut vals = Vec::<SharedValue>::new();
            trace!(
                "Reading array body for {} from offset {}, array len {}, target offset {}, used to have tailing {}", 
                field.name, slice_offset, array_len, target_offset, tail_offset,
            );
            *tail_offset = slice_offset;
            // Recursively read each array element
            for _ in 0..array_len {
                let nxt_val = read_field(base_ptr, field, true, tail_offset, true);
                vals.push(nxt_val);
            }
            let val = SharedValue::Array(vals);
            trace!(
                "Reading schema non-prim field {} shared value {:?}, type {:?}, size {}, offset {}, base {}",
                field.name, val, field.data_type, 0, target_offset, base_ptr
            );
            (val, 0) // Size is tracked through tail_offset for non-primitive arrays
        }
        // Case 4: Maps - read each sub-field recursively
        (_, false, _, Some(sub_fields)) => {
            let mut map = SharedMap::new();
            trace!(
                "Reading map body for {} from offset {}, used to have tailing {}",
                field.name,
                target_offset,
                tail_offset,
            );
            // Read each sub-field recursively
            for sub in sub_fields {
                map.insert_key_id(
                    sub.name_id,
                    read_field(base_ptr, &sub, is_var, tail_offset, false),
                );
            }
            // Store field names for later use
            // Or maybe do not store field names for performance
            // map.fields = sub_fields.iter().map(|sub| &sub.name).cloned().collect();
            let val = SharedValue::Map(map);
            (val, 0) // Size is tracked through tail_offset for maps
        }
    };

    // Update tail_offset if we're reading from variable region
    if tailing {
        *tail_offset += size
    }
    return val;
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
            let base_ty = Type::from_id(base_type);
            if base_ty == Type::NA {
                panic!(
                    "Invalid base_type {} encountered while reading primitive array at ptr {:#x}",
                    base_type, *ptr
                );
            }
            *ptr = align_address_with_ty(base_ty, *ptr);
            if let Some(prim_arr) = types::get_shared_prim_array_val(base_ty, *len as usize, ptr) {
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
            .collect();
        return SharedValue::Map(SharedMap {
            fields: field_names,
            map,
        });
    } else if type_id == NULL_PLACEHOLDER || type_id == Type::Null.id() {
        // Some older/foreign writers used the raw null type id (0) instead of the placeholder
        // value; treat both as null instead of panicking on an unknown type id.
        return SharedValue::Null;
    } else {
        let ty = Type::from_id(type_id);
        if ty == Type::NA {
            panic!(
                "Invalid type_id {} encountered while reading dynamic value at ptr {:#x}",
                type_id, *ptr
            );
        }
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
                    if let Some(mut field) = schema_fields.get(index_path[0]) {
                        // Iterate fields in each level, construct the result map and reach the selected field and value
                        // The map is initialized as res and woulf be updated when going to the next level
                        // ;;; let mut inserting_map = &mut res;
                        // Skip the first level field since we already have the first level
                        for (_l, fid) in index_path.iter().enumerate().skip(1) {
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
        if res.len() == fields.len() {
            return SharedValue::Array(res);
        }
    }
    // Fallback: read the full value and pull out requested keys (covers dynamic-only fields)
    let full = read_by_schema(ptr, schema);
    if let SharedValue::Map(ref map) = full {
        let selected = fields
            .iter()
            .map(|fid| map.get_by_key_id(*fid).clone())
            .collect();
        return SharedValue::Array(selected);
    }
    full
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn read_dynamic_value_accepts_raw_null_type_id() {
        let mut ptr = 0usize;
        let val = read_dynamic_value(&mut ptr, Type::Null.id());
        assert!(matches!(val, SharedValue::Null));
        assert_eq!(ptr, 0);
    }

    /// A null field in the variable region must consume its flag byte on read,
    /// exactly as `plan_write_field` consumes it on write. It used to return
    /// early without advancing, so a null in one array element shifted every
    /// following field back a byte and the *next* element read garbage -- no
    /// error, just wrong data. Two elements is the smallest shape that shows
    /// it: the first reads fine either way.
    #[test]
    fn null_in_variable_region_does_not_shift_later_fields() {
        use crate::ram::io::writer::{execute_plan, plan_write_field, WriteInstructions};
        use crate::ram::schema::Schema;
        use crate::ram::types::{Id, OwnedValue};
        use dovahkiin::types::{OwnedMap, OwnedPrimArray};

        let schema = Schema::new(
            "null_shift",
            None,
            Field::new_schema(vec![Field::new_map_array(
                "entries",
                vec![
                    Field::new_unindexed("tag", Type::U32),
                    // The nullable field sits before nothing in element 1, but
                    // element 2 follows it in the same variable region.
                    Field::new_unindexed_array_nullable("ids", Type::Id),
                ],
            )]),
            false,
            false,
        );

        let entry = |tag: u32, ids: Option<Vec<Id>>| {
            let mut m = OwnedMap::new();
            m.insert_key_id(types::key_hash("tag"), OwnedValue::U32(tag));
            m.insert_key_id(
                types::key_hash("ids"),
                match ids {
                    // First entry is null -- the case that used to desync.
                    None => OwnedValue::Null,
                    Some(ids) => OwnedValue::PrimArray(OwnedPrimArray::Id(ids)),
                },
            );
            OwnedValue::Map(m)
        };

        let mut root = OwnedMap::new();
        root.insert_key_id(
            types::key_hash("entries"),
            OwnedValue::Array(vec![
                entry(11, None),
                entry(22, Some(vec![Id::from_parts(7, 8)])),
            ]),
        );
        let value = OwnedValue::Map(root);

        let mut tail = schema.static_bound;
        let mut ins = WriteInstructions::new();
        plan_write_field(
            &mut tail,
            &schema.fields,
            &value,
            &mut ins,
            false,
            &schema.transform,
        )
        .unwrap();
        let mut buf = vec![0u8; tail + 64];
        execute_plan(buf.as_mut_ptr() as usize, &ins);

        let read = read_by_schema(buf.as_ptr() as usize, &schema);
        let entries = match &read {
            SharedValue::Map(m) => match m.get_by_key_id(types::key_hash("entries")) {
                SharedValue::Array(v) => v.clone(),
                other => panic!("entries not an array: {:?}", other),
            },
            other => panic!("root not a map: {:?}", other),
        };
        assert_eq!(entries.len(), 2, "both entries must be present");

        let tag_of = |e: &SharedValue| match e {
            SharedValue::Map(m) => match m.get_by_key_id(types::key_hash("tag")) {
                SharedValue::U32(t) => **t,
                other => panic!("tag not u32: {:?}", other),
            },
            other => panic!("entry not a map: {:?}", other),
        };
        assert_eq!(tag_of(&entries[0]), 11);
        // This is the assertion that failed before the fix.
        assert_eq!(
            tag_of(&entries[1]),
            22,
            "second entry shifted -- a null in the first entry desynced the cursor"
        );
    }
}
