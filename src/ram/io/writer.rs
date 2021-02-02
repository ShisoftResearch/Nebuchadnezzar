use crate::ram::cell::*;
use crate::ram::schema::Field;
use crate::ram::types;
use crate::ram::types::{OwnedMap, OwnedValue, NULL_TYPE_ID};

use std::collections::{HashMap, HashSet};

use dovahkiin::types::key_hash;

pub struct Instruction {
    type_id: u32,
    val: OwnedValue,
    offset: usize,
}

pub fn plan_write_field(
    mut offset: &mut usize,
    field: &Field,
    value: &OwnedValue,
    mut ins: &mut Vec<Instruction>,
) -> Result<(), WriteError> {
    if field.nullable {
        let null_bit = match value {
            &OwnedValue::Null => 1,
            _ => 0,
        };
        ins.push(Instruction {
            type_id: NULL_TYPE_ID,
            val: OwnedValue::U8(null_bit),
            offset: *offset,
        });
        *offset += 1;
    }
    if field.is_array {
        if let &OwnedValue::Array(ref array) = value {
            let len = array.len();
            let mut sub_field = field.clone();
            sub_field.is_array = false;
            ins.push(Instruction {
                type_id: types::ARRAY_LEN_TYPE_ID,
                val: OwnedValue::U32(len as u32),
                offset: *offset,
            });
            *offset += types::u32_io::size(0);
            for val in array {
                plan_write_field(&mut offset, &sub_field, val, &mut ins)?;
            }
        } else if let &OwnedValue::PrimArray(ref array) = value {
            let len = array.len();
            let size = array.size();
            // for prim array, just clone it and push into the instruction list with length
            ins.push(Instruction {
                type_id: types::ARRAY_LEN_TYPE_ID,
                val: OwnedValue::U32(len as u32),
                offset: *offset,
            });
            *offset += types::u32_io::size(0);
            ins.push(Instruction {
                type_id: field.type_id,
                val: value.clone(),
                offset: *offset,
            });
            *offset += size;
        } else {
            return Err(WriteError::DataMismatchSchema(field.clone(), value.clone()));
        }
    } else if let Some(ref subs) = field.sub_fields {
        if let &OwnedValue::Map(ref map) = value {
            for sub in subs {
                let val = map.get_by_key_id(sub.name_id);
                plan_write_field(&mut offset, &sub, val, &mut ins)?;
            }
        } else {
            return Err(WriteError::DataMismatchSchema(field.clone(), value.clone()));
        }
    } else {
        let is_null = match value {
            &OwnedValue::Null => true,
            _ => false,
        };
        if !field.nullable && is_null {
            return Err(WriteError::DataMismatchSchema(field.clone(), value.clone()));
        }
        if !is_null {
            let size = types::get_vsize(field.type_id, value);
            ins.push(Instruction {
                type_id: field.type_id,
                val: value.clone(),
                offset: *offset,
            });
            *offset += size;
        }
    }
    return Ok(());
}

pub fn plan_write_dynamic_fields(
    offset: &mut usize,
    field: &Field,
    value: &OwnedValue,
    ins: &mut Vec<Instruction>,
) -> Result<(), WriteError> {
    if let (&OwnedValue::Map(ref data_all), &Some(ref fields)) = (value, &field.sub_fields) {
        let schema_keys: HashSet<u64> = fields.iter().map(|f| f.name_id).collect();
        let dynamic_map: HashMap<_, _> = data_all
            .map
            .iter()
            .filter(|(k, v)| !schema_keys.contains(k))
            .map(|(k, v)| (*k, v.clone()))
            .collect();
        let dynamic_names: Vec<_> = data_all
            .fields
            .iter()
            .filter_map(|n| {
                let id = &key_hash(n);
                dynamic_map.get(&id).map(|_| n.clone())
            })
            .collect();
        let composed_map = OwnedMap {
            map: dynamic_map,
            fields: dynamic_names,
        };
        let composed_value = OwnedValue::Map(composed_map);
        plan_write_dynamic_value(offset, &composed_value, ins);
    }
    return Ok(());
}

pub const ARRAY_TYPE_MASK: u32 = !(!0 << 1 >> 1); // 1000000...
pub const NULL_PLACEHOLDER: u32 = ARRAY_TYPE_MASK >> 1; // 1000000...

pub fn plan_write_dynamic_value(
    offset: &mut usize,
    value: &OwnedValue,
    ins: &mut Vec<Instruction>,
) -> Result<(), WriteError> {
    let base_type_id = value.base_type_id();
    match value {
        &OwnedValue::Array(ref array) => {
            // Write type id
            ins.push(Instruction {
                type_id: types::TYPE_CODE_TYPE_ID,
                val: OwnedValue::U32(ARRAY_TYPE_MASK), // Only put the mask cause we don't know the type
                offset: *offset,
            });
            *offset += types::u32_io::size(0);
            let len = array.len();
            // Write array length
            ins.push(Instruction {
                type_id: types::ARRAY_LEN_TYPE_ID,
                val: OwnedValue::U32(len as u32),
                offset: *offset,
            });
            *offset += types::u32_io::size(0);
            for val in array {
                plan_write_dynamic_value(offset, val, ins)?;
            }
        }
        &OwnedValue::PrimArray(ref array) => {
            // Write type id with array tag
            ins.push(Instruction {
                type_id: types::TYPE_CODE_TYPE_ID,
                val: OwnedValue::U32(ARRAY_TYPE_MASK | base_type_id),
                offset: *offset,
            });
            *offset += types::u32_io::size(0);
            let len = array.len();
            ins.push(Instruction {
                type_id: types::ARRAY_LEN_TYPE_ID,
                val: OwnedValue::U32(len as u32),
                offset: *offset,
            });
            *offset += types::u32_io::size(0);
            ins.push(Instruction {
                type_id: base_type_id,
                val: value.clone(),
                offset: *offset,
            });
            *offset += array.size();
        }
        &OwnedValue::Map(ref map) => {
            ins.push(Instruction {
                type_id: types::TYPE_CODE_TYPE_ID,
                val: OwnedValue::U32(0), // zero for mapping
                offset: *offset,
            });
            *offset += types::u32_io::size(0);
            // Write map size
            ins.push(Instruction {
                type_id: types::ARRAY_LEN_TYPE_ID,
                val: OwnedValue::U32(map.fields.len() as u32),
                offset: *offset,
            });
            *offset += types::u32_io::size(0);
            for name in map.fields.iter() {
                let id = key_hash(name);
                let name_bytes = name.as_bytes().len();
                let name_value = OwnedValue::String(name.to_owned());
                let name_size = types::get_vsize(name_value.base_type_id(), &name_value);
                ins.push(Instruction {
                    type_id: name_value.base_type_id(),
                    val: name_value,
                    offset: *offset,
                });
                *offset += name_size;
                plan_write_dynamic_value(offset, map.map.get(&id).unwrap(), ins)?;
            }
        }
        &OwnedValue::Null | &OwnedValue::NA => {
            // Write a placeholder because mapping required
            ins.push(Instruction {
                type_id: types::TYPE_CODE_TYPE_ID,
                val: OwnedValue::U32(NULL_PLACEHOLDER),
                offset: *offset,
            });
            *offset += types::u32_io::size(0);
        }
        _ => {
            // Primitives
            let type_id = value.base_type_id();
            ins.push(Instruction {
                type_id: types::TYPE_CODE_TYPE_ID,
                val: OwnedValue::U32(type_id),
                offset: *offset,
            });
            *offset += types::u32_io::size(0);
            ins.push(Instruction {
                type_id: type_id,
                val: value.clone(),
                offset: *offset,
            });
            *offset += types::get_vsize(type_id, value);
        }
    }
    Ok(())
}

pub fn execute_plan(ptr: usize, instructions: &Vec<Instruction>) {
    for ins in instructions {
        types::set_val(ins.type_id, &ins.val, ptr + ins.offset);
    }
}
