use crate::ram::cell::*;
use crate::ram::schema::Field;
use crate::ram::types;
use crate::ram::types::{OwnedMap, OwnedValue};

use std::{
    collections::{HashMap, HashSet},
    mem,
};

use dovahkiin::types::{key_hash, Type};

enum InstData<'a> {
    Ref(&'a OwnedValue),
    Val(OwnedValue),
}

impl<'a> InstData<'a> {
    fn val_ref(&self) -> &OwnedValue {
        match self {
            InstData::Ref(r) => r,
            InstData::Val(v) => v,
        }
    }
}

pub struct Instruction<'a> {
    data_type: Type,
    val: InstData<'a>,
    offset: usize,
}

pub fn plan_write_field<'a>(
    mut offset: &mut usize,
    field: &Field,
    value: &'a OwnedValue,
    mut ins: &mut Vec<Instruction<'a>>,
) -> Result<(), WriteError> {
    if field.nullable {
        let null_bit = match value {
            OwnedValue::Null => true,
            _ => false,
        };
        ins.push(Instruction {
            data_type: Type::Bool,
            val: InstData::Val(OwnedValue::Bool(null_bit)),
            offset: *offset,
        });
        *offset += 1;
    }
    if field.is_array {
        if let OwnedValue::Array(array) = value {
            let len = array.len();
            let mut sub_field = field.clone();
            sub_field.is_array = false;
            ins.push(Instruction {
                data_type: types::ARRAY_LEN_TYPE,
                val: InstData::Val(OwnedValue::U32(len as u32)),
                offset: *offset,
            });
            *offset += types::u32_io::size(0);
            for val in array {
                plan_write_field(&mut offset, &sub_field, val, &mut ins)?;
            }
        } else if let OwnedValue::PrimArray(ref array) = value {
            let len = array.len();
            let size = array.size();
            // for prim array, just clone it and push into the instruction list with length
            ins.push(Instruction {
                data_type: types::ARRAY_LEN_TYPE,
                val: InstData::Val(OwnedValue::U32(len as u32)),
                offset: *offset,
            });
            *offset += types::u32_io::size(0);
            ins.push(Instruction {
                data_type: field.data_type,
                val: InstData::Ref(value),
                offset: *offset,
            });
            *offset += size;
        } else {
            return Err(WriteError::DataMismatchSchema(field.clone(), value.clone()));
        }
    } else if let Some(ref subs) = field.sub_fields {
        if let OwnedValue::Map(map) = value {
            for sub in subs {
                let val = map.get_by_key_id(sub.name_id);
                plan_write_field(&mut offset, &sub, val, &mut ins)?;
            }
        } else {
            return Err(WriteError::DataMismatchSchema(field.clone(), value.clone()));
        }
    } else {
        let is_null = match value {
            OwnedValue::Null => true,
            _ => false,
        };
        if !field.nullable && is_null {
            return Err(WriteError::DataMismatchSchema(field.clone(), value.clone()));
        }
        if !is_null {
            let size = types::get_vsize(field.data_type, &value);
            ins.push(Instruction {
                data_type: field.data_type,
                val: InstData::Ref(value),
                offset: *offset,
            });
            *offset += size;
        }
    }
    return Ok(());
}

pub fn plan_write_dynamic_fields<'a>(
    offset: &mut usize,
    field: &Field,
    value: &'a OwnedValue,
    ins: &mut Vec<Instruction<'a>>,
) -> Result<(), WriteError> {
    if let (OwnedValue::Map(data_all), &Some(ref fields)) = (value, &field.sub_fields) {
        let schema_keys: HashSet<u64> = fields.iter().map(|f| f.name_id).collect();
        let dynamic_map: HashMap<_, _> = data_all
            .map
            .iter()
            .filter(|(k, _v)| !schema_keys.contains(k))
            .map(|(k, v)| (k, v))
            .collect();
        let dynamic_names: Vec<_> = data_all
            .fields
            .iter()
            .filter_map(|n| {
                let id = key_hash(&n);
                dynamic_map.get(&id).map(|_| n)
            })
            .collect();
        if !dynamic_map.is_empty() {}
        plan_write_dynamic_map(offset, &dynamic_names, &dynamic_map, ins)?;
    }
    return Ok(());
}

pub const ARRAY_TYPE_MASK: u8 = !(!0 << 1 >> 1); // 1000000...
pub const NULL_PLACEHOLDER: u8 = ARRAY_TYPE_MASK >> 1; // 1000000...

pub fn plan_write_dynamic_map<'a>(
    offset: &mut usize,
    names: &Vec<&String>,
    map: &HashMap<&u64, &'a OwnedValue>,
    ins: &mut Vec<Instruction<'a>>,
) -> Result<(), WriteError> {
    ins.push(Instruction {
        data_type: types::TYPE_CODE_TYPE,
        val: InstData::Val(OwnedValue::U8(Type::Map.id())),
        offset: *offset,
    });
    *offset += types::u8_io::size(0);
    // Write map size
    ins.push(Instruction {
        data_type: types::ARRAY_LEN_TYPE,
        val: InstData::Val(OwnedValue::U32(names.len() as u32)),
        offset: *offset,
    });
    *offset += types::u32_io::size(0);
    for name in names {
        let id = key_hash(name);
        let name_value = OwnedValue::String((*name).to_owned());
        let name_size = types::get_vsize(name_value.base_type(), &name_value);
        ins.push(Instruction {
            data_type: name_value.base_type(),
            val: InstData::Val(name_value),
            offset: *offset,
        });
        *offset += name_size;
        plan_write_dynamic_value(offset, map.get(&id).unwrap(), ins)?;
    }
    Ok(())
}

pub fn plan_write_dynamic_value<'a>(
    offset: &mut usize,
    value: &'a OwnedValue,
    ins: &mut Vec<Instruction<'a>>,
) -> Result<(), WriteError> {
    let base_type = value.base_type();
    match &value {
        &OwnedValue::Array(array) => {
            // Write type id
            ins.push(Instruction {
                data_type: types::TYPE_CODE_TYPE,
                val: InstData::Val(OwnedValue::U8(ARRAY_TYPE_MASK)), // Only put the mask cause we don't know the type
                offset: *offset,
            });
            *offset += types::u8_io::size(0);
            let len = array.len();
            // Write array length
            ins.push(Instruction {
                data_type: types::ARRAY_LEN_TYPE,
                val: InstData::Val(OwnedValue::U32(len as u32)),
                offset: *offset,
            });
            *offset += types::u32_io::size(0);
            for val in array {
                plan_write_dynamic_value(offset, val, ins)?;
            }
        }
        &OwnedValue::PrimArray(array) => {
            // Write type id with array tag
            ins.push(Instruction {
                data_type: types::TYPE_CODE_TYPE,
                val: InstData::Val(OwnedValue::U8(ARRAY_TYPE_MASK | base_type.id())),
                offset: *offset,
            });
            *offset += types::u8_io::size(0);
            let len = array.len();
            ins.push(Instruction {
                data_type: types::ARRAY_LEN_TYPE,
                val: InstData::Val(OwnedValue::U32(len as u32)),
                offset: *offset,
            });
            *offset += types::u32_io::size(0);
            let array_size = array.size();
            ins.push(Instruction {
                data_type: base_type,
                val: InstData::Ref(value),
                offset: *offset,
            });
            *offset += array_size;
        }
        &OwnedValue::Map(map) => plan_write_dynamic_map(
            offset,
            &map.fields.iter().collect(),
            &map.map.iter().collect(),
            ins,
        )?,
        &OwnedValue::Null | OwnedValue::NA => {
            // Write a placeholder because mapping required
            ins.push(Instruction {
                data_type: types::TYPE_CODE_TYPE,
                val: InstData::Val(OwnedValue::U8(NULL_PLACEHOLDER)),
                offset: *offset,
            });
            *offset += types::u8_io::size(0);
        }
        _ => {
            // Primitives
            let ty = value.base_type();
            ins.push(Instruction {
                data_type: types::TYPE_CODE_TYPE,
                val: InstData::Val(OwnedValue::U8(ty.id())),
                offset: *offset,
            });
            *offset += types::u8_io::size(0);
            let value_size = types::get_vsize(ty, &value);
            ins.push(Instruction {
                data_type: ty,
                val: InstData::Ref(value),
                offset: *offset,
            });
            *offset += value_size;
        }
    }
    Ok(())
}

pub fn execute_plan(ptr: usize, instructions: &Vec<Instruction>) {
    for ins in instructions {
        types::set_val(ins.data_type, ins.val.val_ref(), ptr + ins.offset);
    }
}
