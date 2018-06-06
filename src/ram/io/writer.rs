use ram::schema::{Field};
use ram::cell::*;
use ram::types;
use ram::types::{Value, Map, NULL_TYPE_ID, Any};

use std::collections::HashSet;

use bifrost_hasher::hash_str;

pub struct Instruction {
    type_id: u32,
    val: Value,
    offset: usize
}

pub fn plan_write_field (
    mut offset: &mut usize,
    field: &Field,
    value: &Value,
    mut ins: &mut Vec<Instruction>
) -> Result<(), WriteError> {
    if field.nullable {
        let null_bit = match value {
            &Value::Null => 1,
            _ => 0
        };
        ins.push(Instruction {
            type_id: NULL_TYPE_ID,
            val: Value::U8(null_bit),
            offset: *offset
        });
        *offset += 1;
    }
    if field.is_array {
        if let &Value::Array(ref array) = value {
            let len = array.len();
            let mut sub_field = field.clone();
            sub_field.is_array = false;
            ins.push(Instruction {
                type_id: types::ARRAY_LEN_TYPE_ID,
                val: Value::U32(len as u32),
                offset: *offset
            });
            *offset += types::u16_io::size(0);
            for val in array {
                plan_write_field(&mut offset, &sub_field, val, &mut ins)?;
            }
        } else {
            return Err(WriteError::DataMismatchSchema(field.clone(), value.clone()));
        }
    } else if let Some(ref subs) = field.sub_fields {
        if let &Value::Map(ref map) = value {
            for sub in subs {
                let val = map.get_by_key_id(sub.name_id);
                plan_write_field(&mut offset, &sub, val, &mut ins)?;
            }
        } else {
            return Err(WriteError::DataMismatchSchema(field.clone(),value.clone()));
        }
    } else {
        let is_null = match value {&Value::Null => true, _ => false};
        if !field.nullable && is_null {
            return Err(WriteError::DataMismatchSchema(field.clone(),value.clone()))
        }
        if !is_null {
            let size = types::get_vsize(field.type_id, value);
            ins.push(Instruction {
                type_id: field.type_id,
                val: value.clone(),
                offset: *offset
            });
            *offset += size;
        }
    }
    return Ok(())
}

pub fn plan_write_dynamic_fields(
    mut offset: &mut usize,
    field: &Field,
    value: &Value,
    mut ins: &mut Vec<Instruction>
) -> Result<(), WriteError> {
    if let (&Value::Map(ref data_all), &Some(ref fields)) = (value, &field.sub_fields) {
        let schema_keys: HashSet<u64> = fields.iter().map(|f| f.name_id).collect();
        let dynamic_keys: HashSet<u64> = data_all.map
            .keys()
            .filter(|k| !schema_keys.contains(k))
            .cloned().collect();
        let dynamic_names: Vec<String> = data_all.fields
            .iter()
            .filter(|n| dynamic_keys.contains(&hash_str(n)))
            .cloned().collect();
        let mut dynamic_map = Map::new();
        for key_id in dynamic_keys {
            dynamic_map.insert_key_id(key_id, data_all.get_by_key_id(key_id).clone());
        }
        dynamic_map.fields = dynamic_names;
        let any_type_id = types::TypeId::Any as u32;
        let dynamic_value = Value::Any(Any::from(&dynamic_map));
        let dynamic_size = types::get_vsize(any_type_id, &dynamic_value);
        ins.push(Instruction {
            type_id: any_type_id,
            val: dynamic_value,
            offset: *offset
        });
        *offset += dynamic_size;
    }
    return Ok(())
}

pub fn execute_plan (ptr: usize, instructions: &Vec<Instruction>) {
    for ins in instructions {
        types::set_val(ins.type_id, &ins.val, ptr + ins.offset);
    }
}