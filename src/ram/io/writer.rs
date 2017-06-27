use ram::schema::{Field};
use ram::cell::*;
use ram::types;
use ram::types::{Value, NULL_TYPE_ID};

pub struct Instruction {
    type_id: u32,
    val: DataValue,
    offset: usize
}

pub fn plan_write_field (
    mut offset: &mut usize,
    field: &Field,
    value: &Value,
    mut ins: &mut Vec<Instruction>
) -> Result<(), WriteError> {
    let data_mismatch = "Data type does not match the schema for";
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
            return Err(WriteError::DataMismatchSchema);
        }
    } else if let Some(ref subs) = field.sub_fields {
        if let &Value::Map(ref map) = value {
            for sub in subs {
                let val = map.get_by_key_id(sub.name_id);
                plan_write_field(&mut offset, &sub, val, &mut ins)?;
            }
        } else {
            return Err(WriteError::DataMismatchSchema);
        }
    } else {
        let is_null = match value {&Value::Null => true, _ => false};
        if !field.nullable && is_null {
            return Err(WriteError::DataMismatchSchema)
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

pub fn execute_plan (ptr: usize, instructions: Vec<Instruction>) {
    for ins in instructions {
        types::set_val(ins.type_id, &ins.val, ptr + ins.offset);
    }
}