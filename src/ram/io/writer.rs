use crate::ram::io::align_ptr_addr;
use crate::ram::schema::Field;
use crate::ram::types;
use crate::ram::types::OwnedValue;
use crate::ram::{cell::*, io::align_address_with_ty};

use std::collections::{HashMap, HashSet};

use dovahkiin::types::{
    key_hash, Map, OwnedMap, SharedMap, SharedValue, Type, ARRAY_LEN_TYPE, TYPE_CODE_TYPE,
};
use itertools::Itertools;

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
    tail_offset: &mut usize,
    field: &Field,
    value: &'a OwnedValue,
    mut ins: &mut Vec<Instruction<'a>>,
    is_var: bool,
) -> Result<(), WriteError> {
    let mut schema_offset = field.offset.clone();
    let is_field_var = field.is_var();
    let is_null = match value {
        OwnedValue::Null | OwnedValue::NA => true,
        _ => false
    };
    let offset = if field.nullable {
        if !is_var {
            let null_flag = is_null.then_some(*tail_offset).unwrap_or(0) as u32;
            ins.push(Instruction {
                data_type: Type::U32,
                val: InstData::Val(OwnedValue::U32(null_flag)),
                offset: schema_offset.unwrap(),
            });
        } else {
            ins.push(Instruction {
                data_type: Type::Bool,
                val: InstData::Val(OwnedValue::Bool(is_null)),
                offset: *tail_offset,
            });
            if !is_null {
                *tail_offset = align_address_with_ty(value.base_type(), *tail_offset + 1);
            }
        }
        if is_null {
            return Ok(());
        } else {
            tail_offset
        }
    } else if let Some(ref subs) = field.sub_fields {
        if let (OwnedValue::Array(_), true) = (value, field.is_array) {
            if !is_var {
                ins.push(Instruction {
                    data_type: Type::U32,
                    val: InstData::Val(OwnedValue::U32(*tail_offset as u32)),
                    offset: schema_offset.unwrap(),
                });
            }
            tail_offset
        } else if let OwnedValue::Map(map) = value {
            for sub in subs {
                let val = map.get_by_key_id(sub.name_id);
                plan_write_field(tail_offset, &sub, val, &mut ins, is_var)?;
            }
            return Ok(());
        } else {
            return Err(WriteError::DataMismatchSchema(field.clone(), value.clone()));
        }
    } else if is_field_var {
        // Write position tag for variable sized field
        if !is_var {
            // No need to jump to var region when it is var
            trace!(
                "Push jump tailing inst with {} at {:?}",
                tail_offset,
                schema_offset
            );
            *tail_offset = align_address_with_ty(field.data_type, *tail_offset);
            ins.push(Instruction {
                data_type: Type::U32,
                val: InstData::Val(OwnedValue::U32(*tail_offset as u32)),
                offset: schema_offset.unwrap(),
            });
        }
        trace!("Using tailing offset for {} at {}", field.name, tail_offset);
        tail_offset
    } else if !is_var {
        schema_offset.as_mut().expect(&format!(
            "schema should have offset is_var: {}, field var: {}",
            is_var, is_field_var
        ))
    } else {
        tail_offset
    };
    trace!(
        "Plan to write {} at {}, field var {}, in var {}, value {:?}, field {:?}",
        field.name,
        offset,
        is_field_var,
        is_var,
        value,
        field
    );
    if field.is_array {
        if let OwnedValue::Array(array) = value {
            let len = array.len();
            let mut sub_field = field.clone();
            sub_field.is_array = false;
            trace!("Pushing array len inst with {} at {}", len, *offset);
            *offset = align_ptr_addr(*offset);
            ins.push(Instruction {
                data_type: types::ARRAY_LEN_TYPE,
                val: InstData::Val(OwnedValue::U32(len as u32)),
                offset: *offset,
            });
            *offset += types::u32_io::type_size();
            for val in array {
                plan_write_field(offset, &sub_field, val, &mut ins, true)?;
            }
        } else if let OwnedValue::PrimArray(ref array) = value {
            let len = array.len();
            let size = array.size();
            // for prim array, just clone it and push into the instruction list with length
            trace!("Pushing prim array len inst with {} at {}", len, *offset);
            *offset = align_ptr_addr(*offset);
            ins.push(Instruction {
                data_type: types::ARRAY_LEN_TYPE,
                val: InstData::Val(OwnedValue::U32(len as u32)),
                offset: *offset,
            });
            *offset += types::u32_io::type_size();
            trace!(
                "Pushing prim array ref inst with {:?} at {}",
                value,
                *offset
            );
            *offset = align_address_with_ty(field.data_type, *offset);
            ins.push(Instruction {
                data_type: field.data_type,
                val: InstData::Ref(value),
                offset: *offset,
            });
            *offset += size;
        } else {
            return Err(WriteError::DataMismatchSchema(field.clone(), value.clone()));
        }
    } else {
        if !field.nullable && is_null {
            return Err(WriteError::DataMismatchSchema(field.clone(), value.clone()));
        }
        let size = types::get_vsize(field.data_type, &value);
        ins.push(Instruction {
            data_type: field.data_type,
            val: InstData::Ref(value),
            offset: *offset,
        });
        let new_offset = *offset + size;
        trace!(
            "Pushing value ref inst with {:?} at {}, size {}, new offset {}",
            value,
            *offset,
            size,
            new_offset
        );
        *offset = new_offset;
    }
    return Ok(());
}

pub fn plan_write_dynamic_fields<'a>(
    offset: &mut usize,
    field: &Field,
    value: &'a OwnedValue,
    ins: &mut Vec<Instruction<'a>>,
) -> Result<(), WriteError> {
    *offset = align_ptr_addr(*offset);
    if let (OwnedValue::Map(data_all), &Some(ref fields)) = (value, &field.sub_fields) {
        let schema_keys: HashSet<u64> = fields.iter().map(|f| f.name_id).collect();
        let dynamic_map: HashMap<_, _> = data_all
            .map
            .iter()
            .filter(|(k, _v)| !schema_keys.contains(k))
            .map(|(k, v)| (*k, v))
            .collect();
        let dynamic_names: Vec<_> = data_all
            .fields
            .iter()
            .filter_map(|n| {
                let id = key_hash(&n);
                dynamic_map.get(&id).map(|_| n)
            })
            .cloned()
            .collect();
        plan_write_dynamic_map(offset, dynamic_map, &dynamic_names, ins)?;
    }
    return Ok(());
}

fn plan_write_dynamic_map<'a>(
    offset: &mut usize,
    map: HashMap<u64, &'a OwnedValue>,
    fields: &[String],
    ins: &mut Vec<Instruction<'a>>,
) -> Result<(), WriteError> {
    *offset = align_address_with_ty(ARRAY_LEN_TYPE, *offset);
    ins.push(Instruction {
        data_type: ARRAY_LEN_TYPE,
        val: InstData::Val(OwnedValue::U32(map.len() as _)),
        offset: *offset,
    });
    *offset += ARRAY_LEN_TYPE.size().unwrap();

    // Write types
    let field_ids = fields.iter().map(|n| key_hash(&n)).collect_vec();
    for (i, _name) in fields.iter().enumerate() {
        let fid = &field_ids[i];
        let val = map[fid];
        ins.push(Instruction {
            data_type: Type::U8,
            val: InstData::Val(OwnedValue::U8(dyn_data_type_id(val))),
            offset: *offset,
        });
        *offset += 1;
    }
    // Write field names
    for name in fields {
        let name_value = OwnedValue::String((*name).to_owned());
        let name_size = types::get_vsize(name_value.base_type(), &name_value);
        *offset = align_address_with_ty(Type::String, *offset);
        ins.push(Instruction {
            data_type: name_value.base_type(),
            val: InstData::Val(name_value),
            offset: *offset,
        });
        *offset += name_size;
    }
    for (i, _name) in fields.iter().enumerate() {
        let fid = &field_ids[i];
        let val = map[fid];
        plan_write_dynamic_value(offset, val, ins)?
    }
    return Ok(());
}

pub const ARRAY_TYPE_MASK: u8 = !(!0 << 1 >> 1); // 1000000...
pub const NULL_PLACEHOLDER: u8 = ARRAY_TYPE_MASK >> 1; // 1000000...

fn dyn_data_type_id<'a>(value: &'a OwnedValue) -> u8 {
    match &value {
        &OwnedValue::Array(_) => ARRAY_TYPE_MASK,
        &OwnedValue::PrimArray(_) => ARRAY_TYPE_MASK | value.base_type().id(),
        &OwnedValue::Map(_) => Type::Map.id(),
        &OwnedValue::Null | OwnedValue::NA => NULL_PLACEHOLDER,
        _ => value.base_type().id(),
    }
}

fn plan_write_dynamic_value<'a>(
    offset: &mut usize,
    value: &'a OwnedValue,
    ins: &mut Vec<Instruction<'a>>,
) -> Result<(), WriteError> {
    if let &OwnedValue::Map(m) = &value {
        return plan_write_dynamic_map(
            offset,
            m.map.iter().map(|(k, v)| (*k, v)).collect(),
            &m.fields,
            ins,
        );
    } else if let Some(len) = value.len() {
        // Record the length of the value if it is a compound
        *offset = align_address_with_ty(ARRAY_LEN_TYPE, *offset);
        ins.push(Instruction {
            data_type: ARRAY_LEN_TYPE,
            val: InstData::Val(OwnedValue::U32(len as _)),
            offset: *offset,
        });
        *offset += ARRAY_LEN_TYPE.size().unwrap();
    } else {
        // Else just write the data
        let ty = value.base_type();
        let value_size = types::get_vsize(ty, &value);
        *offset = align_address_with_ty(ty, *offset);
        ins.push(Instruction {
            data_type: ty,
            val: InstData::Ref(value),
            offset: *offset,
        });
        *offset += value_size;
        return Ok(()); // We are done here
    }

    match &value {
        &OwnedValue::Array(array) => {
            for val in array {
                let ty = dyn_data_type_id(val);
                ins.push(Instruction {
                    data_type: Type::U8,
                    val: InstData::Val(OwnedValue::U8(ty)),
                    offset: *offset,
                });
                *offset += 1;
            }
        }
        &OwnedValue::PrimArray(_array) => {
            // Do noting for primary
        }
        _ => {
            unreachable!()
        }
    }
    // Write actual data for each of the children in the container
    match &value {
        &OwnedValue::Array(array) => {
            for val in array {
                plan_write_dynamic_value(offset, val, ins)?;
            }
        }
        &OwnedValue::PrimArray(array) => {
            let array_size = array.size();
            *offset = align_address_with_ty(value.base_type(), *offset);
            ins.push(Instruction {
                data_type: value.base_type(),
                val: InstData::Ref(value),
                offset: *offset,
            });
            *offset += array_size;
        }
        _ => {
            unreachable!()
        }
    }
    return Ok(());
}

pub fn execute_plan(ptr: usize, instructions: &Vec<Instruction>) {
    for ins in instructions {
        types::set_val(ins.data_type, ins.val.val_ref(), ptr + ins.offset);
    }
}
