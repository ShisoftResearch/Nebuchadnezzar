use dovahkiin::types::{Id, Map, OwnedMap, OwnedValue, SharedMap, SharedValue};

use crate::ram::cell::CellHeader;

pub mod cell_value;
pub mod id_cell;
pub mod id_cell_select;
pub mod owned_cell;
pub mod proc_value;
pub mod referred_cell;
pub mod take;

pub trait Adapter<I, O, P>: Iterator<Item = O> + Sized {
    fn from(input: impl Iterator<Item = I> + 'static, params: P) -> Result<Self, String>;
}

pub fn owned_with_header(header: CellHeader, mut value: OwnedValue) -> OwnedValue {
    match &mut value {
        OwnedValue::Map(m) => {
            let mut header_map = OwnedMap::new();
            header_map.insert("id", OwnedValue::Id(header.id()));
            header_map.insert("ts", OwnedValue::U32(header.timestamp));
            header_map.insert("sch", OwnedValue::U32(header.schema));
            header_map.insert("ver", OwnedValue::U64(header.version));
            m.insert_value("__header", OwnedValue::Map(header_map));
        }
        _ => {}
    }
    return value;
}

pub fn shared_with_header<'a>(header: &'a CellHeader, mut value: SharedValue<'a>) -> SharedValue<'a> {
    let id_ptr = &header.partition as *const _ as usize as *const Id;
    unsafe {
        match &mut value {
            SharedValue::Map(m) => {
                let mut header_map = SharedMap::new();
                header_map.insert("id", SharedValue::Id(id_ptr.as_ref().unwrap()));
                header_map.insert("ts", SharedValue::U32(&header.timestamp));
                header_map.insert("sch", SharedValue::U32(&header.schema));
                header_map.insert("ver", SharedValue::U64(&header.version));
                m.insert("__header", SharedValue::Map(header_map));
            }
            _ => {}
        }
    }
    return value;
}