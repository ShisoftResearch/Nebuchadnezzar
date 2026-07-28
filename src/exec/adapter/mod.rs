use dovahkiin::types::{Id, Map, OwnedMap, OwnedValue, SharedMap, SharedValue};

use crate::ram::cell::CellHeader;

pub mod cell_value;
pub mod filter_value;
pub mod id_cell;
pub mod id_cell_select;
pub mod owned_cell;
pub mod proc_value;
pub mod take;

pub trait Adapter<I, O, P>: Iterator<Item = O> + Sized {
    fn from(input: impl Iterator<Item = I> + 'static, params: P) -> Result<Self, String>;
}

pub fn owned_with_header(header: CellHeader, mut value: OwnedValue) -> OwnedValue {
    match &mut value {
        OwnedValue::Map(m) => {
            let mut header_map = OwnedMap::new();
            header_map.insert("id", OwnedValue::Id(header.id()));
            header_map.insert("ts", OwnedValue::U64(header.revision_ts));
            header_map.insert("sch", OwnedValue::U32(header.schema));
            m.insert_value("__header", OwnedValue::Map(header_map));
        }
        _ => {}
    }
    return value;
}

pub fn shared_with_header<'a>(
    header: &'a CellHeader,
    mut value: SharedValue<'a>,
) -> SharedValue<'a> {
    let id_ptr = &header.partition as *const _ as usize as *const Id;
    unsafe {
        match &mut value {
            SharedValue::Map(m) => {
                let mut header_map = SharedMap::new();
                header_map.insert("id", SharedValue::Id(id_ptr.as_ref().unwrap()));
                header_map.insert("ts", SharedValue::U64(&header.revision_ts));
                header_map.insert("sch", SharedValue::U32(&header.schema));
                m.insert("__header", SharedValue::Map(header_map));
            }
            _ => {}
        }
    }
    return value;
}

#[cfg(test)]
mod tests {
    use super::*;
    use dovahkiin::types::key_hash;

    #[test]
    fn owned_header_exposes_u64_revision_timestamp_without_counter() {
        let header = CellHeader {
            revision_ts: u64::from(u32::MAX) + 17,
            flags: 0,
            schema: 9,
            partition: 10,
            hash: 11,
        };
        let value = owned_with_header(header, OwnedValue::Map(OwnedMap::new()));
        let OwnedValue::Map(value) = value else {
            panic!("adapter output must remain a map");
        };
        let OwnedValue::Map(header_value) = value.get("__header") else {
            panic!("adapter output must contain __header metadata");
        };

        assert_eq!(header_value.get("ts"), &OwnedValue::U64(header.revision_ts));
        assert!(
            header_value.map.get(&key_hash("ver")).is_none(),
            "removed revision counters must not be exposed"
        );
    }
}
