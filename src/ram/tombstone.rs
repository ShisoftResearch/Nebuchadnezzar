use super::mem_cursor::*;
use crate::ram::entry::*;
use byteorder::{ReadBytesExt, WriteBytesExt};
use dovahkiin::types::Id;
use std::{
    io::{Cursor, Write},
    mem,
};

#[derive(Debug)]
pub struct Tombstone {
    pub segment_seq_id: u64,
    pub version: u64,
    pub id: Id,
}

pub const TOMBSTONE_SIZE: usize = 3 * mem::size_of::<u64>();
pub const TOMBSTONE_SIZE_U32: u32 = TOMBSTONE_SIZE as u32;
pub const TOMBSTONE_ENTRY_SIZE: usize = TOMBSTONE_SIZE + ENTRY_HEAD_SIZE;

fn write_u64<W>(buffer: &mut W, value: u64)
where
    W: Write + Sized,
{
    buffer.write_u64::<Endian>(value).unwrap();
}

def_raw_memory_cursor_for_size!(TOMBSTONE_SIZE, addr_to_cursor);

impl Tombstone {
    pub fn write(&self, addr: usize) {
        Entry::encode_to(addr, EntryType::TOMBSTONE, TOMBSTONE_SIZE_U32, |addr| {
            let mut cursor = addr_to_cursor(addr);
            {
                write_u64(&mut cursor, self.segment_seq_id);
                write_u64(&mut cursor, self.version);
                write_u64(&mut cursor, self.id.bits());
            }
            release_cursor(cursor);
        })
    }

    pub fn read_from_entry_content_addr(addr: usize) -> Tombstone {
        let mut cursor = addr_to_cursor(addr);
        let tombstone = Tombstone {
            segment_seq_id: cursor.read_u64::<Endian>().unwrap(),
            version: cursor.read_u64::<Endian>().unwrap(),
            id: Id::from_bits(cursor.read_u64::<Endian>().unwrap()),
        };
        release_cursor(cursor);
        return tombstone;
    }

    pub fn read(addr: usize) -> Tombstone {
        Entry::decode_from(addr, |addr, header| {
            assert_eq!(
                header.entry_type,
                EntryType::TOMBSTONE,
                "Reading entry not tombstone"
            );
            return Self::read_from_entry_content_addr(addr);
        })
        .1
    }

    pub fn put(tombstone_addr: usize, segment_seq_id: u64, version: u64, id: Id) {
        Tombstone {
            segment_seq_id,
            version,
            id,
        }
        .write(tombstone_addr)
    }
}
