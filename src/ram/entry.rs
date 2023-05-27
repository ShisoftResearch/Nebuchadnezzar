use std::{io::Cursor, mem};

use crate::ram::cell::CellHeader;
use crate::ram::tombstone::Tombstone;
use byteorder::{ByteOrder, LittleEndian};
use byteorder::{ReadBytesExt, WriteBytesExt};

use super::mem_cursor::Endian;

bitflags! {
    #[derive(Clone, Copy, Debug, PartialEq, Eq, Hash)]
    pub struct EntryType: u32 {
        const UNDECIDED =   0b0000;
        const CELL =        0b0001;
        const TOMBSTONE =   0b0010;
    }
}

pub const ENTRY_HEAD_SIZE: usize = mem::size_of::<u64>();

#[derive(Copy, Clone, Debug)]
pub struct EntryHeader {
    pub entry_type: EntryType,
    pub content_length: u32,
}

#[derive(Clone)]
pub struct EntryMeta {
    pub body_pos: usize,
    pub entry_pos: usize,
    pub entry_size: usize,
    pub entry_header: EntryHeader,
}

#[derive(Debug)]
pub enum EntryContent {
    Cell(CellHeader),
    Tombstone(Tombstone),
    Undecided,
}

pub struct Entry {
    pub meta: EntryMeta,
    pub content: EntryContent,
}

impl Entry {
    pub fn encode_to<W>(
        mut pos: usize,
        entry_type: EntryType,
        content_len: u32,
        write_content: W,
    ) where
        W: Fn(usize),
    {
        let entry_type_bits = entry_type.bits();
        let mut cursor = Cursor::new(unsafe { Box::from_raw(pos as *mut[u8; 8] as *mut [u8]) });
        cursor.write_u32::<Endian>(entry_type_bits);
        cursor.write_u32::<LittleEndian>(content_len);
        pos += ENTRY_HEAD_SIZE;
        write_content(pos);
        Box::into_raw(cursor.into_inner());
    }

    // Returns the entry header reader returns
    pub fn decode_from<R, RR>(mut pos: usize, content_read: R) -> (EntryHeader, RR)
    where
        R: Fn(usize, EntryHeader) -> RR,
    {
        let mut cursor = Cursor::new(unsafe { Box::from_raw(pos as *mut[u8; 8] as *mut [u8]) });
        let entry_type_bits = cursor.read_u32::<Endian>().unwrap();
        let content_length = cursor.read_u32::<Endian>().unwrap();
        let entry_type = EntryType::from_bits(entry_type_bits).unwrap();
        let entry = EntryHeader { entry_type, content_length };
        pos += ENTRY_HEAD_SIZE;
        Box::into_raw(cursor.into_inner());
        (entry, content_read(pos, entry))
    }
}

impl EntryContent {
    pub fn as_cell_header(&self) -> &CellHeader {
        if let EntryContent::Cell(ref header) = self {
            return header;
        } else {
            panic!("entry not header");
        }
    }
}
