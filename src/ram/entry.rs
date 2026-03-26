use std::panic;
use std::{io::Cursor, mem};

use crate::ram::cell::CellHeader;
use crate::ram::tombstone::Tombstone;
use byteorder::{ReadBytesExt, WriteBytesExt};

use super::mem_cursor::{release_cursor, Endian};

#[derive(Copy, Clone, Debug, PartialEq, Eq, Default)]
#[repr(u32)]
pub enum EntryType {
    #[default]
    UNDECIDED = 0,
    CELL = 1,
    TOMBSTONE = 2,
}

impl EntryType {
    pub fn from_bits(bits: u32) -> Option<Self> {
        match bits {
            0 => Some(Self::UNDECIDED),
            1 => Some(Self::CELL),
            2 => Some(Self::TOMBSTONE),
            _ => None,
        }
    }

    pub fn bits(self) -> u32 {
        self as u32
    }
}

pub const ENTRY_HEAD_SIZE: usize = mem::size_of::<u64>();

#[derive(Copy, Clone, Debug, Default)]
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
    pub fn encode_to<W>(mut pos: usize, entry_type: EntryType, content_len: u32, write_content: W)
    where
        W: Fn(usize),
    {
        let entry_type_bits = entry_type.bits();
        let mut cursor = Cursor::new(unsafe { Box::from_raw(pos as *mut [u8; 8] as *mut [u8]) });
        cursor.write_u32::<Endian>(entry_type_bits).unwrap();
        cursor.write_u32::<Endian>(content_len).unwrap();
        pos += ENTRY_HEAD_SIZE;
        write_content(pos);
        release_cursor(cursor);
    }

    pub fn content_pos(pos: usize) -> usize {
        pos + ENTRY_HEAD_SIZE
    }

    // Returns the entry header reader returns
    pub fn decode_from<R, RR>(mut pos: usize, content_read: R) -> (EntryHeader, RR)
    where
        R: Fn(usize, EntryHeader) -> RR,
    {
        // Validate address alignment before unsafe operations
        #[cfg(debug_assertions)]
        if pos % 8 != 0 {
            panic!(
                "Cannot decode entry header: address 0x{:016x} is misaligned (offset: {}). \
                This indicates memory corruption in the cell address storage. \
                Valid entry addresses must be 8-byte aligned.",
                pos,
                pos % 8
            );
        }

        let mut cursor = Cursor::new(unsafe { Box::from_raw(pos as *mut [u8; 8] as *mut [u8]) });
        let entry_type_bits = cursor.read_u32::<Endian>().unwrap();
        let content_length = cursor.read_u32::<Endian>().unwrap();
        if let Some(entry_type) = EntryType::from_bits(entry_type_bits) {
            let entry = EntryHeader {
                entry_type,
                content_length,
            };
            pos = Self::content_pos(pos);
            release_cursor(cursor);
            (entry, content_read(pos, entry))
        } else {
            let segment_info = crate::ram::chunk::chunk_and_segment_from_addr(pos)
                .map(|(chunk_id, segment_id)| format!("chunk={}, segment={}", chunk_id, segment_id))
                .unwrap_or_else(|| "unknown (address not in allocated range)".to_string());

            panic!(
                "Cannot decode entry header: invalid entry_type_bits={} (0x{:08x}) at address 0x{:016x} ({}). \
                Valid types are: UNDECIDED(0), CELL(1), TOMBSTONE(2). \
                This likely indicates memory corruption or reading from an invalid address.",
                entry_type_bits, entry_type_bits, pos, segment_info
            );
        }
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

    pub fn as_tombstone(&self) -> &Tombstone {
        if let EntryContent::Tombstone(ref tombstone) = self {
            return tombstone;
        } else {
            panic!("entry not tombstone");
        }
    }
}
