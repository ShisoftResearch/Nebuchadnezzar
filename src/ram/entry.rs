use crate::ram::cell::CellHeader;
use crate::ram::tombstone::Tombstone;
use byteorder::{ByteOrder, LittleEndian};
use libc;

bitflags! {
    pub struct EntryType: u8 {
        const UNDECIDED =   0b0000_0000;
        const CELL =        0b0001_0000;
        const TOMBSTONE =   0b0010_0000;
    }
}

#[derive(Copy, Clone, Debug)]
pub struct EntryHeader {
    pub entry_type: EntryType,
    pub content_length: u32,
}

fn encode_len(len: u32, bytes: &mut [u8]) {
    LittleEndian::write_u32(bytes, len);
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
        len_bytes_count: u8,
        write_content: W,
    ) where
        W: Fn(usize),
    {
        let entry_type_bit = entry_type.bits;
        let len_bytes_count_usize = len_bytes_count as usize;
        let flag_byte = len_bytes_count | entry_type_bit;
        let mut len_bytes = [0u8; 4];
        encode_len(content_len, &mut len_bytes);
        let raw_len_bytes = Box::into_raw(box len_bytes);
        trace!("encoding entry header to {}, flag {:#010b}, type bits {:#010b}, len bits {:#010b}, content len {}",
               pos, flag_byte, entry_type_bit, len_bytes_count, content_len);
        unsafe {
            // write entry flags
            *(pos as *mut u8) = flag_byte;
            pos += 1;
            // write entry length
            libc::memmove(
                pos as *mut libc::c_void,
                raw_len_bytes as *mut libc::c_void,
                len_bytes_count_usize,
            );
            pos += len_bytes_count_usize;
            write_content(pos);
            // release raw pointers
            Box::from_raw(raw_len_bytes);
        }
    }

    // Returns the entry header reader returns
    pub fn decode_from<R, RR>(mut pos: usize, content_read: R) -> (EntryHeader, RR)
    where
        R: Fn(usize, EntryHeader) -> RR,
    {
        unsafe {
            let flag_byte = *(pos as *mut u8);
            pos += 1;
            let entry_type_bits = 0b1111_0000 & flag_byte;
            trace!("Entry type bits are {:#b}", entry_type_bits);
            let entry_type = EntryType::from_bits(entry_type_bits).unwrap();
            let entry_bytes_len = 0b0000_1111 & flag_byte;
            let entry_bytes_len_usize = entry_bytes_len as usize;
            let raw_len_bytes = Box::into_raw(box [0u8; 4]);
            libc::memmove(
                raw_len_bytes as *mut libc::c_void,
                pos as *mut libc::c_void,
                entry_bytes_len_usize,
            );
            let boxed_raw_len = Box::from_raw(raw_len_bytes);
            let entry_length = LittleEndian::read_u32(&*boxed_raw_len);
            let entry = EntryHeader {
                entry_type,
                content_length: entry_length,
            };

            trace!("decode entry header {}, flag {:#010b}, type bit: {:#010b}, len bits {:#010b}, len {}",
                   pos, flag_byte, entry_type_bits, entry_bytes_len, entry_length);
            pos += entry_bytes_len_usize;
            (entry, content_read(pos, entry))
        }
    }

    #[inline]
    pub fn size(len_bytes_count: u8, size: u32) -> u32 {
        1 + len_bytes_count as u32 + size
    }

    #[inline]
    pub fn count_len_bytes(len: u32) -> u8 {
        let mut n = 0;
        let mut x = len;
        while x != 0 {
            x = x >> 8;
            n += 1;
        }
        trace!("count len bytes {} -> {}", len, n);
        return n;
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
