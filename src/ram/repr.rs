use byteorder::{LittleEndian, WriteBytesExt};
use std::ptr;
use libc;

bitflags! {
    pub struct EntryTypes: u8 {
        const Cell = 0b00010000;
        const Tomestone = 0b00110000;
    }
}

pub struct Entry {
    entry_flag: u8,
    entry_length: u32,
}

fn count_len_bytes(len: u32) -> u8 {
    let in_bits = 32;
    let msb = 1 << (in_bits - 1);
    let mut count: u8 = 0;
    for i in 0..in_bits
    {
        if (len << i) & msb > 0 {
            break;
        };
        count += 1;
    }
    let bytes = count / 8;
    assert!(bytes <= 4);
    return bytes;
}

fn compress_len(len: u32, bytes: &mut[u8]) {
    bytes.write_u32::<LittleEndian>(len).unwrap();
}

impl Entry {
    pub fn encode_to<W>(mut pos: usize, entry_type: EntryTypes, content_len: u32, write_content: W)
        where W: Fn(usize)
    {
        let len_bytes_count = count_len_bytes(content_len);
        let len_bytes_count_usize = len_bytes_count as usize;
        let flag_byte = len_bytes_count | entry_type.bits;
        let mut len_bytes = Vec::with_capacity(len_bytes_count_usize);
        compress_len(content_len, &mut len_bytes);
        let raw_len_bytes= Box::into_raw(len_bytes.into_boxed_slice());
        unsafe {
            // write entry flags
            *(pos as *mut u8) = flag_byte;
            pos += 1;
            // write entry length
            libc::memmove(
                pos as *mut libc::c_void,
                raw_len_bytes as *mut libc::c_void,
                len_bytes_count_usize);
            pos += len_bytes_count_usize;
            write_content(pos);
            // release raw pointers
            Box::from_raw(raw_len_bytes);
        }
    }

    pub fn decode_from(mut pos: usize) -> (Entry, usize) {
        unsafe {
            let flag_byte = *(pos as *mut u8);
            let entry_type_bits = 0b11110000 & flag_byte;
            let entry_type = EntryTypes::from_bits(entry_type_bits);
            let entry_bytes_len = 0b00001111 & flag_byte;

        }
    }
}

