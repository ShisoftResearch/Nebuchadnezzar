use byteorder::{LittleEndian, WriteBytesExt, ByteOrder};
use std::ptr;
use libc;

bitflags! {
    pub struct EntryType: u8 {
        const Undecided =   0b0000_0000;
        const Cell =        0b0001_0000;
        const Tombstone =   0b0010_0000;
    }
}

#[derive(Copy, Clone)]
pub struct Entry {
    pub entry_type: EntryType,
    pub content_length: u32,
}

fn encode_len(len: u32, bytes: &mut[u8]) {
    LittleEndian::write_u32(bytes, len);
}

impl Entry {
    pub fn encode_to<W>(
        mut pos: usize,
        entry_type: EntryType,
        content_len: u32,
        len_bytes_count: u8,
        write_content: W)
        where W: Fn(usize)
    {
        let len_bytes_count_usize = len_bytes_count as usize;
        let flag_byte = len_bytes_count | entry_type.bits;
        let mut len_bytes = [0u8; 4];
        encode_len(content_len, &mut len_bytes);
        let raw_len_bytes= Box::into_raw(box len_bytes);
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

    // Returns the entry header reader returns
    pub fn decode_from<R, RR>(mut pos: usize, read: R) -> (Entry, RR)
        where R: Fn(usize, Entry) -> RR
    {
        unsafe {
            let flag_byte = *(pos as *mut u8);
            pos += 1;
            let entry_type_bits = 0b1111_0000 & flag_byte;
            let entry_type = EntryType::from_bits(entry_type_bits).unwrap();
            let entry_bytes_len = 0b0000_1111 & flag_byte;
            let entry_bytes_len_usize = entry_bytes_len as usize;
            let raw_len_bytes= Box::into_raw(box [0u8; 4]);
            libc::memmove(
                pos as *mut libc::c_void,
                raw_len_bytes as *mut libc::c_void,
                entry_bytes_len_usize);
            let entry_length = LittleEndian::read_u32(&*Box::from_raw(raw_len_bytes));
            let entry = Entry {
                entry_type,
                content_length: entry_length
            };
            pos += entry_bytes_len_usize;
            (entry, read(pos, entry))
        }
    }

    #[inline]
    pub fn size(len_bytes_count: u8, size: u32) -> u32 {
        1 + len_bytes_count as u32 + size
    }

    #[inline]
    pub fn count_len_bytes(len: u32) -> u8 {
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
}

