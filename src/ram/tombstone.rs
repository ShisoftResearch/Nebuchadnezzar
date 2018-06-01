use std::io::{Cursor, Write, Read};
use ram::repr::{Entry, EntryType};
use dovahkiin::types::Id;
use byteorder::{LittleEndian, ReadBytesExt, WriteBytesExt};

lazy_static! {
    pub static ref LEN_BYTES_COUNT: u8 = Entry::count_len_bytes(TOMBSTONE_SIZE_U32);
    pub static ref ENTRY_SIZE: u32 = Entry::size(*LEN_BYTES_COUNT, TOMBSTONE_SIZE_U32);
}

pub struct Tombstone {
    pub segment_id: u64,
    pub version: u64,
    pub partition: u64,
    pub hash: u64,
}

pub const TOMBSTONE_SIZE: usize = 32;
pub const TOMBSTONE_SIZE_U32: u32 = TOMBSTONE_SIZE as u32;
type Endian = LittleEndian;
type TombstoneCursor = Cursor<Box<[u8]>>;

fn write_u64<W>(buffer: W, value: u64) where W: Write + Sized {
    buffer.write_u64::<Endian>(value).unwrap();
}

fn release_cursor(cursor: TombstoneCursor) {
    Box::into_raw(cursor.into_inner());
}

impl Tombstone {
    fn addr_to_cursor(addr: usize) -> TombstoneCursor {
        let ptr = addr as *mut [u8; TOMBSTONE_SIZE];
        unsafe {
            Cursor::new(Box::from_raw(ptr as *mut [u8]))
        }
    }

    pub fn write(&self, addr: usize) {
        Entry::encode_to(
            addr,
            EntryType::Tombstone,
            TOMBSTONE_SIZE_U32,
            *LEN_BYTES_COUNT,
        |addr| {
            let mut cursor = Tombstone::addr_to_cursor(addr);
            write_u64(cursor, self.segment_id);
            write_u64(cursor, self.version);
            write_u64(cursor, self.partition);
            write_u64(cursor, self.hash);
            release_cursor(cursor);
        })
    }

    pub fn read(addr: usize) -> Tombstone {
        Entry::decode_from(
            addr,
            |addr, _| {
                let mut cursor = Tombstone::addr_to_cursor(addr);
                let tombstone = Tombstone {
                    segment_id: cursor.read_u64::<Endian>().unwrap(),
                    version: cursor.read_u64::<Endian>().unwrap(),
                    partition: cursor.read_u64::<Endian>().unwrap(),
                    hash: cursor.read_u64::<Endian>().unwrap()
                };
                release_cursor(cursor);
                return tombstone;}).1
    }

    pub fn put(
        addr: usize,
        segment_id: u64,
        version: u64,
        partition: u64,
        hash: u64)
    {
        Tombstone {
            segment_id, version,
            partition, hash
        }.write(addr)
    }
}