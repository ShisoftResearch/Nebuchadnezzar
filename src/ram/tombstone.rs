use std::io::{Cursor, Write, Read};
use byteorder::{LittleEndian, ReadBytesExt, WriteBytesExt};

pub struct Tombstone {
    pub segment_id: u64,
    pub version: u64,
    pub partition: u64,
    pub hash: u64,
}

pub const TOMBSTONE_SIZE: usize = 32;
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
        let mut cursor = Tombstone::addr_to_cursor(addr);
        write_u64(cursor, self.segment_id);
        write_u64(cursor, self.version);
        write_u64(cursor, self.partition);
        write_u64(cursor, self.hash);
        release_cursor(cursor);
    }

    pub fn read(addr: usize) -> Tombstone {
        let mut cursor = Tombstone::addr_to_cursor(addr);
        let tombstone = Tombstone {
            segment_id: cursor.read_u64::<Endian>().unwrap(),
            version: cursor.read_u64::<Endian>().unwrap(),
            partition: cursor.read_u64::<Endian>().unwrap(),
            hash: cursor.read_u64::<Endian>().unwrap()
        };
        release_cursor(cursor);
        return tombstone;
    }
}