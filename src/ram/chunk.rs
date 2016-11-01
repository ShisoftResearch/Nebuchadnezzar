use libc;
use ram::segs::{Segment, SEGMENT_SIZE};
use server::ServerMeta;
use std::thread;
use std::sync::{Arc, Mutex};
use concurrent_hashmap::ConcHashMap;
use std::rc::Rc;
use ram::schema::Schemas;

pub struct Chunk {
    pub addr: usize,
    pub segs: Vec<Segment>,
    pub index: ConcHashMap<u64, usize>,
    pub meta: Rc<ServerMeta>
}

pub struct Chunks {
    pub list: Vec<Chunk>,
}

impl Chunk {
    fn new (size: usize, meta: Rc<ServerMeta>) -> Chunk {
        let mem_ptr = unsafe {libc::malloc(size)} as usize;
        let mut segments = Vec::new();
        let seg_count = size / SEGMENT_SIZE;
        for seg_idx in 0..seg_count {
            segments.push(Segment {
               addr: seg_idx * SEGMENT_SIZE
            });
        }
        info!("creating chunk at {}, segments {}", mem_ptr, seg_count + 1);
        Chunk {
            addr: mem_ptr,
            segs: segments,
            index: ConcHashMap::<u64, usize>::new(),
            meta: meta
        }

    }
    fn dispose (&mut self) {
        info!("disposing chunk at {}", self.addr);
        unsafe {
            libc::free(self.addr as *mut libc::c_void)
        }
    }
}

impl Drop for Chunk {
    fn drop(&mut self) {
        self.dispose();
    }
}

impl Chunks {
    pub fn new (count: usize, size: usize, meta: Rc<ServerMeta>) -> Chunks {
        let chunk_size = size / count;
        let mut chunks = Vec::new();
        info!("Creating {} chunks, total {} bytes", count, size);
        for _ in 0..count {
            chunks.push(Chunk::new(chunk_size, meta.clone()));
        }
        Chunks {
            list: chunks
        }
    }

    pub fn new_dummy(count: usize, size: usize) -> Chunks {
        Chunks::new(count, size, Rc::<ServerMeta>::new(ServerMeta {
            schemas: Schemas::new()
        }))
    }
}