use libc;
use ram::segs::{Segment, SEGMENT_SIZE};

pub struct Chunk {
    pub addr: usize,
    pub segs: Vec<Segment>,

}

pub struct ChunkContianer {
    pub size: usize,
    pub list: Vec<Chunk>,
}

impl Chunk {
    fn new (size: &usize) -> Chunk {
        let mem_ptr = unsafe {libc::malloc(*size)} as usize;
        let mut segments = Vec::new();
        let seg_count = *size / SEGMENT_SIZE;
        for seg_idx in 0..seg_count {
            segments.push(Segment {
               addr: seg_idx * SEGMENT_SIZE
            });zsh
        }
        Chunk {
            addr: mem_ptr,
            segs: segments,
        }

    }
}

pub fn init(count: i32, size: u64) {

}