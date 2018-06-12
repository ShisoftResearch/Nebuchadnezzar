use ram::chunk::Chunk;
use std::sync::Arc;
use ram::segs::Segment;

pub struct CombinedCleaner;

// Combine small segments into larger segments
// This cleaner will perform a DP to find out the best combination with fewer segments to allocate
impl CombinedCleaner {
    pub fn combine_segments(chunk: &Chunk, seg: Vec<Arc<Segment>>) {

    }
}