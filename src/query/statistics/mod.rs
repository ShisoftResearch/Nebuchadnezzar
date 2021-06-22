use std::{collections::HashMap, sync::Arc};
use lightning::map::ObjectMap;

use dovahkiin::types::OwnedValue;

use crate::ram::chunk::Chunk;

pub mod sm;

pub struct SchemaStatistics {
  histogram: HashMap<u64, [OwnedValue; 10]>, // min and max is first and last
  count: usize
}

pub struct ChunkStatistics {
  schemas: ObjectMap<Arc<SchemaStatistics>>
}

impl ChunkStatistics {
  pub fn from_chunk(chunk: &Chunk) -> Self {
    // for (hash, ptr) in chunk.cell_index.entries() {
      
    // }
    unimplemented!()
  }
}