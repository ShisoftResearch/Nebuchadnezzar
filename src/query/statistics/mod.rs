use std::collections::BTreeMap;

use dovahkiin::types::OwnedValue;

pub mod sm;

pub struct SchemaStatistics {
  histogram: [OwnedValue; 10], // min and max is first and last
  count: usize
}

pub struct ChunkStatistics {

}