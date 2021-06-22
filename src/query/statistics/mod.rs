use std::{collections::HashMap, sync::Arc};
use itertools::Itertools;
use lightning::map::{Map, ObjectMap};

use dovahkiin::types::{OwnedValue, SharedValue};

use crate::ram::{cell::{CellHeader, header_from_chunk_raw, select_from_chunk_raw}, chunk::Chunk};

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
    for (hash, _) in chunk.cell_index.entries() {
      let loc = if let Ok(ptr) = chunk.location_for_read(hash as u64) {
        ptr
      } else {
        trace!("Cannot obtain cell lock {} for statistics", hash);
        continue;
      };
      match header_from_chunk_raw(*loc) {
        Ok((header, _)) => {
          let schema_id = header.schema;
          if let Some(schema) = chunk.meta.schemas.get(&schema_id) {
            let fields = schema.index_fields.keys().cloned().collect_vec();
            if let Ok(partial_cell) = select_from_chunk_raw(*loc, chunk, fields.as_slice()) {
              drop(loc); // Release lock for the cell early
              let field_array = if fields.len() == 1 {
                vec![partial_cell]
              } else if let SharedValue::Array(arr) = partial_cell {
                arr    
              } else {
                error!("Cannot decode partial cell for statistics {:?}", partial_cell);
                continue;
              };
              for (i, val) in field_array.into_iter().enumerate() {
                if val == SharedValue::Null || val == SharedValue::NA {
                  continue;
                }
                let field_id = fields[i];
                // TODO: build the histogram
              }
            }
          } else {
            warn!("Cannot get schema {} for statistics", schema_id);
          }
        },
        Err(e) => {
          warn!("Failed to read {} for statistics, error: {:?}", hash, e);
        }
      }
    }
    unimplemented!()
  }
}