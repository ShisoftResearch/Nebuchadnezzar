use itertools::Itertools;
use lightning::map::{Map, ObjectMap};
use rayon::prelude::*;
use std::{
    collections::{BTreeSet, HashMap, HashSet},
    sync::Arc,
};
use tokio_stream::StreamExt;

use dovahkiin::types::{OwnedValue, SharedValue};

use crate::ram::{
    cell::{header_from_chunk_raw, select_from_chunk_raw},
    chunk::Chunk,
};

mod histogram;
pub mod sm;

pub struct SchemaStatistics {
    histogram: HashMap<u64, [OwnedValue; 10]>,
    count: usize,
    segs: usize,
    bytes: usize,
    timestamp: u64,
}

pub struct ChunkStatistics {
    schemas: ObjectMap<Arc<SchemaStatistics>>,
}

const HISTOGRAM_PARTITATION_SIZE: usize = 1024;
const HISTOGRAM_PARTITATION_DEPTH: usize = 128;

impl ChunkStatistics {
    pub fn from_chunk(chunk: &Chunk) -> Self {
        let histogram_partitations = chunk
            .cell_index
            .entries()
            .chunks(HISTOGRAM_PARTITATION_SIZE)
            .map(|s| s.to_vec())
            .collect_vec();
        let partitations: Vec<_> = histogram_partitations
            .into_par_iter()
            .map(|partitation| {
                // Build exact histogram for each of the partitation and then approximate overall histogram
                let mut sizes = HashMap::new();
                let mut segs = HashMap::new();
                let mut exact_accumlators = HashMap::new();
                let partitation_size = partitation.len();
                for (hash, _) in partitation {
                    let loc = if let Ok(ptr) = chunk.location_for_read(hash as u64) {
                        ptr
                    } else {
                        trace!("Cannot obtain cell lock {} for statistics", hash);
                        continue;
                    };
                    match header_from_chunk_raw(*loc) {
                        Ok((header, _, entry_header)) => {
                            let cell_size = entry_header.content_length as usize;
                            let cell_seg = chunk.allocator.id_by_addr(*loc);
                            let schema_id = header.schema;
                            if let Some(schema) = chunk.meta.schemas.get(&schema_id) {
                                let fields = schema.index_fields.keys().cloned().collect_vec();
                                if let Ok(partial_cell) =
                                    select_from_chunk_raw(*loc, chunk, fields.as_slice())
                                {
                                    let field_array = if fields.len() == 1 {
                                        vec![partial_cell]
                                    } else if let SharedValue::Array(arr) = partial_cell {
                                        arr
                                    } else {
                                        error!(
                                            "Cannot decode partial cell for statistics {:?}",
                                            partial_cell
                                        );
                                        continue;
                                    };
                                    for (i, val) in field_array.into_iter().enumerate() {
                                        if val == SharedValue::Null || val == SharedValue::NA {
                                            continue;
                                        }
                                        let field_id = fields[i];
                                        exact_accumlators
                                            .entry(schema_id)
                                            .or_insert_with(|| HashMap::new())
                                            .entry(field_id)
                                            .or_insert_with(|| Vec::with_capacity(partitation_size))
                                            .push(val.feature());
                                    }
                                    *sizes.entry(schema_id).or_insert(0) += cell_size;
                                    *segs.entry(schema_id).or_insert_with(0) += 1;
                                }
                            } else {
                                warn!("Cannot get schema {} for statistics", schema_id);
                            }
                        }
                        Err(e) => {
                            warn!("Failed to read {} for statistics, error: {:?}", hash, e);
                        }
                    }
                }
                let histograms: HashMap<_, _> = exact_accumlators
                    .into_iter()
                    .map(|(schema_id, schema_histograms)| {
                        let compiled_histograms = schema_histograms
                            .into_iter()
                            .map(|(field, mut items)| {
                                items.sort();
                                let depth = items.len() / HISTOGRAM_PARTITATION_DEPTH;
                                let mut histogram = (0..HISTOGRAM_PARTITATION_DEPTH)
                                    .map(|tile| items[tile * depth])
                                    .collect_vec();
                                let last_item = &items[items.len() - 1];
                                if histogram.last().unwrap() != last_item {
                                    histogram.push(*last_item);
                                }
                                (field, histogram)
                            })
                            .collect::<HashMap<_, _>>();
                        (schema_id, compiled_histograms)
                    })
                    .collect::<HashMap<_, _>>();
                (sizes, segs, histograms)
            })
            .collect();
        let schema_ids: Vec<_> = partitations
            .iter()
            .map(|(sizes, _, _)| sizes.keys())
            .flatten()
            .dedup()
            .collect();
        let total_size = schema_ids
            .iter()
            .map(|sid| {
                (
                    *sid,
                    partitations
                        .iter()
                        .map(|(sizes, _, _)| sizes.get(sid).unwrap_or(&0))
                        .sum(),
                )
            })
            .collect::<HashMap<_, _>>();
        let total_segs = schema_ids
            .iter()
            .map(|sid| {
                (
                    *sid,
                    partitations
                        .iter()
                        .map(|(_, segs, _)| segs.get(sid).unwrap_or(&0))
                        .sum(),
                )
            })
            .collect::<HashMap<_, _>>();
        
        unimplemented!()
    }
}
