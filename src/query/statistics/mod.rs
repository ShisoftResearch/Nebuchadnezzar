use itertools::Itertools;
use lightning::map::{LiteHashMap, Map};
use rayon::prelude::*;
use std::{
    cmp::max,
    collections::{HashMap, HashSet},
    iter,
    sync::{
        atomic::{AtomicU32, Ordering},
        Arc,
    },
};

use dovahkiin::types::SharedValue;

use crate::ram::{
    cell::{header_from_chunk_raw, select_from_chunk_raw},
    chunk::Chunk,
    clock::now,
};

#[derive(Debug)]
pub struct SchemaStatistics {
    pub histogram: HashMap<u64, TargetHistogram>,
    pub count: usize,
    pub segs: usize,
    pub bytes: usize,
    pub timestamp: u32,
}

pub struct ChunkStatistics {
    pub timestamp: AtomicU32,
    pub changes: AtomicU32,
    pub schemas: LiteHashMap<u32, Arc<SchemaStatistics>>,
}

const HISTOGRAM_PARTITATION_SIZE: usize = 1024;
const HISTOGRAM_PARTITATION_BUCKETS: usize = 100;
#[cfg(test)]
const HISTOGRAM_PARTITATION_KEYS: usize = HISTOGRAM_PARTITATION_BUCKETS + 1;
const HISTOGRAM_TARGET_BUCKETS: usize = HISTOGRAM_PARTITATION_BUCKETS;
const HISTOGRAM_TARGET_KEYS: usize = HISTOGRAM_TARGET_BUCKETS + 1;
const REFRESH_CHANGES_THRESHOLD: u32 = 512;

type HistogramKey = [u8; 8];
type TargetHistogram = [HistogramKey; HISTOGRAM_TARGET_KEYS];

impl ChunkStatistics {
    pub fn new() -> Self {
        Self {
            timestamp: AtomicU32::new(0),
            changes: AtomicU32::new(0),
            schemas: LiteHashMap::with_capacity(32),
        }
    }
    pub fn refresh_from_chunk(&self, chunk: &Chunk) {
        let last_update = self.timestamp.load(Ordering::Relaxed);
        let refresh_changes = self.changes.fetch_add(1, Ordering::Relaxed);
        // Refresh rate 10 seconds
        if refresh_changes < REFRESH_CHANGES_THRESHOLD || now() - last_update < 10 {
            return;
        }
        self.ensured_refresh_chunk(chunk)
    }

    pub fn ensured_refresh_chunk(&self, chunk: &Chunk) {
        let refresh_changes = self.changes.load(Ordering::Relaxed);
        debug!(
            "Building histogram for chunk {}, changes {}",
            chunk.id, refresh_changes
        );
        let histogram_partitations = chunk
            .cell_index
            .entries()
            .chunks(HISTOGRAM_PARTITATION_SIZE)
            .map(|s| s.to_vec())
            .collect_vec();
        debug!(
            "Histogram for chunk {} have {} partitations",
            chunk.id,
            histogram_partitations.len()
        );
        let partitations: Vec<_> = histogram_partitations
            .into_par_iter()
            .map(|partitation| build_partitation_statistics(partitation, chunk))
            .collect();
        debug!("Total of {} partitations", partitations.len());
        let schema_ids: Vec<_> = partitations
            .iter()
            .map(|(sizes, _, _, _)| sizes.keys())
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
                        .map(|(sizes, _, _, _)| sizes.get(sid).unwrap_or(&0))
                        .sum::<usize>(),
                )
            })
            .collect::<HashMap<_, _>>();
        let total_counts = schema_ids
            .iter()
            .map(|sid| {
                (
                    *sid,
                    partitations
                        .iter()
                        .map(|(_, _, counts, _)| counts.get(sid).unwrap_or(&0))
                        .sum::<usize>(),
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
                        .map(|(_, segs, _, _)| segs.get(sid).map(|set| set.len()).unwrap_or(0))
                        .sum::<usize>(),
                )
            })
            .collect::<HashMap<_, _>>();
        let empty_histo = Default::default();
        let mut schema_histograms = schema_ids
            .iter()
            .map(|sid| {
                (*sid, {
                    let parted_histos = partitations
                        .iter()
                        .map(|(_, _, _, histo)| histo.get(sid).unwrap_or(&empty_histo))
                        .collect_vec();
                    let field_ids = parted_histos
                        .iter()
                        .map(|histo_map| histo_map.keys())
                        .flatten()
                        .dedup()
                        .collect::<Vec<_>>();
                    field_ids
                        .par_iter()
                        .map(|field_id| {
                            let schema_field_histograms = parted_histos
                                .iter()
                                .map(|histo_map| &histo_map[field_id])
                                .collect_vec();
                            (**field_id, build_histogram(schema_field_histograms))
                        })
                        .collect::<HashMap<u64, _>>()
                })
            })
            .collect::<HashMap<_, _>>();
        let now = now();
        for schema_id in schema_ids {
            let statistics = SchemaStatistics {
                histogram: schema_histograms.remove(&schema_id).unwrap(),
                count: *total_counts.get(&schema_id).unwrap(),
                segs: *total_segs.get(&schema_id).unwrap(),
                bytes: *total_size.get(&schema_id).unwrap(),
                timestamp: now,
            };
            self.schemas.insert(*schema_id, Arc::new(statistics));
        }
        self.timestamp.store(now, Ordering::Relaxed);
        self.changes.fetch_sub(refresh_changes, Ordering::Relaxed);
    }
}

fn build_partitation_statistics(
    partitation: Vec<(usize, usize)>,
    chunk: &Chunk,
) -> (
    HashMap<u32, usize>,
    HashMap<u32, HashSet<usize>>,
    HashMap<u32, usize>,
    HashMap<u32, HashMap<u64, (Vec<HistogramKey>, usize, usize)>>,
) {
    // Build exact histogram for each of the partitation and then approximate overall histogram
    debug!(
        "Building partitation for chunk {} with {} cells",
        chunk.id,
        partitation.len()
    );
    let mut sizes = HashMap::new();
    let mut segs = HashMap::new();
    let mut counts = HashMap::new();
    let mut exact_accumlators = HashMap::new();
    let partitation_size = partitation.len();
    for (hash, _addr) in partitation {
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
                    if !fields.is_empty() {
                        trace!("Schema {} has fields {:?}", schema_id, fields);
                        if let Ok((partial_cell, _)) =
                            select_from_chunk_raw(*loc, chunk, fields.as_slice())
                        {
                            let field_array = match partial_cell {
                                SharedValue::Map(map) => fields
                                    .iter()
                                    .filter_map(|path_key| schema.id_index.get(path_key))
                                    .map(|key| map.get_in_by_ids(key.iter()).clone())
                                    .collect_vec(),
                                _ => unreachable!(
                                    "Other data structure is not possible. Got {:?}",
                                    partial_cell
                                ),
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
                        }
                    }
                    *counts.entry(schema_id).or_insert(0) += 1;
                    *sizes.entry(schema_id).or_insert(0) += cell_size;
                    segs.entry(schema_id)
                        .or_insert_with(|| HashSet::new())
                        .insert(cell_seg);
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
                .map(|(field, items)| {
                    let num_items = items.len();
                    let (histogram, depth) = build_partitation_histogram(items);
                    (field, (histogram, num_items, depth))
                })
                .collect::<HashMap<_, _>>();
            (schema_id, compiled_histograms)
        })
        .collect::<HashMap<_, _>>();
    (sizes, segs, counts, histograms)
}

fn build_partitation_histogram(mut items: Vec<HistogramKey>) -> (Vec<HistogramKey>, usize) {
    if items.len() <= HISTOGRAM_PARTITATION_BUCKETS {
        return (items, 1);
    }
    items.sort();
    let depth = items.len() / HISTOGRAM_PARTITATION_BUCKETS;
    let mut histogram = (0..HISTOGRAM_PARTITATION_BUCKETS)
        .map(|tile| items[tile * depth])
        .collect_vec();
    let last_item = &items[items.len() - 1];
    if histogram.last().unwrap() != last_item {
        histogram.push(*last_item);
    }
    (histogram, depth)
}

fn build_histogram(partitations: Vec<&(Vec<HistogramKey>, usize, usize)>) -> TargetHistogram {
    let num_all_keys: usize = partitations.iter().map(|(h, _, _)| h.len()).sum();
    if num_all_keys < HISTOGRAM_TARGET_KEYS {
        // debug!("Building histogram with repeatdly keys");
        return repeated_histogram(partitations);
    }
    // Build the approximated histogram from partitation histograms
    // https://arxiv.org/abs/1606.05633
    // debug!("Building histogram with approximation");
    let mut part_idxs = vec![0; partitations.len()];
    let part_histos = partitations
        .iter()
        .map(|(histo, _, _)| histo)
        .filter(|histo| !histo.is_empty())
        .collect_vec();
    let num_total = partitations.iter().map(|(_, num, _)| num).sum::<usize>();
    let part_depths = partitations
        .iter()
        .map(|(_, _, depth)| *depth)
        .collect_vec();
    let max_key = partitations
        .iter()
        .filter_map(|(part, _, _)| part.last())
        .max()
        .unwrap();
    let target_width = num_total / HISTOGRAM_TARGET_BUCKETS;
    let mut target_histogram = [[0u8; 8]; HISTOGRAM_TARGET_KEYS];
    // Perform a merge sort for sorted pre-histogram
    let mut filled = target_width;
    let mut last_key = Default::default();
    'HISTO_CONST: for i in 0..HISTOGRAM_TARGET_BUCKETS {
        loop {
            let (key, ended) = if let Some((part_idx, histo)) = part_histos
                .iter()
                .enumerate()
                .filter(|(i, h)| {
                    let idx = part_idxs[*i];
                    idx < h.len()
                })
                .min_by(|(i1, h1), (i2, h2)| {
                    let h1_idx = part_idxs[*i1];
                    let h2_idx = part_idxs[*i2];
                    h1[h1_idx].cmp(&h2[h2_idx])
                }) {
                let histo_idx = part_idxs[part_idx];
                part_idxs[part_idx] += 1;
                ((histo[histo_idx], part_idx), false)
            } else {
                (last_key, true)
            };
            last_key = key;
            let idx = last_key.1;
            if filled >= target_width || ended {
                target_histogram[i] = last_key.0;
                filled = 0;
                continue 'HISTO_CONST;
            }
            filled += part_depths[idx];
        }
    }
    target_histogram[HISTOGRAM_TARGET_BUCKETS] = max_key.clone();
    target_histogram
}

fn repeated_histogram(partitations: Vec<&(Vec<HistogramKey>, usize, usize)>) -> TargetHistogram {
    let combined = partitations
        .iter()
        .map(|(histo, _, depth)| {
            let depth = *depth;
            histo.iter().map(move |k| (k, depth))
        })
        .flatten()
        .sorted_by(|a, b| Ord::cmp(&a.0, &b.0))
        .collect_vec();
    let total_keys: usize = combined.iter().map(|(_, d)| *d).sum();
    let repeat_ratio: f64 = HISTOGRAM_TARGET_KEYS as f64 / total_keys as f64;
    let repeated = combined
        .iter()
        .map(|(k, d)| iter::repeat(*k).take((*d as f64 * repeat_ratio).ceil() as usize))
        .flatten()
        .collect_vec();
    let mut histo = empty_target_histogram();
    debug_assert!(repeated.len() >= histo.len());
    histo
        .iter_mut()
        .zip(repeated.into_iter())
        .for_each(|(h, k)| {
            *h = *k;
        });
    *histo.last_mut().unwrap() = combined.last().unwrap().0.clone();
    histo
}

fn empty_target_histogram() -> TargetHistogram {
    [[0u8; 8]; HISTOGRAM_TARGET_KEYS]
}

pub fn merge_statistics(all_stats: Vec<Arc<SchemaStatistics>>) -> Option<SchemaStatistics> {
    if all_stats.is_empty() {
        return None;
    }
    let mut count = 0;
    let mut segs = 0;
    let mut bytes = 0;
    let mut timestamp = 0;
    all_stats.iter().for_each(|s| {
        count += s.count;
        segs += s.segs;
        bytes += s.bytes;
        timestamp = max(timestamp, s.timestamp);
    });
    let histogram = all_stats
        .iter()
        .map(|s| {
            let s_count = s.count;
            s.histogram
                .iter()
                .map(move |(field, keys)| (field, keys, s_count))
        })
        .flatten()
        .group_by(|(field, _, _)| **field)
        .into_iter()
        .map(|(field, parts)| {
            let parts = parts
                .into_iter()
                .map(|(_, keys, s_count)| {
                    let num_keys = keys.len();
                    (keys.to_vec(), s_count, s_count / num_keys)
                })
                .collect::<Vec<_>>();
            let histo = build_histogram(parts.iter().collect());
            (field, histo)
        })
        .collect::<HashMap<_, _>>();
    Some(SchemaStatistics {
        histogram,
        count,
        segs,
        bytes,
        timestamp,
    })
}

#[cfg(test)]
mod tests {
    use dovahkiin::types::{key_hash, Id, OwnedMap, OwnedValue};
    use rand::Rng;

    use crate::ram::cell::{CellHeader, OwnedCell};
    use crate::ram::segs::SEGMENT_SIZE;
    use crate::ram::types::RandValue;
    use crate::{
        ram::{
            chunk::Chunks,
            schema::{LocalSchemasCache, Schema},
            tests::default_fields,
        },
        server::ServerMeta,
    };

    use super::*;

    #[test]
    fn partitation_histogram() {
        let small_set = (0..10).map(|n| OwnedValue::U64(n).feature()).collect_vec();
        assert_eq!(
            build_partitation_histogram(small_set.clone()),
            (small_set, 1)
        );
        let eq_set = (0..HISTOGRAM_PARTITATION_BUCKETS)
            .map(|n| OwnedValue::U64(n as u64).feature())
            .collect_vec();
        assert_eq!(build_partitation_histogram(eq_set.clone()), (eq_set, 1));

        let double_set = (0..HISTOGRAM_PARTITATION_BUCKETS * 2)
            .map(|n| OwnedValue::U64(n as u64).feature())
            .collect_vec();
        let mut expect = double_set.iter().step_by(2).cloned().collect_vec();
        expect.push(double_set.last().unwrap().to_owned());
        assert_eq!(build_partitation_histogram(double_set), (expect, 2));

        let triple_set = (0..HISTOGRAM_PARTITATION_BUCKETS * 3)
            .map(|n| OwnedValue::U64(n as u64).feature())
            .collect_vec();
        let mut expect = triple_set.iter().step_by(3).cloned().collect_vec();
        expect.push(triple_set.last().unwrap().to_owned());
        assert_eq!(build_partitation_histogram(triple_set), (expect, 3));
    }

    #[test]
    fn approximated_histogram() {
        // Test with example from the paper
        let histo_1 = vec![2, 7, 18, 25];
        let histo_1_height = 4;

        let histo_2 = vec![3, 15, 24, 30];
        let histo_2_height = 5;

        let test_data = vec![
            (
                histo_1
                    .iter()
                    .map(|n| OwnedValue::U64(*n).feature())
                    .collect::<Vec<_>>(),
                (histo_1.len() - 1) * histo_1_height,
                histo_1_height,
            ),
            (
                histo_2
                    .iter()
                    .map(|n| OwnedValue::U64(*n).feature())
                    .collect::<Vec<_>>(),
                (histo_2.len() - 1) * histo_2_height,
                histo_2_height,
            ),
        ];
        // Test for the repeatdly case
        let histogram = build_histogram(test_data.iter().collect_vec());
        assert!(histogram.is_sorted());
        assert_eq!(histogram.last().unwrap(), &OwnedValue::U64(30).feature());

        let histo_1 = (0..1024)
            .map(|n| OwnedValue::U64(n).feature())
            .collect_vec();
        let histo_2 = (0..1024)
            .map(|n| OwnedValue::U64(n).feature())
            .collect_vec();
        let histo_3 = (0..=1024)
            .map(|n| OwnedValue::U64(n).feature())
            .collect_vec();
        let histo_1_height = histo_1.len();
        let histo_2_height = histo_2.len();
        let test_data = vec![
            (histo_1, histo_1_height, 1),
            (histo_2, histo_2_height, 2),
            (histo_3, histo_2_height, 3),
        ];
        let histogram = build_histogram(test_data.iter().collect_vec());
        assert!(histogram.is_sorted(), "Got {:?}", histogram);
        assert_eq!(histogram.last().unwrap(), &OwnedValue::U64(1024).feature());
    }

    const CHUNK_TEST_SIZE: usize = REFRESH_CHANGES_THRESHOLD as usize * 16;
    #[test]
    fn chunk_statistics() {
        let _ = env_logger::try_init();
        let fields = default_fields();
        let schema = Schema::new("dummy", None, fields, false, true);
        let schemas = LocalSchemasCache::new_local("");
        schemas.new_schema(schema.clone());
        let schema_id = schema.id;
        let chunks = Chunks::new(
            1,
            SEGMENT_SIZE,
            Arc::new(ServerMeta { schemas }),
            None,
            None,
            None,
        );
        let mut rng = rand::thread_rng();
        for i in 0..CHUNK_TEST_SIZE {
            let mut data_map = OwnedMap::new();
            data_map.insert(&String::from("id"), OwnedValue::I64(i as i64));
            data_map.insert(
                &String::from("score"),
                OwnedValue::U64(rng.gen_range(60..100)),
            );
            data_map.insert(
                &String::from("name"),
                OwnedValue::String(String::from("Jack")),
            );
            let data = OwnedValue::Map(data_map);
            let header = CellHeader::new(schema_id, &Id::rand());
            let mut cell = OwnedCell { data, header };
            chunks.write_cell(&mut cell).unwrap();
        }
        let stats = chunks.all_chunk_statistics(schema_id);
        assert_eq!(stats.len(), 1);
        let stat = stats[0].as_ref().unwrap();
        debug!("Stat {:?}", &*stat);
        assert!(stat.count > 0, "Statistics should be triggered");
        assert!(stat.bytes > 0, "Statistics should have bytes");
        assert!(stat.timestamp > 0, "timestamp should not be zero");
        assert!(stat.segs > 0, "Segs should not be zero");
        chunks.ensure_statistics();
        let stats = chunks.all_chunk_statistics(schema_id);
        let stat = stats[0].as_ref().unwrap();
        info!("Statistics fields: {:?}", stat.histogram.keys());
        assert_eq!(stat.histogram.len(), 2, "Should have 2 statistics fields");
        let id_key = key_hash("id");
        let score_key = key_hash("score");
        assert!(stat.histogram.contains_key(&id_key));
        assert!(stat.histogram.contains_key(&score_key));
        let id_histo = stat.histogram[&id_key];
        let score_histo = stat.histogram[&score_key];
        assert_eq!(id_histo.len(), score_histo.len());
        assert_eq!(id_histo.len(), HISTOGRAM_PARTITATION_KEYS);
        assert_eq!(score_histo.len(), HISTOGRAM_PARTITATION_KEYS);
        assert_eq!(
            id_histo[0],
            0u64.to_be_bytes(),
            "Histogram does not include minimal"
        );
        assert_eq!(
            id_histo[id_histo.len() - 1],
            (CHUNK_TEST_SIZE as u64 - 1).to_be_bytes(),
            "Histogram does not include maxnimal"
        );
        // TODO: Test on the distribution
    }
}
