use cuckoofilter::*;
use index::EntryKey;
use index::Slice;
use itertools::Itertools;
use parking_lot::RwLock;
use ram::types::Id;
use std::cell::RefCell;
use std::collections::hash_map::DefaultHasher;
use std::collections::BTreeMap;
use std::collections::BTreeSet;
use std::fmt::Debug;
use std::mem;
use std::sync::Arc;

// LevelTree items cannot been added or removed individually
// Items must been merged from higher level in bulk
// Deletion will be performed when merging to or from higher level tree
// Because tree update will not to be performed in parallel. Unlike memtable, a single r/w lock
// should be sufficient. Thus concurrency control will be simple and efficient.

type Tombstones = BTreeSet<EntryKey>;

pub struct LevelTree<S>
where
    S: Slice<Item = EntryKey> + SortableEntrySlice,
{
    index: BTreeMap<EntryKey, SSPage<S>>,
    tombstones: Tombstones,
    filter: CuckooFilter<DefaultHasher>,
}
struct SSPage<S>
where
    S: Slice<Item = EntryKey> + SortableEntrySlice,
{
    slice: S,
}

impl<S> LevelTree<S>
where
    S: Slice<Item = EntryKey> + SortableEntrySlice,
{
    pub fn new() -> Self {
        Self {
            index: BTreeMap::new(),
            tombstones: Tombstones::new(),
            filter: CuckooFilter::new(), // TODO: carefully set the capacity
        }
    }

    pub fn merge(&mut self, mut source: Vec<EntryKey>, tombstones: &mut Tombstones) {
        let (keys_to_removed, mut merged) = {
            let (target_keys, target_pages) : (Vec<_>, Vec<_>) = {
                let first = source.first().unwrap();
                let last = source.last().unwrap();
                self.index.range_mut::<EntryKey, _>(first ..= last).unzip()
            };
            let page_size = target_pages.first().unwrap().slice.len();
            let mut target = target_pages
                .into_iter()
                .map(|p| p.slice.as_slice())
                .flat_map(|ps| ps)
                .map(|x| mem::replace(x, EntryKey::default()))
                .collect_vec();

            let mut merged: Vec<S> = vec![S::init()];
            let src_len = source.len();
            let tgt_len = target.len();
            let mut srci = 0;
            let mut tgti = 0;

            let mut merging_slice_i = 0;

            while srci < src_len || tgti < tgt_len {
                let mut lowest = if srci >= src_len {
                    // source overflowed
                    tgti += 1;
                    &mut target[tgti - 1]
                } else if tgti >= tgt_len {
                    // target overflowed
                    srci += 1;
                    &mut source[srci - 1]
                } else {
                    // normal case, compare
                    let t = &mut target[tgti];
                    let s = &mut source[srci];
                    if t <= s {
                        tgti += 1;
                        t
                    } else {
                        srci += 1;
                        s
                    }
                };
                mem::swap(&mut merged.last_mut().unwrap().as_slice()[merging_slice_i], lowest);
                merging_slice_i += 1;
                if merging_slice_i >= page_size {
                    merging_slice_i = 0;
                    merged.push(S::init());
                }
            }
            if merging_slice_i == 0 {
                let merged_len = merged.len();
                merged.remove( merged_len - 1);
            }
            (target_keys.into_iter().map(|x| x.clone()).collect_vec(), merged)
        };

        for k in keys_to_removed {
            self.index.remove(&k).unwrap();
        }

        for mut page in merged {
            let index = page.as_slice()[0].clone();
            self.index.insert(index, SSPage { slice: page });
        }
    }
}

pub trait SortableEntrySlice: Sized + Slice<Item = EntryKey> {
    fn merge_sorted_with(
        &mut self,
        y_slice: &mut [EntryKey],
        xlen: &mut usize,
        ylen: &mut usize,
        xtombstones: &mut Tombstones,
        ytombstones: &mut Tombstones,
    ) -> Self {
        let mut x_pos = 0;
        let mut y_pos = 0;
        let mut pos = 0;
        let mut new_x = Self::init();
        let mut new_y = Self::init();
        {
            let mut x_slice = self.as_slice();
            let x_slice_len = x_slice.len();
            debug_assert!(*ylen <= x_slice_len);
            let mut new_slice_x = new_x.as_slice();
            let mut new_slice_y = new_y.as_slice();
            let mut new_x_len = 0;
            let mut new_y_len = 0;
            let mut use_slice_at = |slice: &mut [EntryKey], slice_pos: &mut usize| {
                let item = mem::replace(&mut slice[*slice_pos], EntryKey::default());
                *slice_pos += 1;
                if xtombstones.remove(&item) || ytombstones.remove(&item) {
                    None
                } else {
                    Some(item)
                }
            };
            loop {
                let item_or_removed = if x_pos < *xlen && y_pos < *ylen {
                    if x_slice[x_pos] < y_slice[y_pos] {
                        use_slice_at(x_slice, &mut x_pos)
                    } else {
                        use_slice_at(y_slice, &mut y_pos)
                    }
                } else if x_pos < *xlen {
                    use_slice_at(x_slice, &mut x_pos)
                } else if y_pos < *ylen {
                    use_slice_at(y_slice, &mut y_pos)
                } else {
                    break;
                };

                if let Some(item) = item_or_removed {
                    if pos < x_slice_len {
                        new_slice_x[pos] = item;
                        new_x_len += 1;
                    } else {
                        new_slice_y[pos - x_slice_len] = item;
                        new_y_len += 1;
                    }
                    pos += 1;
                }
            }
            *xlen = new_x_len;
            *ylen = new_y_len;
        }
        mem::swap(self, &mut new_x);
        return new_y;
    }
}

#[cfg(test)]
mod test {}
