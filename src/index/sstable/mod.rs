use cuckoofilter::*;
use index::btree::Ordering;
use index::EntryKey;
use index::Slice;
use itertools::Itertools;
use parking_lot::RwLock;
use ram::types::Id;
use std::cell::RefCell;
use std::collections::btree_map::Range;
use std::collections::hash_map::DefaultHasher;
use std::collections::BTreeMap;
use std::collections::BTreeSet;
use std::fmt::Debug;
use std::mem;
use std::sync::Arc;
use index::id_from_key;

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

    pub fn seek(&self, key: &EntryKey, ordering: Ordering) -> RTCursor<S> {
        let mut range = match ordering {
            Ordering::Forward => self.index.range::<EntryKey, _>(key..),
            Ordering::Backward => self.index.range::<EntryKey, _>(..=key),
        };
        let first = match ordering {
            Ordering::Forward => range.next(),
            Ordering::Backward => range.next_back()
        }.map(|(_, page)| page);

        let (pos, page_len) = if let Some(page) = first {
            (
                page.slice.as_slice_immute().binary_search(key).unwrap_or_else(|i| i),
                page.slice.as_slice_immute().len()
            )
        } else { (0, 0) };
        let mut cursor = RTCursor { ordering, range, page_len, current: first, index: pos};

        if cursor.current.is_some() {
            match ordering {
                Ordering::Forward => {
                    if pos >= page_len {
                        cursor.next();
                    }
                }
                Ordering::Backward => {}
            }
        }

        cursor
    }

    pub fn merge(&mut self, mut source: Vec<EntryKey>, tombstones: &mut Tombstones) {
        let (keys_to_removed, mut merged) = {
            let (target_keys, target_pages): (Vec<_>, Vec<_>) = {
                let first = source.first().unwrap();
                let last = source.last().unwrap();
                self.index.range_mut::<EntryKey, _>(first..=last).unzip()
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
                let mut from_source = false;
                let mut lowest = if srci >= src_len {
                    // source overflowed
                    tgti += 1;
                    &mut target[tgti - 1]
                } else if tgti >= tgt_len {
                    // target overflowed
                    srci += 1;
                    from_source = true;
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
                        from_source = true;
                        s
                    }
                };
                if from_source {
                    if tombstones.remove(lowest) {
                        continue;
                    }
                } else {
                    if self.tombstones.remove(lowest) {
                        continue;
                    }
                }
                mem::swap(
                    &mut merged.last_mut().unwrap().as_slice()[merging_slice_i],
                    lowest,
                );
                merging_slice_i += 1;
                if merging_slice_i >= page_size {
                    merging_slice_i = 0;
                    merged.push(S::init());
                }
            }
            if merging_slice_i == 0 {
                let merged_len = merged.len();
                merged.remove(merged_len - 1);
            }
            (
                target_keys.into_iter().map(|x| x.clone()).collect_vec(),
                merged,
            )
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

pub trait SortableEntrySlice: Sized + Slice<Item = EntryKey> {}

pub struct RTCursor<'a, S>
where
    S: Slice<Item = EntryKey> + SortableEntrySlice,
{
    index: usize,
    ordering: Ordering,
    page_len: usize,
    range: Range<'a, EntryKey, SSPage<S>>,
    current: Option<&'a SSPage<S>>
}

impl<'a, S> RTCursor<'a, S> where S: Slice<Item = EntryKey> + SortableEntrySlice {
    pub fn next(&mut self) -> bool {
        if self.current.is_some() {
            match self.ordering {
                Ordering::Forward => {
                    if self.index + 1 >= self.page_len {
                        if let Some((_, next_page)) = self.range.next() {
                            self.current = Some(next_page);
                            self.index = 0;
                        } else {
                            self.current = None;
                            return false;
                        }
                    } else {
                        self.index += 1;
                    }
                },
                Ordering::Backward => {
                    if self.index == 0 {
                        if let Some((_, next_page)) = self.range.next_back() {
                            self.current = Some(next_page);
                            self.index = self.page_len - 1;
                        } else {
                            self.current = None;
                            return false;
                        }
                    } else {
                        self.index -= 1;
                    }
                }
            }
            true
        } else { false }
    }

    pub fn current(&self) -> Option<&EntryKey> {
        if self.index <= 0 || self.index >= self.page_len {
            return None;
        }
        if let Some(ref page) = self.current {
            Some(&page.slice.as_slice_immute()[self.index])
        } else {
            None
        }
    }
}

#[cfg(test)]
mod test {}
