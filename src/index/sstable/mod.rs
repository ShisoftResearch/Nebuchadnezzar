use bifrost::utils::fut_exec::wait;
use client::AsyncClient;
use cuckoofilter::*;
use dovahkiin::types::custom_types::bytes::SmallBytes;
use dovahkiin::types::custom_types::id::Id;
use dovahkiin::types::custom_types::map::Map;
use dovahkiin::types::type_id_of;
use dovahkiin::types::value::ToValue;
use index::btree::Ordering;
use index::id_from_key;
use index::EntryKey;
use index::Slice;
use itertools::Itertools;
use parking_lot::Mutex;
use parking_lot::RwLock;
use ram::cell::Cell;
use ram::types::*;
use std::cell::RefCell;
use std::collections::btree_map::Range;
use std::collections::hash_map::DefaultHasher;
use std::collections::{BTreeMap, BTreeSet, LinkedList};
use std::fmt::Debug;
use std::marker::PhantomData;
use std::mem;
use std::sync::Arc;
use utils::lru_cache::LRUCache;

// LevelTree items cannot been added or removed individually
// Items must been merged from higher level in bulk
// Deletion will be performed when merging to or from higher level tree
// Because tree update will not to be performed in parallel. Unlike memtable, a single r/w lock
// should be sufficient. Thus concurrency control will be simple and efficient.

type Tombstones = BTreeSet<EntryKey>;
type PageCache<S> = Arc<Mutex<LRUCache<Id, Arc<SSPage<S>>>>>;

const PAGE_SCHEMA: &'static str = "NEB_SSTABLE_PAGE";

lazy_static! {
    static ref PAGE_SCHEMA_HASH: u32 = key_hash(PAGE_SCHEMA) as u32;
}

pub struct LevelTree<S>
where
    S: Slice<Item = EntryKey> + SortableEntrySlice,
{
    neb_client: Arc<AsyncClient>,
    index: BTreeMap<EntryKey, Id>,
    tombstones: Tombstones,
    filter: CuckooFilter<DefaultHasher>,
    pages: PageCache<S>,
    marker: PhantomData<S>,
}
struct SSPage<S>
where
    S: Slice<Item = EntryKey> + SortableEntrySlice,
{
    id: Id,
    slice: S,
}

impl<S> SSPage<S>
where
    S: Slice<Item = EntryKey> + SortableEntrySlice,
{
    pub fn from_cell(cell: Cell) -> Self {
        let mut slice = S::init();
        let slice_len = slice.as_slice().len();
        let cell_id = cell.id();
        let keys = cell.data;
        let keys_len = keys.len().unwrap();
        debug_assert!(slice_len == keys_len);
        let keys_array = if let Value::PrimArray(PrimitiveArray::SmallBytes(ref array)) = keys {
            array
        } else {
            panic!()
        };
        for (i, val) in keys_array.iter().enumerate() {
            slice.as_slice()[i] = EntryKey::from(val.as_slice());
        }
        Self {
            id: cell_id,
            slice,
        }
    }
}

pub fn page_slice_to_cell<S>(slice: S, id: &Id) -> Cell
where
    S: Slice<Item = EntryKey>,
{
    let value = slice
        .as_slice_immute()
        .iter()
        .map(|key| SmallBytes::from_vec(key.as_slice().to_vec()))
        .collect_vec()
        .value();
    Cell::new_with_id(*PAGE_SCHEMA_HASH, id, value)
}

impl<S> LevelTree<S>
where
    S: Slice<Item = EntryKey> + SortableEntrySlice,
{
    pub fn new(neb_client: &Arc<AsyncClient>) -> Self {
        let client = neb_client.clone();
        Self {
            neb_client: neb_client.clone(),
            index: BTreeMap::new(),
            tombstones: Tombstones::new(),
            filter: CuckooFilter::new(), // TODO: carefully set the capacity
            pages: Arc::new(Mutex::new(LRUCache::new(
                100,
                move |id| {
                    wait(client.read_cell(*id))
                        .unwrap()
                        .map(|cell| Arc::new(SSPage::from_cell(cell)))
                        .ok()
                },
                |_, _| {
                    // will do nothing on evict for pages will be write to back storage once updated
                },
            ))),
            marker: PhantomData {},
        }
    }

    pub fn seek(&self, key: &EntryKey, ordering: Ordering) -> RTCursor<S> {
        let mut range = match ordering {
            Ordering::Forward => self.index.range::<EntryKey, _>(key..),
            Ordering::Backward => self.index.range::<EntryKey, _>(..=key),
        };
        let first = match ordering {
            Ordering::Forward => range.next(),
            Ordering::Backward => range.next_back(),
        }
        .map(|(_, page_id)| self.pages.lock().get_or_fetch(page_id).unwrap().clone());

        let (pos, page_len) = if let Some(ref page) = first {
            (
                page.slice
                    .as_slice_immute()
                    .binary_search(key)
                    .unwrap_or_else(|i| i),
                page.slice.as_slice_immute().len(),
            )
        } else {
            (0, 0)
        };
        let mut cursor = RTCursor {
            ordering,
            range,
            page_len,
            current: first,
            index: pos,
            cache: self.pages.clone(),
        };

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
        let mut pages_cache = self.pages.lock();
        let (keys_to_removed, mut merged, mut ids_to_reuse) = {
            let (target_keys, target_pages): (Vec<_>, Vec<_>) = {
                let first = source.first().unwrap();
                let last = source.last().unwrap();
                self.index
                    .range::<EntryKey, _>(first..=last)
                    .map(|(k, id)| (k, pages_cache.get_or_fetch(id).unwrap().clone()))
                    .unzip()
            };
            let page_size = target_pages.first().unwrap().slice.len();
            let target_page_ids = target_pages.iter().map(|p| p.id).collect::<LinkedList<_>>();
            let mut target = target_pages
                .iter()
                .map(|page| page.slice.as_slice_immute())
                .flat_map(|x| x)
                .map(|x| (*x).clone())
                .collect_vec();

            let mut merged: Vec<S> = vec![S::init()];
            let src_len = source.len();
            let tgt_len = target.len();
            let mut srci = 0;
            let mut tgti = 0;

            let mut merging_slice_i = 0;

            while srci < src_len || tgti < tgt_len {
                let mut from_source = false;
                let mut lowest: &mut EntryKey = if srci >= src_len {
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
                target_page_ids
            )
        };

        for k in keys_to_removed {
            self.index.remove(&k).unwrap();
        }

        for id in &ids_to_reuse {
            pages_cache.remove(id);
        }

        for mut page in merged {
            let index_key = page.as_slice()[0].clone();
            let reuse_id = ids_to_reuse.pop_back();
            let id = reuse_id.unwrap_or_else(|| Id::rand());
            let cell = page_slice_to_cell(page, &id);
            if reuse_id.is_some() {
                wait(self.neb_client.update_cell(cell)).unwrap();
            } else {
                wait(self.neb_client.write_cell(cell)).unwrap();
            }
            self.index.insert(index_key, id);
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
    range: Range<'a, EntryKey, Id>,
    current: Option<Arc<SSPage<S>>>,
    cache: PageCache<S>,
}

impl<'a, S> RTCursor<'a, S>
where
    S: Slice<Item = EntryKey> + SortableEntrySlice,
{
    pub fn next(&mut self) -> bool {
        if self.current.is_some() {
            match self.ordering {
                Ordering::Forward => {
                    if self.index + 1 >= self.page_len {
                        if let Some((_, page_id)) = self.range.next() {
                            let next_page = self.get_page(page_id);
                            self.current = Some(next_page);
                            self.index = 0;
                        } else {
                            self.current = None;
                            return false;
                        }
                    } else {
                        self.index += 1;
                    }
                }
                Ordering::Backward => {
                    if self.index == 0 {
                        if let Some((_, page_id)) = self.range.next_back() {
                            let next_page = self.get_page(page_id);
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
        } else {
            false
        }
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

    fn get_page(&self, id: &Id) -> Arc<SSPage<S>> {
        self.cache.lock().get_or_fetch(id).unwrap().clone()
    }
}

#[cfg(test)]
mod test {}
