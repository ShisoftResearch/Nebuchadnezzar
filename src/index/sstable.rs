use bifrost::utils::fut_exec::wait;
use client::AsyncClient;
use dovahkiin::types::custom_types::bytes::SmallBytes;
use dovahkiin::types::custom_types::id::Id;
use dovahkiin::types::custom_types::map::Map;
use dovahkiin::types::type_id_of;
use dovahkiin::types::value::ToValue;
use futures::prelude::*;
use index::*;
use itertools::Itertools;
use owning_ref::OwningHandle;
use parking_lot::Mutex;
use parking_lot::RwLock;
use parking_lot::RwLockReadGuard;
use ram::cell::Cell;
use ram::schema::Field;
use ram::schema::Schema;
use ram::types::*;
use std::cell::RefCell;
use std::collections::btree_map::Range;
use std::collections::hash_map::DefaultHasher;
use std::collections::{BTreeMap, BTreeSet, LinkedList};
use std::fmt::Debug;
use std::marker::PhantomData;
use std::mem;
use std::ops::Bound::*;
use std::ops::RangeBounds;
use std::rc::Rc;
use std::sync::atomic::{AtomicUsize, Ordering::Relaxed};
use std::sync::Arc;
use utils::lru_cache::LRUCache;

// LevelTree items cannot been added or removed individually
// Items must been merged from higher level in bulk
// Deletion will be performed when merging to or from higher level tree
// Because tree update will not to be performed in parallel. Unlike memtable, a single r/w lock
// should be sufficient. Thus concurrency control will be simple and efficient.

pub type TombstonesInner = BTreeSet<EntryKey>;
pub type Tombstones = RwLock<TombstonesInner>;
type PageCache<S> = Arc<Mutex<LRUCache<Id, Arc<SSPage<S>>>>>;
type PageIndex = Arc<RwLock<BTreeMap<EntryKey, Id>>>;
type IndexHandle<'a> = OwningHandle<PageIndex, RwLockReadGuard<'a, BTreeMap<EntryKey, Id>>>;

const PAGE_SCHEMA: &'static str = "NEB_SSTABLE_PAGE";

lazy_static! {
    static ref PAGE_SCHEMA_ID: u32 = key_hash(PAGE_SCHEMA) as u32;
}

pub struct LevelTree<S>
where
    S: Slice<Item = EntryKey> + SortableEntrySlice,
{
    neb_client: Arc<AsyncClient>,
    index: PageIndex,
    tombstones: Arc<Tombstones>,
    pages: PageCache<S>,
    marker: PhantomData<S>,
    len: AtomicUsize,
}
struct SSPage<S>
where
    S: Slice<Item = EntryKey> + SortableEntrySlice,
{
    id: Id,
    slice: S,
    len: usize,
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
            len: keys_array.len(),
        }
    }

    pub fn to_cell(&self) -> Cell {
        let value = self
            .slice
            .as_slice_immute()
            .iter()
            .map(|key| SmallBytes::from_vec(key.as_slice().to_vec()))
            .take(self.len)
            .collect_vec()
            .value();
        Cell::new_with_id(*PAGE_SCHEMA_ID, &self.id, value)
    }

    pub fn new() -> Self {
        SSPage {
            slice: S::init(),
            id: Id::unit_id(),
            len: 0,
        }
    }
}

pub trait SSLevelTree: MergeableTree {
    fn seek(&self, key: &EntryKey, ordering: Ordering) -> Box<Cursor>;
    fn merge(&self, source: &mut [EntryKey], source_tombstones: &mut TombstonesInner);
    fn mark_deleted(&self, key: &EntryKey) -> bool;
    fn is_deleted(&self, key: &EntryKey) -> bool;
    fn len(&self) -> usize;
    fn tombstones(&self) -> Arc<Tombstones>;
}

impl<S> LevelTree<S>
where
    S: Slice<Item = EntryKey> + SortableEntrySlice + 'static,
{
    pub fn new(neb_client: &Arc<AsyncClient>) -> Self {
        let client = neb_client.clone();
        let mut index = Arc::new(RwLock::new(BTreeMap::new()));
        Self {
            neb_client: neb_client.clone(),
            index,
            tombstones: Arc::new(Tombstones::new(BTreeSet::new())),
            len: AtomicUsize::new(0),
            // TODO: carefully set the capacity
            pages: Arc::new(Mutex::new(LRUCache::new(
                100,
                move |id| {
                    client
                        .read_cell(*id)
                        .wait()
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
}

impl<S> SSLevelTree for LevelTree<S>
where
    S: Slice<Item = EntryKey> + SortableEntrySlice + 'static,
{
    fn seek(&self, key: &EntryKey, ordering: Ordering) -> Box<Cursor> {
        let index = self.index.read();
        debug!("Seeking for {:?}, in index len {}", key, index.len());
        let mut range = match ordering {
            Ordering::Forward => index.range::<EntryKey, _>(key..),
            Ordering::Backward => index.range::<EntryKey, _>(..=key),
        };
        let first = match ordering {
            Ordering::Forward => range.next(),
            Ordering::Backward => range.next_back(),
        }
        .map(|(_, page_id)| {
            debug!("First page id is {:?}", page_id);
            self.pages.lock().get_or_fetch(page_id).unwrap().clone()
        });

        let (pos, page_len) = if let Some(ref page) = first {
            debug!("First page has {} elements", page.len);
            (
                page.slice.as_slice_immute()[..page.len]
                    .binary_search(key)
                    .unwrap_or_else(|i| i),
                page.len,
            )
        } else {
            (0, 0)
        };

        debug!("First page search pos is {}", pos);

        let mut cursor = RTCursor {
            ordering,
            page_len,
            current: first,
            pos,
            cache: self.pages.clone(),
            index: self.index.clone(),
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

        box cursor
    }

    fn merge(&self, source: &mut [EntryKey], source_tombstones: &mut TombstonesInner) {
        let mut target_tombstones = self.tombstones.write();
        let mut pages_cache = self.pages.lock();
        let mut index = self.index.write();
        let mut num_new_merged = 0;
        let (keys_to_removed, mut merged, mut ids_to_reuse) = {
            let (target_keys, target_pages): (Vec<_>, Vec<_>) = {
                let first = source.first().unwrap();
                let last = source.last().unwrap();
                index
                    .range::<EntryKey, _>(first..=last)
                    .map(|(k, id)| (k, pages_cache.get_or_fetch(id).unwrap().clone()))
                    .unzip()
            };
            let mut merged: Vec<SSPage<S>> = vec![SSPage::new()];
            let page_size = merged[0].slice.len();
            let target_page_ids = target_pages.iter().map(|p| p.id).collect::<LinkedList<_>>();
            let mut target = target_pages
                .iter()
                .map(|page| &page.slice.as_slice_immute()[..page.len])
                .flat_map(|x| x)
                .map(|x| (*x).clone())
                .collect_vec();

            let src_len = source.len();
            let tgt_len = target.len();

            debug!("Src len {}, target len {}", src_len, tgt_len);

            let mut srci = 0;
            let mut tgti = 0;

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
                    if source_tombstones.remove(lowest) {
                        continue;
                    }
                } else {
                    if target_tombstones.remove(lowest) {
                        continue;
                    }
                }

                if from_source {
                    target_tombstones.remove(lowest);
                    num_new_merged += 1;
                }

                let merging_page_len = {
                    let merging_page = merged.last_mut().unwrap();
                    let page_len = merging_page.len;
                    debug!("i is {}, picking {:?}", page_len, lowest);
                    mem::swap(&mut merging_page.slice.as_slice()[page_len], lowest);
                    merging_page.len += 1;
                    merging_page.len
                };

                if merging_page_len >= page_size {
                    merged.push(SSPage::new());
                }
            }
            if merged.last().unwrap().len == 0 {
                let merged_len = merged.len();
                merged.remove(merged_len - 1);
            }
            (
                target_keys.into_iter().map(|x| x.clone()).collect_vec(),
                merged,
                target_page_ids,
            )
        };

        debug!("Merged {} pages", merged.len());

        for k in keys_to_removed {
            debug!("Remove stale page index {:?}", k);
            index.remove(&k).unwrap();
        }

        for id in &ids_to_reuse {
            debug!("Remove stale page cache {:?}", id);
            pages_cache.remove(id);
        }

        for mut page in merged {
            let reuse_id = ids_to_reuse.pop_back();
            let id = reuse_id.unwrap_or_else(|| Id::rand());
            let index_key = page.slice.as_slice_immute()[0].clone();
            page.id = id;
            let cell = page.to_cell();
            if reuse_id.is_some() {
                debug!("Reuse page id by updating {:?}", cell.id());
                self.neb_client.update_cell(cell).wait().unwrap();
            } else {
                debug!("Inserting page with id {:?}", cell.id());
                self.neb_client.write_cell(cell).wait().unwrap();
            }

            debug!("Insert page index key {:?}, id {:?}", index_key, id);
            index.insert(index_key, id);
        }
        self.len.fetch_add(num_new_merged, Relaxed);
    }

    fn mark_deleted(&self, key: &EntryKey) -> bool {
        if self.seek(key, Ordering::Forward).current() == Some(key) {
            let mut tombstones = self.tombstones.write();
            if tombstones.contains(key) {
                return false;
            } else {
                tombstones.insert(key.clone());
                self.len.fetch_sub(1, Relaxed);
                return true;
            }
        }
        false
    }

    fn is_deleted(&self, key: &EntryKey) -> bool {
        self.tombstones.read().contains(key)
    }

    fn len(&self) -> usize {
        self.len.load(Relaxed)
    }

    fn tombstones(&self) -> Arc<Tombstones> {
        self.tombstones.clone()
    }
}

impl<S> MergeableTree for LevelTree<S>
where
    S: Slice<Item = EntryKey> + SortableEntrySlice + 'static,
{
    fn prepare_level_merge(&self) -> Box<MergingTreeGuard> {
        box LevelTreeMergeGuard {
            index: self.index.clone(),
            pages: self.pages.clone(),
        }
    }

    fn elements(&self) -> usize {
        self.len()
    }
}

pub struct LevelMergingPage<'a, S>
where
    S: Slice<Item = EntryKey> + SortableEntrySlice + 'static,
{
    index: PageIndex,
    page: Arc<SSPage<S>>,
    page_cache: PageCache<S>,
    lock: IndexHandle<'a>,
}

impl<'a, S> MergingPage for LevelMergingPage<'a, S>
where
    S: Slice<Item = EntryKey> + SortableEntrySlice + 'static,
{
    fn next(&self) -> Box<MergingPage> {
        let index = OwningHandle::new_with_fn(self.index.clone(), |x| unsafe { &*x }.read());
        let page = {
            let current_last = self.page.slice.as_slice_immute().first().unwrap();
            let (_, page_id) = index.range::<EntryKey, _>(..current_last).last().unwrap();
            self.page_cache
                .lock()
                .get_or_fetch(page_id)
                .unwrap()
                .clone()
        };
        box LevelMergingPage {
            page,
            lock: index,
            index: self.index.clone(),
            page_cache: self.page_cache.clone(),
        }
    }

    fn keys(&self) -> &[EntryKey] {
        &self.page.slice.as_slice_immute()[..self.page.len]
    }
}

pub struct LevelTreeMergeGuard<S>
where
    S: Slice<Item = EntryKey> + SortableEntrySlice,
{
    index: PageIndex,
    pages: PageCache<S>,
}

impl<S> MergingTreeGuard for LevelTreeMergeGuard<S>
where
    S: Slice<Item = EntryKey> + SortableEntrySlice + 'static,
{
    fn remove_pages(&self, pages: &[&[EntryKey]]) {
        let mut index = self.index.write();
        pages
            .iter()
            .map(|page| page.first().unwrap())
            .for_each(|page_index| {
                // pages cache will be evicted by new requesting pages
                index.remove(page_index);
            });
    }

    fn last_page(&self) -> Box<MergingPage> {
        let index = OwningHandle::new_with_fn(self.index.clone(), |x| unsafe { &*x }.read());
        let page = {
            let (_, page_id) = index.iter().last().unwrap();
            self.pages.lock().get_or_fetch(page_id).unwrap().clone()
        };
        box LevelMergingPage {
            page,
            lock: index,
            index: self.index.clone(),
            page_cache: self.pages.clone(),
        }
    }
}

pub trait SortableEntrySlice: Sized + Slice<Item = EntryKey> {}

pub struct RTCursor<S>
where
    S: Slice<Item = EntryKey> + SortableEntrySlice,
{
    pos: usize,
    ordering: Ordering,
    page_len: usize,
    current: Option<Arc<SSPage<S>>>,
    cache: PageCache<S>,
    index: PageIndex,
}

impl<S> Cursor for RTCursor<S>
where
    S: Slice<Item = EntryKey> + SortableEntrySlice,
{
    fn next(&mut self) -> bool {
        if self.current.is_some() {
            debug!("Next item");
            match self.ordering {
                Ordering::Forward => {
                    debug!("pos: {}, page_len: {}", self.pos, self.page_len);
                    if self.pos + 1 >= self.page_len {
                        debug!("Switching page");
                        let current_key = self.current().unwrap().clone();
                        if let Some((_, page_id)) = self
                            .index
                            .read()
                            .range::<EntryKey, _>((Excluded(&current_key), Unbounded))
                            .next()
                        {
                            let next_page = self.get_page(page_id);
                            self.pos = next_page
                                .slice
                                .as_slice_immute()
                                .binary_search(&current_key)
                                .map(|i| if i < self.page_len - 1 { i + 1 } else { i })
                                .unwrap_or_else(|i| i);
                            self.current = Some(next_page);
                        } else {
                            self.current = None;
                            return false;
                        }
                    } else {
                        debug!("increasing pos by one");
                        self.pos += 1;
                    }
                }
                Ordering::Backward => {
                    if self.pos == 0 {
                        let current_key = self.current().unwrap().clone();
                        if let Some((_, page_id)) = self
                            .index
                            .read()
                            .range::<EntryKey, _>((Unbounded, Excluded(&current_key)))
                            .next_back()
                        {
                            let next_page = self.get_page(page_id);
                            self.pos = next_page
                                .slice
                                .as_slice_immute()
                                .binary_search(&current_key)
                                .map(|i| if i != 0 { i - 1 } else { i })
                                .unwrap_or_else(|i| i);
                            self.current = Some(next_page);
                        } else {
                            self.current = None;
                            return false;
                        }
                    } else {
                        self.pos -= 1;
                    }
                }
            }
            debug!("return true for next");
            true
        } else {
            false
        }
    }

    fn current(&self) -> Option<&EntryKey> {
        if self.pos < 0 || self.pos >= self.page_len {
            debug!("out of flow, exit");
            return None;
        }
        if let Some(ref page) = self.current {
            debug!("has current page, return {}", self.pos);
            Some(&page.slice.as_slice_immute()[self.pos])
        } else {
            debug!("have no current page, return none");
            None
        }
    }
}

impl<S> RTCursor<S>
where
    S: Slice<Item = EntryKey> + SortableEntrySlice,
{
    fn get_page(&self, id: &Id) -> Arc<SSPage<S>> {
        self.cache.lock().get_or_fetch(id).unwrap().clone()
    }
}

pub fn get_schema() -> Schema {
    Schema {
        id: *PAGE_SCHEMA_ID,
        name: String::from(PAGE_SCHEMA),
        key_field: None,
        str_key_field: None,
        is_dynamic: false,
        fields: Field::new("*", type_id_of(Type::SmallBytes), false, true, None),
    }
}

#[cfg(test)]
mod test {
    use super::*;
    use client;
    use futures::prelude::*;
    use index::sstable::LevelTree;
    use server::NebServer;
    use server::ServerOptions;
    use std::ptr;
    use std::sync::Arc;

    type SmallPage = [EntryKey; 5];
    type L1Page = SmallPage;
    type L2Page = [EntryKey; 50];
    type L3Page = [EntryKey; 500];

    impl_sspage_slice!(SmallPage, EntryKey, 5);
    impl_sspage_slice!(L2Page, EntryKey, 50);
    impl_sspage_slice!(L3Page, EntryKey, 500);

    #[test]
    pub fn init() {
        env_logger::init();
        let server_group = "sstable_index_init";
        let server_addr = String::from("127.0.0.1:5200");
        let server = NebServer::new_from_opts(
            &ServerOptions {
                chunk_count: 1,
                memory_size: 16 * 1024 * 1024,
                backup_storage: None,
                wal_storage: None,
            },
            &server_addr,
            &server_group,
        );
        let client = Arc::new(
            client::AsyncClient::new(&server.rpc, &vec![server_addr], server_group).unwrap(),
        );
        client.new_schema_with_id(super::get_schema()).wait();

        let mut tree: LevelTree<SmallPage> = LevelTree::new(&client);
        let id = Id::unit_id();
        let key = smallvec![1, 2, 3, 4, 5, 6];
        let mut tombstones = BTreeSet::new();
        info!("test insertion");

        let mut key_id = key.clone();
        key_with_id(&mut key_id, &id);

        tree.merge(vec![key_id].as_mut_slice(), &mut tombstones);
        let mut cursor = tree.seek(&key, Ordering::Forward);
        assert_eq!(id_from_key(cursor.current().unwrap()), id);
        assert_eq!(tree.len(), 1);
    }

    #[test]
    pub fn merging() {
        env_logger::init();
        let server_group = "sstable_index_init";
        let server_addr = String::from("127.0.0.1:5201");
        let server = NebServer::new_from_opts(
            &ServerOptions {
                chunk_count: 1,
                memory_size: 16 * 1024 * 1024,
                backup_storage: None,
                wal_storage: None,
            },
            &server_addr,
            &server_group,
        );
        let client = Arc::new(
            client::AsyncClient::new(&server.rpc, &vec![server_addr], server_group).unwrap(),
        );
        client.new_schema_with_id(super::get_schema()).wait();

        let mut tree_1: LevelTree<L1Page> = LevelTree::new(&client);
        let mut tree_2: LevelTree<L2Page> = LevelTree::new(&client);
        let mut tombstones = BTreeSet::new();

        let merging_count = 100;

        // merge to empty
        let mut initial_parts = vec![];
        let key_prefix = smallvec![1, 2, 3, 4];
        let initial_offset = merging_count;
        for i in 0..merging_count {
            let id = Id::new(0, initial_offset + i);
            let mut key = key_prefix.clone();
            key_with_id(&mut key, &id);
            initial_parts.push(key);
        }
        tree_1.merge(initial_parts.as_mut_slice(), &mut tombstones);
        for i in 0..merging_count {
            let id = Id::new(0, initial_offset + i);
            let mut key = key_prefix.clone();
            key_with_id(&mut key, &id);
            let mut cursor = tree_1.seek(&smallvec!(0), Ordering::Forward);
            for j in 0..=i {
                let id = Id::new(0, initial_offset + j);
                let mut key = key_prefix.clone();
                key_with_id(&mut key, &id);
                assert_eq!(cursor.current(), Some(&key), "{}/{}", i, j);
                if i != 0 {
                    assert_eq!(cursor.next(), j != merging_count - 1, "{}/{}", i, j);
                }
            }
        }

        // merge to left
        let mut left_parts = vec![];
        for i in 0..merging_count {
            let id = Id::new(0, i);
            let mut key = key_prefix.clone();
            key_with_id(&mut key, &id);
            left_parts.push(key);
        }
        tree_1.merge(left_parts.as_mut_slice(), &mut tombstones);
        for i in 0..merging_count * 2 {
            let id = Id::new(0, i);
            let mut key = key_prefix.clone();
            key_with_id(&mut key, &id);
            let mut cursor = tree_1.seek(&smallvec!(0), Ordering::Forward);
            for j in 0..=i {
                let id = Id::new(0, j);
                let mut key = key_prefix.clone();
                key_with_id(&mut key, &id);
                assert_eq!(cursor.current(), Some(&key), "{}/{}", i, j);
                assert_eq!(cursor.next(), j != 199, "{}/{}", i, j);
            }
        }

        //merge to right
        let mut right_parts = vec![];
        let right_parts_starting = 200;
        for i in 0..merging_count {
            let id = Id::new(0, right_parts_starting + i);
            let mut key = key_prefix.clone();
            key_with_id(&mut key, &id);
            right_parts.push(key);
        }
        tree_1.merge(right_parts.as_mut_slice(), &mut tombstones);
        for i in 0..merging_count * 3 {
            let id = Id::new(0, i);
            let mut key = key_prefix.clone();
            key_with_id(&mut key, &id);
            let mut cursor = tree_1.seek(&smallvec!(0), Ordering::Forward);
            for j in 0..=i {
                let id = Id::new(0, j);
                let mut key = key_prefix.clone();
                key_with_id(&mut key, &id);
                assert_eq!(cursor.current(), Some(&key), "{}/{}", i, j);
                assert_eq!(cursor.next(), j != 299, "{}/{}", i, j);
            }
        }
    }
}
