use bifrost::utils::fut_exec::wait;
use client::AsyncClient;
use dovahkiin::types::custom_types::bytes::SmallBytes;
use dovahkiin::types::custom_types::id::Id;
use dovahkiin::types::custom_types::map::Map;
use dovahkiin::types::type_id_of;
use dovahkiin::types::value::ToValue;
use index::btree::Ordering;
use index::EntryKey;
use index::Slice;
use index::{id_from_key, key_with_id};
use itertools::Itertools;
use parking_lot::Mutex;
use parking_lot::RwLock;
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
    static ref PAGE_SCHEMA_ID: u32 = key_hash(PAGE_SCHEMA) as u32;
}

pub struct LevelTree<S>
where
    S: Slice<Item = EntryKey> + SortableEntrySlice,
{
    neb_client: Arc<AsyncClient>,
    index: BTreeMap<EntryKey, Id>,
    tombstones: Tombstones,
    pages: PageCache<S>,
    marker: PhantomData<S>,
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
            len: keys_array.len(),
        }
    }

    pub fn to_cell(&self) -> Cell
    {
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

impl<S> LevelTree<S>
where
    S: Slice<Item = EntryKey> + SortableEntrySlice,
{
    pub fn new(neb_client: &Arc<AsyncClient>) -> Self {
        let client = neb_client.clone();
        let mut index = BTreeMap::new();
        Self {
            neb_client: neb_client.clone(),
            index,
            tombstones: Tombstones::new(),
            // TODO: carefully set the capacity
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
        debug!("Seeking for {:?}, in index len {}", key, self.index.len());
        let mut range = match ordering {
            Ordering::Forward => self.index.range::<EntryKey, _>(key..),
            Ordering::Backward => self.index.range::<EntryKey, _>(..=key),
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
                    if tombstones.remove(lowest) {
                        continue;
                    }
                } else {
                    if self.tombstones.remove(lowest) {
                        continue;
                    }
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
            self.index.remove(&k).unwrap();
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
                wait(self.neb_client.update_cell(cell)).unwrap();
            } else {
                debug!("Inserting page with id {:?}", cell.id());
                wait(self.neb_client.write_cell(cell)).unwrap();
            }

            debug!("Insert page index key {:?}, id {:?}", index_key, id);
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

macro_rules! impl_sspage_slice {
    ($t: ty, $et: ty, $n: expr) => {
        impl_slice_ops!($t, $et, $n);
        impl SortableEntrySlice for $t {}
    };
}

#[cfg(test)]
mod test {
    use super::*;
    use client;
    use index::sstable::LevelTree;
    use server::NebServer;
    use server::ServerOptions;
    use std::ptr;
    use std::sync::Arc;

    type SmallPage = [EntryKey; 5];

    impl_sspage_slice!(SmallPage, EntryKey, 5);


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
        client.new_schema_with_id(super::get_schema());

        let mut tree: LevelTree<SmallPage> = LevelTree::new(&client);
        let id = Id::unit_id();
        let key = smallvec![1, 2, 3, 4, 5, 6];
        let mut tombstones = BTreeSet::new();
        info!("test insertion");

        let mut key_id = key.clone();
        key_with_id(&mut key_id, &id);

        tree.merge(vec![key_id], &mut tombstones);
        let mut cursor = tree.seek(&key, Ordering::Forward);
        assert_eq!(id_from_key(cursor.current().unwrap()), id);
    }
}
