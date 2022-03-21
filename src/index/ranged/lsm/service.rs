use super::super::sm::client::SMClient;
use super::super::trees::*;
pub use super::btree::level::{LEVEL_1 as MIGRATE_SIZE, LEVEL_M as BLOCK_SIZE};
use super::btree::storage;
use super::tree::*;
use crate::client::AsyncClient;
use crate::ram::types::Id;
use crate::ram::types::RandValue;
use bifrost::conshash::ConsistentHashing;
use bifrost_plugins::hash_ident;
use futures::future::BoxFuture;
use futures::prelude::*;
use lightning::map::{Map, PtrHashMap as HashMap};
use parking_lot::RwLock;
use serde::{Deserialize, Serialize};
use std::time::Duration;

pub type IdBlock = [Id; BLOCK_SIZE];
pub static DEFAULT_SERVICE_ID: u64 = hash_ident!(LSM_TREE_RPC_SERVICE) as u64;

#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct Boundary {
    upper: EntryKey,
    lower: EntryKey,
}

#[derive(Clone, Serialize, Deserialize)]
pub enum OpResult<T> {
    Successful(T),
    NotFound,
    OutOfBound,
    EpochMissMatch(u64, u64),
    Migrating,
}

#[derive(Clone, Serialize, Deserialize)]
pub struct ServBlock {
    pub buffer: Vec<Id>,
    pub next: Option<EntryKey>,
}

pub struct DistLSMTree {
    id: Id,
    tree: LSMTree,
    prop: RwLock<DistProp>,
}

#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct DistProp {
    boundary: Boundary,
    migration: Option<Migration>,
    epoch: u64,
}

#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct Migration {
    pivot: EntryKey,
}

#[derive(Debug, Serialize, Deserialize)]
pub struct BTreeStat {
    pub size: usize,
    pub count: usize,
    pub head: Id,
    pub ideal_cap: usize,
    pub oversized: bool,
}

#[derive(Debug, Serialize, Deserialize)]
pub struct LSMTreeStat {
    pub id: Id,
    pub prop: DistProp,
    pub trees: Vec<BTreeStat>,
}

#[derive(Debug, Serialize, Deserialize, Clone)]
pub enum RangeTerm {
    Inclusive(EntryKey),
    Exclusive(EntryKey),
    Open,
}

#[derive(Debug, Serialize, Deserialize, Clone)]
pub struct Range {
    pub start: RangeTerm,
    pub end: RangeTerm,
    pub ordering: Ordering,
}

service! {
    rpc crate_tree(id: Id, boundary: Boundary, epoch: u64);
    rpc load_tree(id: Id, boundary: Boundary, epoch: u64);
    rpc insert(id: Id, entry: EntryKey, epoch: u64) -> OpResult<bool>;
    rpc delete(id: Id, entry: EntryKey, epoch: u64) -> OpResult<bool>;
    rpc seek(id: Id, range: Range, pattern: Option<Vec<u8>>, buffer_size: u16, epoch: u64)
        -> OpResult<ServBlock>;
    rpc stat(id: Id) -> OpResult<LSMTreeStat>;
}

pub struct LSMTreeService {
    client: Arc<AsyncClient>,
    trees: Arc<HashMap<Id, Arc<DistLSMTree>>>,
}

impl Service for LSMTreeService {
    fn crate_tree(&self, id: Id, boundary: Boundary, epoch: u64) -> BoxFuture<()> {
        async move {
            if self.trees.contains_key(&id) {
                return;
            }
            let tree = LSMTree::create(&self.client, &id).await;
            self.trees.insert(
                id,
                Arc::new(DistLSMTree::new(id, tree, boundary, None, epoch)),
            );
        }
        .boxed()
    }

    fn load_tree(&self, id: Id, boundary: Boundary, epoch: u64) -> BoxFuture<()> {
        async move {
            if self.trees.contains_key(&id) {
                debug!("Tree loaded, skip {:?}", id);
                return;
            }
            info!("Called to load tree {:?}, boundary {:?}", id, boundary);
            let tree = LSMTree::recover(&self.client, &id).await;
            debug!(
                "LSM tree loaded with {} keys, capacity {}.",
                tree.count(),
                tree.ideal_capacity()
            );
            self.trees.insert(
                id,
                Arc::new(DistLSMTree::new(id, tree, boundary, None, epoch)),
            );
        }
        .boxed()
    }

    fn insert(&self, id: Id, entry: EntryKey, epoch: u64) -> BoxFuture<OpResult<bool>> {
        self.apply_in_ranged_tree(id, &entry, epoch, |entry, tree| {
            if tree.insert(&entry) {
                OpResult::Successful(true)
            } else {
                OpResult::Successful(false)
            }
        })
    }

    fn delete(&self, id: Id, entry: EntryKey, epoch: u64) -> BoxFuture<OpResult<bool>> {
        self.apply_in_ranged_tree(id, &entry, epoch, |entry, tree| {
            if tree.delete(&entry) {
                OpResult::Successful(true)
            } else {
                OpResult::Successful(false)
            }
        })
    }

    fn seek(
        &self,
        id: Id,
        range: Range,
        pattern: Option<Vec<u8>>,
        buffer_size: u16,
        epoch: u64,
    ) -> BoxFuture<OpResult<ServBlock>> {
        let entry = range.key();
        let ordering = range.ordering;
        self.apply_in_ranged_tree(id, entry, epoch, |entry, tree| {
            let buffer_size = buffer_size as usize;
            let mut tree_cursor = tree.seek(&entry, ordering);
            let mut buffer = Vec::with_capacity(buffer_size);
            let mut num_collected = 0;
            let pattern = pattern.as_ref().map(|p| (p.as_slice(), p.len()));
            while num_collected < buffer_size {
                if let Some(key) = tree_cursor.next() {
                    if let Some((patt_key, patt_len)) = pattern {
                        if &key.as_slice()[..patt_len] != patt_key {
                            // Pattern unmatch
                            debug!(
                                "Pattern unmatch for key {:?}, expect {:?}, break cursor.",
                                key, patt_key
                            );
                            break;
                        }
                    }
                    let key_id = key.id();
                    if let Some(last_key) = buffer.last() {
                        if last_key == &key_id {
                            continue;
                        }
                    }
                    match ordering {
                        Ordering::Forward => {
                            match &range.start {
                                RangeTerm::Inclusive(k) => {
                                    if &key < k {
                                        continue;
                                    }
                                }
                                RangeTerm::Exclusive(k) => {
                                    if &key <= k {
                                        continue;
                                    }
                                }
                                RangeTerm::Open => {}
                            }
                            match &range.end {
                                RangeTerm::Inclusive(k) => {
                                    if &key > k {
                                        debug!("Seek terminated due to inclusive end condition {:?} > {:?}", key, k);
                                        break;
                                    }
                                }
                                RangeTerm::Exclusive(k) => {
                                    if &key >= k {
                                        debug!("Seek terminated due to exclusive end condition {:?} >= {:?}", key, k);
                                        break;
                                    }
                                }
                                RangeTerm::Open => {}
                            }
                        }
                        Ordering::Backward => {
                            match &range.end {
                                RangeTerm::Inclusive(k) => {
                                    if &key > k {
                                        continue;
                                    }
                                }
                                RangeTerm::Exclusive(k) => {
                                    if &key >= k {
                                        continue;
                                    }
                                }
                                RangeTerm::Open => {}
                            }
                            match &range.start {
                                RangeTerm::Inclusive(k) => {
                                    if &key < k {
                                        debug!("Seek terminated due to inclusive start condition {:?} > {:?}", key, k);
                                        break;
                                    }
                                }
                                RangeTerm::Exclusive(k) => {
                                    if &key <= k {
                                        debug!("Seek terminated due to exclusive start condition {:?} > {:?}", key, k);
                                        break;
                                    }
                                }
                                RangeTerm::Open => {}
                            }
                        }
                    }
                    buffer.push(key_id);
                    num_collected += 1;
                } else {
                    break;
                }
            }
            let mut next = tree_cursor.current.as_ref().map(|(_, k)| k.clone());
            // Skip next duplicates
            while next.is_some() && next.as_ref().map(|k| k.id()).as_ref() == buffer.last() {
                next = tree_cursor.next();
            }
            let lsm_cursor = ServBlock { buffer, next };
            OpResult::Successful(lsm_cursor)
        })
    }

    fn stat(&self, id: Id) -> BoxFuture<OpResult<LSMTreeStat>> {
        future::ready(if let Some(tree) = self.trees.get(&id) {
            OpResult::Successful(LSMTreeStat {
                id,
                prop: tree.prop.read().clone(),
                trees: tree
                    .tree
                    .disk_trees
                    .iter()
                    .map(|t| BTreeStat {
                        size: t.size(),
                        count: t.count(),
                        head: t.head_id(),
                        ideal_cap: t.ideal_capacity(),
                        oversized: t.oversized(),
                    })
                    .collect(),
            })
        } else {
            OpResult::NotFound
        })
        .boxed()
    }
}

impl LSMTreeService {
    pub fn new(client: &Arc<AsyncClient>, sm_client: &Arc<SMClient>) -> Self {
        info!("Initializing LSM tree service");
        let trees_map = Arc::new(HashMap::with_capacity(32));
        super::btree::storage::start_external_nodes_write_back(client);
        Self::start_tree_balancer(&trees_map, client, sm_client);
        Self {
            client: client.clone(),
            trees: trees_map,
        }
    }

    pub fn start_tree_balancer(
        trees_map: &Arc<HashMap<Id, Arc<DistLSMTree>>>,
        client: &Arc<AsyncClient>,
        sm_client: &Arc<SMClient>,
    ) {
        debug!("Starting range indexer tree balancer");
        let trees_map = trees_map.clone();
        let client = client.clone();
        let sm_client = sm_client.clone();
        tokio::spawn(async move {
            loop {
                let mut fast_mode = false;
                for (_, dist_tree) in trees_map.entries() {
                    let tree = &dist_tree.tree;
                    fast_mode = tree.merge_levels().await | fast_mode;
                    if tree.oversized() {
                        info!("LSM Tree oversized {:?}, start migration", dist_tree.id);
                        // Tree oversized, need to migrate
                        let pivot_key = tree.pivot_key().unwrap();
                        let migration_target_id = Id::rand();
                        debug!(
                            "Creating migration target tree {:?} split at {:?}",
                            migration_target_id, pivot_key
                        );
                        let migration_tree = LSMTree::create(&client, &migration_target_id).await;
                        {
                            let mut dist_tree_prop = dist_tree.prop.write();
                            dist_tree_prop.migration = Some(Migration {
                                pivot: pivot_key.clone(),
                            });
                        }
                        debug!("Marking migration for tree {:?}", dist_tree.id);
                        tree.mark_migration(&dist_tree.id, Some(migration_target_id), &client)
                            .await;
                        let buffer_size = MIGRATE_SIZE << 4;
                        let mut cursor = tree.seek(&pivot_key, Ordering::Forward);
                        let mut entry_buffer = Vec::with_capacity(buffer_size);
                        debug!(
                            "Start moving keys from {:?} to {:?}",
                            dist_tree.id, migration_target_id
                        );
                        while cursor.current().is_some() {
                            if let Some(entry) = cursor.next() {
                                entry_buffer.push(entry);
                                if entry_buffer.len() >= buffer_size {
                                    debug!("Merging entry buffer, size {}", entry_buffer.len());
                                    migration_tree.merge_keys(entry_buffer);
                                    entry_buffer = Vec::with_capacity(buffer_size);
                                }
                            }
                        }
                        debug!("Merging last batch of keys, size {}", entry_buffer.len());
                        migration_tree.merge_keys(entry_buffer);
                        debug!("Waiting for new tree {:?} persisted", migration_target_id);
                        storage::wait_until_updated().await;
                        debug!("Calling placement for split to {:?}", migration_target_id);
                        sm_client
                            .split(&dist_tree.id, &migration_target_id, &pivot_key)
                            .await
                            .unwrap();
                        // Reset state on current tree
                        {
                            let mut dist_prop = dist_tree.prop.write();
                            dist_prop.boundary.upper = pivot_key.clone();
                            dist_prop.migration = None;
                            dist_prop.epoch += 1;
                        }
                        debug!("Unmark migration {:?}", dist_tree.id);
                        tree.mark_migration(&dist_tree.id, None, &client).await;
                        tree.retain(&pivot_key);
                        debug!(
                            "LSM tree migration from {:?} to {:?} succeed",
                            dist_tree.id, migration_target_id
                        );
                    }
                }
                if !fast_mode {
                    // Sleep for a while to check for trees to be merge in levels
                    tokio::time::sleep(Duration::from_millis(500)).await;
                }
            }
        });
    }

    fn apply_in_ranged_tree<F, R>(
        &self,
        id: Id,
        entry: &EntryKey,
        epoch: u64,
        func: F,
    ) -> BoxFuture<OpResult<R>>
    where
        F: Fn(&EntryKey, &LSMTree) -> OpResult<R>,
        R: Send + 'static,
    {
        future::ready(if let Some(tree) = self.trees.get(&id) {
            let tree_prop = tree.prop.read();
            if epoch < tree_prop.epoch {
                OpResult::EpochMissMatch(tree_prop.epoch, epoch)
            } else if tree_prop.boundary.in_boundary(entry) {
                if let &Some(ref migration) = &tree_prop.migration {
                    if entry < &migration.pivot {
                        // Entries lower than pivot should be safe to work on
                        func(&entry, &tree.tree)
                    } else {
                        OpResult::Migrating
                    }
                } else {
                    func(entry, &tree.tree)
                }
            } else {
                OpResult::OutOfBound
            }
        } else {
            OpResult::NotFound
        })
        .boxed()
    }
}

impl DistLSMTree {
    fn new(
        id: Id,
        tree: LSMTree,
        boundary: Boundary,
        migration: Option<Migration>,
        epoch: u64,
    ) -> Self {
        let prop = RwLock::new(DistProp {
            boundary,
            migration,
            epoch,
        });
        Self { id, tree, prop }
    }
}

impl Boundary {
    pub fn new(lower: EntryKey, upper: EntryKey) -> Self {
        Boundary { lower, upper }
    }
    fn in_boundary(&self, entry: &EntryKey) -> bool {
        // Allow max/min query as special cases
        (entry >= &self.lower && entry < &self.upper)
            || entry == &*MIN_ENTRY_KEY
            || entry == &*MAX_ENTRY_KEY
    }
}

impl Range {
    pub fn new_inclusive_opened(key: EntryKey, ordering: Ordering) -> Self {
        match ordering {
            Ordering::Forward => Self {
                start: RangeTerm::Inclusive(key),
                end: RangeTerm::Open,
                ordering,
            },
            Ordering::Backward => Self {
                start: RangeTerm::Open,
                end: RangeTerm::Inclusive(key),
                ordering,
            },
        }
    }
    pub fn move_to(mut self, key: EntryKey) -> Self {
        match self.ordering {
            Ordering::Forward => self.start = RangeTerm::Inclusive(key),
            Ordering::Backward => self.end = RangeTerm::Exclusive(key),
        }
        self
    }
    pub fn key(&self) -> &EntryKey {
        match self.ordering {
            Ordering::Forward => match self.start {
                RangeTerm::Inclusive(ref e) | RangeTerm::Exclusive(ref e) => e,
                RangeTerm::Open => &*MIN_ENTRY_KEY,
            },
            Ordering::Backward => match self.end {
                RangeTerm::Inclusive(ref e) | RangeTerm::Exclusive(ref e) => e,
                RangeTerm::Open => &*MAX_ENTRY_KEY,
            },
        }
    }
}

dispatch_rpc_service_functions!(LSMTreeService);

unsafe impl Send for DistLSMTree {}
unsafe impl Sync for DistLSMTree {}

pub fn client_by_rpc_client(rpc: &Arc<RPCClient>) -> Arc<AsyncServiceClient> {
    AsyncServiceClient::new(DEFAULT_SERVICE_ID, rpc)
}

pub async fn locate_tree_server_from_conshash(
    id: &Id,
    conshash: &Arc<ConsistentHashing>,
) -> Result<Arc<AsyncServiceClient>, RPCError> {
    if let Some(server_id) = conshash.get_server_id_by(id) {
        DEFAULT_CLIENT_POOL
            .get_by_id(server_id, move |sid| conshash.to_server_name(sid))
            .await
            .map_err(|e| RPCError::IOError(e))
            .map(|c| client_by_rpc_client(&c))
    } else {
        Err(RPCError::RequestError(RPCRequestError::Other))
    }
}
