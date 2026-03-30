use super::super::sm::client::SMClient;
use super::super::trees::*;
pub use super::btree::level::BTREE_NODE_SIZE as MIGRATE_SIZE;
use super::btree::storage;
use super::tree::*;
use crate::client::AsyncClient;
use crate::ram::types::Id;
use crate::ram::types::RandValue;
use bifrost::conshash::ConsistentHashing;
use bifrost_hasher::hash_str;
use bifrost_plugins::hash_ident;
use futures::future::BoxFuture;
use futures::prelude::*;
use lightning::map::{Map, PtrHashMap as HashMap};
use parking_lot::RwLock;
use serde::{Deserialize, Serialize};
use std::collections::HashSet;
use std::time::Duration;

pub type IdBlock = [Id; MIGRATE_SIZE]; // Fixed size for ID arrays (not related to tree node size)
pub static DEFAULT_SERVICE_ID: u64 = hash_ident!(RANGED_TREE_RPC_SERVICE) as u64;

pub fn generate_scoped_service_id(group_name: &str, database_name: &str) -> u64 {
    if group_name == database_name || group_name.is_empty() || database_name.is_empty() {
        DEFAULT_SERVICE_ID
    } else {
        hash_str(&format!(
            "RANGED_TREE_RPC_SERVICE-{}-{}",
            group_name, database_name
        ))
    }
}

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

pub struct DistTree {
    id: Id,
    tree: RangedTree,
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
pub struct TreeStat {
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
    rpc seek(id: Id, range: Range, pattern: &Option<Vec<u8>>, buffer_size: u16, epoch: u64)
        -> OpResult<ServBlock>;
    rpc stat(id: Id) -> OpResult<TreeStat>;
    rpc flush_all();
}

service_with_id!(TreeService, DEFAULT_SERVICE_ID);

pub struct TreeService {
    client: Arc<AsyncClient>,
    trees: Arc<HashMap<Id, Arc<DistTree>>>,
}

impl Service for TreeService {
    fn crate_tree(&self, id: Id, boundary: Boundary, epoch: u64) -> BoxFuture<'_, ()> {
        async move {
            if self.trees.contains_key(&id) {
                return;
            }
            let tree = RangedTree::create(&self.client, &id).await;
            self.trees
                .insert(id, Arc::new(DistTree::new(id, tree, boundary, None, epoch)));
        }
        .boxed()
    }

    fn load_tree(&self, id: Id, boundary: Boundary, epoch: u64) -> BoxFuture<'_, ()> {
        async move {
            if self.trees.contains_key(&id) {
                debug!("Tree loaded, skip {:?}", id);
                return;
            }
            info!("Called to load tree {:?}, boundary {:?}", id, boundary);
            let tree = RangedTree::recover(&self.client, &id).await;
            debug!(
                "LSM tree loaded with {} keys, capacity {}.",
                tree.count(),
                tree.ideal_capacity()
            );
            self.trees
                .insert(id, Arc::new(DistTree::new(id, tree, boundary, None, epoch)));
        }
        .boxed()
    }

    fn insert(&self, id: Id, entry: EntryKey, epoch: u64) -> BoxFuture<'_, OpResult<bool>> {
        self.apply_in_ranged_tree(id, &entry, epoch, |entry, tree| {
            let inserted = tree.insert(&entry);
            if inserted {
                OpResult::Successful(true)
            } else {
                OpResult::Successful(false)
            }
        })
    }

    fn delete(&self, id: Id, entry: EntryKey, epoch: u64) -> BoxFuture<'_, OpResult<bool>> {
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
        pattern: &Option<Vec<u8>>,
        buffer_size: u16,
        epoch: u64,
    ) -> BoxFuture<'_, OpResult<ServBlock>> {
        let entry = range.key();
        let ordering = range.ordering;
        self.apply_in_ranged_tree(id, entry, epoch, |entry, tree| {
            let buffer_size = buffer_size as usize;
            let mut tree_cursor = tree.seek(&entry, ordering);
            let mut buffer = Vec::with_capacity(buffer_size);
            let mut seen_ids: HashSet<Id> = HashSet::with_capacity(buffer_size);
            let mut num_collected = 0;
            let pattern = pattern.as_ref().map(|p| (p.as_slice(), p.len()));
            // Process current() first to avoid skipping first element
            if let Some(key) = tree_cursor.current() {
                let key = key.clone();
                if let Some((patt_key, patt_len)) = pattern {
                    if &key.as_slice()[..patt_len] != patt_key {
                        return OpResult::Successful(ServBlock { buffer, next: None });
                    }
                }
                let key_id = key.id();
                let feature_val = {
                    let mut bytes = [0u8; 8];
                    bytes.copy_from_slice(&key.as_slice()[8..16]);
                    u64::from_be_bytes(bytes)
                };
                let mut should_add = true;
                match ordering {
                    Ordering::Forward => {
                        match &range.start {
                            RangeTerm::Inclusive(k) => {
                                if key.prefix_lt(k) {
                                    should_add = false;
                                }
                            }
                            RangeTerm::Exclusive(k) => {
                                if key.prefix_le(k) {
                                    should_add = false;
                                }
                            }
                            RangeTerm::Open => {}
                        }
                        if should_add {
                            match &range.end {
                                RangeTerm::Inclusive(k) => {
                                    if key.prefix_gt(k) {
                                        should_add = false;
                                    }
                                }
                                RangeTerm::Exclusive(k) => {
                                    if key.prefix_ge(k) {
                                        should_add = false;
                                    }
                                }
                                RangeTerm::Open => {}
                            }
                        }
                    }
                    Ordering::Backward => {
                        match &range.end {
                            RangeTerm::Inclusive(k) => {
                                if key.prefix_gt(k) {
                                    should_add = false;
                                }
                            }
                            RangeTerm::Exclusive(k) => {
                                if key.prefix_ge(k) {
                                    should_add = false;
                                }
                            }
                            RangeTerm::Open => {}
                        }
                        if should_add {
                            match &range.start {
                                RangeTerm::Inclusive(k) => {
                                    if key.prefix_lt(k) {
                                        should_add = false;
                                    }
                                }
                                RangeTerm::Exclusive(k) => {
                                    if key.prefix_le(k) {
                                        should_add = false;
                                    }
                                }
                                RangeTerm::Open => {}
                            }
                        }
                    }
                }
                if should_add && seen_ids.insert(key_id) {
                    buffer.push(key_id);
                    num_collected += 1;
                }
            }
            while num_collected < buffer_size {
                if let Some(key) = tree_cursor.next() {
                    if let Some((patt_key, patt_len)) = pattern {
                        if &key.as_slice()[..patt_len] != patt_key {
                            break;
                        }
                    }
                    let key_id = key.id();
                    match ordering {
                        Ordering::Forward => {
                            match &range.start {
                                RangeTerm::Inclusive(k) => {
                                    if key.prefix_lt(k) {
                                        continue;
                                    }
                                }
                                RangeTerm::Exclusive(k) => {
                                    if key.prefix_le(k) {
                                        continue;
                                    }
                                }
                                RangeTerm::Open => {}
                            }
                            match &range.end {
                                RangeTerm::Inclusive(k) => {
                                    if key.prefix_gt(k) {
                                        break;
                                    }
                                }
                                RangeTerm::Exclusive(k) => {
                                    if key.prefix_ge(k) {
                                        break;
                                    }
                                }
                                RangeTerm::Open => {}
                            }
                        }
                        Ordering::Backward => {
                            match &range.end {
                                RangeTerm::Inclusive(k) => {
                                    if key.prefix_gt(k) {
                                        continue;
                                    }
                                }
                                RangeTerm::Exclusive(k) => {
                                    if key.prefix_ge(k) {
                                        continue;
                                    }
                                }
                                RangeTerm::Open => {}
                            }
                            match &range.start {
                                RangeTerm::Inclusive(k) => {
                                    if key.prefix_lt(k) {
                                        break;
                                    }
                                }
                                RangeTerm::Exclusive(k) => {
                                    if key.prefix_le(k) {
                                        break;
                                    }
                                }
                                RangeTerm::Open => {}
                            }
                        }
                    }
                    if seen_ids.insert(key_id) {
                        buffer.push(key_id);
                        num_collected += 1;
                    }
                } else {
                    break;
                }
            }
            let mut next = tree_cursor.current().cloned();
            // Skip next duplicates
            while next.is_some() && next.as_ref().map(|k| k.id()).as_ref() == buffer.last() {
                next = tree_cursor.next();
            }
            let result_block = ServBlock { buffer, next };
            OpResult::Successful(result_block)
        })
    }

    fn flush_all(&self) -> BoxFuture<'_, ()> {
        async move {
            self.flush_all_trees().await;
        }
        .boxed()
    }

    fn stat(&self, id: Id) -> BoxFuture<'_, OpResult<TreeStat>> {
        future::ready(if let Some(tree) = self.trees.get(&id) {
            OpResult::Successful(TreeStat {
                id,
                prop: tree.prop.read().clone(),
                trees: vec![BTreeStat {
                    size: MIGRATE_SIZE, // Single tree uses BTREE_NODE_SIZE (512)
                    count: tree.tree.count(),
                    head: tree.tree.head_id(),
                    ideal_cap: tree.tree.ideal_capacity(),
                    oversized: tree.tree.oversized(),
                }],
            })
        } else {
            OpResult::NotFound
        })
        .boxed()
    }
}

impl TreeService {
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

    /// Flush all trees to disk - called during shutdown
    pub async fn flush_all_trees(&self) {
        info!("Flushing all LSM trees to disk before shutdown");
        for (tree_id, dist_tree) in self.trees.entries() {
            let tree = &dist_tree.tree;
            let disk_count = tree.count();
            let mem_count = tree.mem_tree_count();
            info!(
                "Flushing tree {:?} with {} items (disk) + {} items (mem)",
                tree_id, disk_count, mem_count
            );

            // Force merge regardless of whether oversized
            tree.force_merge_levels().await;
        }

        // CRITICAL: Wait for all external B-tree node writes to complete BEFORE marking migration
        // Otherwise, mark_migration() will update the LSM tree cell with head IDs pointing to
        // B-tree pages that haven't been persisted yet, causing "CellDoesNotExisted" on recovery
        super::btree::storage::wait_until_updated().await;
        info!("All B-tree nodes persisted, now marking migrations");

        // Now it's safe to update the LSM tree cells with the new head IDs
        for (tree_id, dist_tree) in self.trees.entries() {
            let tree = &dist_tree.tree;
            if let Err(e) = tree.mark_migration(&tree_id, None, &self.client).await {
                warn!(
                    "Failed to mark LSM tree migration after flush for tree {:?}: {:?}",
                    tree_id, e
                );
            }
        }

        info!("All LSM trees flushed to disk");
    }

    pub fn start_tree_balancer(
        trees_map: &Arc<HashMap<Id, Arc<DistTree>>>,
        client: &Arc<AsyncClient>,
        sm_client: &Arc<SMClient>,
    ) {
        debug!("Starting range indexer tree balancer");
        let trees_map = trees_map.clone();
        let client = client.clone();
        let sm_client = sm_client.clone();
        tokio::spawn(async move {
            // Periodic checkpoint: flush B-tree pages and update tree root cells
            // every ~60 seconds to ensure durability even without explicit shutdown.
            const CHECKPOINT_INTERVAL_LOOPS: u32 = 120; // 120 * 500ms = 60s
            let mut checkpoint_counter: u32 = 0;

            loop {
                let mut fast_mode = false;
                checkpoint_counter = checkpoint_counter.wrapping_add(1);
                let do_checkpoint = checkpoint_counter % CHECKPOINT_INTERVAL_LOOPS == 0;

                if do_checkpoint {
                    // Ensure all pending B-tree node writes are flushed, then update
                    // tree root cells. This limits the recovery window to ~60 seconds
                    // of writes even if the server is killed without a clean shutdown.
                    storage::wait_until_updated().await;
                }

                for (_, dist_tree) in trees_map.entries() {
                    let tree = &dist_tree.tree;
                    let merged = tree.merge_levels().await;
                    fast_mode = merged | fast_mode;

                    // If merge happened, wait for external nodes to be written, then update the tree cell
                    if merged {
                        // Wait for all external B+tree nodes to be written to storage
                        storage::wait_until_updated().await;
                        // Now update the cell with the new head IDs
                        if let Err(e) = tree.mark_migration(&dist_tree.id, None, &client).await {
                            warn!("Failed to mark LSM tree migration after merge: {:?}", e);
                        }
                    } else if do_checkpoint {
                        // Periodic checkpoint: update tree root cell to reflect current state
                        if let Err(e) = tree.mark_migration(&dist_tree.id, None, &client).await {
                            warn!("Failed to checkpoint tree {:?}: {:?}", dist_tree.id, e);
                        }
                    }

                    if tree.oversized() {
                        info!("LSM Tree oversized {:?}, start migration", dist_tree.id);
                        // Tree oversized, need to migrate
                        let Some(pivot_key) = tree.pivot_key() else {
                            warn!(
                                "Skipping migration for {:?}: oversized tree has no pivot key",
                                dist_tree.id
                            );
                            continue;
                        };
                        let migration_target_id = Id::rand();
                        debug!(
                            "Creating migration target tree {:?} split at {:?}",
                            migration_target_id, pivot_key
                        );
                        let migration_tree =
                            RangedTree::create(&client, &migration_target_id).await;
                        {
                            let mut dist_tree_prop = dist_tree.prop.write();
                            dist_tree_prop.migration = Some(Migration {
                                pivot: pivot_key.clone(),
                            });
                        }
                        debug!("Marking migration for tree {:?}", dist_tree.id);
                        if let Err(e) = tree
                            .mark_migration(&dist_tree.id, Some(migration_target_id), &client)
                            .await
                        {
                            warn!(
                                "Failed to mark LSM tree migration for oversized tree {:?}: {:?}",
                                dist_tree.id, e
                            );
                        }
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
                        if let Err(e) = tree.mark_migration(&dist_tree.id, None, &client).await {
                            warn!(
                                "Failed to unmark LSM tree migration for tree {:?}: {:?}",
                                dist_tree.id, e
                            );
                        }
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
    ) -> BoxFuture<'_, OpResult<R>>
    where
        F: Fn(&EntryKey, &RangedTree) -> OpResult<R>,
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

impl DistTree {
    fn new(
        id: Id,
        tree: RangedTree,
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

dispatch_rpc_service_functions!(TreeService);

unsafe impl Send for DistTree {}
unsafe impl Sync for DistTree {}

pub fn client_by_rpc_client(
    rpc: &Arc<RPCClient>,
    group_name: &str,
    database_name: &str,
) -> Arc<AsyncServiceClient> {
    AsyncServiceClient::new_with_service_id(
        generate_scoped_service_id(group_name, database_name),
        rpc,
    )
}

pub async fn locate_tree_server_from_conshash(
    id: &Id,
    conshash: &Arc<ConsistentHashing>,
    group_name: &str,
    database_name: &str,
) -> Result<Arc<AsyncServiceClient>, RPCError> {
    if let Some(server_id) = conshash.get_server_id_by(id) {
        DEFAULT_CLIENT_POOL
            .get_by_id(server_id, move |sid| conshash.to_server_name(sid))
            .await
            .map_err(|e| RPCError::IOError(e))
            .map(|c| client_by_rpc_client(&c, group_name, database_name))
    } else {
        Err(RPCError::RequestError(RPCRequestError::Other))
    }
}
