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
use std::collections::HashMap as StdHashMap;
use std::collections::HashSet;
use std::env;
use std::time::{Duration, Instant};

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
    pub last_key: Option<EntryKey>,
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
    target_id: Id,
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
    sm_client: Arc<SMClient>,
    trees: Arc<HashMap<Id, Arc<DistTree>>>,
    pending_migrations: Arc<HashMap<Id, Arc<DistTree>>>,
}

fn trace_schema_from_key(key: &EntryKey) -> u32 {
    let mut schema = [0u8; 4];
    schema.copy_from_slice(&key.as_slice()[..4]);
    u32::from_be_bytes(schema)
}

fn should_trace_range_seek(key: &EntryKey, pattern: Option<&[u8]>) -> bool {
    let Ok(value) = env::var("NEB_RANGE_TRACE_SCHEMA") else {
        return false;
    };
    if value == "*" {
        return true;
    }
    let Ok(schema_id) = value.parse::<u32>() else {
        return false;
    };
    if trace_schema_from_key(key) != schema_id {
        return false;
    }
    match pattern {
        Some(p) => p.len() == 16,
        None => true,
    }
}

fn trace_id_gap(ids: &[Id]) -> Option<(Id, Id)> {
    ids.windows(2)
        .find(|pair| pair[0].higher == pair[1].higher && pair[1].lower != pair[0].lower + 1)
        .map(|pair| (pair[0], pair[1]))
}

fn trace_seek_block(
    tree_id: Id,
    entry: &EntryKey,
    range: &Range,
    boundary: &Boundary,
    buffer: &[Id],
    next: &Option<EntryKey>,
) {
    let first = buffer.first().copied();
    let last = buffer.last().copied();
    let gap = trace_id_gap(buffer);
    if gap.is_some() || buffer.is_empty() {
        debug!(
            "RANGE_SEEK_BLOCK tree={:?} schema={} ordering={:?} start={:?} end={:?} boundary_lower={:?} boundary_upper={:?} first={:?} last={:?} len={} next={:?} gap={:?} entry={:?}",
            tree_id,
            trace_schema_from_key(entry),
            range.ordering,
            range.start,
            range.end,
            boundary.lower,
            boundary.upper,
            first,
            last,
            buffer.len(),
            next.as_ref().map(|k| k.id()),
            gap,
            entry.id()
        );
    }
}

fn trace_seek_progress(tree_id: Id, entry: &EntryKey, progress: &[String]) {
    if progress.is_empty() {
        return;
    }
    trace!(
        "RANGE_SEEK_PROGRESS tree={:?} schema={} entry={:?} {}",
        tree_id,
        trace_schema_from_key(entry),
        entry.id(),
        progress.join(" | ")
    );
}

fn trace_probe_missing_key(tree_id: Id, tree: &RangedTree, schema_id: u32, gap: (Id, Id)) {
    if gap.1.lower != gap.0.lower + 2 || gap.0.higher != gap.1.higher {
        return;
    }

    let missing_id = Id::new(gap.0.higher, gap.0.lower + 1);
    let probe_key = EntryKey::for_scannable(&missing_id, schema_id);
    let mut cursor = tree.seek(&probe_key, Ordering::Forward);
    let mut seen = Vec::new();
    if let Some(current) = cursor.current() {
        seen.push(current.id());
    }
    while seen.len() < 4 {
        let Some(next) = cursor.next() else {
            break;
        };
        if seen.last() != Some(&next.id()) {
            seen.push(next.id());
        }
    }

    debug!(
        "RANGE_SEEK_PROBE tree={:?} schema={} missing={:?} gap={:?} probe_seen={:?}",
        tree_id, schema_id, missing_id, gap, seen
    );
}

fn bump_entry_key(key: &EntryKey, ordering: Ordering) -> Option<EntryKey> {
    let mut next = key.clone();
    match ordering {
        Ordering::Forward => {
            for byte in next.as_mut_slice().iter_mut().rev() {
                if *byte != u8::MAX {
                    *byte += 1;
                    return Some(next);
                }
                *byte = 0;
            }
            None
        }
        Ordering::Backward => {
            for byte in next.as_mut_slice().iter_mut().rev() {
                if *byte != 0 {
                    *byte -= 1;
                    return Some(next);
                }
                *byte = u8::MAX;
            }
            None
        }
    }
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
            if let Some(pending_tree) = self.pending_migrations.remove(&id) {
                {
                    let mut pending_prop = pending_tree.prop.write();
                    pending_prop.boundary = boundary;
                    pending_prop.epoch = epoch;
                    pending_prop.migration = None;
                }
                self.trees.insert(id, pending_tree);
                info!(
                    "Promoted in-memory migration target {:?} into active tree map",
                    id
                );
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
        self.apply_in_ranged_tree(id, entry, epoch, true, move |entry, tree, _dist_prop| {
            let inserted = tree.insert(&entry);
            if inserted {
                OpResult::Successful(true)
            } else {
                OpResult::Successful(false)
            }
        })
    }

    fn delete(&self, id: Id, entry: EntryKey, epoch: u64) -> BoxFuture<'_, OpResult<bool>> {
        self.apply_in_ranged_tree(id, entry, epoch, true, move |entry, tree, _dist_prop| {
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
        let entry = range.key().clone();
        let ordering = range.ordering;
        let pattern = pattern.clone();
        self.apply_in_ranged_tree(id, entry, epoch, false, move |entry, tree, dist_prop| {
            let buffer_size = buffer_size as usize;
            let mut buffer = Vec::with_capacity(buffer_size);
            let mut seen_ids: HashSet<Id> = HashSet::with_capacity(buffer_size);
            let mut num_collected = 0;
            let pattern = pattern.as_ref().map(|p| (p.as_slice(), p.len()));
            let trace_seek = should_trace_range_seek(entry, pattern.map(|(bytes, _)| bytes));
            let mut trace_progress = if trace_seek { Some(Vec::new()) } else { None };
            let boundary = &dist_prop.boundary;

            let key_is_before_boundary = |key: &EntryKey| key < &boundary.lower;
            let key_is_after_boundary = |key: &EntryKey| key >= &boundary.upper;
            let advance_after = |anchor: &EntryKey| {
                let Some(next_start) = bump_entry_key(anchor, ordering) else {
                    return None;
                };
                tree.seek(&next_start, ordering).current().cloned()
            };
            let mut cursor = tree.seek(&entry, ordering);
            let mut current = cursor.current().cloned();
            let mut last_key = None;
            while num_collected < buffer_size {
                let Some(key) = current.clone() else {
                    break;
                };

                if let Some((patt_key, patt_len)) = pattern {
                    if &key.as_slice()[..patt_len] != patt_key {
                        break;
                    }
                }

                match ordering {
                    Ordering::Forward => {
                        if key_is_before_boundary(&key) {
                            let next_candidate = cursor.next();
                            if let Some(progress) = trace_progress.as_mut() {
                                progress.push(format!(
                                    "skip-before current={:?} next={:?}",
                                    key.id(),
                                    next_candidate.as_ref().map(|k| k.id())
                                ));
                            }
                            current = next_candidate;
                            continue;
                        }
                        if key_is_after_boundary(&key) {
                            break;
                        }
                        match &range.start {
                            RangeTerm::Inclusive(k) => {
                                if key.prefix_lt(k) {
                                    let next_candidate = cursor.next();
                                    if let Some(progress) = trace_progress.as_mut() {
                                        progress.push(format!(
                                            "skip-start current={:?} next={:?}",
                                            key.id(),
                                            next_candidate.as_ref().map(|k| k.id())
                                        ));
                                    }
                                    current = next_candidate;
                                    continue;
                                }
                            }
                            RangeTerm::Exclusive(k) => {
                                if key.prefix_le(k) {
                                    let next_candidate = cursor.next();
                                    if let Some(progress) = trace_progress.as_mut() {
                                        progress.push(format!(
                                            "skip-start-ex current={:?} next={:?}",
                                            key.id(),
                                            next_candidate.as_ref().map(|k| k.id())
                                        ));
                                    }
                                    current = next_candidate;
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
                        if key_is_after_boundary(&key) {
                            let next_candidate = cursor.next();
                            if let Some(progress) = trace_progress.as_mut() {
                                progress.push(format!(
                                    "skip-after current={:?} next={:?}",
                                    key.id(),
                                    next_candidate.as_ref().map(|k| k.id())
                                ));
                            }
                            current = next_candidate;
                            continue;
                        }
                        if key_is_before_boundary(&key) {
                            break;
                        }
                        match &range.end {
                            RangeTerm::Inclusive(k) => {
                                if key.prefix_gt(k) {
                                    let next_candidate = cursor.next();
                                    if let Some(progress) = trace_progress.as_mut() {
                                        progress.push(format!(
                                            "skip-end current={:?} next={:?}",
                                            key.id(),
                                            next_candidate.as_ref().map(|k| k.id())
                                        ));
                                    }
                                    current = next_candidate;
                                    continue;
                                }
                            }
                            RangeTerm::Exclusive(k) => {
                                if key.prefix_ge(k) {
                                    let next_candidate = cursor.next();
                                    if let Some(progress) = trace_progress.as_mut() {
                                        progress.push(format!(
                                            "skip-end-ex current={:?} next={:?}",
                                            key.id(),
                                            next_candidate.as_ref().map(|k| k.id())
                                        ));
                                    }
                                    current = next_candidate;
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

                if seen_ids.insert(key.id()) {
                    last_key = Some(key.clone());
                    buffer.push(key.id());
                    num_collected += 1;
                    if let Some(progress) = trace_progress.as_mut() {
                        progress.push(format!("push current={:?}", key.id()));
                    }
                } else if let Some(progress) = trace_progress.as_mut() {
                    progress.push(format!("dedup-drop current={:?}", key.id()));
                }

                let next_candidate = cursor.next();
                if let Some(progress) = trace_progress.as_mut() {
                    progress.push(format!(
                        "advance current={:?} next={:?}",
                        key.id(),
                        next_candidate.as_ref().map(|k| k.id())
                    ));
                }
                current = next_candidate;
            }
            let mut next = current;
            if let Some(key) = &next {
                if key_is_after_boundary(key) {
                    next = Some(boundary.upper.clone());
                } else if key_is_before_boundary(key) {
                    next = Some(boundary.lower.clone());
                }
            }
            // Skip next duplicates
            while next.is_some() && next.as_ref().map(|k| k.id()).as_ref() == buffer.last() {
                next = advance_after(next.as_ref().unwrap());
                if let Some(key) = &next {
                    if key_is_after_boundary(key) {
                        next = Some(boundary.upper.clone());
                        break;
                    }
                    if key_is_before_boundary(key) {
                        next = Some(boundary.lower.clone());
                        break;
                    }
                }
            }
            if !buffer.is_empty() {
                match ordering {
                    Ordering::Forward => {
                        if next.as_ref() == Some(&boundary.upper) {
                            next = None;
                        }
                    }
                    Ordering::Backward => {
                        if next.as_ref() == Some(&boundary.lower) {
                            next = None;
                        }
                    }
                }
            }
            let result_block = ServBlock {
                buffer,
                next,
                last_key,
            };
            if trace_seek {
                trace_seek_block(
                    id,
                    entry,
                    &range,
                    boundary,
                    &result_block.buffer,
                    &result_block.next,
                );
                if let Some(gap) = trace_id_gap(&result_block.buffer) {
                    if let Some(progress) = trace_progress.as_ref() {
                        trace_seek_progress(id, entry, progress);
                    }
                    trace_probe_missing_key(id, tree, trace_schema_from_key(entry), gap);
                }
            }
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
        let pending_migrations = Arc::new(HashMap::with_capacity(32));
        super::btree::storage::start_external_nodes_write_back(client);
        Self::start_tree_balancer(&trees_map, &pending_migrations, client, sm_client);
        Self {
            client: client.clone(),
            sm_client: sm_client.clone(),
            trees: trees_map,
            pending_migrations,
        }
    }

    async fn hydrate_missing_tree(&self, id: Id, entry: &EntryKey) -> bool {
        if self.trees.contains_key(&id) {
            return true;
        }

        match self.sm_client.locate_key(entry).await {
            Ok((lower, placement, upper)) if placement.id == id => {
                if let Some(pending_tree) = self.pending_migrations.remove(&id) {
                    {
                        let mut pending_prop = pending_tree.prop.write();
                        pending_prop.boundary = Boundary::new(lower.clone(), upper.clone());
                        pending_prop.epoch = placement.epoch;
                        pending_prop.migration = None;
                    }
                    self.trees.insert(id, pending_tree);
                    info!(
                        "Promoted missing active tree {:?} from pending migration for entry {:?}",
                        id,
                        entry.id()
                    );
                    return true;
                }

                info!(
                    "Recovering missing active tree {:?} for entry {:?} with boundary [{:?}, {:?}), epoch={}",
                    id,
                    entry.id(),
                    lower,
                    upper,
                    placement.epoch
                );
                let tree = RangedTree::recover(&self.client, &id).await;
                if self.trees.contains_key(&id) {
                    return true;
                }
                self.trees.insert(
                    id,
                    Arc::new(DistTree::new(
                        id,
                        tree,
                        Boundary::new(lower, upper),
                        None,
                        placement.epoch,
                    )),
                );
                true
            }
            Ok((_lower, placement, _upper)) => {
                warn!(
                    "Cannot hydrate missing tree {:?} for entry {:?}: placement currently points to {:?} (epoch={})",
                    id,
                    entry.id(),
                    placement.id,
                    placement.epoch
                );
                false
            }
            Err(e) => {
                warn!(
                    "Cannot hydrate missing tree {:?} for entry {:?}: placement lookup failed: {:?}",
                    id,
                    entry.id(),
                    e
                );
                false
            }
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

    async fn split_visible_in_placement(
        sm_client: &Arc<SMClient>,
        pivot_key: &EntryKey,
        target_id: Id,
    ) -> bool {
        const SPLIT_RECONCILE_ATTEMPTS: usize = 8;
        const SPLIT_RECONCILE_DELAY_MS: u64 = 100;

        for attempt in 0..SPLIT_RECONCILE_ATTEMPTS {
            match sm_client.locate_key(pivot_key).await {
                Ok((_lower, placement, _upper)) if placement.id == target_id => {
                    debug!(
                        "Placement split reconciliation succeeded for pivot {:?} -> {:?} on attempt {}",
                        pivot_key,
                        target_id,
                        attempt + 1
                    );
                    return true;
                }
                Ok((_lower, placement, _upper)) => {
                    debug!(
                        "Placement split reconciliation attempt {} for pivot {:?} still points to {:?} instead of {:?}",
                        attempt + 1,
                        pivot_key,
                        placement.id,
                        target_id
                    );
                }
                Err(e) => {
                    warn!(
                        "Placement split reconciliation attempt {} failed for pivot {:?} -> {:?}: {:?}",
                        attempt + 1,
                        pivot_key,
                        target_id,
                        e
                    );
                }
            }
            tokio::time::sleep(Duration::from_millis(SPLIT_RECONCILE_DELAY_MS)).await;
        }
        false
    }

    async fn rollback_pending_split(
        dist_tree: &Arc<DistTree>,
        target_id: Id,
        pending_migrations: &Arc<HashMap<Id, Arc<DistTree>>>,
        client: &Arc<AsyncClient>,
    ) {
        pending_migrations.remove(&target_id);
        {
            let mut dist_prop = dist_tree.prop.write();
            dist_prop.migration = None;
        }
        if let Err(unmark_err) = dist_tree
            .tree
            .mark_migration(&dist_tree.id, None, client)
            .await
        {
            warn!(
                "Failed to clear migration marker for tree {:?} after split rollback: {:?}",
                dist_tree.id, unmark_err
            );
        }
    }

    async fn ensure_split_target_loaded(
        client: &Arc<AsyncClient>,
        target_id: Id,
        boundary: Boundary,
        epoch: u64,
    ) -> bool {
        const TARGET_LOAD_ATTEMPTS: usize = 5;
        const TARGET_LOAD_RETRY_DELAY_MS: u64 = 100;

        let load_started = Instant::now();
        for attempt in 0..TARGET_LOAD_ATTEMPTS {
            match locate_tree_server_from_conshash(
                &target_id,
                &client.conshash,
                client.group_name(),
                client.database_name(),
            )
            .await
            {
                Ok(tree_client) => match tree_client
                    .load_tree(target_id, boundary.clone(), epoch)
                    .await
                {
                    Ok(()) => {
                        debug!(
                            "Loaded split target {:?} on attempt {} in {:?}",
                            target_id,
                            attempt + 1,
                            load_started.elapsed()
                        );
                        return true;
                    }
                    Err(e) => {
                        warn!(
                            "Failed to load split target {:?} on attempt {} with boundary {:?}, epoch {}: {:?}",
                            target_id,
                            attempt + 1,
                            boundary,
                            epoch,
                            e
                        );
                    }
                },
                Err(e) => {
                    warn!(
                        "Failed to locate split target {:?} on attempt {} for boundary {:?}, epoch {}: {:?}",
                        target_id,
                        attempt + 1,
                        boundary,
                        epoch,
                        e
                    );
                }
            }
            tokio::time::sleep(Duration::from_millis(TARGET_LOAD_RETRY_DELAY_MS)).await;
        }

        warn!(
            "Exhausted attempts loading split target {:?} after {:?}; routing will rely on retries until the target is loaded",
            target_id,
            load_started.elapsed()
        );
        false
    }

    pub fn start_tree_balancer(
        trees_map: &Arc<HashMap<Id, Arc<DistTree>>>,
        pending_migrations: &Arc<HashMap<Id, Arc<DistTree>>>,
        client: &Arc<AsyncClient>,
        sm_client: &Arc<SMClient>,
    ) {
        debug!("Starting range indexer tree balancer");
        let trees_map = trees_map.clone();
        let pending_migrations = pending_migrations.clone();
        let client = client.clone();
        let sm_client = sm_client.clone();
        tokio::spawn(async move {
            const SPLIT_RETRY_BACKOFF_MS: u64 = 2_000;
            // Periodic checkpoint: flush B-tree pages and update tree root cells
            // every ~60 seconds to ensure durability even without explicit shutdown.
            const CHECKPOINT_INTERVAL_LOOPS: u32 = 120; // 120 * 500ms = 60s
            let mut checkpoint_counter: u32 = 0;
            let mut split_backoff_until = StdHashMap::<Id, Instant>::new();

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

                    let tree_prop_snapshot = dist_tree.prop.read().clone();
                    if tree.should_split() {
                        if tree_prop_snapshot.migration.is_some() {
                            debug!(
                                "Skipping split for {:?}: migration already active, tree_count={}, ideal_cap={}, epoch={}",
                                dist_tree.id,
                                tree.count(),
                                tree.ideal_capacity(),
                                tree_prop_snapshot.epoch
                            );
                            continue;
                        }

                        let now = Instant::now();
                        if let Some(backoff_until) = split_backoff_until.get(&dist_tree.id) {
                            if *backoff_until > now {
                                debug!(
                                    "Skipping split for {:?}: retry backoff active for {:?}, tree_count={}, ideal_cap={}",
                                    dist_tree.id,
                                    backoff_until.saturating_duration_since(now),
                                    tree.count(),
                                    tree.ideal_capacity()
                                );
                                continue;
                            }
                        }
                        split_backoff_until.remove(&dist_tree.id);

                        let split_started = Instant::now();
                        info!(
                            "LSM Tree reached split threshold {:?}, start migration; tree_count={}, ideal_cap={}, epoch={}",
                            dist_tree.id,
                            tree.count(),
                            tree.ideal_capacity(),
                            tree_prop_snapshot.epoch
                        );
                        // Tree oversized, need to migrate
                        let Some(pivot_key) = tree.pivot_key() else {
                            warn!(
                                "Skipping migration for {:?}: oversized tree has no pivot key",
                                dist_tree.id
                            );
                            continue;
                        };
                        let migration_target_id = Id::rand();
                        let source_upper = { dist_tree.prop.read().boundary.upper.clone() };
                        debug!(
                            "Creating migration target tree {:?} split at {:?}",
                            migration_target_id, pivot_key
                        );
                        let migration_tree = Arc::new(DistTree::new(
                            migration_target_id,
                            RangedTree::create(&client, &migration_target_id).await,
                            Boundary::new(pivot_key.clone(), source_upper),
                            None,
                            INITIAL_TREE_EPOCH,
                        ));
                        pending_migrations.insert(migration_target_id, migration_tree.clone());
                        {
                            let mut dist_tree_prop = dist_tree.prop.write();
                            dist_tree_prop.migration = Some(Migration {
                                pivot: pivot_key.clone(),
                                target_id: migration_target_id,
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
                        let mut cursor = tree.seek(&*MIN_ENTRY_KEY, Ordering::Forward);
                        let mut entry_buffer = Vec::with_capacity(buffer_size);
                        debug!(
                            "Start moving keys from {:?} to {:?}",
                            dist_tree.id, migration_target_id
                        );
                        while let Some(entry) = cursor.current().cloned() {
                            if entry >= pivot_key {
                                entry_buffer.push(entry);
                            }
                            break;
                        }
                        while let Some(entry) = cursor.next() {
                            if entry < pivot_key {
                                continue;
                            }
                            entry_buffer.push(entry);
                            if entry_buffer.len() >= buffer_size {
                                entry_buffer.dedup();
                                debug!("Merging entry buffer, size {}", entry_buffer.len());
                                migration_tree.tree.merge_keys(entry_buffer);
                                entry_buffer = Vec::with_capacity(buffer_size);
                            }
                        }
                        entry_buffer.dedup();
                        debug!("Merging last batch of keys, size {}", entry_buffer.len());
                        migration_tree.tree.merge_keys(entry_buffer);
                        let mut reconcile_cursor = tree.seek(&*MIN_ENTRY_KEY, Ordering::Forward);
                        if let Some(entry) = reconcile_cursor.current().cloned() {
                            if entry >= pivot_key {
                                let _ = migration_tree.tree.insert(&entry);
                            }
                        }
                        while let Some(entry) = reconcile_cursor.next() {
                            if entry < pivot_key {
                                continue;
                            }
                            let _ = migration_tree.tree.insert(&entry);
                        }
                        debug!("Waiting for new tree {:?} persisted", migration_target_id);
                        storage::wait_until_updated().await;

                        debug!(
                            "Publishing new tree {:?} head before placement split",
                            migration_target_id
                        );
                        if let Err(e) = migration_tree
                            .tree
                            .mark_migration(&migration_target_id, None, &client)
                            .await
                        {
                            warn!(
                                "Failed to publish target tree {:?} before split from {:?}: {:?}",
                                migration_target_id, dist_tree.id, e
                            );
                            pending_migrations.remove(&migration_target_id);
                            {
                                let mut dist_prop = dist_tree.prop.write();
                                dist_prop.migration = None;
                            }
                            if let Err(unmark_err) =
                                tree.mark_migration(&dist_tree.id, None, &client).await
                            {
                                warn!(
                                    "Failed to clear migration marker for tree {:?} after target publish failure: {:?}",
                                    dist_tree.id, unmark_err
                                );
                            }
                            continue;
                        }

                        debug!("Calling placement for split to {:?}", migration_target_id);
                        match sm_client.locate_key(&pivot_key).await {
                            Ok((_lower, placement, _upper)) => {
                                debug!(
                                    "Preflighted split for source {:?} at pivot {:?}; current placement tree {:?}, epoch {}",
                                    dist_tree.id, pivot_key, placement.id, placement.epoch
                                );
                            }
                            Err(e) => {
                                warn!(
                                    "Failed to preflight split for source {:?} at pivot {:?}: {:?}",
                                    dist_tree.id, pivot_key, e
                                );
                            }
                        }
                        let target_boundary = migration_tree.prop.read().boundary.clone();
                        let split_committed = match sm_client
                            .split(&dist_tree.id, &migration_target_id, &pivot_key)
                            .await
                        {
                            Ok(()) => true,
                            Err(e) => {
                                warn!(
                                    "Placement split from {:?} to {:?} at {:?} returned error: {:?}",
                                    dist_tree.id, migration_target_id, pivot_key, e
                                );
                                if Self::split_visible_in_placement(
                                    &sm_client,
                                    &pivot_key,
                                    migration_target_id,
                                )
                                .await
                                {
                                    warn!(
                                        "Placement split from {:?} to {:?} at {:?} appears committed despite RPC error after {:?}; continuing",
                                        dist_tree.id,
                                        migration_target_id,
                                        pivot_key,
                                        split_started.elapsed()
                                    );
                                    true
                                } else {
                                    warn!(
                                        "Placement split from {:?} to {:?} at {:?} did not become visible after {:?}; rolling back in-memory migration state and backing off",
                                        dist_tree.id,
                                        migration_target_id,
                                        pivot_key,
                                        split_started.elapsed()
                                    );
                                    Self::rollback_pending_split(
                                        &dist_tree,
                                        migration_target_id,
                                        &pending_migrations,
                                        &client,
                                    )
                                    .await;
                                    split_backoff_until.insert(
                                        dist_tree.id,
                                        Instant::now()
                                            + Duration::from_millis(SPLIT_RETRY_BACKOFF_MS),
                                    );
                                    false
                                }
                            }
                        };
                        if !split_committed {
                            continue;
                        }
                        let target_loaded = Self::ensure_split_target_loaded(
                            &client,
                            migration_target_id,
                            target_boundary,
                            INITIAL_TREE_EPOCH,
                        )
                        .await;
                        if !target_loaded {
                            split_backoff_until.insert(
                                dist_tree.id,
                                Instant::now() + Duration::from_millis(SPLIT_RETRY_BACKOFF_MS),
                            );
                        } else {
                            split_backoff_until.remove(&dist_tree.id);
                        }
                        // Update routing state on the source tree immediately; actual pruning runs in the background.
                        {
                            let mut dist_prop = dist_tree.prop.write();
                            dist_prop.boundary.upper = pivot_key.clone();
                            dist_prop.epoch += 1;
                        }
                        let source_tree = dist_tree.clone();
                        let pivot_for_retain = pivot_key.clone();
                        let client_for_retain = client.clone();
                        let pending_migrations = pending_migrations.clone();
                        tokio::spawn(async move {
                            let retain_tree = source_tree.clone();
                            let retain_pivot = pivot_for_retain.clone();
                            let retain_result = tokio::task::spawn_blocking(move || {
                                retain_tree.tree.retain(&retain_pivot);
                            })
                            .await;
                            if let Err(e) = retain_result {
                                warn!(
                                    "Background retain failed for tree {:?}: {:?}",
                                    source_tree.id, e
                                );
                                return;
                            }

                            storage::wait_until_updated().await;
                            {
                                let mut dist_prop = source_tree.prop.write();
                                dist_prop.migration = None;
                            }
                            debug!("Unmark migration {:?}", source_tree.id);
                            if let Err(e) = source_tree
                                .tree
                                .mark_migration(&source_tree.id, None, &client_for_retain)
                                .await
                            {
                                warn!(
                                    "Failed to publish retained tree {:?}: {:?}",
                                    source_tree.id, e
                                );
                            }
                            pending_migrations.remove(&migration_target_id);
                        });
                        debug!(
                            "LSM tree migration from {:?} to {:?} succeed in {:?}",
                            dist_tree.id,
                            migration_target_id,
                            split_started.elapsed()
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

    fn apply_in_ranged_tree<'a, F, R>(
        &'a self,
        id: Id,
        entry: EntryKey,
        epoch: u64,
        _route_pending_migration_to_target: bool,
        func: F,
    ) -> BoxFuture<'a, OpResult<R>>
    where
        F: Fn(&EntryKey, &RangedTree, &DistProp) -> OpResult<R> + Send + 'a,
        R: Send + 'static,
    {
        async move {
            for attempt in 0..2 {
                if let Some(tree) = self.trees.get(&id) {
                    let tree_prop = tree.prop.read().clone();
                    if epoch < tree_prop.epoch {
                        return OpResult::EpochMissMatch(tree_prop.epoch, epoch);
                    }
                    if tree_prop.boundary.in_boundary(&entry) {
                        if tree_prop.migration.is_some() {
                            // Keep the source tree immutable while the split snapshot is copied and
                            // placement catches up. Clients retry against fresh placement once the
                            // migration window closes.
                            return OpResult::Migrating;
                        }
                        return func(&entry, &tree.tree, &tree_prop);
                    }
                    return OpResult::OutOfBound;
                }

                if attempt == 0 && self.hydrate_missing_tree(id, &entry).await {
                    continue;
                }

                return OpResult::NotFound;
            }

            OpResult::NotFound
        }
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
            Ordering::Backward => self.end = RangeTerm::Inclusive(key),
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
