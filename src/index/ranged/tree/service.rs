use super::super::sm::client::SMClient;
use super::super::trees::*;
pub use super::btree::level::BTREE_NODE_SIZE as MIGRATE_SIZE;
use super::btree::storage;
use super::tree::*;
use crate::client::AsyncClient;
use crate::ram::schema::SchemaUid;
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
    // Operations currently inside this tree (between the routing checks and
    // the end of their tree call). The split balancer sets the migration
    // marker and then waits for this to reach zero before touching the
    // structure -- an operation that passed the checks before the marker
    // landed is still counted, so no writer races split_off. A counter
    // rather than holding the prop read guard across the call: a guard
    // would let parking_lot's writer-fairness park every routing check
    // behind a pending freeze.
    in_flight: std::sync::atomic::AtomicUsize,
}

impl DistTree {
    /// Live keys in this tree's in-memory B-tree.
    pub fn key_count(&self) -> usize {
        self.tree.tree.len()
    }
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
    rpc contains(id: Id, entry: EntryKey, epoch: u64) -> OpResult<bool>;
    rpc seek(id: Id, range: Range, pattern: &Option<Vec<u8>>, buffer_size: u16, epoch: u64)
        -> OpResult<ServBlock>;
    rpc stat(id: Id) -> OpResult<TreeStat>;
    rpc flush_all();
}

service_with_id!(TreeService, DEFAULT_SERVICE_ID);

/// Process-local handles to this node's tree services, one per database,
/// published at construction. The services are otherwise only reachable
/// through the RPC registry, so status reporting has no way to ask them how
/// large the ranged index is. Read-only; nothing on an operation path touches
/// this.
///
/// Keyed per database because a `OnceLock` here reported the wrong index
/// entirely: the tree service is built once per database, so only the first
/// one to initialize was ever published, and every other database's health
/// then reported *its* numbers. On TB15 that meant wikidata's health said
/// 1 tree and 0 keys -- the default database's -- while wikidata's index held
/// 43 trees and 117,677,730 keys. Under-reporting to zero is the worst
/// direction for this particular number: an empty ranged index is exactly the
/// failure being looked for.
/// A plain `RwLock<HashMap>`, and it must stay removable. This was a
/// `PtrHashMap` that was only ever inserted into -- so every database's tree
/// service, and with it every page its trees held, stayed alive for the life of
/// the process even after the database was unloaded or dropped. Two separate
/// mistakes: nothing retired the entry, and `PtrHashMap::remove` would not have
/// released it anyway (it returns a clone and leaves the original in the retired
/// node, so removal is not a release; see
/// `neb::server::tests::ptr_hash_map_remove_does_not_release_the_value`).
///
/// Per the comment above, nothing on an operation path reads this, so a lock
/// costs nothing here.
static LOCAL_TREE_SERVICES: std::sync::LazyLock<
    parking_lot::RwLock<std::collections::HashMap<String, std::sync::Arc<TreeService>>>,
> = std::sync::LazyLock::new(|| parking_lot::RwLock::new(std::collections::HashMap::new()));

/// Key a database's tree service by the pair that scopes it everywhere else.
pub fn local_tree_service_key(group_name: &str, database_name: &str) -> String {
    format!("{group_name}/{database_name}")
}

/// Publish this database's tree service for status reporting.
pub fn publish_local_tree_service(
    group_name: &str,
    database_name: &str,
    service: std::sync::Arc<TreeService>,
) {
    LOCAL_TREE_SERVICES
        .write()
        .insert(local_tree_service_key(group_name, database_name), service);
}

/// Forget a database's tree service, because the database is going away.
///
/// Must be called when a runtime is unloaded or dropped. Without it the service
/// -- and every page its trees hold -- outlives the database that owned it, for
/// the life of the process.
pub fn retire_local_tree_service(group_name: &str, database_name: &str) -> bool {
    LOCAL_TREE_SERVICES
        .write()
        .remove(&local_tree_service_key(group_name, database_name))
        .is_some()
}

/// This database's tree service, if it has one.
pub fn local_tree_service(
    group_name: &str,
    database_name: &str,
) -> Option<std::sync::Arc<TreeService>> {
    LOCAL_TREE_SERVICES
        .read()
        .get(&local_tree_service_key(group_name, database_name))
        .cloned()
}

pub struct TreeService {
    client: Arc<AsyncClient>,
    sm_client: Arc<SMClient>,
    trees: Arc<HashMap<Id, Arc<DistTree>>>,
    pending_migrations: Arc<HashMap<Id, Arc<DistTree>>>,
    // Set by flush_all_trees: freezes the balancer so no split or checkpoint
    // races the shutdown flush and lands writes after the segment archive.
    balancer_stopped: Arc<std::sync::atomic::AtomicBool>,
    // Cleared by the balancer when it has actually left its loop. Setting
    // `balancer_stopped` alone is NOT enough: the balancer only observes the
    // flag between iterations, and an iteration can be mid-merge or mid-split
    // for seconds. Flushing while it still runs lets it enqueue page writes
    // after the drain barrier and publish head pointers for them -- and the
    // hub reset then discards those queued writes, leaving durable heads that
    // name pages which never landed (TB16/17/18 MissingPage corpses).
    balancer_running: Arc<std::sync::atomic::AtomicBool>,
}

/// The family named by a key's prefix. Diagnostics only.
fn trace_schema_from_key(key: &EntryKey) -> SchemaUid {
    let mut schema = [0u8; 4];
    schema.copy_from_slice(&key.as_slice()[..4]);
    SchemaUid(u32::from_be_bytes(schema))
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
    if trace_schema_from_key(key) != SchemaUid(schema_id) {
        return false;
    }
    match pattern {
        Some(p) => p.len() == 16,
        None => true,
    }
}

fn trace_id_gap(ids: &[Id]) -> Option<(Id, Id)> {
    ids.windows(2)
        .find(|pair| pair[1].bits() != pair[0].bits() + 1)
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

fn trace_probe_missing_key(tree_id: Id, tree: &RangedTree, schema_id: SchemaUid, gap: (Id, Id)) {
    if gap.1.bits() != gap.0.bits() + 2 {
        return;
    }

    let missing_id = Id::from_bits(gap.0.bits() + 1);
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
            self.reconcile_split_marker(&id, &boundary).await;
            let tree =
                match RangedTree::recover_bounded(&self.client, &id, Some(&boundary.upper)).await {
                    Ok(tree) => tree,
                    Err(error) => {
                        // Leave it absent rather than installing an empty tree.
                        // An empty tree answers every range scan with "no rows",
                        // which is indistinguishable from a correct answer and is
                        // how a store that had not finished recovering came to
                        // look like a store with no data. Absent means the next
                        // operation on this range tries again.
                        error!(
                            "Not loading ranged tree {:?}: {}. The range it covers will report \
                         errors until it can be read, which is preferable to reporting empty.",
                            id, error
                        );
                        return;
                    }
                };
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

    /// Read-only exact presence, for the index scrub.
    ///
    /// `false` for the write flag: this must never hydrate-and-install a
    /// tree as a side effect of being asked a question. A scrub that
    /// repaired trees by looking at them would make its own verify pass
    /// unrepeatable.
    fn contains(&self, id: Id, entry: EntryKey, epoch: u64) -> BoxFuture<'_, OpResult<bool>> {
        self.apply_in_ranged_tree(id, entry, epoch, false, move |entry, tree, _dist_prop| {
            OpResult::Successful(tree.contains(&entry))
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
            // A scan's keys are strictly monotonic; a cursor that yields a
            // key not past the previous one is walking pages a concurrent
            // structural split has already relinked -- left alone it can
            // cycle forever (2026-08-31: a sidecar-flush seek spun for hours
            // after five back-to-back splits, its duplicates silently eaten
            // by the id dedup below). On regression, re-descend from the
            // root past the last yielded key; the fresh descent uses the
            // post-split structure. Bounded so a pathological tree returns
            // a partial block instead of never returning.
            const MAX_SEEK_RESTARTS: usize = 64;
            let mut prev_key: Option<EntryKey> = None;
            let mut restarts = 0usize;
            while num_collected < buffer_size {
                let Some(key) = current.clone() else {
                    break;
                };
                if let Some(prev) = &prev_key {
                    // An EQUAL key is a benign replay -- an LSM level merge
                    // moving the key between trees mid-scan -- and the id
                    // dedup below already drops it; just advance. Only a key
                    // STRICTLY behind the previous one means the chain under
                    // this cursor is broken.
                    if &key == prev {
                        current = cursor.next();
                        continue;
                    }
                    let regressed = match ordering {
                        Ordering::Forward => &key < prev,
                        Ordering::Backward => &key > prev,
                    };
                    if regressed {
                        restarts += 1;
                        if restarts > MAX_SEEK_RESTARTS {
                            warn!(
                                "range seek gave up after {} chain restarts at {:?}; \
                                 returning a partial block",
                                restarts,
                                key.id()
                            );
                            break;
                        }
                        let Some(next_start) = bump_entry_key(prev, ordering) else {
                            break;
                        };
                        cursor = tree.seek(&next_start, ordering);
                        current = cursor.current().cloned();
                        continue;
                    }
                }
                prev_key = Some(key.clone());

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
            // Skip next duplicates. advance_after re-seeks the tree, and a
            // seek disturbed by concurrent level merges can hand back its
            // anchor (or an earlier key) -- trusted blindly, this loop
            // re-seeked the whole tree per iteration, silently, forever
            // (2026-08-31: one worker pinned at 100% for hours under an
            // ensure_scannable verification; the gdb dump landed exactly
            // here). Require strict progress and bound the hops; on either
            // failure hand the client the bumped anchor -- its own
            // cross-block dedup makes resuming there correct.
            const MAX_DUP_SKIP_HOPS: usize = 128;
            let mut dup_hops = 0usize;
            while next.is_some() && next.as_ref().map(|k| k.id()).as_ref() == buffer.last() {
                let anchor = next.clone().unwrap();
                let candidate = advance_after(&anchor);
                let progressed = match (&candidate, ordering) {
                    (Some(c), Ordering::Forward) => c > &anchor,
                    (Some(c), Ordering::Backward) => c < &anchor,
                    (None, _) => true,
                };
                dup_hops += 1;
                if !progressed || dup_hops > MAX_DUP_SKIP_HOPS {
                    if dup_hops > MAX_DUP_SKIP_HOPS {
                        warn!(
                            "duplicate-skip gave up after {} hops at {:?}; resuming client at the bumped anchor",
                            dup_hops,
                            anchor.id()
                        );
                    }
                    next = bump_entry_key(&anchor, ordering);
                    break;
                }
                next = candidate;
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
    /// Ranged-index size: (trees, total keys, registry map resident bytes).
    ///
    /// Key counts come from each B-tree's `len` atomic, which is what actually
    /// tracks the in-memory node population -- the nodes are plain heap
    /// (`NodeCellRef`), invisible to mincore, so counting them is the only way
    /// to size this index.
    pub fn index_stats(&self) -> (usize, usize, usize) {
        let trees = self.trees.entries();
        let keys = trees.iter().map(|(_, t)| t.key_count()).sum();
        (trees.len(), keys, self.resident_bytes())
    }

    /// Resident bytes of the tree registries. The B-tree *pages* themselves are
    /// stored as cells inside the tiered segments, so they are already counted
    /// there -- this is only the in-memory index of trees.
    pub fn resident_bytes(&self) -> usize {
        (self.trees.resident_pages() + self.pending_migrations.resident_pages()) * 4096
    }

    pub fn new(client: &Arc<AsyncClient>, sm_client: &Arc<SMClient>) -> Self {
        info!("Initializing LSM tree service");
        let trees_map = Arc::new(HashMap::with_capacity(32));
        let pending_migrations = Arc::new(HashMap::with_capacity(32));
        let balancer_stopped = Arc::new(std::sync::atomic::AtomicBool::new(false));
        let balancer_running = Arc::new(std::sync::atomic::AtomicBool::new(true));
        super::btree::storage::start_external_nodes_write_back(client);
        Self::start_tree_balancer(
            &trees_map,
            &pending_migrations,
            client,
            sm_client,
            &balancer_stopped,
            &balancer_running,
        );
        Self {
            client: client.clone(),
            sm_client: sm_client.clone(),
            trees: trees_map,
            pending_migrations,
            balancer_stopped,
            balancer_running,
        }
    }

    /// Reconcile a durable split marker before loading `id`. A crash after
    /// the seam barrier but before the placement flip leaves the source's
    /// chain cut with an orphaned target tree; a crash before the barrier
    /// leaves the chain intact with a stale marker. Committed splits (the
    /// placement map knows the target) just shed the marker. The decision
    /// key: whoever owns the key at this tree's placement upper bound.
    async fn reconcile_split_marker(&self, id: &Id, boundary: &Boundary) {
        let Some((head, Some(target))) = super::tree::read_tree_metadata(&self.client, id).await
        else {
            return;
        };
        let committed = match self.sm_client.locate_key(&boundary.upper).await {
            Ok(Some((_, placement, _))) => placement.id == target,
            Ok(None) => {
                warn!(
                    "Cannot reconcile split marker on {:?} (target {:?}): no placement \
                     covers the boundary yet; leaving the marker for the next load",
                    id, target
                );
                return;
            }
            Err(e) => {
                warn!(
                    "Cannot reconcile split marker on {:?} (target {:?}): placement \
                     lookup failed: {:?}; leaving the marker for the next load",
                    id, target, e
                );
                return;
            }
        };
        if committed {
            info!(
                "Tree {:?} carries a committed split marker for {:?}; clearing it",
                id, target
            );
            super::tree::clear_migration_marker(&self.client, id).await;
            return;
        }
        // Uncommitted: the placement never flipped, so this tree still owns
        // the whole range and the split must be undone.
        if let Some((target_head, _)) = super::tree::read_tree_metadata(&self.client, &target).await
        {
            let chain = super::tree::walk_chain_page_ids(&self.client, head).await;
            if chain.contains(&target_head) {
                debug!(
                    "Uncommitted split of {:?}: chain still reaches the moved head; \
                     no relink needed",
                    id
                );
            } else if let Some(last) = chain.last() {
                match super::tree::relink_page_next(&self.client, *last, target_head).await {
                    Ok(()) => info!(
                        "Uncommitted split of {:?}: rejoined severed chain at {:?} -> {:?}",
                        id, last, target_head
                    ),
                    Err(e) => {
                        error!(
                            "Uncommitted split of {:?}: failed to rejoin chain: {}; \
                             leaving marker so the next load retries",
                            id, e
                        );
                        return;
                    }
                }
            }
            let _ = self.client.remove_cell(target).await;
        }
        super::tree::clear_migration_marker(&self.client, id).await;
        info!(
            "Rolled back uncommitted split of {:?} (orphaned target {:?})",
            id, target
        );
    }

    async fn hydrate_missing_tree(&self, id: Id, entry: &EntryKey) -> bool {
        if self.trees.contains_key(&id) {
            return true;
        }

        match self.sm_client.locate_key(entry).await {
            Ok(Some((lower, placement, upper))) if placement.id == id => {
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
                self.reconcile_split_marker(&id, &Boundary::new(lower.clone(), upper.clone()))
                    .await;
                let tree = match RangedTree::recover_bounded(&self.client, &id, Some(&upper)).await
                {
                    Ok(tree) => tree,
                    Err(error) => {
                        // Same reasoning as the load path: an unreadable tree
                        // stays absent so the next attempt can succeed, rather
                        // than becoming a permanently empty one.
                        error!(
                            "Cannot hydrate ranged tree {:?} for entry {:?}: {}",
                            id,
                            entry.id(),
                            error
                        );
                        return false;
                    }
                };
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
            Ok(None) => {
                warn!(
                    "Cannot hydrate missing tree {:?} for entry {:?}: no placement covers it yet",
                    id,
                    entry.id()
                );
                false
            }
            Ok(Some((_lower, placement, _upper))) => {
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
        // Structural churn stops first -- and we WAIT for it to actually
        // stop. The balancer observes the flag only between iterations, and
        // an iteration can sit inside a merge or a split for seconds; every
        // page write or head publish it makes after the barrier below is
        // either lost to the hub reset or lands after the segment archive.
        self.balancer_stopped
            .store(true, std::sync::atomic::Ordering::SeqCst);
        let ack_started = Instant::now();
        const BALANCER_ACK_TIMEOUT: Duration = Duration::from_secs(120);
        while self
            .balancer_running
            .load(std::sync::atomic::Ordering::SeqCst)
        {
            if ack_started.elapsed() > BALANCER_ACK_TIMEOUT {
                error!(
                    "flush_all_trees: tree balancer did not acknowledge the stop within {:?}; \
                     proceeding, but page writes it makes from here may be discarded",
                    BALANCER_ACK_TIMEOUT
                );
                break;
            }
            tokio::time::sleep(Duration::from_millis(20)).await;
        }
        info!(
            "Tree balancer acknowledged stop in {:?}",
            ack_started.elapsed()
        );
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
        let barrier = super::btree::storage::wait_until_updated().await;
        if barrier {
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
        } else {
            // Publishing heads over an unestablished barrier is exactly the
            // recovery corruption this flush exists to prevent. The current
            // metadata cells still point at the previous consistent chains,
            // so skipping the publish loses at most recent writes -- never
            // consistency.
            error!(
                "flush_all_trees: write-back barrier NOT established; refusing to publish \
                 tree head pointers over undrained pages"
            );
        }

        // Head publishes can themselves dirty pages (tombstone compaction
        // during to_cell), so re-drain before stopping the hub: the reset
        // DISCARDS whatever is still queued, and anything discarded here is
        // a page some durable head may already name.
        if !super::btree::storage::wait_until_updated().await {
            error!("flush_all_trees: post-publish drain barrier NOT established");
        }

        // Stop THIS server's write-back hub so no worker persists pages
        // after the caller's segment sync/archive. Other servers in the
        // process keep their hubs.
        super::btree::storage::hub_for(&self.client).reset().await;

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
                Ok(Some((_lower, placement, _upper))) if placement.id == target_id => {
                    debug!(
                        "Placement split reconciliation succeeded for pivot {:?} -> {:?} on attempt {}",
                        pivot_key,
                        target_id,
                        attempt + 1
                    );
                    return true;
                }
                Ok(None) => {
                    debug!(
                        "Placement split reconciliation attempt {} for pivot {:?} found no placement covering it yet",
                        attempt + 1,
                        pivot_key
                    );
                }
                Ok(Some((_lower, placement, _upper))) => {
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
        moved: &Arc<DistTree>,
        target_id: Id,
        pending_migrations: &Arc<HashMap<Id, Arc<DistTree>>>,
        client: &Arc<AsyncClient>,
    ) {
        // split_off already truncated the LIVE source tree, so clearing the
        // markers alone orphans every moved key -- routing still sends their
        // range to the source, which no longer holds them (2026-08-30: a
        // full store failed the seam barrier and this "rollback" quietly
        // dropped 5.7M of 8.4M keys in the stress test, and a handful of
        // scannable keys per BANC bulk import). Reabsorb the moved half
        // before dropping the target; a failed split must degrade into
        // "split deferred", never into loss.
        let mut cursor = moved.tree.seek(&min_entry_key(), Ordering::Forward);
        let mut restored = 0usize;
        while let Some(key) = cursor.next() {
            if dist_tree.tree.insert(&key) {
                restored += 1;
            }
        }
        info!(
            "Rolled back split of {:?}: reabsorbed {} moved key(s) from target {:?}",
            dist_tree.id, restored, target_id
        );
        pending_migrations.remove(&target_id);
        // The target's metadata cell (if its publish got that far) must not
        // survive to be recovered as a tree; its leaves now belong to the
        // source again.
        let _ = client.remove_cell(target_id).await;
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
        stopped: &Arc<std::sync::atomic::AtomicBool>,
        running: &Arc<std::sync::atomic::AtomicBool>,
    ) {
        debug!("Starting range indexer tree balancer");
        let trees_map = trees_map.clone();
        let pending_migrations = pending_migrations.clone();
        let client = client.clone();
        let sm_client = sm_client.clone();
        let stopped = stopped.clone();
        let running = running.clone();
        // Clears `running` however the loop ends -- returning on the stop
        // flag, panicking, or being cancelled with its runtime -- so a
        // shutdown waiting on the acknowledgment can never wait forever on
        // a balancer that is already gone.
        struct RunningGuard(Arc<std::sync::atomic::AtomicBool>);
        impl Drop for RunningGuard {
            fn drop(&mut self) {
                self.0.store(false, std::sync::atomic::Ordering::SeqCst);
            }
        }
        tokio::spawn(async move {
            let _running_guard = RunningGuard(running);
            const SPLIT_RETRY_BACKOFF_MS: u64 = 2_000;
            // Periodic checkpoint: flush B-tree pages and update tree root cells
            // every ~60 seconds to ensure durability even without explicit shutdown.
            const CHECKPOINT_INTERVAL_LOOPS: u32 = 120; // 120 * 500ms = 60s
            let mut checkpoint_counter: u32 = 0;
            let mut split_backoff_until = StdHashMap::<Id, Instant>::new();

            loop {
                if stopped.load(std::sync::atomic::Ordering::SeqCst) {
                    info!("Tree balancer stopped for shutdown");
                    return;
                }
                let mut fast_mode = false;
                checkpoint_counter = checkpoint_counter.wrapping_add(1);
                let do_checkpoint = checkpoint_counter % CHECKPOINT_INTERVAL_LOOPS == 0;

                // Head publishes below are gated on the drain barrier: a
                // checkpoint over an unestablished barrier would point tree
                // metadata at pages that never became durable.
                let mut checkpoint_barrier = false;
                if do_checkpoint {
                    // Ensure all pending B-tree node writes are flushed, then update
                    // tree root cells. This limits the recovery window to ~60 seconds
                    // of writes even if the server is killed without a clean shutdown.
                    checkpoint_barrier = storage::wait_until_updated().await;
                    if !checkpoint_barrier {
                        error!(
                            "balancer checkpoint: write-back barrier NOT established; \
                             skipping this round's head publishes"
                        );
                    }
                }

                for (_, dist_tree) in trees_map.entries() {
                    let tree = &dist_tree.tree;
                    let merged = tree.merge_levels().await;
                    fast_mode = merged | fast_mode;

                    // If merge happened, wait for external nodes to be written, then update the tree cell
                    if merged {
                        // Wait for all external B+tree nodes to be written to storage
                        if storage::wait_until_updated().await {
                            // Now update the cell with the new head IDs
                            if let Err(e) = tree.mark_migration(&dist_tree.id, None, &client).await
                            {
                                warn!("Failed to mark LSM tree migration after merge: {:?}", e);
                            }
                        } else {
                            error!(
                                "post-merge publish for {:?} skipped: write-back barrier \
                                 NOT established",
                                dist_tree.id
                            );
                        }
                    } else if do_checkpoint && checkpoint_barrier {
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
                        // Freeze the source before touching its structure: with
                        // the marker set, apply_in_ranged_tree returns Migrating
                        // and rejects writes (docs/tla/StructuralSplit.tla).
                        {
                            let mut dist_tree_prop = dist_tree.prop.write();
                            dist_tree_prop.migration = Some(Migration {
                                pivot: pivot_key.clone(),
                                target_id: migration_target_id,
                            });
                        }
                        // ... and then DRAIN: an operation that passed the
                        // routing checks before the marker landed is still
                        // inside the tree, counted in `in_flight`. Splitting
                        // under it is how entries landed in leaves the split
                        // truncated. New operations see the marker and back
                        // out, so this converges quickly.
                        {
                            let drain_started = Instant::now();
                            let mut drain_warned = false;
                            while dist_tree
                                .in_flight
                                .load(std::sync::atomic::Ordering::SeqCst)
                                > 0
                            {
                                if !drain_warned && drain_started.elapsed() > Duration::from_secs(10)
                                {
                                    warn!(
                                        "Split of {:?} has waited {:?} for {} in-flight \
                                         operation(s) to drain",
                                        dist_tree.id,
                                        drain_started.elapsed(),
                                        dist_tree
                                            .in_flight
                                            .load(std::sync::atomic::Ordering::SeqCst)
                                    );
                                    drain_warned = true;
                                }
                                tokio::task::yield_now().await;
                            }
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
                        // Structural split: re-parent the leaves >= pivot into a
                        // new tree sharing the source's leaf nodes — O(leaves)
                        // instead of copying every key. This also truncates the
                        // source, so no separate retain pass is needed.
                        debug!(
                            "Structurally splitting {:?} at {:?} into {:?}",
                            dist_tree.id, pivot_key, migration_target_id
                        );
                        let Some((moved_tree, moved_len)) = tree.split_off(&pivot_key, &client)
                        else {
                            // Nothing moved (pivot past every key); undo the
                            // marker and try again later.
                            warn!(
                                "Structural split of {:?} moved no keys; clearing marker",
                                dist_tree.id
                            );
                            {
                                let mut dist_prop = dist_tree.prop.write();
                                dist_prop.migration = None;
                            }
                            let _ = tree.mark_migration(&dist_tree.id, None, &client).await;
                            continue;
                        };
                        debug!("Structural split moved {} keys", moved_len);
                        let migration_tree = Arc::new(DistTree::new(
                            migration_target_id,
                            moved_tree,
                            Boundary::new(pivot_key.clone(), source_upper),
                            None,
                            INITIAL_TREE_EPOCH,
                        ));
                        pending_migrations.insert(migration_target_id, migration_tree.clone());
                        // No global write-back drain here: the new tree's leaf
                        // cells are already durable (they are the source's
                        // shared leaves), mark_migration below writes its
                        // metadata cell synchronously, and the O(1) seam-pointer
                        // changes persist asynchronously (recovery reconstructs
                        // forward from the head, tolerating a stale head prev).
                        // Waiting for the whole insert backlog would hold the
                        // source's migration marker for minutes and fail client
                        // readbacks against a "migrating" tree.
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
                            Self::rollback_pending_split(
                                &dist_tree,
                                &migration_tree,
                                migration_target_id,
                                &pending_migrations,
                                &client,
                            )
                            .await;
                            split_backoff_until.insert(
                                dist_tree.id,
                                Instant::now() + Duration::from_millis(SPLIT_RETRY_BACKOFF_MS),
                            );
                            continue;
                        }

                        // Seam barrier: the placement flip below is the split's
                        // commit point. Once it lands, the target may rewrite
                        // or (fence-ordered) delete the formerly shared pages,
                        // so the source's severed chain and the moved head's
                        // cleared prev must be durable FIRST. The drain also
                        // covers the insert backlog, but a split only fires on
                        // an oversized tree during rebalancing, where that
                        // cost is acceptable; a failed barrier rolls the split
                        // back rather than committing over undrained pages.
                        if !storage::wait_until_updated().await {
                            error!(
                                "Seam barrier for split of {:?} NOT established; rolling back",
                                dist_tree.id
                            );
                            Self::rollback_pending_split(
                                &dist_tree,
                                &migration_tree,
                                migration_target_id,
                                &pending_migrations,
                                &client,
                            )
                            .await;
                            split_backoff_until.insert(
                                dist_tree.id,
                                Instant::now() + Duration::from_millis(SPLIT_RETRY_BACKOFF_MS),
                            );
                            continue;
                        }

                        debug!("Calling placement for split to {:?}", migration_target_id);
                        match sm_client.locate_key(&pivot_key).await {
                            Ok(Some((_lower, placement, _upper))) => {
                                debug!(
                                    "Preflighted split for source {:?} at pivot {:?}; current placement tree {:?}, epoch {}",
                                    dist_tree.id, pivot_key, placement.id, placement.epoch
                                );
                            }
                            Ok(None) => {
                                debug!(
                                    "Preflighted split for source {:?} at pivot {:?}; no placement covers the pivot yet",
                                    dist_tree.id, pivot_key
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
                                        &migration_tree,
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
                        // The structural split already truncated the source, so
                        // there is no background retain to run. Update routing
                        // and clear the migration marker in place. No global
                        // drain: the metadata cell is written synchronously
                        // below and the seam-pointer changes persist async, so
                        // the serial balancer is not stalled on the whole
                        // write-back backlog after every migration.
                        {
                            let mut dist_prop = dist_tree.prop.write();
                            dist_prop.boundary.upper = pivot_key.clone();
                            dist_prop.epoch += 1;
                            dist_prop.migration = None;
                        }
                        if let Err(e) = tree.mark_migration(&dist_tree.id, None, &client).await {
                            warn!(
                                "Failed to publish split source tree {:?}: {:?}",
                                dist_tree.id, e
                            );
                        }
                        pending_migrations.remove(&migration_target_id);
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
                    // Count this operation in BEFORE reading the migration
                    // marker. The balancer freezes a tree for a structural
                    // split by setting the marker and then draining
                    // `in_flight` to zero; incrementing first means an
                    // operation either sees the marker and backs out, or is
                    // counted and the split waits for it. Checking-then-
                    // running uncounted let an insert race split_off, landing
                    // entries in leaves the split was truncating (2026-08-30:
                    // ~190 scannable keys lost per bulk import, and one
                    // corrupted-latch hang).
                    struct InFlight<'a>(&'a std::sync::atomic::AtomicUsize);
                    impl Drop for InFlight<'_> {
                        fn drop(&mut self) {
                            self.0.fetch_sub(1, std::sync::atomic::Ordering::SeqCst);
                        }
                    }
                    tree.in_flight
                        .fetch_add(1, std::sync::atomic::Ordering::SeqCst);
                    let _counted = InFlight(&tree.in_flight);
                    let routed = {
                        let tree_prop = tree.prop.read();
                        if epoch < tree_prop.epoch {
                            Err(OpResult::EpochMissMatch(tree_prop.epoch, epoch))
                        } else if !tree_prop.boundary.in_boundary(&entry) {
                            Err(OpResult::OutOfBound)
                        } else if tree_prop.migration.is_some() {
                            // Keep the source tree immutable while the split snapshot is copied and
                            // placement catches up. Clients retry against fresh placement once the
                            // migration window closes.
                            Err(OpResult::Migrating)
                        } else {
                            Ok(tree_prop.clone())
                        }
                    };
                    return match routed {
                        Ok(tree_prop) => func(&entry, &tree.tree, &tree_prop),
                        Err(refusal) => refusal,
                    };
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
        Self {
            id,
            tree,
            prop,
            in_flight: std::sync::atomic::AtomicUsize::new(0),
        }
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
    // Slot-keyed: see `locate_tree_server`. A tree's pages inherit its locality,
    // so its service has to be found the same way or the two part company the
    // first time that slot migrates.
    if let Some(server_id) = conshash.get_server_id_for_slot(id.locality() as u64) {
        DEFAULT_CLIENT_POOL
            .get_by_id(server_id, move |sid| conshash.try_server_name(sid))
            .await
            .map_err(|e| RPCError::IOError(e))
            .map(|c| client_by_rpc_client(&c, group_name, database_name))
    } else {
        Err(RPCError::RequestError(RPCRequestError::Other))
    }
}
