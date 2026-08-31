use super::sm::client::SMClient;
use super::tree::service::*;
use super::{sm::TreePlacement, tree::btree::Ordering, trees::min_entry_key};
use crate::index::EntryKey;
use crate::ram::types::Id;
use bifrost::raft::client::AsRaftPlaneClient;
use bifrost::rpc::RPCError;
use bifrost::{conshash::ConsistentHashing, raft::state_machine::master::ExecError};
use futures::future::BoxFuture;
use futures::prelude::*;
use parking_lot::RwLock;
use std::collections::BTreeMap;
use std::io;
use std::ops::Bound::*;
use std::sync::Arc;
use std::time::Duration;

pub mod cursor;

pub(super) const MAX_RETRY_ATTEMPTS: i32 = 300;
/// Attempts to spend on a NotFound placement *after* refreshing it from the
/// state machine. A refreshed placement that still cannot be found names a tree
/// with no metadata cell; retrying to the full bound only delays the report.
pub(super) const NOT_FOUND_GIVE_UP_ATTEMPTS: i32 = 8;
pub(super) const RETRY_BACKOFF_MS: u64 = 500;
pub(super) const MIGRATION_REFRESH_INTERVAL: i32 = 8;

pub(super) fn too_many_retry_error(last_retry_reason: Option<&str>) -> RPCError {
    let message = match last_retry_reason {
        Some(reason) => format!("Too many retry; last retry reason: {reason}"),
        None => "Too many retry; last retry reason: unknown".to_string(),
    };
    RPCError::IOError(io::Error::new(io::ErrorKind::Other, message))
}

pub(super) fn migration_retry_delay_ms(retried: i32, key: &EntryKey) -> u64 {
    // Use exponential backoff with deterministic jitter so concurrent writers do
    // not wake up in lockstep and stampede the same migrating tree.
    let capped_shift = retried.clamp(0, 4) as u32;
    let base = 50u64
        .saturating_mul(1u64 << capped_shift)
        .min(RETRY_BACKOFF_MS);
    let jitter = u64::from(key.as_slice()[key.len() - 1] % 37);
    (base + jitter).min(RETRY_BACKOFF_MS)
}

pub struct RangedIndexerClient {
    conshash: Arc<ConsistentHashing>,
    sm: Arc<SMClient>,
    group_name: String,
    database_name: String,
    placement: RwLock<BTreeMap<EntryKey, (TreePlacement, EntryKey)>>,
}

/// The three outcomes of asking for the tree after a key. See
/// `RangedIndexerClient::next_tree`.
#[derive(Debug, Clone)]
pub enum NextTree {
    Found(EntryKey, TreePlacement),
    /// The state machine says nothing follows. This, and only this, ends a scan.
    End,
    /// Placement could not be resolved right now -- a split or migration in
    /// flight. Retry; do NOT treat it as the end.
    Unresolved(&'static str),
}

impl RangedIndexerClient {
    pub fn new<C>(conshash: &Arc<ConsistentHashing>, raft_client: &Arc<C>) -> Self
    where
        C: AsRaftPlaneClient + 'static,
    {
        let sm = SMClient::new(crate::index::ranged::sm::DEFAULT_SM_ID, raft_client);
        Self {
            conshash: conshash.clone(),
            sm: Arc::new(sm),
            group_name: String::new(),
            database_name: String::new(),
            placement: RwLock::new(BTreeMap::new()),
        }
    }

    pub fn new_for_database<C>(
        conshash: &Arc<ConsistentHashing>,
        raft_client: &Arc<C>,
        group_name: &str,
        database_name: &str,
    ) -> Self
    where
        C: AsRaftPlaneClient + 'static,
    {
        let sm_id = crate::index::ranged::sm::generate_scoped_sm_id(group_name, database_name);
        let sm = SMClient::new(sm_id, raft_client);
        Self {
            conshash: conshash.clone(),
            sm: Arc::new(sm),
            group_name: group_name.to_string(),
            database_name: database_name.to_string(),
            placement: RwLock::new(BTreeMap::new()),
        }
    }

    pub async fn seek(
        self_ref: &Arc<Self>,
        range: Range,
        buffer_size: u16,
        pattern: Option<Vec<u8>>,
    ) -> Result<Option<cursor::ClientCursor>, RPCError> {
        let trace_seek = log::log_enabled!(log::Level::Debug);
        let mut range = range;
        loop {
            let key = range.key().clone();
            let block = self_ref
                .run_on_destinated_tree(
                    &key,
                    |_key, client, tree_id, epoch| {
                        let pattern = pattern.clone();
                        let range = range.clone();
                        async move {
                            client
                                .seek(tree_id, range, &pattern, buffer_size, epoch)
                                .await
                        }
                        .boxed()
                    },
                    |action_res, _tree_client, _lower, _upper| {
                        async move { Ok(action_res) }.boxed()
                    },
                )
                .await?;

            if trace_seek {
                debug!(
                    "MIGRATION_SEEK_BLOCK request_key={:?} ordering={:?} first={:?} last={:?} len={} next={:?} empty={}",
                    range.key().id(),
                    range.ordering,
                    block.buffer.first().copied(),
                    block.buffer.last().copied(),
                    block.buffer.len(),
                    block.next.as_ref().map(|k| k.id()),
                    block.buffer.is_empty()
                );
            }

            if block.buffer.is_empty() {
                if let Some(next_key) = block.next.clone() {
                    let should_follow = match range.ordering {
                        Ordering::Forward => next_key > *range.key(),
                        Ordering::Backward => next_key < *range.key(),
                    };
                    if should_follow {
                        if trace_seek {
                            debug!(
                                "MIGRATION_SEEK_FOLLOW_EMPTY request_key={:?} next_key={:?} ordering={:?}",
                                range.key().id(),
                                next_key.id(),
                                range.ordering
                            );
                        }
                        range = range.move_to(next_key);
                        continue;
                    }
                }
                return Ok(None);
            }

            return Ok(Some(
                cursor::ClientCursor::new(
                    block,
                    range,
                    self_ref.clone(),
                    buffer_size,
                    pattern.clone(),
                )
                .await?,
            ));
        }
    }

    pub async fn delete(&self, key: &EntryKey) -> Result<bool, RPCError> {
        self.run_on_destinated_tree(
            key,
            |key, client, tree_id, epoch| {
                async move { client.delete(tree_id, key.clone(), epoch).await }.boxed()
            },
            |action_res, _, _, _| future::ready(Ok(action_res)).boxed(),
        )
        .await
    }

    pub async fn insert(&self, key: &EntryKey) -> Result<bool, RPCError> {
        self.run_on_destinated_tree(
            key,
            |key, client, tree_id, epoch| {
                async move { client.insert(tree_id, key.clone(), epoch).await }.boxed()
            },
            |action_res, _, _, _| future::ready(Ok(action_res)).boxed(),
        )
        .await
    }

    /// Exact, read-only presence of one key. Used by the index scrub to
    /// tell a hole in the index from a cell that was never indexable.
    pub async fn contains(&self, key: &EntryKey) -> Result<bool, RPCError> {
        self.run_on_destinated_tree(
            key,
            |key, client, tree_id, epoch| {
                async move { client.contains(tree_id, key.clone(), epoch).await }.boxed()
            },
            |action_res, _, _, _| future::ready(Ok(action_res)).boxed(),
        )
        .await
    }

    /// Statistics for every tree in the index, walked from the state machine's
    /// placements. The client's own placement cache only knows the trees it
    /// has touched, so a cache walk reported one tree for an index that had
    /// split into several.
    pub async fn tree_stats(&self) -> Result<Vec<TreeStat>, RPCError> {
        let exec_err = |e: ExecError| {
            RPCError::IOError(io::Error::new(
                io::ErrorKind::Other,
                format!("Cannot walk tree placements: {:?}", e),
            ))
        };
        let mut res = vec![];
        let Some((mut lower, mut placement, _)) = self
            .refresh_key_mapping(&min_entry_key())
            .await
            .map_err(exec_err)?
        else {
            return Ok(res);
        };
        loop {
            let tree_client = locate_tree_server_from_conshash(
                &placement.id,
                &self.conshash,
                &self.group_name,
                &self.database_name,
            )
            .await?;
            match tree_client.stat(placement.id).await? {
                OpResult::Successful(stat_res) => {
                    res.push(stat_res);
                }
                _ => unreachable!(),
            }
            match self.next_tree(&lower, Ordering::Forward).await.map_err(exec_err)? {
                NextTree::Found(next_lower, next_placement) => {
                    lower = next_lower;
                    placement = next_placement;
                }
                NextTree::End | NextTree::Unresolved(_) => break,
            }
        }
        Ok(res)
    }

    #[inline(always)]
    async fn run_on_destinated_tree<'a, AR, PR, A, P>(
        &'a self,
        key: &EntryKey,
        action: A,
        proc: P,
    ) -> Result<PR, RPCError>
    where
        A: Fn(
            EntryKey,
            Arc<AsyncServiceClient>,
            Id,
            u64,
        ) -> BoxFuture<'a, Result<OpResult<AR>, RPCError>>,
        P: Fn(
            Option<AR>,
            Arc<AsyncServiceClient>,
            EntryKey,
            EntryKey,
        ) -> BoxFuture<'a, Result<Option<PR>, RPCError>>,
    {
        let mut ensure_updated = false;
        let mut retried: i32 = 0;
        let mut last_retry_reason: Option<String> = None;
        let trace_seek = log::log_enabled!(log::Level::Debug);
        loop {
            if retried >= MAX_RETRY_ATTEMPTS {
                // Retry attempts all failed
                warn!(
                    "Ranged client exhausted retries for key {:?} after {} attempts; last reason: {}",
                    key,
                    retried,
                    last_retry_reason.as_deref().unwrap_or("unknown")
                );
                return Err(too_many_retry_error(last_retry_reason.as_deref()));
            }
            let Some((placement, tree_client, lower, upper)) =
                self.locate_key_server(&key, ensure_updated).await?
            else {
                // No tree covers the key yet -- the placement map has not
                // been populated on this member. Retry like any other
                // transient routing state; the loop's own bound turns a
                // permanent absence into an error instead of a hang.
                retried += 1;
                last_retry_reason = Some("no ranged placement covers the key yet".to_string());
                ensure_updated = true;
                tokio::time::sleep(std::time::Duration::from_millis(migration_retry_delay_ms(
                    retried, &key,
                )))
                .await;
                continue;
            };
            if trace_seek {
                debug!(
                    "MIGRATION_SEEK_ROUTE key={:?} retry={} ensure_updated={} tree_id={:?} epoch={} lower={:?} upper={:?}",
                    key.id(),
                    retried,
                    ensure_updated,
                    placement.id,
                    placement.epoch,
                    lower.id(),
                    upper.id()
                );
            }
            match action(
                key.clone(),
                tree_client.clone(),
                placement.id,
                placement.epoch,
            )
            .await?
            {
                OpResult::Successful(res) => {
                    if let Some(proc_res) = proc(Some(res), tree_client, lower, upper).await? {
                        return Ok(proc_res);
                    }
                    last_retry_reason =
                        Some("successful tree operation requested another retry".to_string());
                    debug!(
                        "Ranged client retry {} for key {:?}: {}",
                        retried + 1,
                        key,
                        last_retry_reason.as_deref().unwrap_or("unknown")
                    );
                }
                OpResult::Migrating => {
                    if trace_seek {
                        debug!(
                            "MIGRATION_SEEK_RETRY key={:?} tree_id={:?} epoch={} reason=migrating retry={}",
                            key.id(),
                            placement.id,
                            placement.epoch,
                            retried + 1
                        );
                    }
                    last_retry_reason = Some("tree is migrating".to_string());
                    // Keep retrying, but periodically force a fresh placement lookup so
                    // clients converge on the new tree promptly once the split commits.
                    ensure_updated = (retried + 1) % MIGRATION_REFRESH_INTERVAL == 0;
                    debug!(
                        "Ranged client retry {} for key {:?}: {}",
                        retried + 1,
                        key,
                        last_retry_reason.as_deref().unwrap_or("unknown")
                    );
                    tokio::time::sleep(Duration::from_millis(migration_retry_delay_ms(
                        retried, key,
                    )))
                    .await;
                }
                OpResult::OutOfBound => {
                    if trace_seek {
                        debug!(
                            "MIGRATION_SEEK_RETRY key={:?} tree_id={:?} epoch={} reason=out_of_bound retry={}",
                            key.id(),
                            placement.id,
                            placement.epoch,
                            retried + 1
                        );
                    }
                    last_retry_reason = Some("tree placement was out of bound".to_string());
                    debug!(
                        "Ranged client retry {} for key {:?}: {}",
                        retried + 1,
                        key,
                        last_retry_reason.as_deref().unwrap_or("unknown")
                    );
                    ensure_updated = true;
                }
                OpResult::NotFound => {
                    if trace_seek {
                        debug!(
                            "MIGRATION_SEEK_RETRY key={:?} tree_id={:?} epoch={} reason=not_found retry={}",
                            key.id(),
                            placement.id,
                            placement.epoch,
                            retried + 1
                        );
                    }
                    last_retry_reason = Some(format!(
                        "tree placement was not found (tree {:?})",
                        placement.id
                    ));
                    debug!(
                        "Ranged client retry {} for key {:?}: {}",
                        retried + 1,
                        key,
                        last_retry_reason.as_deref().unwrap_or("unknown")
                    );
                    // A placement we just refreshed from the state machine that
                    // still reports NotFound is dangling, not racing: the tree
                    // it names has no metadata cell to load. Spinning 300 times
                    // turns that into an opaque timeout far from the cause, so
                    // stop early and say what is wrong.
                    if ensure_updated && retried >= NOT_FOUND_GIVE_UP_ATTEMPTS {
                        warn!(
                            "Ranged client giving up on key {:?}: placement names tree {:?} that \
                             no server can load; the placement map has outlived its trees",
                            key, placement.id
                        );
                        return Err(too_many_retry_error(last_retry_reason.as_deref()));
                    }
                    ensure_updated = true;
                }
                OpResult::EpochMissMatch(expect, actual) => {
                    if trace_seek {
                        debug!(
                            "MIGRATION_SEEK_RETRY key={:?} tree_id={:?} epoch={} reason=epoch_mismatch expected={} actual={} retry={}",
                            key.id(),
                            placement.id,
                            placement.epoch,
                            expect,
                            actual,
                            retried + 1
                        );
                    }
                    debug!("Epoch mismatch, expect {}, actual {}", expect, actual);
                    last_retry_reason = Some(format!(
                        "tree epoch mismatch (expected {expect}, actual {actual})"
                    ));
                    debug!(
                        "Ranged client retry {} for key {:?}: {}",
                        retried + 1,
                        key,
                        last_retry_reason.as_deref().unwrap_or("unknown")
                    );
                    ensure_updated = true;
                }
            }
            retried += 1;
        }
    }

    async fn locate_key_server(
        &self,
        key: &EntryKey,
        ensure_updated: bool,
    ) -> Result<Option<(TreePlacement, Arc<AsyncServiceClient>, EntryKey, EntryKey)>, RPCError>
    {
        let mut tree_prop = None;
        if !ensure_updated {
            if let Some((lower, (placement, upper))) =
                self.placement.read().range(..key.clone()).last()
            {
                if key >= lower && key < upper {
                    tree_prop = Some((lower.clone(), placement.clone(), upper.clone()));
                }
            }
        }
        if tree_prop.is_none() {
            // A placement lookup that fails outright is an error; one that
            // simply has no tree yet is not. This used to `expect`, so a
            // member querying before it applied genesis took the process
            // down with it.
            tree_prop = self.refresh_key_mapping(key).await.map_err(|e| {
                RPCError::IOError(io::Error::new(
                    io::ErrorKind::Other,
                    format!("Cannot locate key {:?}: {:?}", key, e),
                ))
            })?;
        }
        let Some((lower, tree_placement, upper)) = tree_prop else {
            return Ok(None);
        };
        let tree_client = locate_tree_server_from_conshash(
            &tree_placement.id,
            &self.conshash,
            &self.group_name,
            &self.database_name,
        )
        .await?;
        Ok(Some((tree_placement, tree_client, lower, upper)))
    }

    /// `Ok(None)` when the placement map has no tree covering the key yet.
    /// That is a routine startup state, not an error: callers retry.
    async fn refresh_key_mapping(
        &self,
        key: &EntryKey,
    ) -> Result<Option<(EntryKey, TreePlacement, EntryKey)>, ExecError> {
        let located = match self.sm.locate_key(key).await {
            Ok(located) => located,
            Err(ExecError::SmNotFound(sm_id)) => {
                // The placement state machine is not registered on the
                // serving member yet -- registration races the first
                // queries at startup, and bifrost buffers commands for
                // exactly this window. The key is not unlocatable, the
                // member is not ready: same answer as "no tree yet", and
                // the caller's bounded retry loop.
                warn!(
                    "Placement SM {} not yet registered while locating {:?}; \
                     treating as no placement yet",
                    sm_id, key
                );
                return Ok(None);
            }
            Err(e) => return Err(e),
        };
        let Some((lower, placement, upper)) = located else {
            return Ok(None);
        };
        debug_assert!(
            key >= &lower && key < &upper,
            "Key {:?}, lower {:?}, upper {:?}",
            key,
            lower,
            upper
        );
        self.placement
            .write()
            .insert(lower.clone(), (placement.clone(), upper.clone()));
        return Ok(Some((lower, placement, upper)));
    }

    /// What `next_tree` found.
    ///
    /// `End` and `Unresolved` used to be the same answer -- both `None` -- and
    /// that is what silently truncated scans. A cursor that cannot resolve
    /// placement mid-migration would clear itself and return success, so a
    /// scan crossing a boundary while a split committed returned FEWER ROWS
    /// WITH NO ERROR. Only the state machine saying "nothing follows" ends a
    /// scan; everything else is a retry.
    pub async fn next_tree(
        &self,
        origin_key: &EntryKey,
        ordering: Ordering,
    ) -> Result<NextTree, ExecError> {
        // Next tree for cursor
        // This function must be able to detect tree changes and ensure consistency
        let (origin_lower, origin_upper) = {
            let placement = self.placement.read();
            if let Some((lower, (_placement, upper))) =
                placement.range(..=origin_key.clone()).last()
            {
                if origin_key >= lower && origin_key < upper {
                    (lower.clone(), upper.clone())
                } else {
                    drop(placement);
                    let Some((lower, _placement, upper)) =
                        self.refresh_key_mapping(origin_key).await?
                    else {
                        return Ok(NextTree::Unresolved("no tree covers the current key"));
                    };
                    (lower, upper)
                }
            } else {
                drop(placement);
                let Some((lower, _placement, upper)) = self.refresh_key_mapping(origin_key).await?
                else {
                    return Ok(NextTree::Unresolved("no tree covers the current key"));
                };
                (lower, upper)
            }
        };

        {
            let placement = self.placement.read();
            let (_origin_place, origin_upper) = match placement.get(&origin_lower) {
                Some(t) => t,
                None => {
                    warn!(
                        "Cannot find next tree placement for {:?}. Mapping {:?}",
                        origin_lower, &*placement
                    );
                    return Ok(NextTree::Unresolved(
                        "placement entry vanished between two reads",
                    ));
                }
            };
            let cached_next = match ordering {
                Ordering::Forward => placement.range((Excluded(&origin_lower), Unbounded)).next(),
                Ordering::Backward => placement.range((Unbounded, Excluded(&origin_lower))).last(),
            };
            // Check cache consistency against origin
            if let Some((cached_lower, (cached_placement, cached_upper))) = cached_next {
                let matched_with_origin = match ordering {
                    Ordering::Forward => cached_lower == origin_upper,
                    Ordering::Backward => cached_upper == &origin_lower,
                };
                if matched_with_origin {
                    return Ok(NextTree::Found(
                        cached_lower.clone(),
                        cached_placement.clone(),
                    ));
                } else {
                    debug!(
                        "Cached tree does not match. Ordering {:?}, origin lower {:?}, origin upper {:?}, cached lower {:?}, cached upper {:?}",
                        ordering, origin_lower, origin_upper, cached_lower, cached_upper
                    )
                }
            } else {
                debug!(
                    "Next tree does not have cache, origin {:?}, ordering {:?}",
                    origin_lower, ordering
                );
            }
        }
        // The state machine is the authority. Only ITS "nothing follows" ends
        // a scan.
        Ok(match self.sm.next_tree(&origin_lower, &ordering).await? {
            Some(next) => {
                self.placement
                    .write()
                    .insert(next.lower.clone(), (next.placement.clone(), next.upper));
                NextTree::Found(next.lower, next.placement)
            }
            None => NextTree::End,
        })
    }
}
