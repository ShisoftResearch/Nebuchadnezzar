use super::super::tree::service::*;
use crate::index::ranged::trees::Ordering;
use crate::index::ranged::{
    client::{
        migration_retry_delay_ms, too_many_retry_error, RangedIndexerClient, MAX_RETRY_ATTEMPTS,
        MIGRATION_REFRESH_INTERVAL,
    },
    trees::{max_entry_key, min_entry_key},
};
use crate::index::EntryKey;
use crate::ram::types::Id;
use bifrost::rpc::RPCError;
use std::collections::HashSet;
use std::env;
use std::sync::Arc;

fn cursor_trace_schema_from_key(key: &EntryKey) -> u32 {
    let mut schema = [0u8; 4];
    schema.copy_from_slice(&key.as_slice()[..4]);
    u32::from_be_bytes(schema)
}

fn should_trace_cursor(key: &EntryKey, pattern: Option<&[u8]>) -> bool {
    let Ok(value) = env::var("NEB_RANGE_TRACE_SCHEMA") else {
        return false;
    };
    if value == "*" {
        return true;
    }
    let Ok(schema_id) = value.parse::<u32>() else {
        return false;
    };
    if cursor_trace_schema_from_key(key) != schema_id {
        return false;
    }
    match pattern {
        Some(p) => p.len() == 16,
        None => true,
    }
}

fn cursor_trace_gap(ids: &[Id]) -> Option<(Id, Id)> {
    ids.windows(2)
        .find(|pair| pair[1].bits() != pair[0].bits() + 1)
        .map(|pair| (pair[0], pair[1]))
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

pub struct ClientCursor {
    pub ids: Vec<Id>,
    next: Option<EntryKey>,
    last_key: Option<EntryKey>,
    query_client: Arc<RangedIndexerClient>,
    pub pos: usize,
    buffer_size: u16,
    pattern: Option<Vec<u8>>,
    range: Range,
    seen_ids: HashSet<Id>,
}

impl ClientCursor {
    pub async fn new(
        block: ServBlock,
        range: Range,
        query_client: Arc<RangedIndexerClient>,
        buffer_size: u16,
        pattern: Option<Vec<u8>>,
    ) -> Result<Self, RPCError> {
        trace!(
            "Client cursor created with buffer next {:?}, tree key {:?}, block keys {:?}",
            block.next,
            range.key(),
            block.buffer
        );
        let trace_cursor = should_trace_cursor(range.key(), pattern.as_deref());
        if trace_cursor {
            let first = block.buffer.first().copied();
            let last = block.buffer.last().copied();
            let gap = cursor_trace_gap(&block.buffer);
            if gap.is_some() || block.buffer.is_empty() {
                debug!(
                    "RANGE_CURSOR_NEW schema={} first={:?} last={:?} len={} next={:?} gap={:?} key={:?}",
                    cursor_trace_schema_from_key(range.key()),
                    first,
                    last,
                    block.buffer.len(),
                    block.next.as_ref().map(|k| k.id()),
                    gap,
                    range.key().id()
                );
            }
        }
        let next = block.next;
        let last_key = block.last_key;
        let mut seen_ids: HashSet<Id> = HashSet::new();
        let ids: Vec<Id> = block
            .buffer
            .into_iter()
            .filter(|id| seen_ids.insert(*id))
            .collect();
        if trace_cursor {
            let gap = cursor_trace_gap(&ids);
            if gap.is_some() {
                debug!(
                    "RANGE_CURSOR_DEDUP schema={} len={} gap={:?} key={:?}",
                    cursor_trace_schema_from_key(range.key()),
                    ids.len(),
                    gap,
                    range.key().id()
                );
            }
        }
        Ok(Self {
            ids,
            query_client,
            next,
            last_key,
            buffer_size,
            pos: 0,
            pattern,
            range,
            seen_ids,
        })
    }

    pub async fn next(&mut self) -> Result<Option<Id>, RPCError> {
        let mut res;
        if self.pos < self.ids.len() {
            res = Some(self.ids[self.pos]);
            self.pos += 1;
            if self.pos < self.ids.len() {
                return Ok(res);
            }
        }
        res = if self.pos == 0 {
            None
        } else {
            self.ids.get(self.pos - 1).cloned()
        };
        let next_key = if let Some(key) = &self.next {
            // Have next, use it
            key.clone()
        } else if let Some(key) = self
            .last_key
            .as_ref()
            .and_then(|key| bump_entry_key(key, self.range.ordering))
        {
            key
        } else {
            // Key does not in the tree, and next key is unknown.
            // Should refill by next tree and return the previous key
            self.ids.clear();
            self.pos = 0;
            return Ok(res);
        };
        trace!(
            "Buffer all used, refilling using key {:?}, current id {:?}, next id {:?}",
            next_key,
            res,
            next_key.id()
        );
        if log::log_enabled!(log::Level::Debug) {
            debug!(
                "MIGRATION_BLOCK_STEP current_return={:?} resume_key={:?} range_key={:?} buffer_len={} seen_ids={} last_buffer_id={:?}",
                res,
                next_key.id(),
                self.range.key().id(),
                self.ids.len(),
                self.seen_ids.len(),
                self.ids.last().copied()
            );
        }
        let next_cursor = RangedIndexerClient::seek(
            &self.query_client,
            self.range.clone().move_to(next_key.clone()),
            self.buffer_size,
            self.pattern.clone(),
        )
        .await?;
        if let Some(mut cursor) = next_cursor {
            if log::log_enabled!(log::Level::Debug) {
                debug!(
                    "MIGRATION_BLOCK_RESULT resume_key={:?} first={:?} last={:?} len={} next={:?} gap={:?}",
                    next_key.id(),
                    cursor.ids.first().copied(),
                    cursor.ids.last().copied(),
                    cursor.ids.len(),
                    cursor.next.as_ref().map(|k| k.id()),
                    cursor_trace_gap(&cursor.ids)
                );
            }
            let mut seen = std::mem::take(&mut self.seen_ids);
            cursor.ids.retain(|id| seen.insert(*id));
            cursor.seen_ids = seen;
            *self = cursor;
        } else {
            self.refill_by_next_tree().await?;
        }
        return Ok(res);
    }

    pub async fn next_block(&mut self) -> Result<bool, RPCError> {
        self.pos = self.ids.len();
        self.next().await?;
        Ok(!self.ids.is_empty())
    }

    pub fn current(&self) -> Option<&Id> {
        match self.ids.get(self.pos) {
            Some(id) => Some(id),
            _ => None,
        }
    }

    pub fn current_block(&self) -> &Vec<Id> {
        &self.ids
    }

    async fn refill_by_next_tree(&mut self) -> Result<(), RPCError> {
        let mut retried: i32 = 0;
        let mut last_retry_reason: Option<String> = None;
        loop {
            if retried >= MAX_RETRY_ATTEMPTS {
                warn!(
                    "Ranged cursor exhausted retries for key {:?} after {} attempts; last reason: {}",
                    self.range.key(),
                    retried,
                    last_retry_reason.as_deref().unwrap_or("unknown")
                );
                return Err(too_many_retry_error(last_retry_reason.as_deref()));
            }
            let current_key = self.range.key();
            let trace_refill = log::log_enabled!(log::Level::Debug);
            debug!(
                "Refill by next tree, key {:?}, ordering {:?}",
                self.range.key(),
                self.range.ordering
            );
            if let Some((tree_key, tree)) = self
                .query_client
                .next_tree(current_key, self.range.ordering)
                .await
                .unwrap()
            {
                debug!(
                    "Next tree for {:?} returns {:?}, lower key {:?}, ordering {:?}",
                    current_key, tree, tree_key, self.range.ordering
                );
                if trace_refill {
                    debug!(
                        "MIGRATION_REFILL_SELECT current_key={:?} ordering={:?} next_tree_lower={:?} next_tree_id={:?} next_tree_epoch={} buffer_pos={} seen_ids={} last_id={:?}",
                        current_key.id(),
                        self.range.ordering,
                        tree_key.id(),
                        tree.id,
                        tree.epoch,
                        self.pos,
                        self.seen_ids.len(),
                        self.ids.last().copied()
                    );
                }
                let tree_client = locate_tree_server_from_conshash(
                    &tree.id,
                    &self.query_client.conshash,
                    &self.query_client.group_name,
                    &self.query_client.database_name,
                )
                .await?;
                let range = Range {
                    start: RangeTerm::Inclusive(min_entry_key()),
                    end: RangeTerm::Inclusive(max_entry_key()),
                    ordering: self.range.ordering,
                };
                let seek_res = tree_client
                    .seek(tree.id, range, &self.pattern, self.buffer_size, tree.epoch)
                    .await?;
                match seek_res {
                    OpResult::Successful(block) => {
                        if trace_refill {
                            debug!(
                                "MIGRATION_REFILL_BLOCK current_key={:?} next_tree_lower={:?} next_tree_id={:?} next_tree_epoch={} first={:?} last={:?} len={} next={:?} gap={:?}",
                                current_key.id(),
                                tree_key.id(),
                                tree.id,
                                tree.epoch,
                                block.buffer.first().copied(),
                                block.buffer.last().copied(),
                                block.buffer.len(),
                                block.next.as_ref().map(|k| k.id()),
                                cursor_trace_gap(&block.buffer)
                            );
                        }
                        let trace_cursor =
                            should_trace_cursor(current_key, self.pattern.as_deref());
                        if trace_cursor {
                            let first = block.buffer.first().copied();
                            let last = block.buffer.last().copied();
                            let gap = cursor_trace_gap(&block.buffer);
                            if gap.is_some() || block.buffer.is_empty() {
                                debug!(
                                    "RANGE_CURSOR_REFILL schema={} tree={:?} tree_key={:?} first={:?} last={:?} len={} next={:?} gap={:?} current_key={:?}",
                                    cursor_trace_schema_from_key(current_key),
                                    tree.id,
                                    tree_key.id(),
                                    first,
                                    last,
                                    block.buffer.len(),
                                    block.next.as_ref().map(|k| k.id()),
                                    gap,
                                    current_key.id()
                                );
                            }
                        }
                        if block.buffer.is_empty() {
                            // Clear, this will ensure the cursor returns 0
                            debug!("Tree refill seek returns empty block");
                            self.ids.clear();
                        } else {
                            debug!(
                                "Tree refill seek returns block sized {}",
                                block.buffer.len()
                            );
                            let mut cursor = Self::new(
                                block,
                                self.range.clone().move_to(tree_key),
                                self.query_client.clone(),
                                self.buffer_size,
                                self.pattern.clone(),
                            )
                            .await?;
                            let mut seen = std::mem::take(&mut self.seen_ids);
                            cursor.ids.retain(|id| seen.insert(*id));
                            cursor.seen_ids = seen;
                            *self = cursor;
                        }
                        return Ok(());
                    }
                    OpResult::Migrating => {
                        last_retry_reason =
                            Some("tree is migrating during cursor refill".to_string());
                        if trace_refill {
                            debug!(
                                "MIGRATION_REFILL_RETRY current_key={:?} next_tree_lower={:?} next_tree_id={:?} next_tree_epoch={} reason=migrating retry={}",
                                current_key.id(),
                                tree_key.id(),
                                tree.id,
                                tree.epoch,
                                retried + 1
                            );
                        }
                        self.query_client.placement.write().remove(&tree_key);
                        if (retried + 1) % MIGRATION_REFRESH_INTERVAL == 0 {
                            let _ = self.query_client.refresh_key_mapping(current_key).await;
                        }
                        debug!(
                            "Ranged cursor retry {} for key {:?}: {}",
                            retried + 1,
                            current_key,
                            last_retry_reason.as_deref().unwrap_or("unknown")
                        );
                        tokio::time::sleep(std::time::Duration::from_millis(
                            migration_retry_delay_ms(retried, current_key),
                        ))
                        .await;
                    }
                    OpResult::OutOfBound | OpResult::NotFound => unreachable!(),
                    OpResult::EpochMissMatch(expect, actual) => {
                        last_retry_reason = Some(format!(
                            "tree epoch mismatch during cursor refill (expected {expect}, actual {actual})"
                        ));
                        if trace_refill {
                            debug!(
                                "MIGRATION_REFILL_RETRY current_key={:?} next_tree_lower={:?} next_tree_id={:?} next_tree_epoch={} reason=epoch_mismatch expected={} actual={} retry={}",
                                current_key.id(),
                                tree_key.id(),
                                tree.id,
                                tree.epoch,
                                expect,
                                actual,
                                retried + 1
                            );
                        }
                        self.query_client.placement.write().remove(&tree_key);
                        let _ = self.query_client.refresh_key_mapping(current_key).await;
                        debug!(
                            "Ranged cursor retry {} for key {:?}: {}",
                            retried + 1,
                            current_key,
                            last_retry_reason.as_deref().unwrap_or("unknown")
                        );
                        tokio::time::sleep(std::time::Duration::from_millis(
                            migration_retry_delay_ms(retried, current_key),
                        ))
                        .await;
                    }
                }
            } else {
                if trace_refill {
                    debug!(
                        "MIGRATION_REFILL_END current_key={:?} ordering={:?} reason=no_next_tree",
                        current_key.id(),
                        self.range.ordering
                    );
                }
                debug!(
                    "Next tree for {:?} does not return anything. ordering {:?}",
                    current_key, self.range.ordering
                );
                // Clear the cursor
                self.ids.clear();
                self.pos = 0;
                return Ok(());
            }
            retried += 1;
        }
    }
}
