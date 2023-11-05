use super::super::lsm::btree::Ordering;
use super::super::lsm::service::*;
use crate::index::ranged::{
    client::RangedQueryClient,
    trees::{max_entry_key, min_entry_key},
};
use crate::index::EntryKey;
use crate::ram::types::Id;
use bifrost::rpc::RPCError;
use std::sync::Arc;
use std::time::Duration;

pub struct ClientCursor {
    pub ids: Vec<Id>,
    next: Option<EntryKey>,
    query_client: Arc<RangedQueryClient>,
    pub pos: usize,
    buffer_size: u16,
    pattern: Option<Vec<u8>>,
    range: Range,
}

impl ClientCursor {
    pub async fn new(
        block: ServBlock,
        range: Range,
        query_client: Arc<RangedQueryClient>,
        buffer_size: u16,
        pattern: Option<Vec<u8>>,
    ) -> Result<Self, RPCError> {
        trace!(
            "Client cursor created with buffer next {:?}, tree key {:?}, block keys {:?}",
            block.next,
            range.key(),
            block.buffer
        );
        let next = block.next;
        let ids = block.buffer;
        Ok(Self {
            ids,
            query_client,
            next,
            buffer_size,
            pos: 0,
            pattern,
            range,
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
            key
        } else {
            // Key does not in the tree, and next key is unknown.
            // Should refill by next tree and return the previous key
            self.refill_by_next_tree().await?;
            return Ok(res);
        };
        trace!(
            "Buffer all used, refilling using key {:?}, current id {:?}, next id {:?}",
            next_key,
            res,
            next_key.id()
        );
        let next_cursor = RangedQueryClient::seek(
            &self.query_client,
            self.range.clone().move_to(next_key.clone()),
            self.buffer_size,
            self.pattern.clone(),
        )
        .await?;
        if let Some(cursor) = next_cursor {
            *self = cursor;
        } else {
            self.ids = vec![];
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
        loop {
            let current_key = self.range.key();
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
                let tree_client =
                    locate_tree_server_from_conshash(&tree.id, &self.query_client.conshash).await?;
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
                        if block.buffer.is_empty() {
                            // Clear, this will ensure the cursor returns 0
                            debug!("Tree refill seek returns empty block");
                            self.ids.clear();
                        } else {
                            debug!(
                                "Tree refill seek returns block sized {}",
                                block.buffer.len()
                            );
                            *self = Self::new(
                                block,
                                self.range.clone().move_to(tree_key),
                                self.query_client.clone(),
                                self.buffer_size,
                                self.pattern.clone(),
                            )
                            .await?;
                        }
                        return Ok(());
                    }
                    OpResult::Migrating => {
                        tokio::time::sleep(Duration::from_millis(500)).await;
                    }
                    OpResult::OutOfBound | OpResult::NotFound => unreachable!(),
                    OpResult::EpochMissMatch(expect, actual) => {
                        debug!(
                            "Epoch mismatch on refill, expected {}, actual {}",
                            expect, actual
                        );
                    }
                }
            } else {
                debug!(
                    "Next tree for {:?} does not return anything. ordering {:?}",
                    current_key, self.range.ordering
                );
                // Clear the cursor
                self.ids.clear();
                self.pos = 0;
                return Ok(());
            }
        }
    }
}
