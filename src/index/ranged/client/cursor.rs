use super::super::lsm::btree::Ordering;
use super::super::lsm::service::AsyncServiceClient;
use super::super::lsm::service::{EntryKeyBlock, ServCursor};
use crate::index::ranged::client::RangedQueryClient;
use crate::index::EntryKey;
use std::mem;
use std::sync::Arc;
use bifrost::rpc::RPCError;

pub struct ClientCursor<'a> {
    entry: Option<EntryKey>,
    entry_block: Option<EntryKeyBlock>,
    query_client: &'a RangedQueryClient,
    tree_client: Arc<AsyncServiceClient>,
    remote_cursor: ServCursor,
    ordering: Ordering,
    tree_boundary: EntryKey,
    pos: usize,
}

impl<'a> ClientCursor<'a> {
    pub fn new(
        remote: ServCursor,
        init_entry: EntryKey,
        ordering: Ordering,
        tree_boundary: EntryKey,
        tree_client: Arc<AsyncServiceClient>,
        query_client: &'a RangedQueryClient,
    ) -> Self {
        Self {
            entry: Some(init_entry),
            remote_cursor: remote,
            tree_client,
            query_client,
            entry_block: None,
            tree_boundary,
            ordering,
            pos: 0,
        }
    }

    pub async fn next(&'a mut self) -> Result<Option<EntryKey>, RPCError> {
        loop {
            let res;
            if self.entry.is_some() && self.entry_block.is_none() {
                res = mem::replace(&mut self.entry, None);
                self.entry_block =
                    Some(Self::refresh_block(&self.tree_client, self.remote_cursor).await?);
            } else if let &mut Some(ref mut entries) = &mut self.entry_block {
                let min_entry: EntryKey = Default::default();
                if entries[0] <= min_entry {
                    // have empty block will try to reload the cursor from the client for
                    // next key may been placed on another 
                    let replacement = self
                        .query_client
                        .seek(self.tree_boundary.clone(), self.ordering)
                        .await?;
                    if let Some(new_cursor) = replacement
                    {
                        *self = new_cursor;
                        continue;
                    } else {
                        return Ok(None);
                    }
                }
                let old_key = mem::replace(&mut entries[self.pos], smallvec![1]);
                debug_assert!(old_key > min_entry);
                res = Some(old_key);
                self.pos += 1;
                // Check if pos is in range and have value. If not, get next block.
                if self.pos >= entries.len() || entries[self.pos] <= min_entry {
                    let new_block =
                        Self::refresh_block(&self.tree_client, self.remote_cursor).await?;
                    *entries = new_block;
                    self.entry = res.clone();
                    self.pos = 0;
                }
            } else {
                unimplemented!();
            }
            return Ok(res);
        }
    }

    pub fn current(&self) -> Option<&EntryKey> {
        if self.entry.is_some() {
            self.entry.as_ref()
        } else {
            let min_entry: EntryKey = Default::default();
            let block = self.entry_block.as_ref().unwrap();
            if block[self.pos] == min_entry {
                return None;
            } else {
                return Some(&block[self.pos]);
            }
        }
    }

    async fn refresh_block(
        tree_client: &Arc<AsyncServiceClient>,
        remote_cursor: ServCursor,
    ) -> Result<EntryKeyBlock, RPCError> {
        Ok(
            tree_client
            .cursor_next(remote_cursor)
            .await?
            .unwrap()
        )
    }
}

impl <'a> Drop for ClientCursor<'a> {
    fn drop(&mut self) {
        let tree_client = self.tree_client.clone();
        let remote_cursor = self.remote_cursor;
        tokio::spawn( async move {
            tree_client.dispose_cursor(remote_cursor).await
        });
    }
}
