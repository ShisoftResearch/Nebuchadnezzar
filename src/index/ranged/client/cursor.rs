use super::super::lsm::btree::Ordering;
use super::super::lsm::service::*;
use crate::index::ranged::client::RangedQueryClient;
use crate::index::EntryKey;
use crate::ram::cell::Cell;
use crate::ram::cell::ReadError;
use crate::ram::types::Id;
use bifrost::rpc::RPCError;
use std::mem;
use std::sync::Arc;

type CellBlock = Vec<Option<IndexedCell>>;
pub type IndexedCell = (Id, Result<Cell, ReadError>);

pub struct ClientCursor {
    cell_block: CellBlock,
    next: Option<EntryKey>,
    query_client: Arc<RangedQueryClient>,
    ordering: Ordering,
    tree_boundary: EntryKey,
    pos: usize,
    buffer_size: u16
}

impl ClientCursor {
    pub async fn new(
        ordering: Ordering,
        block: ServBlock,
        tree_boundary: EntryKey,
        query_client: Arc<RangedQueryClient>,
        buffer_size: u16
    ) -> Result<Self, RPCError> {
        debug!("Client cursor created with buffer next {:?}, bound {:?}", block.next, tree_boundary);
        let next = block.next;
        let ids = block.buffer.clone();
        let cell_block = query_client.neb_client
            .read_all_cells(block.buffer)
            .await?
            .into_iter()
            .zip(ids)
            .map(|(cell_res, id)| Some((id, cell_res)))
            .collect();
        Ok(Self {
            query_client,
            cell_block,
            tree_boundary,
            ordering,
            next,
            buffer_size,
            pos: 0,
        })
    }

    pub async fn next(&mut self) -> Result<Option<IndexedCell>, RPCError> {
        if self.pos < self.cell_block.len() {
            self.pos += 1;
            return Ok(Some(mem::take(&mut self.cell_block[self.pos]).unwrap()));
        }
        let next_key = if let Some(key) = &self.next {
            // Have next, use it
            key
        } else {
            // Key does not in the tree, and next key is unknown.
            // Should use boundary
            &self.tree_boundary
        };
        debug!("Buffer all used, refilling using key {:?}", next_key);
        let next_cursor = 
            RangedQueryClient::seek(
                &self.query_client, 
                next_key, 
                self.ordering,
                self.buffer_size
            ).await?;
        if let Some(cursor) = next_cursor {
            *self = cursor;
            Ok(self.current().cloned())
        } else {
            Ok(None)
        }
    }

    pub fn current(&self) -> Option<&IndexedCell> {
        match &self.cell_block[self.pos] {
            Some(cell) => Some(cell),
            _ => None,
        }
    }
}