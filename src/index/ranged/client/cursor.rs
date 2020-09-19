use super::super::lsm::btree::Ordering;
use super::super::lsm::service::*;
use crate::client::AsyncClient;
use crate::index::ranged::client::RangedQueryClient;
use crate::index::EntryKey;
use crate::ram::cell::Cell;
use crate::ram::cell::ReadError;
use bifrost::rpc::RPCError;
use std::mem;
use std::sync::Arc;

type CellBlock = Vec<CellSlot>;

enum CellSlot {
    Some(Cell),
    None,
    Taken,
}

pub struct ClientCursor {
    cell: Option<Cell>,
    cell_block: Option<CellBlock>,
    query_client: Arc<RangedQueryClient>,
    tree_client: Arc<AsyncServiceClient>,
    remote_cursor: ServCursor,
    ordering: Ordering,
    tree_boundary: EntryKey,
    pos: usize,
}

impl ClientCursor {
    pub fn new(
        remote: ServCursor,
        init_cell: Cell,
        ordering: Ordering,
        tree_boundary: EntryKey,
        tree_client: Arc<AsyncServiceClient>,
        query_client: Arc<RangedQueryClient>,
    ) -> Self {
        Self {
            cell: Some(init_cell),
            remote_cursor: remote,
            tree_client,
            query_client,
            cell_block: None,
            tree_boundary,
            ordering,
            pos: 0,
        }
    }

    pub async fn next(&mut self) -> Result<Option<Cell>, RPCError> {
        loop {
            let res;
            if self.cell.is_some() && self.cell_block.is_none() {
                res = mem::replace(&mut self.cell, None);
                let cells = Self::refresh_block(
                    &self.tree_client,
                    &self.query_client.neb_client,
                    self.remote_cursor,
                )
                .await?;
                self.cell_block = Some(cells);
            } else if let &mut Some(ref mut cells) = &mut self.cell_block {
                if cells[0].is_none() {
                    // have empty block will try to reload the cursor from the client for
                    // next key may been placed on another
                    let replacement = RangedQueryClient::seek(
                        &self.query_client,
                        &self.tree_boundary,
                        self.ordering,
                    )
                    .await?;
                    if let Some(new_cursor) = replacement {
                        *self = new_cursor;
                        continue;
                    } else {
                        return Ok(None);
                    }
                }
                let old_cell = mem::replace(&mut cells[self.pos], CellSlot::Taken);
                res = Some(old_cell.get_in());
                self.pos += 1;
                // Check if pos is in range and have value. If not, get next block.
                if self.pos >= cells.len() || cells[self.pos].is_none() {
                    *cells = Self::refresh_block(
                        &self.tree_client,
                        &self.query_client.neb_client,
                        self.remote_cursor,
                    )
                    .await?;
                    self.cell = None;
                    self.pos = 0;
                }
            } else {
                unimplemented!();
            }
            return Ok(res);
        }
    }

    pub fn current(&self) -> Option<&Cell> {
        if self.cell.is_some() {
            self.cell.as_ref()
        } else {
            self.cell_block.as_ref().unwrap()[self.pos].borrow_into()
        }
    }

    async fn refresh_block(
        tree_client: &Arc<AsyncServiceClient>,
        neb_client: &Arc<AsyncClient>,
        remote_cursor: ServCursor,
    ) -> Result<CellBlock, RPCError> {
        let cell_ids = tree_client.cursor_next(remote_cursor).await?.unwrap();
        let cells = neb_client
            .read_all_cells(Vec::from(cell_ids))
            .await?
            .into_iter()
            .map(|cell_res| match cell_res {
                Ok(cell) => CellSlot::Some(cell),
                Err(ReadError::CellIdIsUnitId) => CellSlot::None,
                Err(e) => panic!("{:?}", e),
            })
            .collect();
        Ok(cells)
    }
}

impl Drop for ClientCursor {
    fn drop(&mut self) {
        let remote_cursor = self.remote_cursor;
        let tree_client = self.tree_client.clone();
        tokio::spawn(async move { tree_client.dispose_cursor(remote_cursor).await });
    }
}

impl CellSlot {
    fn is_none(&self) -> bool {
        match self {
            CellSlot::None => true,
            _ => false,
        }
    }
    fn get_in(self) -> Cell {
        match self {
            CellSlot::Some(cell) => cell,
            _ => unreachable!(),
        }
    }
    fn borrow_into(&self) -> Option<&Cell> {
        match self {
            CellSlot::Some(cell) => Some(cell),
            CellSlot::None => None,
            CellSlot::Taken => unreachable!(),
        }
    }
}
