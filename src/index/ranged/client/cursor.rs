use super::super::lsm::btree::Ordering;
use super::super::lsm::service::*;
use crate::client::AsyncClient;
use crate::index::ranged::client::RangedQueryClient;
use crate::index::EntryKey;
use crate::ram::cell::Cell;
use crate::ram::cell::ReadError;
use crate::ram::types::Id;
use bifrost::rpc::RPCError;
use std::mem;
use std::sync::Arc;

type CellBlock = Vec<CellSlot>;
pub type IndexedCell = (Id, Result<Cell, ReadError>);

enum CellSlot {
    Some(IndexedCell),
    None,
    Taken,
}

pub struct ClientCursor {
    cell: CellSlot,
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
        init_cell: IndexedCell,
        ordering: Ordering,
        tree_boundary: EntryKey,
        tree_client: Arc<AsyncServiceClient>,
        query_client: Arc<RangedQueryClient>,
    ) -> Self {
        Self {
            cell: CellSlot::Some(init_cell),
            remote_cursor: remote,
            tree_client,
            query_client,
            cell_block: None,
            tree_boundary,
            ordering,
            pos: 0,
        }
    }

    pub async fn next(&mut self) -> Result<Option<IndexedCell>, RPCError> {
        loop {
            let res: IndexedCell;
            if self.cell.is_some() && self.cell_block.is_none() {
                res = mem::take(&mut self.cell).get_in();
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
                res = old_cell.get_in();
                self.pos += 1;
                // Check if pos is in range and have value. If not, get next block.
                if self.pos >= cells.len() || cells[self.pos].is_none() {
                    *cells = Self::refresh_block(
                        &self.tree_client,
                        &self.query_client.neb_client,
                        self.remote_cursor,
                    )
                    .await?;
                    self.cell = CellSlot::Taken;
                    self.pos = 0;
                }
            } else {
                unimplemented!();
            }
            return Ok(Some(res));
        }
    }

    pub fn current(&self) -> Option<&IndexedCell> {
        match &self.cell {
            CellSlot::Some(cell) => Some(cell),
            _ => {
                match  &self.cell_block.as_ref().unwrap()[self.pos] {
                    CellSlot::Some(cell) => Some(cell),
                    _ => None
                }
            }
        }
    }

    async fn refresh_block(
        tree_client: &Arc<AsyncServiceClient>,
        neb_client: &Arc<AsyncClient>,
        remote_cursor: ServCursor,
    ) -> Result<CellBlock, RPCError> {
        let cell_ids = tree_client.cursor_next(remote_cursor).await?.unwrap();
        let id_vec = Vec::from(cell_ids);
        let id_vec_copy = id_vec.clone();
        let cells = neb_client
            .read_all_cells(id_vec)
            .await?
            .into_iter()
            .zip(id_vec_copy)
            .map(|(cell_res, id)| CellSlot::Some((id, cell_res)))
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
    fn is_some(&self) -> bool {
        match self {
            CellSlot::Some(_) => true,
            _ => false
        }
    }
    fn get_in(self) -> IndexedCell {
        match self {
            CellSlot::Some(cell) => cell,
            _ => unreachable!(),
        }
    }
    fn borrow_into(&self) -> Option<&IndexedCell> {
        match self {
            CellSlot::Some(cell) => Some(cell),
            CellSlot::None => None,
            CellSlot::Taken => unreachable!(),
        }
    }
}

impl Default for CellSlot {
    fn default() -> Self {
        CellSlot::None
    }
}
