use std::mem;

use super::IndexedDataClient;
use crate::{index::ranged::client::cursor::ClientCursor, ram::cell::OwnedCell};
use bifrost::rpc::RPCError;
use dovahkiin::ahash::HashMap;
use dovahkiin::{
    expr::serde::Expr,
    types::{Id, OwnedValue},
};

pub struct DataCursor {
    pub(super) checks: Vec<crate::index::verification::IndexCheck>,
    pub(super) index_cursor: Option<ClientCursor>,
    pub(super) buffer: Vec<OwnedCell>,
    pub(super) projection: Vec<u64>,
    pub(super) selection: Expr,
    pub(super) proc: Expr,
    pub(super) client: IndexedDataClient,
    pub(super) pos: usize,
}

/// One page of a resumable schema scan; see
/// `IndexedDataClient::query_ids_resumable`.
#[derive(Debug, Clone, Default, PartialEq, Eq)]
pub struct ScanPage {
    /// Matching ids in ascending id order.
    pub ids: Vec<Id>,
    /// Pass as `start_after` to continue; `None` when the scan reached the
    /// end of the schema.
    pub next_cursor: Option<Id>,
}

pub struct IdCursor {
    pub(super) cells: HashMap<Id, OwnedCell>,
    pub(super) buffer: Vec<Id>,
    pub(super) pos: usize,
}

#[derive(Clone, Debug, PartialEq)]
pub struct AggregateRow {
    pub group_values: Vec<(u64, OwnedValue)>,
    pub aggregate_values: Vec<(String, OwnedValue)>,
}

pub struct AggregateResultCursor {
    pub(super) buffer: Vec<AggregateRow>,
    pub(super) pos: usize,
}

impl IdCursor {
    pub async fn next(&mut self) -> Result<Option<Id>, RPCError> {
        if self.buffer.len() <= self.pos {
            return Ok(None);
        }
        let id = self.buffer[self.pos];
        self.pos += 1;
        Ok(Some(id))
    }
}

impl AggregateResultCursor {
    pub async fn next(&mut self) -> Result<Option<AggregateRow>, RPCError> {
        if self.buffer.len() <= self.pos {
            return Ok(None);
        }
        let row = self.buffer[self.pos].clone();
        self.pos += 1;
        Ok(Some(row))
    }
}

impl DataCursor {
    pub async fn next(&mut self) -> Result<Option<OwnedCell>, RPCError> {
        if self.buffer.len() <= self.pos {
            if self.next_block().await? {
                if !self.refresh_batch().await {
                    return Ok(None);
                }
            } else {
                return Ok(None);
            }
        }
        if self.buffer.is_empty() {
            return Ok(None);
        }
        let cell = mem::take(&mut self.buffer[self.pos]);
        self.pos += 1;
        Ok(Some(cell))
    }

    pub async fn next_block(&mut self) -> Result<bool, RPCError> {
        if let Some(cursor) = &mut self.index_cursor {
            if cursor.next_block().await? {
                return Ok(true);
            }
        }
        self.index_cursor = None;
        self.buffer = vec![];
        self.pos = 0;
        Ok(false)
    }

    pub async fn refresh_batch(&mut self) -> bool {
        while let Some(cursor) = self.index_cursor.as_ref() {
            self.buffer = self
                .client
                .read_cells_from_ids_checked(
                    cursor.current_block(),
                    &self.projection,
                    &self.selection,
                    &self.proc,
                    &self.checks,
                )
                .await;
            self.pos = 0;
            if !self.buffer.is_empty() {
                return true;
            }
            if !matches!(
                self.index_cursor.as_mut().unwrap().next_block().await,
                Ok(true)
            ) {
                break;
            }
        }
        self.index_cursor = None;
        self.buffer.clear();
        self.pos = 0;
        false
    }
}
