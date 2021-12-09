use std::{mem, sync::Arc};

use bifrost::{conshash::ConsistentHashing, raft::client::RaftClient, rpc::RPCError};
use dovahkiin::{expr::serde::Expr, types::OwnedValue};
use futures::stream::{FuturesUnordered, StreamExt};
use itertools::Itertools;

use crate::{
    client::{client_by_server_id, client_by_server_name},
    index::{
        ranged::{client::cursor::ClientCursor, lsm::btree::Ordering},
        EntryKey, IndexerClients, SCHEMA_SCAN_PATT_SIZE,
    },
    ram::cell::{OwnedCell, ReadError},
};

const SCAN_BUFFER_SIZE: u16 = 64;

pub struct IndexedDataClient {
    conshash: Arc<ConsistentHashing>,
    index_clients: Arc<IndexerClients>,
}

pub struct DataCursor<'a> {
    index_cursor: Option<ClientCursor>,
    buffer: Vec<OwnedCell>,
    projection: Vec<u64>,
    selection: Expr,
    proc: Expr,
    client: &'a IndexedDataClient,
    pos: usize,
}

impl IndexedDataClient {
    pub fn new(conshash: &Arc<ConsistentHashing>, raft_client: &Arc<RaftClient>) -> Self {
        Self {
            conshash: conshash.clone(),
            index_clients: Arc::new(IndexerClients::new(conshash, raft_client)),
        }
    }
    pub async fn range_index_scan<'a>(
        &self,
        schema: u32,
        field: u64,
        key: u64,
        selection: OwnedValue,
        projection: OwnedValue,
    ) -> DataCursor<'a> {
        unimplemented!()
    }
    pub async fn scan_all<'a>(
        &'a self,
        schema: u32,
        projection: Vec<u64>, // Column array
        selection: Expr,      // Checker expression
        proc: Expr,
        ordering: Ordering,
    ) -> Result<DataCursor<'a>, RPCError> {
        let key = EntryKey::for_schema(schema);
        let index_cursor = self
            .index_clients
            .range_seek(
                &key,
                ordering,
                SCAN_BUFFER_SIZE,
                Some(SCHEMA_SCAN_PATT_SIZE),
            )
            .await?;
        let mut cursor = DataCursor {
            index_cursor,
            projection,
            selection,
            proc,
            client: self,
            buffer: vec![],
            pos: 0,
        };
        cursor.refresh_batch().await;
        Ok(cursor)
    }
}

impl<'a> DataCursor<'a> {
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
        let cell = mem::take(&mut self.buffer[self.pos]);
        self.pos += 1;
        return Ok(Some(cell));
    }

    pub async fn next_block(&mut self) -> Result<bool, RPCError> {
        if let Some(cursor) = &mut self.index_cursor {
            if cursor.next_block().await? {
                return Ok(true);
            }
        }
        // If cannot get next block, set the index cursor to none
        // We are done with this cursor
        self.index_cursor = None;
        self.buffer = vec![];
        self.pos = 0;
        return Ok(false);
    }

    pub async fn refresh_batch(&mut self) -> bool {
        if let Some(cursor) = &self.index_cursor {
            let all_ids = cursor.current_block();
            let mut tasks = all_ids
                .iter()
                .enumerate()
                .filter_map(|(i, id)| {
                    self.client
                        .conshash
                        .get_server_id_by(id)
                        .map(|sid| (i, sid, id))
                })
                .group_by(|(_i, sid, _id)| *sid)
                .into_iter()
                .map(|(sid, pairs)| {
                    let mut ids = vec![];
                    let mut idx = vec![];
                    pairs.for_each(|(i, _, id)| {
                        idx.push(i);
                        ids.push(*id);
                    });
                    let projection = self.projection.clone();
                    let selection = self.selection.clone();
                    let proc = self.proc.clone();
                    let server_name = self.client.conshash.to_server_name(sid);
                    async move {
                        match client_by_server_name(sid, server_name).await {
                            Ok(client) => {
                                let read_res = client
                                    .read_all_cells_proced(ids, projection, selection, proc)
                                    .await
                                    .map(|v| {
                                        v.into_iter()
                                            .zip(idx)
                                            .collect_vec()
                                    });
                                match read_res {
                                    Ok(cells) => {
                                        Ok(cells.into_iter().filter_map(|(c, i)| c.ok().map(|c| (c, i))).collect_vec())
                                    }
                                    Err(e) => return Err(e),
                                }
                            }
                            Err(e) => return Err(e),
                        }
                    }
                })
                .collect::<FuturesUnordered<_>>();
            let mut all_cells = vec![];
            while let Some(task_res) = tasks.next().await {
                if let Ok(mut cells) = task_res {
                    all_cells.append(&mut cells);
                }
            }
            all_cells.sort_by(|(_, i1), (_, i2)| i1.cmp(i2));
            self.buffer = all_cells.into_iter().map(|(c, _)| c).collect_vec();
            self.pos = 0;
            true
        } else {
            self.buffer = vec![];
            self.pos = 0;
            false
        }
    }
}
