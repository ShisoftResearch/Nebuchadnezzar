use std::mem;

use bifrost::rpc::RPCError;
use dovahkiin::{expr::serde::Expr, types::Id};
use futures::stream::{FuturesUnordered, StreamExt};
use itertools::Itertools;

use crate::{
    client::client_by_server_name, index::ranged::client::cursor::ClientCursor,
    ram::cell::OwnedCell,
};

use super::IndexedDataClient;

pub struct DataCursor {
    pub(super) index_cursor: Option<ClientCursor>,
    pub(super) buffer: Vec<OwnedCell>,
    pub(super) projection: Vec<u64>,
    pub(super) selection: Expr,
    pub(super) proc: Expr,
    pub(super) client: IndexedDataClient,
    pub(super) pos: usize,
}

pub struct IdCursor {
    pub(super) buffer: Vec<Id>,
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
        if self.index_cursor.is_some() {
            let mut all_cells = vec![];
            loop {
                {
                    let cursor = self.index_cursor.as_ref().unwrap();
                    let mut tasks = cursor
                        .current_block()
                        .iter()
                        .enumerate()
                        .filter_map(|(i, id)| {
                            self.client
                                .conshash
                                .get_server_id_by(id)
                                .map(|sid| (i, sid, id))
                        })
                        .sorted_by_key(|(_i, sid, _id)| *sid)
                        .chunk_by(|(_i, sid, _id)| *sid)
                        .into_iter()
                        .map(|(sid, pairs)| {
                            let mut ids = vec![];
                            let mut idx = vec![];
                            for (i, _, id) in pairs {
                                idx.push(i);
                                ids.push(*id);
                            }
                            let projection = &self.projection;
                            let selection = &self.selection;
                            let proc = &self.proc;
                            let server_name = self.client.conshash.to_server_name(sid);
                            async move {
                                match client_by_server_name(sid, server_name).await {
                                    Ok(client) => {
                                        let read_res = client
                                            .read_all_cells_proced(&ids, projection, selection, proc)
                                            .await
                                            .map(|v| v.into_iter().zip(idx).collect_vec());
                                        match read_res {
                                            Ok(cells) => Ok(cells
                                                .into_iter()
                                                .filter_map(|(c, i)| match c {
                                                    Ok(cell) => Some((cell, i)),
                                                    Err(e) => {
                                                        warn!("Cursor cell read error at index {}: {:?}", i, e);
                                                        None
                                                    }
                                                })
                                                .collect_vec()),
                                            Err(e) => Err(e),
                                        }
                                    }
                                    Err(e) => Err(e),
                                }
                            }
                        })
                        .collect::<FuturesUnordered<_>>();
                    while let Some(task_res) = tasks.next().await {
                        match task_res {
                            Ok(mut cells) => all_cells.append(&mut cells),
                            Err(e) => {
                                warn!("Cursor task error: {:?}", e);
                            }
                        }
                    }
                }
                if !all_cells.is_empty() {
                    break;
                } else {
                    let cursor = self.index_cursor.as_mut().unwrap();
                    match cursor.next_block().await {
                        Ok(true) => continue,
                        _ => {
                            self.buffer = vec![];
                            self.pos = 0;
                            return false;
                        }
                    }
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
