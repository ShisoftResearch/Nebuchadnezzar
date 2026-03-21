use bifrost::rpc::RPCError;
use dovahkiin::{expr::serde::Expr, types::{Id, OwnedValue}};
use futures::stream::{FuturesUnordered, StreamExt};
use itertools::Itertools;

use crate::{
    client::client_by_server_name,
    index::{
        EntryKey, SCHEMA_SCAN_PATT_SIZE,
        ranged::{client::cursor::ClientCursor, tree::{btree::Ordering, service::Range}},
    },
    ram::cell::OwnedCell,
};

use super::{DataCursor, IndexedDataClient, QueryOrdering, SCAN_BUFFER_SIZE};

impl IndexedDataClient {
    pub(super) async fn scan_schema_index<'a>(
        &'a self,
        schema: u32,
        projection: Vec<u64>,
        selection: Expr,
        proc: Expr,
        ordering: Ordering,
    ) -> Result<DataCursor, RPCError> {
        let key = EntryKey::for_schema(schema);
        let index_cursor = self
            .index_clients
            .range_seek(
                Range::new_inclusive_opened(key, ordering),
                SCAN_BUFFER_SIZE,
                Some(SCHEMA_SCAN_PATT_SIZE),
            )
            .await?;
        Ok(self
            .new_cursor(index_cursor, projection, selection, proc)
            .await)
    }

    pub(super) async fn scan_schema_ids(
        &self,
        schema: u32,
        _ordering: QueryOrdering,
    ) -> Result<Vec<Id>, RPCError> {
        let key = EntryKey::for_schema(schema);
        let Some(mut index_cursor) = self
            .index_clients
            .range_seek(
                Range::new_inclusive_opened(key, Ordering::Forward),
                SCAN_BUFFER_SIZE,
                Some(SCHEMA_SCAN_PATT_SIZE),
            )
            .await?
        else {
            return Ok(vec![]);
        };
        let mut ids = vec![];
        loop {
            ids.extend_from_slice(index_cursor.current_block());
            if !index_cursor.next_block().await? {
                break;
            }
        }
        Ok(ids)
    }

    pub(super) async fn filter_ids_by_selection_limit(
        &self,
        candidate_ids: &[Id],
        selection: &Expr,
        limit: Option<usize>,
    ) -> Vec<Id> {
        let empty_projection: Vec<u64> = vec![];
        let Some(limit) = limit else {
            let selected_cells = self
                .read_cells_from_ids(
                    candidate_ids,
                    &empty_projection,
                    selection,
                    &Expr::nothing(),
                )
                .await;
            return selected_cells.into_iter().map(|cell| cell.id()).collect_vec();
        };
        if limit == 0 || candidate_ids.is_empty() {
            return vec![];
        }

        let mut selected_ids = Vec::with_capacity(limit);
        let batch = usize::from(SCAN_BUFFER_SIZE.max(1));
        for chunk in candidate_ids.chunks(batch) {
            let selected_cells = self
                .read_cells_from_ids(chunk, &empty_projection, selection, &Expr::nothing())
                .await;
            selected_ids.extend(selected_cells.into_iter().map(|cell| cell.id()));
            if selected_ids.len() >= limit {
                selected_ids.truncate(limit);
                break;
            }
        }
        selected_ids
    }

    pub(super) async fn new_cursor<'a>(
        &'a self,
        index_cursor: Option<ClientCursor>,
        projection: Vec<u64>,
        selection: Expr,
        proc: Expr,
    ) -> DataCursor {
        let mut cursor = DataCursor {
            index_cursor,
            projection,
            selection,
            proc,
            client: self.clone(),
            buffer: vec![],
            pos: 0,
        };
        cursor.refresh_batch().await;
        cursor
    }

    pub(super) async fn read_cells_from_ids(
        &self,
        ids: &[Id],
        projection: &Vec<u64>,
        selection: &Expr,
        proc: &Expr,
    ) -> Vec<OwnedCell> {
        let mut all_cells = vec![];
        let mut tasks = ids
            .iter()
            .enumerate()
            .filter_map(|(i, id)| self.conshash.get_server_id_by(id).map(|sid| (i, sid, *id)))
            .sorted_by_key(|(_i, sid, _id)| *sid)
            .chunk_by(|(_i, sid, _id)| *sid)
            .into_iter()
            .map(|(sid, pairs)| {
                let mut grouped_ids = vec![];
                let mut idx = vec![];
                for (i, _, id) in pairs {
                    idx.push(i);
                    grouped_ids.push(id);
                }
                let projection = projection.clone();
                let selection = selection.clone();
                let proc = proc.clone();
                let server_name = self.conshash.to_server_name(sid);
                async move {
                    match client_by_server_name(sid, server_name).await {
                        Ok(client) => {
                            let read_res = client
                                .read_all_cells_proced(&grouped_ids, &projection, &selection, &proc)
                                .await
                                .map(|cells| {
                                    cells
                                        .into_iter()
                                        .zip(idx)
                                        .filter_map(|(cell_res, original_idx)| match cell_res {
                                            Ok(cell) => Some((cell, original_idx)),
                                            Err(e) => {
                                                warn!(
                                                    "Cell read error at index {}: {:?}",
                                                    original_idx, e
                                                );
                                                None
                                            }
                                        })
                                        .collect_vec()
                                });
                            match read_res {
                                Ok(cells) => Ok(cells),
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
                    warn!("Task error in read_cells_from_ids: {:?}", e);
                }
            }
        }

        all_cells.sort_by(|(_, i1), (_, i2)| i1.cmp(i2));
        all_cells.into_iter().map(|(cell, _)| cell).collect_vec()
    }

    pub(super) async fn read_projected_rows_from_ids(
        &self,
        ids: &[Id],
        projection: &Vec<u64>,
    ) -> Vec<Vec<OwnedValue>> {
        let projected_cells = self
            .read_cells_from_ids(ids, projection, &Expr::nothing(), &Expr::nothing())
            .await;
        projected_cells
            .into_iter()
            .map(|cell| {
                projection
                    .iter()
                    .enumerate()
                    .map(|(index, _field_id)| cell[index].clone())
                    .collect_vec()
            })
            .collect_vec()
    }

    pub(super) async fn read_selected_cells_from_ids(
        &self,
        ids: &[Id],
        fields: &[u64],
    ) -> Vec<OwnedCell> {
        let mut all_cells = vec![];
        let mut tasks = ids
            .iter()
            .enumerate()
            .filter_map(|(i, id)| self.conshash.get_server_id_by(id).map(|sid| (i, sid, *id)))
            .sorted_by_key(|(_i, sid, _id)| *sid)
            .chunk_by(|(_i, sid, _id)| *sid)
            .into_iter()
            .map(|(sid, pairs)| {
                let mut grouped_ids = vec![];
                let mut idx = vec![];
                for (i, _, id) in pairs {
                    idx.push(i);
                    grouped_ids.push(id);
                }
                let fields = fields.to_vec();
                let server_name = self.conshash.to_server_name(sid);
                async move {
                    match client_by_server_name(sid, server_name).await {
                        Ok(client) => client
                            .read_all_cells_selected(&grouped_ids, &fields, true)
                            .await
                            .map(|cells| {
                                cells
                                    .into_iter()
                                    .zip(idx)
                                    .filter_map(|(cell_res, original_idx)| match cell_res {
                                        Ok(cell) => Some((cell, original_idx)),
                                        Err(e) => {
                                            warn!(
                                                "Selected cell read error at index {}: {:?}",
                                                original_idx, e
                                            );
                                            None
                                        }
                                    })
                                    .collect_vec()
                            }),
                        Err(e) => Err(e),
                    }
                }
            })
            .collect::<FuturesUnordered<_>>();

        while let Some(task_res) = tasks.next().await {
            match task_res {
                Ok(mut cells) => all_cells.append(&mut cells),
                Err(e) => warn!("Task error in read_selected_cells_from_ids: {:?}", e),
            }
        }

        all_cells.sort_by(|(_, i1), (_, i2)| i1.cmp(i2));
        all_cells.into_iter().map(|(cell, _)| cell).collect_vec()
    }
}
