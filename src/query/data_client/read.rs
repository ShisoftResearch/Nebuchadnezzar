use bifrost::rpc::RPCError;
use dovahkiin::{
    expr::serde::Expr,
    types::{Id, OwnedValue},
};
use futures::stream::{FuturesUnordered, StreamExt};
use itertools::Itertools;

use crate::{
    client::client_by_server_name_for_database,
    index::{
        ranged::{
            client::cursor::ClientCursor,
            tree::{btree::Ordering, service::Range},
        },
        EntryKey, SCHEMA_SCAN_PATT_SIZE,
    },
    query::planner::normalize_selection_for_eval,
    ram::{
        cell::OwnedCell,
        types::{index_query_scalars, values_semantically_equal},
    },
};

use super::{DataCursor, IndexedDataClient, QueryOrdering, SCAN_BUFFER_SIZE};

const SCHEMA_SCAN_BUFFER_SIZE: u16 = 2048;

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
                SCHEMA_SCAN_BUFFER_SIZE,
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
                SCHEMA_SCAN_BUFFER_SIZE,
                Some(SCHEMA_SCAN_PATT_SIZE),
            )
            .await?
        else {
            return Ok(vec![]);
        };
        let mut ids = vec![];
        while let Some(id) = index_cursor.next().await? {
            ids.push(id);
        }
        Ok(ids)
    }

    /// Schema scan that filters as it walks and stops at `limit`, holding at
    /// most one batch of ids and the matches found so far.
    ///
    /// The eager form -- enumerate every id for the schema, then filter -- is
    /// O(rows scanned) in memory no matter how small the limit is. Pushing the
    /// limit into the filter bounded the cells it READS, which is what costs
    /// disk and pins segments, but `scan_schema_ids` still built a `Vec` of
    /// every id in the schema first (~2GB at 117.7M entities), because it
    /// drains a perfectly lazy cursor into a vector for no reason. Pulling
    /// from that cursor instead makes the whole path O(batch + limit).
    ///
    /// Forward only. `Ordering::Backward` needs the scan to seek from the
    /// schema's upper key bound, which `EntryKey::for_schema` does not
    /// provide; the caller keeps the materialising path for that case, where
    /// correctness comes from sorting the full set.
    pub(super) async fn stream_schema_scan_filtered(
        &self,
        schema: u32,
        selection: &Expr,
        limit: usize,
    ) -> Result<Vec<Id>, RPCError> {
        if limit == 0 {
            return Ok(vec![]);
        }
        let key = EntryKey::for_schema(schema);
        let Some(mut index_cursor) = self
            .index_clients
            .range_seek(
                Range::new_inclusive_opened(key, Ordering::Forward),
                SCHEMA_SCAN_BUFFER_SIZE,
                Some(SCHEMA_SCAN_PATT_SIZE),
            )
            .await?
        else {
            return Ok(vec![]);
        };
        let batch = usize::from(SCAN_BUFFER_SIZE.max(1));
        let mut selected: Vec<Id> = Vec::with_capacity(limit);
        let mut pending: Vec<Id> = Vec::with_capacity(batch);
        loop {
            pending.clear();
            while pending.len() < batch {
                match index_cursor.next().await? {
                    Some(id) => pending.push(id),
                    None => break,
                }
            }
            if pending.is_empty() {
                break;
            }
            let cells = self
                .read_cells_from_ids(&pending, &vec![], &Expr::nothing(), &Expr::nothing())
                .await;
            for cell in cells {
                if cell_matches_selection(&cell, selection) {
                    selected.push(cell.id());
                    if selected.len() >= limit {
                        return Ok(selected);
                    }
                }
            }
        }
        Ok(selected)
    }

    pub(super) async fn filter_ids_by_selection_limit(
        &self,
        candidate_ids: &[Id],
        selection: &Expr,
        limit: Option<usize>,
    ) -> Vec<Id> {
        let Some(limit) = limit else {
            let cells = self
                .read_cells_from_ids(candidate_ids, &vec![], &Expr::nothing(), &Expr::nothing())
                .await;
            return cells
                .into_iter()
                .filter(|cell| cell_matches_selection(cell, selection))
                .map(|cell| cell.id())
                .collect_vec();
        };
        if limit == 0 || candidate_ids.is_empty() {
            return vec![];
        }

        let mut selected_ids = Vec::with_capacity(limit);
        let batch = usize::from(SCAN_BUFFER_SIZE.max(1));
        for chunk in candidate_ids.chunks(batch) {
            let cells = self
                .read_cells_from_ids(chunk, &vec![], &Expr::nothing(), &Expr::nothing())
                .await;
            selected_ids.extend(
                cells
                    .into_iter()
                    .filter(|cell| cell_matches_selection(cell, selection))
                    .map(|cell| cell.id()),
            );
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
        let normalized_selection = normalize_selection_for_eval(selection);
        let expect_full_batch = normalized_selection.is_empty() && proc.is_empty();
        let mut all_cells = vec![];
        let mut ordered_cells = if expect_full_batch {
            Some(vec![None; ids.len()])
        } else {
            None
        };
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
                let selection = normalized_selection.clone();
                let proc = proc.clone();
                let group_name = self.group_name.clone();
                let database_name = self.database_name.clone();
                let server_name = self.conshash.to_server_name(sid);
                async move {
                    match client_by_server_name_for_database(
                        sid,
                        server_name,
                        &group_name,
                        &database_name,
                    )
                    .await
                    {
                        Ok(client) => {
                            let read_res = client
                                .read_all_cells_proced(&grouped_ids, &projection, &selection, &proc)
                                .await
                                .map(|cells| {
                                    if cells.len() != idx.len() {
                                        warn!(
                                            "Batch cell read count mismatch: requested {}, got {}",
                                            idx.len(),
                                            cells.len()
                                        );
                                    }
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
                Ok(mut cells) => {
                    if let Some(ordered_cells) = ordered_cells.as_mut() {
                        for (cell, original_idx) in cells.drain(..) {
                            ordered_cells[original_idx] = Some(cell);
                        }
                    } else {
                        all_cells.append(&mut cells);
                    }
                }
                Err(e) => {
                    warn!("Task error in read_cells_from_ids: {:?}", e);
                }
            }
        }

        if let Some(mut ordered_cells) = ordered_cells {
            for (idx, slot) in ordered_cells.iter_mut().enumerate() {
                if slot.is_some() {
                    continue;
                }
                let Some(sid) = self.conshash.get_server_id_by(&ids[idx]) else {
                    warn!("Missing server mapping for id {:?}", ids[idx]);
                    continue;
                };
                let server_name = self.conshash.to_server_name(sid);
                match client_by_server_name_for_database(
                    sid,
                    server_name,
                    &self.group_name,
                    &self.database_name,
                )
                .await
                {
                    Ok(client) => {
                        let single_id = vec![ids[idx]];
                        match client
                            .read_all_cells_proced(&single_id, projection, selection, proc)
                            .await
                        {
                            Ok(mut cells) => match cells.pop() {
                                Some(Ok(cell)) => {
                                    *slot = Some(cell);
                                }
                                Some(Err(e)) => {
                                    warn!("Retry cell read error at index {}: {:?}", idx, e);
                                }
                                None => {
                                    warn!("Retry cell read returned no cell at index {}", idx);
                                }
                            },
                            Err(e) => {
                                warn!("Retry task error in read_cells_from_ids: {:?}", e);
                            }
                        }
                    }
                    Err(e) => {
                        warn!(
                            "Retry client creation error in read_cells_from_ids: {:?}",
                            e
                        );
                    }
                }
            }

            return ordered_cells.into_iter().flatten().collect_vec();
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
                let group_name = self.group_name.clone();
                let database_name = self.database_name.clone();
                let server_name = self.conshash.to_server_name(sid);
                async move {
                    match client_by_server_name_for_database(
                        sid,
                        server_name,
                        &group_name,
                        &database_name,
                    )
                    .await
                    {
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

fn cell_matches_selection(cell: &OwnedCell, selection: &Expr) -> bool {
    let Expr::List(items) = selection else {
        return true;
    };

    if items.is_empty() {
        return true;
    }

    if is_symbol_named(&items[0], "and") {
        return items
            .iter()
            .skip(1)
            .all(|child| cell_matches_selection(cell, child));
    }

    if is_symbol_named(&items[0], "or") {
        return items
            .iter()
            .skip(1)
            .any(|child| cell_matches_selection(cell, child));
    }

    if is_symbol_named(&items[0], "not") {
        return items
            .get(1)
            .map(|child| !cell_matches_selection(cell, child))
            .unwrap_or(false);
    }

    if is_symbol_named(&items[0], "is-null") {
        return items
            .get(1)
            .and_then(expr_field_id)
            .map(|field_id| matches!(cell[field_id], OwnedValue::Null | OwnedValue::NA))
            .unwrap_or(false);
    }

    if is_symbol_named(&items[0], "is-not-null") {
        return items
            .get(1)
            .and_then(expr_field_id)
            .map(|field_id| !matches!(cell[field_id], OwnedValue::Null | OwnedValue::NA))
            .unwrap_or(false);
    }

    if is_symbol_named(&items[0], "in") {
        let Some(field_id) = items.get(1).and_then(expr_field_id) else {
            return false;
        };
        let field_value = &cell[field_id];
        return items
            .iter()
            .skip(2)
            .filter_map(expr_owned_value)
            .any(|value| values_semantically_equal(field_value, value));
    }

    if is_symbol_named(&items[0], "between") {
        let (Some(field_id), Some(lower), Some(upper)) = (
            items.get(1).and_then(expr_field_id),
            items.get(2).and_then(expr_owned_value),
            items.get(3).and_then(expr_owned_value),
        ) else {
            return false;
        };
        let field_value = &cell[field_id];
        return compare_values(field_value, lower, CompareOp::Ge)
            && compare_values(field_value, upper, CompareOp::Le);
    }

    if let Some((op, field_id, value)) = comparison_clause(selection) {
        return compare_values(&cell[field_id], &value, op);
    }

    false
}

#[derive(Clone, Copy)]
enum CompareOp {
    Eq,
    Ne,
    Gt,
    Ge,
    Lt,
    Le,
}

fn comparison_clause(selection: &Expr) -> Option<(CompareOp, u64, OwnedValue)> {
    let Expr::List(items) = selection else {
        return None;
    };
    if items.len() != 3 {
        return None;
    }

    let mut op = parse_compare_op(&items[0])?;
    if let (Some(field_id), Some(value)) = (expr_field_id(&items[1]), expr_owned_value(&items[2])) {
        return Some((op, field_id, value.clone()));
    }

    if let (Some(value), Some(field_id)) = (expr_owned_value(&items[1]), expr_field_id(&items[2])) {
        op = reverse_compare_op(op);
        return Some((op, field_id, value.clone()));
    }

    None
}

fn parse_compare_op(expr: &Expr) -> Option<CompareOp> {
    if is_symbol_named(expr, "=") {
        Some(CompareOp::Eq)
    } else if is_symbol_named(expr, "!=") {
        Some(CompareOp::Ne)
    } else if is_symbol_named(expr, ">") {
        Some(CompareOp::Gt)
    } else if is_symbol_named(expr, ">=") {
        Some(CompareOp::Ge)
    } else if is_symbol_named(expr, "<") {
        Some(CompareOp::Lt)
    } else if is_symbol_named(expr, "<=") {
        Some(CompareOp::Le)
    } else {
        None
    }
}

fn reverse_compare_op(op: CompareOp) -> CompareOp {
    match op {
        CompareOp::Eq => CompareOp::Eq,
        CompareOp::Ne => CompareOp::Ne,
        CompareOp::Gt => CompareOp::Lt,
        CompareOp::Ge => CompareOp::Le,
        CompareOp::Lt => CompareOp::Gt,
        CompareOp::Le => CompareOp::Ge,
    }
}

fn compare_values(left: &OwnedValue, right: &OwnedValue, op: CompareOp) -> bool {
    let (Some(left_values), Some(right_values)) =
        (index_query_scalars(left), index_query_scalars(right))
    else {
        return false;
    };

    match op {
        CompareOp::Eq => values_semantically_equal(left, right),
        CompareOp::Ne => left_values.iter().all(|left_value| {
            right_values
                .iter()
                .all(|right_value| left_value != right_value)
        }),
        CompareOp::Gt => left_values.iter().any(|left_value| {
            right_values.iter().any(|right_value| {
                left_value
                    .partial_cmp(right_value)
                    .is_some_and(|ord| ord.is_gt())
            })
        }),
        CompareOp::Ge => left_values.iter().any(|left_value| {
            right_values.iter().any(|right_value| {
                left_value
                    .partial_cmp(right_value)
                    .is_some_and(|ord| ord.is_ge())
            })
        }),
        CompareOp::Lt => left_values.iter().any(|left_value| {
            right_values.iter().any(|right_value| {
                left_value
                    .partial_cmp(right_value)
                    .is_some_and(|ord| ord.is_lt())
            })
        }),
        CompareOp::Le => left_values.iter().any(|left_value| {
            right_values.iter().any(|right_value| {
                left_value
                    .partial_cmp(right_value)
                    .is_some_and(|ord| ord.is_le())
            })
        }),
    }
}

fn expr_field_id(expr: &Expr) -> Option<u64> {
    match expr {
        Expr::Symbol(field_id, _) => Some(*field_id),
        _ => None,
    }
}

fn expr_owned_value(expr: &Expr) -> Option<&OwnedValue> {
    match expr {
        Expr::Value(value) => Some(value),
        _ => None,
    }
}

fn is_symbol_named(expr: &Expr, expected: &str) -> bool {
    matches!(expr, Expr::Symbol(_, name) if name == expected)
}
