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
        ranged::tree::{btree::Ordering, service::Range},
        EntryKey, SCHEMA_SCAN_PATT_SIZE,
    },
    query::planner::normalize_selection_for_eval,
    ram::{
        cell::OwnedCell,
        types::{index_query_scalars, values_semantically_equal},
    },
};

use super::{IndexedDataClient, QueryOrdering, ScanPage, SCAN_BUFFER_SIZE};
use crate::ram::schema::SchemaUid;

const SCHEMA_SCAN_BUFFER_SIZE: u16 = 2048;

/// Total cells this client has read from ids, ever.
///
/// A correctness suite cannot see this bug. Reverting the limit pushdown
/// returns the same rows in the same order for every test we have -- it just
/// reads the whole schema to produce them, which only becomes visible when
/// the schema is 117.7M entities on cold storage. The rows are the same; the
/// work is not. So the work is what has to be asserted, and counting cells
/// read makes the shape testable at any size: a bounded scan reads a number
/// of cells proportional to the LIMIT, an unbounded one proportional to the
/// ROWS. One relaxed add per batch, so it costs nothing to leave compiled in.
pub(crate) static CELLS_READ: std::sync::atomic::AtomicUsize =
    std::sync::atomic::AtomicUsize::new(0);

/// Cells read so far; take two readings and subtract to measure one query.
pub fn cells_read_count() -> usize {
    CELLS_READ.load(std::sync::atomic::Ordering::Relaxed)
}

impl IndexedDataClient {
    pub(super) async fn scan_schema_ids(
        &self,
        schema: SchemaUid,
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
        schema: SchemaUid,
        selection: &Expr,
        limit: usize,
    ) -> Result<Vec<Id>, RPCError> {
        Ok(self
            .stream_schema_scan_page(schema, selection, limit, None)
            .await?
            .ids)
    }

    /// One resumable page of the filtered schema scan.
    ///
    /// The scan walks the schema's index entries in ascending key order. A
    /// schema's entries share their leading bytes and differ only in the
    /// trailing id, so that is ascending id order, across every ranged-index
    /// tree (partition) the schema spans: the client cursor chains the trees
    /// in key order.
    ///
    /// `start_after` is a position, not a row count: the scan resumes at the
    /// first entry whose id is STRICTLY greater than it. The cursor id need
    /// not exist -- a cell deleted since the previous page leaves the
    /// position intact and the scan continues at the next surviving id, so
    /// resuming never skips or repeats a live cell on the account of one
    /// that is gone. Cells inserted behind the position after it was taken
    /// are not seen; cells inserted ahead of it are.
    ///
    /// The returned `next_cursor` is the id of the last match when the page
    /// stopped because `limit` was reached (more may follow), and `None` when
    /// the scan ran to the end of the schema.
    pub(super) async fn stream_schema_scan_page(
        &self,
        schema: SchemaUid,
        selection: &Expr,
        limit: usize,
        start_after: Option<Id>,
    ) -> Result<ScanPage, RPCError> {
        if limit == 0 {
            return Ok(ScanPage {
                ids: vec![],
                next_cursor: start_after,
            });
        }
        let key = match start_after {
            Some(after) => EntryKey::for_scannable(&after, schema),
            None => EntryKey::for_schema(schema),
        };
        let Some(mut index_cursor) = self
            .index_clients
            .range_seek(
                Range::new_inclusive_opened(key, Ordering::Forward),
                SCHEMA_SCAN_BUFFER_SIZE,
                Some(SCHEMA_SCAN_PATT_SIZE),
            )
            .await?
        else {
            return Ok(ScanPage::default());
        };
        let batch = usize::from(SCAN_BUFFER_SIZE.max(1));
        let mut selected: Vec<Id> = Vec::with_capacity(limit.min(batch * 16));
        let mut pending: Vec<Id> = Vec::with_capacity(batch);
        // The seek is inclusive, so the entry AT the cursor is the first one
        // returned when it still exists; it is the one to drop.
        let mut skip_cursor = start_after;
        loop {
            pending.clear();
            while pending.len() < batch {
                match index_cursor.next().await? {
                    Some(id) => {
                        if skip_cursor.take() == Some(id) {
                            continue;
                        }
                        pending.push(id);
                    }
                    None => break,
                }
            }
            if pending.is_empty() {
                break;
            }
            let cells = self
                .read_cells_from_ids_checked(
                    &pending,
                    &vec![],
                    &Expr::nothing(),
                    &Expr::nothing(),
                    &vec![crate::index::verification::IndexCheck::Schema(schema)],
                )
                .await;
            for cell in cells {
                if cell_matches_selection(&cell, selection) {
                    selected.push(cell.id());
                    if selected.len() >= limit {
                        let next_cursor = selected.last().copied();
                        return Ok(ScanPage {
                            ids: selected,
                            next_cursor,
                        });
                    }
                }
            }
        }
        Ok(ScanPage {
            ids: selected,
            next_cursor: None,
        })
    }

    pub(super) async fn filter_ids_by_selection_limit(
        &self,
        candidate_ids: &[Id],
        schema: SchemaUid,
        selection: &Expr,
        limit: Option<usize>,
    ) -> Vec<Id> {
        let Some(limit) = limit else {
            let cells = self
                .read_cells_from_ids_checked(
                    candidate_ids,
                    &vec![],
                    &Expr::nothing(),
                    &Expr::nothing(),
                    &vec![crate::index::verification::IndexCheck::Schema(schema)],
                )
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
                .read_cells_from_ids_checked(
                    chunk,
                    &vec![],
                    &Expr::nothing(),
                    &Expr::nothing(),
                    &vec![crate::index::verification::IndexCheck::Schema(schema)],
                )
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

    pub(super) async fn existing_ids(&self, ids: &[Id]) -> Result<Vec<Id>, RPCError> {
        CellReader::new(self.conshash.clone(), &self.group_name, &self.database_name)
            .existing_ids(ids)
            .await
    }

    pub(super) async fn read_cells_from_ids(
        &self,
        ids: &[Id],
        projection: &Vec<u64>,
        selection: &Expr,
        proc: &Expr,
    ) -> Vec<OwnedCell> {
        self.read_cells_from_ids_checked(ids, projection, selection, proc, &vec![])
            .await
    }

    pub(super) async fn read_cells_from_ids_checked(
        &self,
        ids: &[Id],
        projection: &Vec<u64>,
        selection: &Expr,
        proc: &Expr,
        checks: &Vec<crate::index::verification::IndexCheck>,
    ) -> Vec<OwnedCell> {
        CellReader::new(self.conshash.clone(), &self.group_name, &self.database_name)
            .read_cells_from_ids_checked(ids, projection, selection, proc, checks)
            .await
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
            .filter_map(|(i, id)| {
                self.conshash
                    .get_server_id_for_slot(crate::slots::slot_of(id) as u64)
                    .map(|sid| (i, sid, *id))
            })
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
                let server_name = self.conshash.try_server_name(sid);
                async move {
                    let Some(server_name) = server_name else {
                        // See above: an unroutable member fails these ids rather
                        // than taking the process down.
                        return Err(RPCError::IOError(std::io::Error::new(
                            std::io::ErrorKind::NotFound,
                            format!("no address known for server id {sid}"),
                        )));
                    };
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

#[derive(Clone)]
pub(crate) struct CellReader {
    conshash: std::sync::Arc<bifrost::conshash::ConsistentHashing>,
    group_name: String,
    database_name: String,
}

impl CellReader {
    pub(crate) fn new(
        conshash: std::sync::Arc<bifrost::conshash::ConsistentHashing>,
        group_name: &str,
        database_name: &str,
    ) -> Self {
        Self {
            conshash,
            group_name: group_name.into(),
            database_name: database_name.into(),
        }
    }
    pub(crate) async fn existing_ids(&self, ids: &[Id]) -> Result<Vec<Id>, RPCError> {
        let mut groups = std::collections::BTreeMap::<u64, Vec<(usize, Id)>>::new();
        for (index, id) in ids.iter().enumerate() {
            let sid = self
                .conshash
                .get_server_id_for_slot(crate::slots::slot_of(id) as u64)
                .ok_or_else(|| {
                    RPCError::IOError(std::io::Error::new(
                        std::io::ErrorKind::NotFound,
                        "no owner for index candidate",
                    ))
                })?;
            groups.entry(sid).or_default().push((index, *id));
        }
        let mut tasks = groups
            .into_iter()
            .map(|(sid, entries)| async move {
                let name = self.conshash.try_server_name(sid).ok_or_else(|| {
                    RPCError::IOError(std::io::Error::new(
                        std::io::ErrorKind::NotFound,
                        "no owner address",
                    ))
                })?;
                let client = client_by_server_name_for_database(
                    sid,
                    name,
                    &self.group_name,
                    &self.database_name,
                )
                .await?;
                let keys = entries.iter().map(|(_, id)| *id).collect();
                let present = client.existing_ids(&keys).await?;
                if present.len() != entries.len() {
                    return Err(RPCError::IOError(std::io::Error::new(
                        std::io::ErrorKind::InvalidData,
                        "existence response length mismatch",
                    )));
                }
                let mut result = Vec::new();
                for ((index, id), present) in entries.into_iter().zip(present) {
                    match present {
                        Ok(true) => result.push((index, id)),
                        Ok(false) => {}
                        Err(error) => {
                            return Err(RPCError::IOError(std::io::Error::other(format!(
                                "existence check: {error:?}"
                            ))))
                        }
                    }
                }
                Ok::<_, RPCError>(result)
            })
            .collect::<FuturesUnordered<_>>();
        let mut result = Vec::new();
        while let Some(batch) = tasks.next().await {
            result.extend(batch?);
        }
        result.sort_unstable_by_key(|(index, _)| *index);
        Ok(result.into_iter().map(|(_, id)| id).collect())
    }

    pub(crate) async fn read_cells_from_ids_checked(
        &self,
        ids: &[Id],
        projection: &Vec<u64>,
        selection: &Expr,
        proc: &Expr,
        checks: &Vec<crate::index::verification::IndexCheck>,
    ) -> Vec<OwnedCell> {
        CELLS_READ.fetch_add(ids.len(), std::sync::atomic::Ordering::Relaxed);
        let normalized_selection = normalize_selection_for_eval(selection);
        let preserve_order = normalized_selection.is_empty() && proc.is_empty();
        let expect_full_batch = preserve_order && checks.is_empty();
        let mut all_cells = vec![];
        let mut ordered_cells = if preserve_order {
            Some(vec![None; ids.len()])
        } else {
            None
        };
        let mut tasks = ids
            .iter()
            .enumerate()
            .filter_map(|(i, id)| {
                self.conshash
                    .get_server_id_for_slot(crate::slots::slot_of(id) as u64)
                    .map(|sid| (i, sid, *id))
            })
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
                let checks = checks.clone();
                let projection = projection.clone();
                let selection = normalized_selection.clone();
                let proc = proc.clone();
                let group_name = self.group_name.clone();
                let database_name = self.database_name.clone();
                let server_name = self.conshash.try_server_name(sid);
                async move {
                    let Some(server_name) = server_name else {
                        // See above: an unroutable member fails these ids rather
                        // than taking the process down.
                        return Err(RPCError::IOError(std::io::Error::new(
                            std::io::ErrorKind::NotFound,
                            format!("no address known for server id {sid}"),
                        )));
                    };
                    match client_by_server_name_for_database(
                        sid,
                        server_name,
                        &group_name,
                        &database_name,
                    )
                    .await
                    {
                        Ok(client) => {
                            let read_res = if checks.is_empty() {
                                client
                                    .read_all_cells_proced(
                                        &grouped_ids,
                                        &projection,
                                        &selection,
                                        &proc,
                                    )
                                    .await
                            } else {
                                client
                                    .read_all_cells_verified(
                                        &grouped_ids,
                                        &projection,
                                        &selection,
                                        &proc,
                                        &checks,
                                    )
                                    .await
                            }
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
                if slot.is_some() || !expect_full_batch {
                    continue;
                }
                let Some(sid) = self.conshash.get_server_id_for_slot(crate::slots::slot_of(&ids[idx]) as u64) else {
                    warn!("Missing server mapping for id {:?}", ids[idx]);
                    continue;
                };
                // Unknown member: it has left the ring, or a stale placement table
                // still names it. Skipped exactly like a missing mapping above --
                // an unroutable id is not grounds for panicking the process.
                let Some(server_name) = self.conshash.try_server_name(sid) else {
                    warn!("No address known for server {} holding {:?}", sid, ids[idx]);
                    continue;
                };
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
                        match if checks.is_empty() {
                            client
                                .read_all_cells_proced(&single_id, projection, selection, proc)
                                .await
                        } else {
                            client
                                .read_all_cells_verified(
                                    &single_id, projection, selection, proc, checks,
                                )
                                .await
                        } {
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
}

pub(super) fn cell_matches_selection(cell: &OwnedCell, selection: &Expr) -> bool {
    selection_matches_fields(selection, &|field| &cell[field])
}

fn selection_matches_fields<'a>(selection: &Expr, field: &impl Fn(u64) -> &'a OwnedValue) -> bool {
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
            .all(|child| selection_matches_fields(child, field));
    }

    if is_symbol_named(&items[0], "or") {
        return items
            .iter()
            .skip(1)
            .any(|child| selection_matches_fields(child, field));
    }

    if is_symbol_named(&items[0], "not") {
        return items
            .get(1)
            .map(|child| !selection_matches_fields(child, field))
            .unwrap_or(false);
    }

    if is_symbol_named(&items[0], "is-null") {
        return items
            .get(1)
            .and_then(expr_field_id)
            .map(|field_id| matches!(field(field_id), OwnedValue::Null | OwnedValue::NA))
            .unwrap_or(false);
    }

    if is_symbol_named(&items[0], "is-not-null") {
        return items
            .get(1)
            .and_then(expr_field_id)
            .map(|field_id| !matches!(field(field_id), OwnedValue::Null | OwnedValue::NA))
            .unwrap_or(false);
    }

    if is_symbol_named(&items[0], "in") {
        let Some(field_id) = items.get(1).and_then(expr_field_id) else {
            return false;
        };
        let field_value = field(field_id);
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
        let field_value = field(field_id);
        return compare_values(field_value, lower, CompareOp::Ge)
            && compare_values(field_value, upper, CompareOp::Le);
    }

    if let Some((op, field_id, value)) = comparison_clause(selection) {
        return compare_values(field(field_id), &value, op);
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

/// A scalar selection whose field accesses and operators are supported locally.
/// Construction validates once. Evaluation uses the same comparisons as Neb reads.
#[derive(Clone, Debug)]
pub struct ScalarSelection {
    selection: Expr,
    fields: Vec<u64>,
}

#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub struct UnsupportedScalarSelection;

impl std::fmt::Display for UnsupportedScalarSelection {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        f.write_str("selection requires the query evaluator")
    }
}
impl std::error::Error for UnsupportedScalarSelection {}

impl ScalarSelection {
    pub fn new(selection: &Expr) -> Result<Self, UnsupportedScalarSelection> {
        let mut fields = Vec::new();
        scalar_fields(selection, &mut fields)?;
        fields.sort_unstable();
        fields.dedup();
        Ok(Self {
            selection: selection.clone(),
            fields,
        })
    }

    pub fn fields(&self) -> &[u64] {
        &self.fields
    }

    pub fn matches_cell(&self, cell: &OwnedCell) -> bool {
        cell_matches_selection(cell, &self.selection)
    }

    /// The accessor maps field ids to selected positions or full map fields.
    pub fn matches_fields<'a>(&self, field: impl Fn(u64) -> &'a OwnedValue) -> bool {
        #[cfg(test)]
        if std::env::var_os("NEB_TEST_IDCONTRACT_SKIP_SCALAR").is_some() {
            return true;
        }
        selection_matches_fields(&self.selection, &field)
    }
}

fn scalar_fields(
    selection: &Expr,
    fields: &mut Vec<u64>,
) -> Result<(), UnsupportedScalarSelection> {
    let Expr::List(items) = selection else {
        return Err(UnsupportedScalarSelection);
    };
    if items.is_empty() {
        return Ok(());
    }
    if is_symbol_named(&items[0], "and") || is_symbol_named(&items[0], "or") {
        for child in &items[1..] {
            scalar_fields(child, fields)?;
        }
        return Ok(());
    }
    if is_symbol_named(&items[0], "not") && items.len() == 2 {
        return scalar_fields(&items[1], fields);
    }
    let unary = is_symbol_named(&items[0], "is-null") || is_symbol_named(&items[0], "is-not-null");
    let membership = is_symbol_named(&items[0], "in") && items.len() >= 2;
    let between = is_symbol_named(&items[0], "between") && items.len() == 4;
    if (unary && items.len() == 2) || membership || between {
        let field = items
            .get(1)
            .and_then(expr_field_id)
            .ok_or(UnsupportedScalarSelection)?;
        if items[2..]
            .iter()
            .any(|value| expr_owned_value(value).is_none())
        {
            return Err(UnsupportedScalarSelection);
        }
        fields.push(field);
        return Ok(());
    }
    if let Some((_, field, _)) = comparison_clause(selection) {
        fields.push(field);
        return Ok(());
    }
    Err(UnsupportedScalarSelection)
}

#[cfg(test)]
mod scalar_tests {
    use super::*;
    use crate::ram::schema::SchemaVid;
    use bifrost_hasher::hash_str;
    use dovahkiin::types::{Map, OwnedMap};

    fn selection(op: &str, values: Vec<OwnedValue>) -> Expr {
        let mut items = vec![
            Expr::Symbol(hash_str(op), op.into()),
            Expr::Symbol(9, "field".into()),
        ];
        items.extend(values.into_iter().map(Expr::Value));
        Expr::List(items)
    }

    #[test]
    fn idcontract_scalar_adapter_matches_neb_and_projection() {
        // U32 covers the stored dictionary-code representation. Strings cover decoded values.
        let values = [
            OwnedValue::U64(7),
            OwnedValue::I64(7),
            OwnedValue::F64(7.0),
            OwnedValue::U32(7),
            OwnedValue::Null,
            OwnedValue::NA,
            OwnedValue::String("seven".into()),
        ];
        let mut cases = Vec::new();
        for value in &values {
            for op in ["=", "!=", ">", ">=", "<", "<="] {
                cases.push(selection(op, vec![value.clone()]));
            }
        }
        cases.extend([
            selection("is-null", vec![]),
            selection("is-not-null", vec![]),
            selection(
                "in",
                vec![OwnedValue::U64(7), OwnedValue::String("seven".into())],
            ),
            selection("between", vec![OwnedValue::U64(6), OwnedValue::U64(8)]),
            Expr::nothing(),
        ]);
        for op in ["and", "or"] {
            cases.push(Expr::List(vec![
                Expr::Symbol(hash_str(op), op.into()),
                selection("=", vec![OwnedValue::U64(7)]),
                selection("is-null", vec![]),
            ]));
        }
        cases.push(Expr::List(vec![
            Expr::Symbol(hash_str("not"), "not".into()),
            selection("is-null", vec![]),
        ]));
        for value in values {
            let mut map = OwnedMap::new();
            map.insert_key_id(9, value.clone());
            let cell =
                OwnedCell::new_with_id(SchemaVid(1), &Id::from_parts(1, 1), OwnedValue::Map(map));
            for expr in &cases {
                let adapter = ScalarSelection::new(expr).unwrap();
                let expected = cell_matches_selection(&cell, expr);
                assert_eq!(adapter.matches_cell(&cell), expected);
                assert_eq!(
                    adapter.matches_fields(|id| {
                        assert_eq!(id, 9);
                        &value
                    }),
                    expected
                );
            }
        }
        let adapter = ScalarSelection::new(&selection("=", vec![OwnedValue::U64(7)])).unwrap();
        assert!(adapter.matches_fields(|_| &OwnedValue::U64(7)));
        assert!(!adapter.matches_fields(|_| &OwnedValue::U64(8)));
        assert_eq!(adapter.fields(), &[9]);
    }

    #[test]
    fn idcontract_scalar_adapter_rejects_unsupported() {
        for expr in [
            Expr::Value(OwnedValue::Bool(true)),
            selection("similar", vec![OwnedValue::String("text".into())]),
            selection("=", vec![]),
            selection("between", vec![OwnedValue::U64(7)]),
            Expr::List(vec![Expr::Symbol(1, "not".into())]),
        ] {
            assert_eq!(
                ScalarSelection::new(&expr).unwrap_err(),
                UnsupportedScalarSelection
            );
        }
    }
}
