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
        cell::{OwnedCell, ReadError},
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
                .await?;
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
    ) -> Result<Vec<Id>, RPCError> {
        let Some(limit) = limit else {
            let cells = self
                .read_cells_from_ids_checked(
                    candidate_ids,
                    &vec![],
                    &Expr::nothing(),
                    &Expr::nothing(),
                    &vec![crate::index::verification::IndexCheck::Schema(schema)],
                )
                .await?;
            return Ok(cells
                .into_iter()
                .filter(|cell| cell_matches_selection(cell, selection))
                .map(|cell| cell.id())
                .collect_vec());
        };
        if limit == 0 || candidate_ids.is_empty() {
            return Ok(vec![]);
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
                .await?;
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
        Ok(selected_ids)
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
    ) -> Result<Vec<OwnedCell>, RPCError> {
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
    ) -> Result<Vec<OwnedCell>, RPCError> {
        self.cell_reader()
            .read_cells_from_ids_checked(ids, projection, selection, proc, checks)
            .await
    }

    pub(super) async fn read_projected_rows_from_ids(
        &self,
        ids: &[Id],
        projection: &Vec<u64>,
    ) -> Result<Vec<Vec<OwnedValue>>, RPCError> {
        let projected_cells = self
            .read_cells_from_ids(ids, projection, &Expr::nothing(), &Expr::nothing())
            .await?;
        Ok(projected_cells
            .into_iter()
            .map(|cell| {
                projection
                    .iter()
                    .enumerate()
                    .map(|(index, _field_id)| cell[index].clone())
                    .collect_vec()
            })
            .collect_vec())
    }

    pub(super) async fn read_selected_cells_from_ids(
        &self,
        ids: &[Id],
        fields: &[u64],
    ) -> Result<Vec<OwnedCell>, RPCError> {
        self.cell_reader().read_selected_cells(ids, fields).await
    }

    pub(super) async fn read_full_cells_aligned(
        &self,
        ids: &[Id],
    ) -> Result<Vec<Option<OwnedCell>>, RPCError> {
        self.cell_reader().read_full_cells_aligned(ids).await
    }

    fn cell_reader(&self) -> CellReader {
        CellReader::new(self.conshash.clone(), &self.group_name, &self.database_name)
    }
}

/// A cell read that did not complete: the server that owns the cell did not
/// answer, or answered with a failure that is not "absent" or "does not match".
///
/// A read never answers with fewer rows because a server failed. It retries
/// the server's batch, then reads the ids one by one, and then returns this.
/// It is carried inside `RPCError::IOError`; use [`CellReadFailure::of`] to
/// find it.
#[derive(Clone, Debug, PartialEq, Eq)]
pub struct CellReadFailure {
    /// The member the read went to, or `None` when no member owns the slot.
    pub server_id: Option<u64>,
    /// The first id that could not be read.
    pub id: Id,
    /// What failed, as the transport or the server reported it.
    pub reason: String,
}

impl CellReadFailure {
    /// This failure as the error of a read API. Not a `From` impl: a second
    /// `From` for `RPCError` breaks type inference of `?` in callers.
    pub fn into_rpc_error(self) -> RPCError {
        RPCError::IOError(std::io::Error::other(self))
    }

    /// The read failure inside an error from a read API, if it is one.
    pub fn of(error: &RPCError) -> Option<&CellReadFailure> {
        match error {
            RPCError::IOError(error) => error.get_ref()?.downcast_ref(),
            _ => None,
        }
    }
}

impl std::fmt::Display for CellReadFailure {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        match self.server_id {
            Some(server) => write!(
                f,
                "cell read of {:?} failed on server {}: {}",
                self.id, server, self.reason
            ),
            None => write!(f, "cell read of {:?} failed: {}", self.id, self.reason),
        }
    }
}

impl std::error::Error for CellReadFailure {}

/// Batch sends to one server before its ids are read one by one.
const BATCH_READ_ATTEMPTS: u32 = 3;
/// Wait before the second batch send. Each later wait is 4 times longer.
const BATCH_READ_BACKOFF: std::time::Duration = std::time::Duration::from_millis(10);

/// One kind of cell read, sent to the server that owns the ids.
enum CellRead {
    Proced {
        projection: Vec<u64>,
        selection: Expr,
        proc: Expr,
    },
    Verified {
        projection: Vec<u64>,
        selection: Expr,
        proc: Expr,
        checks: Vec<crate::index::verification::IndexCheck>,
    },
    Selected {
        fields: Vec<u64>,
    },
}

impl CellRead {
    async fn send(
        &self,
        client: &crate::server::cell_rpc::AsyncServiceClient,
        ids: &Vec<Id>,
    ) -> Result<Vec<Result<OwnedCell, ReadError>>, RPCError> {
        match self {
            CellRead::Proced {
                projection,
                selection,
                proc,
            } => {
                client
                    .read_all_cells_proced(ids, projection, selection, proc)
                    .await
            }
            CellRead::Verified {
                projection,
                selection,
                proc,
                checks,
            } => {
                client
                    .read_all_cells_verified(ids, projection, selection, proc, checks)
                    .await
            }
            CellRead::Selected { fields } => {
                client.read_all_cells_selected(ids, fields, true).await
            }
        }
    }
}

/// Whether a per-cell error means "no row": the cell is absent, or the server
/// read it and it does not pass the index check or the selection
/// (`NotMatch`). These cells are dropped; that is the verified-read contract.
///
/// Every other per-cell error is a failed read of a cell that exists: an
/// unavailable schema (`SchemaDoesNotExisted`), a selection or expression
/// that could not be evaluated
/// (`ExecError`), a wrong owner or unreadable storage. Dropping such a cell
/// would be a shorter answer, so it is read again alone and then reported.
pub(crate) fn is_absent_row(error: &ReadError) -> bool {
    matches!(
        error,
        ReadError::CellDoesNotExisted | ReadError::CellIdIsUnitId | ReadError::NotMatch
    )
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
    ) -> Result<Vec<OwnedCell>, RPCError> {
        CELLS_READ.fetch_add(ids.len(), std::sync::atomic::Ordering::Relaxed);
        let selection = normalize_selection_for_eval(selection);
        let read = if checks.is_empty() {
            CellRead::Proced {
                projection: projection.clone(),
                selection,
                proc: proc.clone(),
            }
        } else {
            CellRead::Verified {
                projection: projection.clone(),
                selection,
                proc: proc.clone(),
                checks: checks.clone(),
            }
        };
        self.read_rows(ids, &read).await
    }

    pub(crate) async fn read_selected_cells(
        &self,
        ids: &[Id],
        fields: &[u64],
    ) -> Result<Vec<OwnedCell>, RPCError> {
        let read = CellRead::Selected {
            fields: fields.to_vec(),
        };
        self.read_rows(ids, &read).await
    }

    /// The rows for `ids`, in the order of `ids`, without the absent ones.
    async fn read_rows(&self, ids: &[Id], read: &CellRead) -> Result<Vec<OwnedCell>, RPCError> {
        Ok(self
            .read_aligned(ids, read)
            .await?
            .into_iter()
            .flatten()
            .collect_vec())
    }

    /// Whole cells for `ids`, one slot per id: `None` is an absent row.
    pub(crate) async fn read_full_cells_aligned(
        &self,
        ids: &[Id],
    ) -> Result<Vec<Option<OwnedCell>>, RPCError> {
        CELLS_READ.fetch_add(ids.len(), std::sync::atomic::Ordering::Relaxed);
        let read = CellRead::Proced {
            projection: vec![],
            selection: Expr::nothing(),
            proc: Expr::nothing(),
        };
        self.read_aligned(ids, &read).await
    }

    /// One slot per id, in the order of `ids`; `None` is an absent row.
    ///
    /// One batch per owning server, sent at most `BATCH_READ_ATTEMPTS` times.
    /// The ids of a batch that still fails, and the ids whose cell answered
    /// with a failure, are read one by one. A failure there is the error.
    async fn read_aligned(
        &self,
        ids: &[Id],
        read: &CellRead,
    ) -> Result<Vec<Option<OwnedCell>>, RPCError> {
        let mut rows: Vec<Option<OwnedCell>> = vec![None; ids.len()];
        let mut by_server = std::collections::BTreeMap::<u64, Vec<usize>>::new();
        let mut one_by_one = Vec::new();
        for (index, id) in ids.iter().enumerate() {
            match self.owner(id) {
                Some(server) => by_server.entry(server).or_default().push(index),
                None => one_by_one.push(index),
            }
        }
        let mut batches = by_server
            .into_iter()
            .map(|(server, indices)| async move {
                let batch = indices.iter().map(|index| ids[*index]).collect_vec();
                let answer = self.send_batch_with_retry(server, &batch, read).await;
                (server, indices, answer)
            })
            .collect::<FuturesUnordered<_>>();
        while let Some((server, indices, answer)) = batches.next().await {
            match answer {
                Ok(cells) => {
                    for (index, cell) in indices.into_iter().zip(cells) {
                        match cell {
                            Ok(cell) => rows[index] = Some(cell),
                            Err(error) if is_absent_row(&error) => {}
                            Err(error) => {
                                warn!(
                                    "cell {:?} on server {} answered {:?}; reading it alone",
                                    ids[index], server, error
                                );
                                one_by_one.push(index);
                            }
                        }
                    }
                }
                Err(error) => {
                    warn!(
                        "batch read of {} cell(s) on server {} failed {} time(s) ({:?}); \
                         reading them one by one",
                        indices.len(),
                        server,
                        BATCH_READ_ATTEMPTS,
                        error
                    );
                    one_by_one.extend(indices);
                }
            }
        }
        one_by_one.sort_unstable();
        for index in one_by_one {
            rows[index] = self.read_one(ids[index], read).await?;
        }
        Ok(rows)
    }

    fn owner(&self, id: &Id) -> Option<u64> {
        self.conshash
            .get_server_id_for_slot(crate::slots::slot_of(id) as u64)
    }

    async fn send_batch_with_retry(
        &self,
        server: u64,
        batch: &Vec<Id>,
        read: &CellRead,
    ) -> Result<Vec<Result<OwnedCell, ReadError>>, String> {
        let mut last_error = String::new();
        for attempt in 0..BATCH_READ_ATTEMPTS {
            if attempt > 0 {
                tokio::time::sleep(BATCH_READ_BACKOFF * 4u32.pow(attempt - 1)).await;
            }
            match self.send(server, batch, read, false).await {
                Ok(cells) if cells.len() == batch.len() => return Ok(cells),
                Ok(cells) => {
                    last_error = format!(
                        "server answered {} cell(s) for {} id(s)",
                        cells.len(),
                        batch.len()
                    )
                }
                Err(error) => last_error = format!("{error:?}"),
            }
        }
        Err(last_error)
    }

    /// Read one cell from its owner. `Ok(None)` is an absent row. A cell that
    /// names another owner is asked there once.
    async fn read_one(&self, id: Id, read: &CellRead) -> Result<Option<OwnedCell>, RPCError> {
        let Some(mut server) = self.owner(&id) else {
            return Err(CellReadFailure {
                server_id: None,
                id,
                reason: "no member owns the slot of this id".into(),
            }
            .into_rpc_error());
        };
        let single = vec![id];
        for redirected in [false, true] {
            let failure = |reason: String| CellReadFailure {
                server_id: Some(server),
                id,
                reason,
            };
            let mut cells = self
                .send(server, &single, read, true)
                .await
                .map_err(|error| failure(format!("{error:?}")).into_rpc_error())?;
            if cells.len() != 1 {
                return Err(
                    failure(format!("server answered {} cell(s) for 1 id", cells.len()))
                        .into_rpc_error(),
                );
            }
            match cells.pop().unwrap() {
                Ok(cell) => return Ok(Some(cell)),
                Err(error) if is_absent_row(&error) => return Ok(None),
                Err(ReadError::NotSlotOwner { owner, .. }) if !redirected && owner != server => {
                    server = owner;
                }
                Err(error) => return Err(failure(format!("{error:?}")).into_rpc_error()),
            }
        }
        unreachable!("the redirect is taken at most once")
    }

    async fn send(
        &self,
        server: u64,
        ids: &Vec<Id>,
        read: &CellRead,
        one_by_one: bool,
    ) -> Result<Vec<Result<OwnedCell, ReadError>>, RPCError> {
        #[cfg(test)]
        if let Some(error) = read_faults::take(server, one_by_one) {
            return Err(error);
        }
        #[cfg(not(test))]
        let _ = one_by_one;
        let name = self.conshash.try_server_name(server).ok_or_else(|| {
            RPCError::IOError(std::io::Error::new(
                std::io::ErrorKind::NotFound,
                format!("no address known for server id {server}"),
            ))
        })?;
        let client =
            client_by_server_name_for_database(server, name, &self.group_name, &self.database_name)
                .await?;
        #[allow(unused_mut)]
        let mut cells = read.send(&client, ids).await?;
        #[cfg(test)]
        read_faults::rewrite_cells(server, ids, &mut cells);
        Ok(cells)
    }
}

/// Injected read failures per server, for the fault tests of `CellReader`.
///
/// Keyed by server id in a Lightning map, so tests on different servers do not
/// share a lock; one server's counters are atomics.
#[cfg(test)]
pub(crate) mod read_faults {
    use bifrost::rpc::RPCError;
    use lightning::map::{Map, PtrHashMap};
    use std::sync::atomic::{AtomicU32, Ordering::SeqCst};
    use std::sync::{Arc, LazyLock};

    /// Failures for one server. A send first spends `pass`, then fails while
    /// the failure count of its kind is above 0. `u32::MAX` fails every send.
    #[derive(Debug, Default)]
    pub(crate) struct ReadFault {
        /// The answer for this cell is replaced by this error in every send.
        pub(crate) cell_error: Option<(crate::ram::types::Id, crate::ram::cell::ReadError)>,
        pub(crate) pass: AtomicU32,
        pub(crate) batch_failures: AtomicU32,
        pub(crate) single_failures: AtomicU32,
        pub(crate) batch_sends: AtomicU32,
        pub(crate) single_sends: AtomicU32,
    }

    impl ReadFault {
        pub(crate) fn batch_sends(&self) -> u32 {
            self.batch_sends.load(SeqCst)
        }

        pub(crate) fn single_sends(&self) -> u32 {
            self.single_sends.load(SeqCst)
        }
    }

    static FAULTS: LazyLock<PtrHashMap<u64, Arc<ReadFault>>> =
        LazyLock::new(|| PtrHashMap::with_capacity(16));

    /// Fail the sends to `server`, after `pass` sends of any kind succeed.
    pub(crate) fn inject(
        server: u64,
        pass: u32,
        batch_failures: u32,
        single_failures: u32,
    ) -> Arc<ReadFault> {
        let fault = Arc::new(ReadFault {
            pass: AtomicU32::new(pass),
            batch_failures: AtomicU32::new(batch_failures),
            single_failures: AtomicU32::new(single_failures),
            ..Default::default()
        });
        FAULTS.insert(server, fault.clone());
        fault
    }

    /// Answer `error` for the cell `id` in every send to `server`, as a server
    /// that read the cell and failed would.
    pub(crate) fn inject_cell_error(
        server: u64,
        id: crate::ram::types::Id,
        error: crate::ram::cell::ReadError,
    ) -> Arc<ReadFault> {
        let fault = Arc::new(ReadFault {
            cell_error: Some((id, error)),
            ..Default::default()
        });
        FAULTS.insert(server, fault.clone());
        fault
    }

    pub(super) fn rewrite_cells(
        server: u64,
        ids: &[crate::ram::types::Id],
        cells: &mut [Result<crate::ram::cell::OwnedCell, crate::ram::cell::ReadError>],
    ) {
        let Some(fault) = FAULTS.get(&server) else {
            return;
        };
        let Some((id, error)) = &fault.cell_error else {
            return;
        };
        for (cell_id, cell) in ids.iter().zip(cells.iter_mut()) {
            if cell_id == id {
                *cell = Err(error.clone());
            }
        }
    }

    pub(crate) fn clear(server: u64) {
        FAULTS.remove(&server);
    }

    pub(super) fn take(server: u64, one_by_one: bool) -> Option<RPCError> {
        let fault = FAULTS.get(&server)?;
        let remaining = if one_by_one {
            fault.single_sends.fetch_add(1, SeqCst);
            &fault.single_failures
        } else {
            fault.batch_sends.fetch_add(1, SeqCst);
            &fault.batch_failures
        };
        if fault
            .pass
            .try_update(SeqCst, SeqCst, |pass| pass.checked_sub(1))
            .is_ok()
        {
            return None;
        }
        remaining
            .try_update(SeqCst, SeqCst, |left| match left {
                0 => None,
                u32::MAX => Some(u32::MAX),
                left => Some(left - 1),
            })
            .ok()?;
        Some(RPCError::IOError(std::io::Error::new(
            std::io::ErrorKind::ConnectionReset,
            "injected read failure",
        )))
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
