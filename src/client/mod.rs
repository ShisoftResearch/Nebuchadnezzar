use bifrost::conshash::{CHError, ConsistentHashing};
use bifrost::membership::client::ObserverClient;
use bifrost::raft;
use bifrost::raft::client::{ClientError, RaftClient};
use bifrost::raft::state_machine::master::ExecError;
use bifrost::rpc::{RPCClient, RPCError, Server as RPCServer, ServiceClient, DEFAULT_CLIENT_POOL};
use dovahkiin::types::OwnedValue;
use futures::prelude::*;
use futures::stream::FuturesUnordered;
use futures::stream::StreamExt;
use itertools::Itertools;
use std::cell::Cell as StdCell;
use std::collections::HashMap;
use std::io;
use std::mem;
use std::sync::Arc;

use crate::ram::cell::{CellHeader, OwnedCell, ReadError, WriteError};
use crate::ram::schema::sm::client::SMClient as SchemaClient;
use crate::ram::schema::sm::generate_scoped_sm_id;
use crate::ram::schema::{
    DelSchemaError, EvolutionOutcome, EvolveSchemaError, NewSchemaError, RenameSchemaError, Schema,
    SchemaEdit, SchemaVid, SetAppMetaError,
};
use crate::ram::types::Id;
use crate::server::database::client::SMClient as DatabaseCatalogClient;
use crate::server::database::{
    generate_sm_id as generate_database_catalog_sm_id, CreateDatabaseError, DatabaseCatalogEntry,
    DeleteDatabaseError,
};
use crate::server::transactions::TxnId;
use crate::server::{cell_rpc as plain_server, transactions as txn_server, CONS_HASH_ID};
use crate::server::{database_meta_plane_id, shared_meta_plane_id};

use self::transaction::*;

pub static TRANSACTION_RETRIES: std::sync::atomic::AtomicU64 = std::sync::atomic::AtomicU64::new(0);

static TRANSACTION_MAX_RETRY: u32 = 1000;

pub mod embedding;
pub mod fulltext;
pub mod ranged;
#[cfg(test)]
mod tests;
pub mod transaction;
pub mod vector;

pub use embedding::{EmbeddingClient, SemanticHit};
pub use fulltext::{FullTextClient, SearchHit};
pub use ranged::{RangedClient, RangedCursor, ScanOrder};
pub use vector::{SimilarityHit, VectorClient};

#[derive(Debug)]
pub enum NebClientError {
    RaftClientError(ClientError),
    ConsistentHashtableError(CHError),
}

/// A deferred covering barrier. Captured calls acknowledge application only;
/// finish must succeed before a caller acknowledges durability.
pub struct CapturedWriteBatch {
    clients: parking_lot::Mutex<Vec<(Arc<plain_server::AsyncServiceClient>, Vec<Id>)>>,
    closed: std::sync::atomic::AtomicBool,
}
impl CapturedWriteBatch {
    pub fn new() -> Self {
        Self {
            clients: parking_lot::Mutex::new(Vec::new()),
            closed: std::sync::atomic::AtomicBool::new(false),
        }
    }
    fn record_call<T>(
        &self,
        client: Arc<plain_server::AsyncServiceClient>,
        response: Result<(Result<T, WriteError>, Vec<Id>), RPCError>,
    ) -> Result<Result<T, WriteError>, RPCError> {
        let (result, tokens) = response?;
        if !tokens.is_empty() {
            let mut clients = self.clients.lock();
            if let Some((_, old)) = clients.iter_mut().find(|(old, _)| {
                let same = old.service_id == client.service_id
                    && old.client.server_id == client.client.server_id;
                #[cfg(test)]
                let same = if std::env::var_os("NEB_TEST_REVERT_CAPTURE_TARGET_GROUP").is_some() {
                    Arc::ptr_eq(old, &client)
                } else {
                    same
                };
                same
            }) {
                old.extend(tokens);
            } else {
                clients.push((client, tokens));
            }
        }
        Ok(result)
    }
    pub async fn finish(&self) -> Result<Result<(), WriteError>, RPCError> {
        if self.closed.swap(true, std::sync::atomic::Ordering::AcqRel) {
            return Ok(Err(WriteError::Durability(
                crate::ram::durability::DurabilityError::Rpc {
                    message: "captured batch already completed".into(),
                },
            )));
        }
        let clients = std::mem::take(&mut *self.clients.lock());
        let results =
            futures::future::join_all(clients.into_iter().map(|(client, tokens)| async move {
                client.finish_captured_receipts(tokens, true).await
            }))
            .await;
        let mut error = None;
        for result in results {
            match result {
                Err(error) => return Err(error),
                Ok(Err(failure)) => {
                    error.get_or_insert(failure);
                }
                Ok(Ok(())) => {}
            }
        }
        Ok(error.map_or(Ok(()), Err))
    }
}
impl Drop for CapturedWriteBatch {
    fn drop(&mut self) {
        let clients = std::mem::take(self.clients.get_mut());
        if clients.is_empty() {
            return;
        }
        if let Ok(runtime) = tokio::runtime::Handle::try_current() {
            runtime.spawn(async move {
                for (client, tokens) in clients {
                    let _ = client.finish_captured_receipts(tokens, false).await;
                }
            });
        }
        // Lost replies and unavailable peers expire their server receipts.
    }
}

pub struct AsyncClient {
    pub(crate) write_back_hub: std::sync::OnceLock<Arc<crate::index::ranged::tree::btree::storage::WriteBackHub>>,
    pub conshash: Arc<ConsistentHashing>,
    pub raft_client: Arc<RaftClient>,
    pub schema_client: SchemaClient,
    pub database_catalog_client: DatabaseCatalogClient,
    pub group_name: String,
    pub database_name: String,
}

impl AsyncClient {
    pub async fn new<'a>(
        subscription_server: &Arc<RPCServer>,
        membership: &Arc<ObserverClient>,
        meta_servers: &Vec<String>,
        group: &'a str,
    ) -> Result<Self, NebClientError> {
        Self::new_for_database(subscription_server, membership, meta_servers, group, group).await
    }

    pub async fn new_for_database<'a>(
        subscription_server: &Arc<RPCServer>,
        membership: &Arc<ObserverClient>,
        meta_servers: &Vec<String>,
        group: &'a str,
        database_name: &'a str,
    ) -> Result<Self, NebClientError> {
        match RaftClient::new(meta_servers, raft::DEFAULT_SERVICE_ID).await {
            Ok(raft_client) => {
                RaftClient::prepare_subscription(subscription_server).await;
                assert!(RaftClient::can_callback().await);
                match ConsistentHashing::new_client_with_id(
                    CONS_HASH_ID,
                    group,
                    &raft_client,
                    membership,
                )
                .await
                {
                    Ok(chash) => Ok(Self {
                        write_back_hub: std::sync::OnceLock::new(),
                        schema_client: {
                            let schema_plane_client =
                                raft_client.plane(database_meta_plane_id(group, database_name));
                            SchemaClient::new(
                                generate_scoped_sm_id(group, database_name),
                                &schema_plane_client,
                            )
                        },
                        conshash: {
                            // One placement oracle per ring object. A client
                            // builds its own `ConsistentHashing`, so it must
                            // install the table too -- otherwise this client
                            // routes by the ring while the server it talks to
                            // routes by the table.
                            match crate::slots::load_owner_vec(
                                group,
                                &raft_client,
                                crate::server::SLOTS_SM_ID,
                            )
                            .await
                            {
                                Ok((owners, applied_index)) => {
                                    chash.set_slot_overrides(owners, applied_index);
                                }
                                // Route by the ring exactly as before rather
                                // than refuse to start.
                                Err(reason) => warn!(
                                    "could not load the slot placement table for group {} ({}); \
                                     routing by the ring",
                                    group, reason
                                ),
                            }
                            chash
                        },
                        raft_client: raft_client.clone(),
                        database_catalog_client: {
                            let shared_plane_client =
                                raft_client.plane(shared_meta_plane_id(group));
                            DatabaseCatalogClient::new(
                                generate_database_catalog_sm_id(group),
                                &shared_plane_client,
                            )
                        },
                        group_name: group.to_string(),
                        database_name: database_name.to_string(),
                    }),
                    Err(err) => Err(NebClientError::ConsistentHashtableError(err)),
                }
            }
            Err(err) => Err(NebClientError::RaftClientError(err)),
        }
    }

    pub fn database_name(&self) -> &str {
        &self.database_name
    }

    pub fn group_name(&self) -> &str {
        &self.group_name
    }

    pub async fn get_database(
        &self,
        name: impl Into<String>,
    ) -> Result<Option<DatabaseCatalogEntry>, ExecError> {
        let name = name.into();
        self.database_catalog_client.get_by_name(&name).await
    }

    pub async fn get_all_databases(&self) -> Result<Vec<DatabaseCatalogEntry>, ExecError> {
        self.database_catalog_client.get_all().await
    }

    pub async fn create_database(
        &self,
        name: impl Into<String>,
    ) -> Result<Result<(), CreateDatabaseError>, ExecError> {
        self.database_catalog_client
            .create_database(&DatabaseCatalogEntry { name: name.into() })
            .await
    }

    pub async fn delete_database(
        &self,
        name: impl Into<String>,
    ) -> Result<Result<(), DeleteDatabaseError>, ExecError> {
        let name = name.into();
        self.database_catalog_client.delete_database(&name).await
    }

    pub async fn ensure_database(&self) -> Result<(), ExecError> {
        match self.create_database(self.database_name.clone()).await? {
            Ok(()) | Err(CreateDatabaseError::NameExists(_)) => Ok(()),
        }
    }
    /// Which server holds this id's cell.
    ///
    /// Delegates to the ring object, which consults the stored slot table before
    /// computing an answer. That indirection is the point: placement must have
    /// exactly one oracle. A client that kept its own copy of the table would be
    /// a second one, and a write routed by the table while a read routes by the
    /// ring does not fail loudly -- it looks like the cell was never written.
    pub fn locate_server_id(&self, id: &Id) -> Result<u64, RPCError> {
        if id.is_unit_id() {
            return Ok(0);
        }
        match self
            .conshash
            .get_server_id_for_slot(crate::slots::slot_of(id) as u64)
        {
            Some(n) => Ok(n),
            None => Err(RPCError::IOError(io::Error::new(
                io::ErrorKind::NotFound,
                format!("cannot locate server for id {:?}", id),
            ))),
        }
    }

    /// Replace the placement table this client's ring routes by.
    ///
    /// Kept explicit rather than refreshed on a timer so that a stale table is
    /// always the result of a missed notification, never of a poll that has not
    /// come round yet.
    pub fn refresh_slot_owners(&self, owners: Option<Vec<u64>>, applied_index: u64) -> bool {
        self.conshash.set_slot_overrides(owners, applied_index)
    }

    /// Record one slot's owner without re-reading the table.
    ///
    /// Used by a migration on its own commit, and it deliberately does not read
    /// back. A raft *command* returns the value its own apply produced, but a
    /// *query* issued straight afterwards round-robins over the members and can
    /// be served by one that has the log entry and has not applied it yet --
    /// measured here: reading the table immediately after
    /// `complete_slot_migration` returned the OLD owner, and reading it again a
    /// moment later returned the new one. So the committer applies what it
    /// already knows, which is both authoritative and cheaper than pulling all
    /// 32768 entries.
    pub fn note_slot_owner(&self, slot: u32, owner: u64, applied_index: u64) -> bool {
        self.conshash
            .note_slot_owner(slot as u64, owner, crate::slots::SLOT_COUNT, applied_index)
    }

    #[cfg(test)]
    pub(crate) fn force_slot_owner_for_test(&self, slot: u32, owner: u64) {
        let applied_index = self
            .conshash
            .slot_override_with_index(slot as u64)
            .map(|(_, applied_index)| applied_index)
            .expect("test requires an installed owner for the slot");
        assert!(self.note_slot_owner(slot, owner, applied_index));
    }

    /// Re-read the placement table from the state machine.
    ///
    /// For a member catching up on somebody else's migration. Returns whether
    /// the table was actually replaced.
    ///
    /// A failed command keeps the current table rather than clearing it.
    /// Clearing would fall back to the ring, and the ring is precisely the
    /// answer the table exists to override -- so a transient raft hiccup would
    /// silently resume routing cells to wherever `jump_hash` happens to point.
    pub async fn reload_slot_owners(&self) -> bool {
        match crate::slots::load_owner_vec(
            &self.group_name,
            &self.raft_client,
            crate::server::SLOTS_SM_ID,
        )
        .await
        {
            Ok((owners, applied_index)) => self.refresh_slot_owners(owners, applied_index),
            Err(reason) => {
                warn!(
                    "could not reload the slot placement table for group {} ({}); \
                     keeping the table this client already has",
                    self.group_name, reason
                );
                false
            }
        }
    }

    pub fn client_by_server_id<'a>(
        &'a self,
        server_id: u64,
    ) -> impl Future<Output = Result<Arc<plain_server::AsyncServiceClient>, RPCError>> + 'a {
        client_by_server_id_for_database(
            &self.conshash,
            server_id,
            self.group_name(),
            self.database_name(),
        )
    }

    pub async fn locate_plain_server(
        &self,
        id: Id,
    ) -> Result<Arc<plain_server::AsyncServiceClient>, RPCError> {
        let server_id = self.locate_server_id(&id)?;
        if server_id == 0 {
            return Err(RPCError::IOError(io::Error::new(
                io::ErrorKind::InvalidData,
                format!("invalid server id 0 for id {:?}", id),
            )));
        }
        self.client_by_server_id(server_id).await
    }

    pub async fn read_cell(&self, id: Id) -> Result<Result<OwnedCell, ReadError>, RPCError> {
        let client = self.locate_plain_server(id).await?;
        match client.read_cell(id).await? {
            // A stale table sent this read to a member that no longer holds the
            // cell. Without following, the answer is "does not exist" -- which
            // a caller cannot tell from the truth. Same single-redirect rule as
            // the write path: a second refusal means placement is moving faster
            // than one read can follow, and looping would hang instead of
            // surfacing that.
            Err(ReadError::NotSlotOwner {
                owner,
                applied_index,
            }) => {
                let owner_client = self
                    .redirect_to_slot_owner(&id, owner, applied_index)
                    .await?;
                owner_client.read_cell(id).await
            }
            other => Ok(other),
        }
    }

    pub async fn read_cell_select(
        &self,
        id: Id,
        fields: &Vec<u64>,
        need_header: bool,
    ) -> Result<Result<OwnedCell, ReadError>, RPCError> {
        let client = self.locate_plain_server(id).await?;
        match client.read_cell_select(id, fields, need_header).await? {
            Err(ReadError::NotSlotOwner {
                owner,
                applied_index,
            }) => {
                let owner_client = self
                    .redirect_to_slot_owner(&id, owner, applied_index)
                    .await?;
                owner_client.read_cell_select(id, fields, need_header).await
            }
            other => Ok(other),
        }
    }

    pub async fn read_all_cells_selected(
        &self,
        ids: &Vec<Id>,
        fields: &Vec<u64>,
        need_header: bool,
    ) -> Result<Vec<Result<OwnedCell, ReadError>>, RPCError> {
        let mut cells_by_client = ids
            .iter()
            .dedup()
            .map(|id| (self.locate_server_id(&id).unwrap(), id))
            .sorted_by_key(|(server_id, _)| *server_id)
            .chunk_by(|(server_id, _)| *server_id)
            .into_iter()
            .map(|(server_id, ids)| (server_id, ids.map(|(_, id)| *id).collect_vec()))
            .map(|(server_id, ids)| {
                let fields = fields.clone();
                async move {
                    if server_id > 0 {
                        let client = self.client_by_server_id(server_id).await.unwrap();
                        (
                            client
                                .read_all_cells_selected(&ids, &fields, need_header)
                                .await,
                            ids,
                        )
                    } else {
                        (
                            Ok(vec![Err(ReadError::CellIdIsUnitId)]),
                            vec![Id::unit_id()],
                        )
                    }
                }
            })
            .collect::<FuturesUnordered<_>>();
        let mut id_cell_map = HashMap::new();
        while let Some((cells, ids)) = cells_by_client.next().await {
            let cells = cells?;
            for (id, cell) in ids.into_iter().zip(cells) {
                id_cell_map.insert(id, Some(cell));
            }
        }
        Ok(ids
            .iter()
            .map(|id| {
                let id_ref = id_cell_map.get_mut(id).unwrap();
                if cfg!(debug_assertions) && id_ref.is_none() {
                    let msg = format!("Cannot find {:?} for read_all_cells_selected", id);
                    error!("{}", msg);
                    panic!("{}", msg);
                }
                mem::replace(id_ref, None).unwrap()
            })
            .collect())
    }

    pub async fn read_all_cells(
        &self,
        ids: &Vec<Id>,
    ) -> Result<Vec<Result<OwnedCell, ReadError>>, RPCError> {
        let mut cells_by_client = ids
            .iter()
            .dedup()
            .map(|id| (self.locate_server_id(&id).unwrap(), id))
            .sorted_by_key(|(server_id, _)| *server_id)
            .chunk_by(|(server_id, _)| *server_id)
            .into_iter()
            .map(|(server_id, ids)| (server_id, ids.map(|(_, id)| *id).collect_vec()))
            .map(|(server_id, ids)| async move {
                if server_id > 0 {
                    let client = self.client_by_server_id(server_id).await.unwrap();
                    (client.read_all_cells(&ids).await, ids)
                } else {
                    (
                        Ok(vec![Err(ReadError::CellIdIsUnitId)]),
                        vec![Id::unit_id()],
                    )
                }
            })
            .collect::<FuturesUnordered<_>>();
        let mut id_cell_map = HashMap::new();
        while let Some((cells, ids)) = cells_by_client.next().await {
            let cells = cells?;
            for (id, cell) in ids.into_iter().zip(cells) {
                id_cell_map.insert(id, Some(cell));
            }
        }
        Ok(ids
            .iter()
            .map(|id| {
                // Use mem::replace to avoid additional cost when hash map shriking by remove
                let id_ref = id_cell_map.get_mut(id).unwrap();
                if cfg!(debug_assertions) && id_ref.is_none() {
                    let msg = format!("Cannot find {:?} for read_all_cells", id);
                    error!("{}", msg);
                    panic!("{}", msg);
                }
                mem::replace(id_ref, None).unwrap()
            })
            .collect())
    }
    /// Follow a `NotSlotOwner` refusal to the member that really owns the slot.
    ///
    /// The refusal carries the owner, so this costs no table reload: merge what
    /// the refusing member told us, then route by the cache after that monotonic
    /// merge. The second step matters when the reply itself is stale; its owner
    /// is rejected, and this write must use the newer owner already cached.
    ///
    /// One redirect only. A second refusal means placement is moving faster than
    /// a single write can follow it, and retrying in a loop would turn that into
    /// a hang instead of an error the caller can see.
    pub(crate) async fn redirect_to_slot_owner(
        &self,
        id: &Id,
        owner: u64,
        applied_index: u64,
    ) -> Result<Arc<plain_server::AsyncServiceClient>, RPCError> {
        self.note_slot_owner(crate::slots::slot_of(id), owner, applied_index);
        let current_owner = self.locate_server_id(id)?;
        self.client_by_server_id(current_owner).await
    }

    /// Contract: a cell takes plain (non-transactional) writes or transactions,
    /// not both. If a caller mixes them on one cell, Neb does not guarantee that
    /// an acknowledged plain write or delete survives the resolution of an
    /// in-doubt transaction: after a restart with a transaction T in doubt, the
    /// plain write and T's write can get the same version, and only one is kept;
    /// an acknowledged delete is undone when the resolution installs T's cell.
    pub async fn write_cell(
        &self,
        cell: OwnedCell,
    ) -> Result<Result<CellHeader, WriteError>, RPCError> {
        // Local-shortcut RPC futures can complete without ever returning
        // Pending, so a hot caller loop never yields to the runtime: it
        // starves timers and peer tasks, and JoinHandle::abort can never
        // land (the task must return from poll to be cancelled). Consuming
        // coop budget bounds such loops to tokio's task budget. The yield
        // sits AFTER the call so that on the all-local path a cancellation
        // can only land once the write's side effects are complete -- never
        // between the caller reserving work and the cell landing.
        let res = async {
            let client = self.locate_plain_server(cell.id()).await?;
            match client.write_cell(cell.clone()).await? {
                Err(WriteError::SchemaDoesNotExisted(schema_id)) => {
                    self.refresh_owner_schema_cache_for_retry(&client, schema_id)
                        .await?;
                    client.write_cell(cell).await
                }
                Err(WriteError::NotSlotOwner {
                    owner,
                    applied_index,
                }) => {
                    let owner_client = self
                        .redirect_to_slot_owner(&cell.id(), owner, applied_index)
                        .await?;
                    owner_client.write_cell(cell).await
                }
                other => Ok(other),
            }
        }
        .await;
        tokio::task::consume_budget().await;
        res
    }
    /// Contract: a cell takes plain (non-transactional) writes or transactions,
    /// not both. If a caller mixes them on one cell, Neb does not guarantee that
    /// an acknowledged plain write or delete survives the resolution of an
    /// in-doubt transaction: after a restart with a transaction T in doubt, the
    /// plain write and T's write can get the same version, and only one is kept;
    /// an acknowledged delete is undone when the resolution installs T's cell.
    pub async fn update_cell(
        &self,
        cell: OwnedCell,
    ) -> Result<Result<CellHeader, WriteError>, RPCError> {
        // See write_cell: keep always-ready shortcut calls cooperative,
        // yielding only after the call's side effects are complete.
        let res = async {
            let client = self.locate_plain_server(cell.id()).await?;
            match client.update_cell(cell.clone()).await? {
                Err(WriteError::SchemaDoesNotExisted(schema_id)) => {
                    self.refresh_owner_schema_cache_for_retry(&client, schema_id)
                        .await?;
                    client.update_cell(cell).await
                }
                Err(WriteError::NotSlotOwner {
                    owner,
                    applied_index,
                }) => {
                    let owner_client = self
                        .redirect_to_slot_owner(&cell.id(), owner, applied_index)
                        .await?;
                    owner_client.update_cell(cell).await
                }
                other => Ok(other),
            }
        }
        .await;
        tokio::task::consume_budget().await;
        res
    }
    /// Contract: a cell takes plain (non-transactional) writes or transactions,
    /// not both. If a caller mixes them on one cell, Neb does not guarantee that
    /// an acknowledged plain write or delete survives the resolution of an
    /// in-doubt transaction: after a restart with a transaction T in doubt, the
    /// plain write and T's write can get the same version, and only one is kept;
    /// an acknowledged delete is undone when the resolution installs T's cell.
    pub async fn upsert_cell(
        &self,
        cell: OwnedCell,
    ) -> Result<Result<CellHeader, WriteError>, RPCError> {
        // See write_cell: keep always-ready shortcut calls cooperative,
        // yielding only after the call's side effects are complete.
        let res = async {
            let client = self.locate_plain_server(cell.id()).await?;
            // Clone only for the rare schema-miss retry path, not on every call.
            match client.upsert_cell(cell.clone()).await? {
                Err(WriteError::SchemaDoesNotExisted(schema_id)) => {
                    self.refresh_owner_schema_cache_for_retry(&client, schema_id)
                        .await?;
                    client.upsert_cell(cell).await
                }
                Err(WriteError::NotSlotOwner {
                    owner,
                    applied_index,
                }) => {
                    let owner_client = self
                        .redirect_to_slot_owner(&cell.id(), owner, applied_index)
                        .await?;
                    owner_client.upsert_cell(cell).await
                }
                other => Ok(other),
            }
        }
        .await;
        tokio::task::consume_budget().await;
        res
    }

    /// Batch upsert: groups cells by owning server and issues one RPC per
    /// server, amortizing per-cell location lookup and RPC round-trips.
    /// Results are returned in the same order as `cells`. Used by B-tree
    /// write-back to drain many dirty pages at once.
    ///
    /// Contract: a cell takes plain (non-transactional) writes or transactions,
    /// not both. If a caller mixes them on one cell, Neb does not guarantee that
    /// an acknowledged plain write or delete survives the resolution of an
    /// in-doubt transaction: after a restart with a transaction T in doubt, the
    /// plain write and T's write can get the same version, and only one is kept;
    /// an acknowledged delete is undone when the resolution installs T's cell.
    pub async fn upsert_all_cells(
        &self,
        cells: Vec<OwnedCell>,
    ) -> Result<Vec<Result<CellHeader, WriteError>>, RPCError> {
        // Preserve input order: tag each cell with its index, group by server.
        let mut by_server: HashMap<u64, Vec<(usize, OwnedCell)>> = HashMap::new();
        for (i, cell) in cells.into_iter().enumerate() {
            let server_id = self.locate_server_id(&cell.id())?;
            by_server.entry(server_id).or_default().push((i, cell));
        }
        let total: usize = by_server.values().map(|v| v.len()).sum();
        let mut batches = by_server
            .into_iter()
            .map(|(server_id, tagged)| async move {
                let (indices, batch): (Vec<usize>, Vec<OwnedCell>) = tagged.into_iter().unzip();
                let client = self.client_by_server_id(server_id).await?;
                let results = client.upsert_all_cells(batch).await?;
                Ok::<_, RPCError>((indices, results))
            })
            .collect::<FuturesUnordered<_>>();
        let mut out: Vec<Option<Result<CellHeader, WriteError>>> =
            (0..total).map(|_| None).collect();
        while let Some(res) = batches.next().await {
            let (indices, results) = res?;
            for (idx, r) in indices.into_iter().zip(results) {
                out[idx] = Some(r);
            }
        }
        Ok(out.into_iter().map(|r| r.unwrap()).collect())
    }
    /// Contract: a cell takes plain (non-transactional) writes or transactions,
    /// not both. If a caller mixes them on one cell, Neb does not guarantee that
    /// an acknowledged plain write or delete survives the resolution of an
    /// in-doubt transaction: after a restart with a transaction T in doubt, the
    /// plain write and T's write can get the same version, and only one is kept;
    /// an acknowledged delete is undone when the resolution installs T's cell.
    pub async fn remove_cell(&self, id: Id) -> Result<Result<(), WriteError>, RPCError> {
        let client = self.locate_plain_server(id).await?;
        match client.remove_cell(id).await? {
            Err(WriteError::SchemaDoesNotExisted(schema_id)) => {
                self.refresh_owner_schema_cache_for_retry(&client, schema_id)
                    .await?;
                client.remove_cell(id).await
            }
            Err(WriteError::NotSlotOwner {
                owner,
                applied_index,
            }) => {
                let owner_client = self
                    .redirect_to_slot_owner(&id, owner, applied_index)
                    .await?;
                owner_client.remove_cell(id).await
            }
            other => Ok(other),
        }
    }
    /// Headers (no bodies) for many cells, in the order of `ids`, each from the
    /// server that owns it.
    pub async fn head_all_cells(
        &self,
        ids: &[Id],
    ) -> Result<Vec<Result<CellHeader, ReadError>>, RPCError> {
        let mut by_server: HashMap<u64, Vec<(usize, Id)>> = HashMap::new();
        for (index, id) in ids.iter().enumerate() {
            let server_id = self.locate_server_id(id)?;
            by_server.entry(server_id).or_default().push((index, *id));
        }
        let mut out: Vec<Option<Result<CellHeader, ReadError>>> =
            (0..ids.len()).map(|_| None).collect();
        let mut batches = by_server
            .into_iter()
            .map(|(server_id, tagged)| async move {
                let (indices, batch): (Vec<usize>, Vec<Id>) = tagged.into_iter().unzip();
                let client = self.client_by_server_id(server_id).await?;
                let heads = client.head_all_cells(&batch).await?;
                Ok::<_, RPCError>((indices, heads))
            })
            .collect::<FuturesUnordered<_>>();
        while let Some(batch) = batches.next().await {
            let (indices, heads) = batch?;
            for (index, head) in indices.into_iter().zip(heads) {
                out[index] = Some(head);
            }
        }
        Ok(out
            .into_iter()
            .map(|head| head.expect("every id was routed"))
            .collect())
    }

    /// Fence writes to `families` on every member and wait for in-flight writes
    /// to drain. The fence is leased: a holder that dies stops renewing and the
    /// members lift it on their own. Hold the returned [`ClusterFence`] for as
    /// long as the copy runs and call [`ClusterFence::release`] when done.
    pub async fn engage_write_fence(
        &self,
        families: &[u32],
        drain: std::time::Duration,
        lease: std::time::Duration,
    ) -> Result<ClusterFence, String> {
        let (members, _) = self
            .conshash
            .membership()
            .all_members(true)
            .await
            .map_err(|e| format!("cannot list members: {e:?}"))?;
        let families = families.to_vec();
        let lease_ms = lease.as_millis() as u64;
        let mut holds: Vec<(Arc<plain_server::AsyncServiceClient>, u64)> = Vec::new();
        for member in members {
            let rpc = self
                .client_by_server_id(member.id)
                .await
                .map_err(|e| format!("cannot reach member {}: {e:?}", member.id))?;
            let engaged = rpc
                .fence_engage(&families, drain.as_millis() as u64, lease_ms)
                .await
                .map_err(|e| format!("{e:?}"))
                .and_then(|answer| answer);
            match engaged {
                Ok(token) => holds.push((rpc, token)),
                Err(reason) => {
                    for (rpc, token) in holds {
                        let _ = rpc.fence_release(token).await;
                    }
                    return Err(format!("member {} refused the fence: {reason}", member.id));
                }
            }
        }
        Ok(ClusterFence::start(holds, lease_ms))
    }

    /// The ids of every member. A membership read that fails is an RPC error,
    /// not a panic in the caller.
    async fn all_member_ids(&self) -> Result<Vec<u64>, RPCError> {
        let (members, _) = self
            .conshash
            .membership()
            .all_members(true)
            .await
            .map_err(|error| {
                RPCError::IOError(std::io::Error::other(format!(
                    "membership query failed: {error:?}"
                )))
            })?;
        Ok(members.into_iter().map(|member| member.id).collect())
    }

    /// Cells the cell indexes of every member hold live, summed.
    pub async fn live_cell_count(&self) -> Result<u64, RPCError> {
        let mut total = 0;
        for member in self.all_member_ids().await? {
            total += self
                .client_by_server_id(member)
                .await?
                .live_cell_count()
                .await?;
        }
        Ok(total)
    }

    /// Begin a bounded-memory walk over every live cell id in the database,
    /// server by server and chunk by chunk, straight off each chunk's primary
    /// `cell_index`.
    ///
    /// The walk only yields ids the visited server OWNS. After a migration the
    /// donor keeps a deferred copy of a cell until it is reclaimed, and
    /// reporting both copies would visit one cell twice. Concurrent writes are
    /// not frozen by this call: a caller that needs a stable set holds the
    /// database's write fence for the walk.
    pub async fn cell_id_scan(&self) -> Result<CellIdScan<'_>, RPCError> {
        let mut servers = self.all_member_ids().await?;
        servers.sort_unstable();
        Ok(CellIdScan {
            client: self,
            servers,
            next_server: 0,
            current: None,
            not_owned: 0,
        })
    }

    pub async fn count(&self) -> Result<u64, RPCError> {
        let mut member_futs: FuturesUnordered<_> = self
            .all_member_ids()
            .await?
            .into_iter()
            .map(|member| async move {
                let client = self.client_by_server_id(member).await?;
                Ok(client.count().await?)
            })
            .collect();
        let mut sum = 0;
        while let Some(res) = member_futs.next().await {
            sum += res?;
        }
        Ok(sum)
    }
    /// Re-derive every node's index entries and report (or fill) the holes, for
    /// both secondary indexes. The maintenance command for an index that has lost
    /// entries their cells can still supply.
    ///
    /// Fans out to EVERY member and sums, because a ranged tree covers a key
    /// range across the whole cluster while each node can only walk its own
    /// chunks, and because a bucket cell sits on whichever member owns its slot
    /// while the ids it lists can live anywhere. A single node's pass is sound but
    /// partial: it can prove an entry is missing, never that the index is complete.
    /// Only the union rebuilds an index in full.
    ///
    /// `repair` inserts what is missing and prunes what no cell implies, so it is
    /// safe to run on a healthy store -- on which it is a no-op.
    pub async fn scrub_index(
        &self,
        repair: bool,
    ) -> Result<crate::index::scrub::ScrubReport, String> {
        let members = self
            .all_member_ids()
            .await
            .map_err(|e| format!("scrub RPC failed: {:?}", e))?;
        let mut member_futs: FuturesUnordered<_> = members
            .into_iter()
            .map(|member| async move {
                let client = self.client_by_server_id(member).await?;
                Ok::<_, RPCError>(client.scrub_index(repair).await?)
            })
            .collect();
        let mut total = crate::index::scrub::ScrubReport::default();
        while let Some(res) = member_futs.next().await {
            match res.map_err(|e| format!("scrub RPC failed: {:?}", e))? {
                Ok(report) => total.merge(&report),
                Err(reason) => return Err(reason),
            }
        }
        Ok(total)
    }

    /// Contract: a cell takes plain (non-transactional) writes or transactions,
    /// not both. If a caller mixes them on one cell, Neb does not guarantee that
    /// an acknowledged plain write or delete survives the resolution of an
    /// in-doubt transaction: after a restart with a transaction T in doubt, the
    /// plain write and T's write can get the same version, and only one is kept;
    /// an acknowledged delete is undone when the resolution installs T's cell.
    pub async fn compare_version_and_update_cell(
        &self,
        id: Id,
        version: u64,
        cell: OwnedCell,
    ) -> Result<Result<CellHeader, WriteError>, RPCError> {
        let client = self.locate_plain_server(id).await?;
        match client
            .compare_version_and_update_cell(id, version, cell.clone())
            .await?
        {
            Err(WriteError::SchemaDoesNotExisted(schema_id)) => {
                self.refresh_owner_schema_cache_for_retry(&client, schema_id)
                    .await?;
                client
                    .compare_version_and_update_cell(id, version, cell)
                    .await
            }
            other => Ok(other),
        }
    }
    /// Contract: a cell takes plain (non-transactional) writes or transactions,
    /// not both. If a caller mixes them on one cell, Neb does not guarantee that
    /// an acknowledged plain write or delete survives the resolution of an
    /// in-doubt transaction: after a restart with a transaction T in doubt, the
    /// plain write and T's write can get the same version, and only one is kept;
    /// an acknowledged delete is undone when the resolution installs T's cell.
    pub async fn compare_version_and_set_field(
        &self,
        id: Id,
        version: u64,
        field: u64,
        value: OwnedValue,
    ) -> Result<Result<CellHeader, WriteError>, RPCError> {
        let client = self.locate_plain_server(id).await?;
        match client
            .compare_version_and_set_field(id, version, field, value.clone())
            .await?
        {
            Err(WriteError::SchemaDoesNotExisted(schema_id)) => {
                self.refresh_owner_schema_cache_for_retry(&client, schema_id)
                    .await?;
                client
                    .compare_version_and_set_field(id, version, field, value)
                    .await
            }
            other => Ok(other),
        }
    }
    /// Contract: a cell takes plain (non-transactional) writes or transactions,
    /// not both. If a caller mixes them on one cell, Neb does not guarantee that
    /// an acknowledged plain write or delete survives the resolution of an
    /// in-doubt transaction: after a restart with a transaction T in doubt, the
    /// plain write and T's write can get the same version, and only one is kept;
    /// an acknowledged delete is undone when the resolution installs T's cell.
    pub async fn write_cell_durable(
        &self,
        cell: OwnedCell,
    ) -> Result<Result<CellHeader, WriteError>, RPCError> {
        // Local-shortcut RPC futures can complete without ever returning
        // Pending, so a hot caller loop never yields to the runtime: it
        // starves timers and peer tasks, and JoinHandle::abort can never
        // land (the task must return from poll to be cancelled). Consuming
        // coop budget bounds such loops to tokio's task budget. The yield
        // sits AFTER the call so that on the all-local path a cancellation
        // can only land once the write's side effects are complete -- never
        // between the caller reserving work and the cell landing.
        let res = async {
            let client = self.locate_plain_server(cell.id()).await?;
            match client.write_cell_durable(cell.clone()).await? {
                Err(WriteError::SchemaDoesNotExisted(schema_id)) => {
                    self.refresh_owner_schema_cache_for_retry(&client, schema_id)
                        .await?;
                    client.write_cell_durable(cell).await
                }
                Err(WriteError::NotSlotOwner {
                    owner,
                    applied_index,
                }) => {
                    let owner_client = self
                        .redirect_to_slot_owner(&cell.id(), owner, applied_index)
                        .await?;
                    owner_client.write_cell_durable(cell).await
                }
                other => Ok(other),
            }
        }
        .await;
        tokio::task::consume_budget().await;
        res
    }

    /// Contract: a cell takes plain (non-transactional) writes or transactions,
    /// not both. If a caller mixes them on one cell, Neb does not guarantee that
    /// an acknowledged plain write or delete survives the resolution of an
    /// in-doubt transaction: after a restart with a transaction T in doubt, the
    /// plain write and T's write can get the same version, and only one is kept;
    /// an acknowledged delete is undone when the resolution installs T's cell.
    pub async fn update_cell_durable(
        &self,
        cell: OwnedCell,
    ) -> Result<Result<CellHeader, WriteError>, RPCError> {
        // See write_cell: keep always-ready shortcut calls cooperative,
        // yielding only after the call's side effects are complete.
        let res = async {
            let client = self.locate_plain_server(cell.id()).await?;
            match client.update_cell_durable(cell.clone()).await? {
                Err(WriteError::SchemaDoesNotExisted(schema_id)) => {
                    self.refresh_owner_schema_cache_for_retry(&client, schema_id)
                        .await?;
                    client.update_cell_durable(cell).await
                }
                Err(WriteError::NotSlotOwner {
                    owner,
                    applied_index,
                }) => {
                    let owner_client = self
                        .redirect_to_slot_owner(&cell.id(), owner, applied_index)
                        .await?;
                    owner_client.update_cell_durable(cell).await
                }
                other => Ok(other),
            }
        }
        .await;
        tokio::task::consume_budget().await;
        res
    }

    /// Contract: a cell takes plain (non-transactional) writes or transactions,
    /// not both. If a caller mixes them on one cell, Neb does not guarantee that
    /// an acknowledged plain write or delete survives the resolution of an
    /// in-doubt transaction: after a restart with a transaction T in doubt, the
    /// plain write and T's write can get the same version, and only one is kept;
    /// an acknowledged delete is undone when the resolution installs T's cell.
    pub async fn upsert_cell_durable(
        &self,
        cell: OwnedCell,
    ) -> Result<Result<CellHeader, WriteError>, RPCError> {
        // See write_cell: keep always-ready shortcut calls cooperative,
        // yielding only after the call's side effects are complete.
        let res = async {
            let client = self.locate_plain_server(cell.id()).await?;
            // Clone only for the rare schema-miss retry path, not on every call.
            match client.upsert_cell_durable(cell.clone()).await? {
                Err(WriteError::SchemaDoesNotExisted(schema_id)) => {
                    self.refresh_owner_schema_cache_for_retry(&client, schema_id)
                        .await?;
                    client.upsert_cell_durable(cell).await
                }
                Err(WriteError::NotSlotOwner {
                    owner,
                    applied_index,
                }) => {
                    let owner_client = self
                        .redirect_to_slot_owner(&cell.id(), owner, applied_index)
                        .await?;
                    owner_client.upsert_cell_durable(cell).await
                }
                other => Ok(other),
            }
        }
        .await;
        tokio::task::consume_budget().await;
        res
    }

    /// Contract: a cell takes plain (non-transactional) writes or transactions,
    /// not both. If a caller mixes them on one cell, Neb does not guarantee that
    /// an acknowledged plain write or delete survives the resolution of an
    /// in-doubt transaction: after a restart with a transaction T in doubt, the
    /// plain write and T's write can get the same version, and only one is kept;
    /// an acknowledged delete is undone when the resolution installs T's cell.
    pub async fn remove_cell_durable(&self, id: Id) -> Result<Result<(), WriteError>, RPCError> {
        let client = self.locate_plain_server(id).await?;
        match client.remove_cell_durable(id).await? {
            Err(WriteError::SchemaDoesNotExisted(schema_id)) => {
                self.refresh_owner_schema_cache_for_retry(&client, schema_id)
                    .await?;
                client.remove_cell_durable(id).await
            }
            Err(WriteError::NotSlotOwner {
                owner,
                applied_index,
            }) => {
                let owner_client = self
                    .redirect_to_slot_owner(&id, owner, applied_index)
                    .await?;
                owner_client.remove_cell_durable(id).await
            }
            other => Ok(other),
        }
    }

    /// Certify current WAL prefixes on every member of this database.
    pub async fn flush_wal_durable(&self) -> Result<Result<(), WriteError>, RPCError> {
        let (members, _) = self
            .conshash
            .membership()
            .group_members(&self.group_name, false)
            .await
            .map_err(|error| {
                RPCError::IOError(std::io::Error::other(format!(
                    "membership query failed: {error:?}"
                )))
            })?
            .ok_or_else(|| RPCError::IOError(std::io::Error::other("database group is missing")))?;
        let mut waits: FuturesUnordered<_> = members
            .into_iter()
            .map(|member| async move {
                self.client_by_server_id(member.id)
                    .await?
                    .flush_wal_durable()
                    .await
            })
            .collect();
        let mut first = None;
        while let Some(result) = waits.next().await {
            if let Err(error) = result? {
                first.get_or_insert(error);
            }
        }
        Ok(first.map_or(Ok(()), Err))
    }

    /// Contract: a cell takes plain (non-transactional) writes or transactions,
    /// not both. If a caller mixes them on one cell, Neb does not guarantee that
    /// an acknowledged plain write or delete survives the resolution of an
    /// in-doubt transaction: after a restart with a transaction T in doubt, the
    /// plain write and T's write can get the same version, and only one is kept;
    /// an acknowledged delete is undone when the resolution installs T's cell.
    pub async fn write_cell_captured(
        &self,
        capture: &CapturedWriteBatch,
        cell: OwnedCell,
    ) -> Result<Result<CellHeader, WriteError>, RPCError> {
        // Local-shortcut RPC futures can complete without ever returning
        // Pending, so a hot caller loop never yields to the runtime: it
        // starves timers and peer tasks, and JoinHandle::abort can never
        // land (the task must return from poll to be cancelled). Consuming
        // coop budget bounds such loops to tokio's task budget. The yield
        // sits AFTER the call so that on the all-local path a cancellation
        // can only land once the write's side effects are complete -- never
        // between the caller reserving work and the cell landing.
        let res = async {
            let client = self.locate_plain_server(cell.id()).await?;
            match capture.record_call(
                client.clone(),
                client.write_cell_captured(cell.clone()).await,
            )? {
                Err(WriteError::SchemaDoesNotExisted(schema_id)) => {
                    self.refresh_owner_schema_cache_for_retry(&client, schema_id)
                        .await?;
                    capture.record_call(client.clone(), client.write_cell_captured(cell).await)
                }
                Err(WriteError::NotSlotOwner {
                    owner,
                    applied_index,
                }) => {
                    let owner_client = self
                        .redirect_to_slot_owner(&cell.id(), owner, applied_index)
                        .await?;
                    capture.record_call(
                        owner_client.clone(),
                        owner_client.write_cell_captured(cell).await,
                    )
                }
                other => Ok(other),
            }
        }
        .await;
        tokio::task::consume_budget().await;
        res
    }

    /// Contract: a cell takes plain (non-transactional) writes or transactions,
    /// not both. If a caller mixes them on one cell, Neb does not guarantee that
    /// an acknowledged plain write or delete survives the resolution of an
    /// in-doubt transaction: after a restart with a transaction T in doubt, the
    /// plain write and T's write can get the same version, and only one is kept;
    /// an acknowledged delete is undone when the resolution installs T's cell.
    pub async fn remove_cell_captured(
        &self,
        capture: &CapturedWriteBatch,
        id: Id,
    ) -> Result<Result<(), WriteError>, RPCError> {
        let client = self.locate_plain_server(id).await?;
        match capture.record_call(client.clone(), client.remove_cell_captured(id).await)? {
            Err(WriteError::SchemaDoesNotExisted(schema_id)) => {
                self.refresh_owner_schema_cache_for_retry(&client, schema_id)
                    .await?;
                capture.record_call(client.clone(), client.remove_cell_captured(id).await)
            }
            Err(WriteError::NotSlotOwner {
                owner,
                applied_index,
            }) => {
                let owner_client = self
                    .redirect_to_slot_owner(&id, owner, applied_index)
                    .await?;
                capture.record_call(
                    owner_client.clone(),
                    owner_client.remove_cell_captured(id).await,
                )
            }
            other => Ok(other),
        }
    }

    /// Compare-and-set one cell, and record its durability receipt in `capture`.
    ///
    /// Contract: a cell takes plain (non-transactional) writes or transactions,
    /// not both. If a caller mixes them on one cell, Neb does not guarantee that
    /// an acknowledged plain write or delete survives the resolution of an
    /// in-doubt transaction: after a restart with a transaction T in doubt, the
    /// plain write and T's write can get the same version, and only one is kept;
    /// an acknowledged delete is undone when the resolution installs T's cell.
    pub async fn compare_version_and_update_cell_captured(
        &self,
        capture: &CapturedWriteBatch,
        id: Id,
        version: u64,
        cell: OwnedCell,
    ) -> Result<Result<CellHeader, WriteError>, RPCError> {
        #[cfg(test)]
        if std::env::var_os("NEB_TEST_REVERT_CAPTURE_CAS_ROUTING").is_some() { return self.compare_version_and_update_cell_durable(id, version, cell).await; }
        let client = self.locate_plain_server(id).await?;
        match capture.record_call(
            client.clone(),
            client
                .compare_version_and_update_cell_captured(id, version, cell.clone())
                .await,
        )? {
            Err(WriteError::SchemaDoesNotExisted(schema_id)) => {
                self.refresh_owner_schema_cache_for_retry(&client, schema_id)
                    .await?;
                capture.record_call(
                    client.clone(),
                    client
                        .compare_version_and_update_cell_captured(id, version, cell)
                        .await,
                )
            }
            other => Ok(other),
        }
    }

    /// Contract: a cell takes plain (non-transactional) writes or transactions,
    /// not both. If a caller mixes them on one cell, Neb does not guarantee that
    /// an acknowledged plain write or delete survives the resolution of an
    /// in-doubt transaction: after a restart with a transaction T in doubt, the
    /// plain write and T's write can get the same version, and only one is kept;
    /// an acknowledged delete is undone when the resolution installs T's cell.
    pub async fn compare_version_and_set_field_captured(
        &self,
        capture: &CapturedWriteBatch,
        id: Id,
        version: u64,
        field: u64,
        value: OwnedValue,
    ) -> Result<Result<CellHeader, WriteError>, RPCError> {
        #[cfg(test)]
        if std::env::var_os("NEB_TEST_REVERT_CAPTURE_CAS_ROUTING").is_some() { return self.compare_version_and_set_field_durable(id, version, field, value).await; }
        let client = self.locate_plain_server(id).await?;
        match capture.record_call(
            client.clone(),
            client
                .compare_version_and_set_field_captured(id, version, field, value.clone())
                .await,
        )? {
            Err(WriteError::SchemaDoesNotExisted(schema_id)) => {
                self.refresh_owner_schema_cache_for_retry(&client, schema_id)
                    .await?;
                capture.record_call(
                    client.clone(),
                    client
                        .compare_version_and_set_field_captured(id, version, field, value)
                        .await,
                )
            }
            other => Ok(other),
        }
    }

    /// Contract: a cell takes plain (non-transactional) writes or transactions,
    /// not both. If a caller mixes them on one cell, Neb does not guarantee that
    /// an acknowledged plain write or delete survives the resolution of an
    /// in-doubt transaction: after a restart with a transaction T in doubt, the
    /// plain write and T's write can get the same version, and only one is kept;
    /// an acknowledged delete is undone when the resolution installs T's cell.
    pub async fn write_all_cells_durable(
        &self,
        cells: Vec<OwnedCell>,
    ) -> Result<Vec<Result<CellHeader, WriteError>>, RPCError> {
        // Preserve input order: tag each cell with its index, group by server.
        let mut by_server: HashMap<u64, Vec<(usize, OwnedCell)>> = HashMap::new();
        for (i, cell) in cells.into_iter().enumerate() {
            let server_id = self.locate_server_id(&cell.id())?;
            by_server.entry(server_id).or_default().push((i, cell));
        }
        let total: usize = by_server.values().map(|v| v.len()).sum();
        let mut batches = by_server
            .into_iter()
            .map(|(server_id, tagged)| async move {
                let (indices, batch): (Vec<usize>, Vec<OwnedCell>) = tagged.into_iter().unzip();
                let client = self.client_by_server_id(server_id).await?;
                let results = client.write_all_cells_durable(batch).await?;
                Ok::<_, RPCError>((indices, results))
            })
            .collect::<FuturesUnordered<_>>();
        let mut out: Vec<Option<Result<CellHeader, WriteError>>> =
            (0..total).map(|_| None).collect();
        while let Some(res) = batches.next().await {
            let (indices, results) = res?;
            for (idx, r) in indices.into_iter().zip(results) {
                out[idx] = Some(r);
            }
        }
        Ok(out.into_iter().map(|r| r.unwrap()).collect())
    }

    /// Contract: a cell takes plain (non-transactional) writes or transactions,
    /// not both. If a caller mixes them on one cell, Neb does not guarantee that
    /// an acknowledged plain write or delete survives the resolution of an
    /// in-doubt transaction: after a restart with a transaction T in doubt, the
    /// plain write and T's write can get the same version, and only one is kept;
    /// an acknowledged delete is undone when the resolution installs T's cell.
    pub async fn upsert_all_cells_durable(
        &self,
        cells: Vec<OwnedCell>,
    ) -> Result<Vec<Result<CellHeader, WriteError>>, RPCError> {
        // Preserve input order: tag each cell with its index, group by server.
        let mut by_server: HashMap<u64, Vec<(usize, OwnedCell)>> = HashMap::new();
        for (i, cell) in cells.into_iter().enumerate() {
            let server_id = self.locate_server_id(&cell.id())?;
            by_server.entry(server_id).or_default().push((i, cell));
        }
        let total: usize = by_server.values().map(|v| v.len()).sum();
        let mut batches = by_server
            .into_iter()
            .map(|(server_id, tagged)| async move {
                let (indices, batch): (Vec<usize>, Vec<OwnedCell>) = tagged.into_iter().unzip();
                let client = self.client_by_server_id(server_id).await?;
                let results = client.upsert_all_cells_durable(batch).await?;
                Ok::<_, RPCError>((indices, results))
            })
            .collect::<FuturesUnordered<_>>();
        let mut out: Vec<Option<Result<CellHeader, WriteError>>> =
            (0..total).map(|_| None).collect();
        while let Some(res) = batches.next().await {
            let (indices, results) = res?;
            for (idx, r) in indices.into_iter().zip(results) {
                out[idx] = Some(r);
            }
        }
        Ok(out.into_iter().map(|r| r.unwrap()).collect())
    }

    /// Contract: a cell takes plain (non-transactional) writes or transactions,
    /// not both. If a caller mixes them on one cell, Neb does not guarantee that
    /// an acknowledged plain write or delete survives the resolution of an
    /// in-doubt transaction: after a restart with a transaction T in doubt, the
    /// plain write and T's write can get the same version, and only one is kept;
    /// an acknowledged delete is undone when the resolution installs T's cell.
    pub async fn compare_version_and_update_cell_durable(
        &self,
        id: Id,
        version: u64,
        cell: OwnedCell,
    ) -> Result<Result<CellHeader, WriteError>, RPCError> {
        let client = self.locate_plain_server(id).await?;
        match client
            .compare_version_and_update_cell_durable(id, version, cell.clone())
            .await?
        {
            Err(WriteError::SchemaDoesNotExisted(schema_id)) => {
                self.refresh_owner_schema_cache_for_retry(&client, schema_id)
                    .await?;
                client
                    .compare_version_and_update_cell_durable(id, version, cell)
                    .await
            }
            other => Ok(other),
        }
    }

    /// Contract: a cell takes plain (non-transactional) writes or transactions,
    /// not both. If a caller mixes them on one cell, Neb does not guarantee that
    /// an acknowledged plain write or delete survives the resolution of an
    /// in-doubt transaction: after a restart with a transaction T in doubt, the
    /// plain write and T's write can get the same version, and only one is kept;
    /// an acknowledged delete is undone when the resolution installs T's cell.
    pub async fn compare_version_and_set_field_durable(
        &self,
        id: Id,
        version: u64,
        field: u64,
        value: OwnedValue,
    ) -> Result<Result<CellHeader, WriteError>, RPCError> {
        let client = self.locate_plain_server(id).await?;
        match client
            .compare_version_and_set_field_durable(id, version, field, value.clone())
            .await?
        {
            Err(WriteError::SchemaDoesNotExisted(schema_id)) => {
                self.refresh_owner_schema_cache_for_retry(&client, schema_id)
                    .await?;
                client
                    .compare_version_and_set_field_durable(id, version, field, value)
                    .await
            }
            other => Ok(other),
        }
    }
    pub async fn head_cell(&self, id: Id) -> Result<Result<CellHeader, ReadError>, RPCError> {
        let client = self.locate_plain_server(id).await?;
        client.head_cell(id).await
    }
    pub async fn transaction<'a, TFN, TR, RF>(&self, func: TFN) -> Result<TR, TxnError>
    where
        TFN: Fn(&'static Transaction) -> RF + 'a,
        RF: Future<Output = Result<TR, TxnError>> + 'a,
    {
        //unimplemented!()
        let server_name = match self.conshash.rand_server() {
            Some(name) => name,
            None => return Err(TxnError::CannotFindAServer),
        };
        let txn_client = match txn_server::new_async_client_for_database(
            &server_name,
            self.group_name(),
            self.database_name(),
        )
        .await
        {
            Ok(client) => client,
            Err(e) => return Err(TxnError::IoError(e)),
        };
        let mut retried = 0;
        let mut retry_reason_counts = HashMap::new();
        let mut last_retry_reason = None;
        let mut txn = Transaction {
            tid: TxnId::default(),
            state: StdCell::new(txn_server::TxnState::Started),
            client: txn_client,
        };
        while retried < TRANSACTION_MAX_RETRY {
            // Exponential backoff before retry (skip on first attempt)
            if retried > 0 {
                TRANSACTION_RETRIES.fetch_add(1, std::sync::atomic::Ordering::Relaxed);
                let backoff_ms = 2u64.pow(retried.min(10)) * 5; // 5ms, 10ms, 20ms, 40ms, 80ms, 160ms, 320ms, 640ms, 1280ms, 2560ms, cap at 5120ms
                debug!(
                    "Client retrying transaction after {}ms backoff (attempt {}/{})",
                    backoff_ms,
                    retried + 1,
                    TRANSACTION_MAX_RETRY
                );
                tokio::time::sleep(tokio::time::Duration::from_millis(backoff_ms)).await;
            }

            txn.state = StdCell::new(txn_server::TxnState::Started);
            txn.tid = match txn.client.begin().await {
                Ok(Ok(id)) => id,
                Ok(Err(e)) => {
                    return {
                        error!(
                            "Transaction {:?} cannot begin, manager error: {:?}",
                            txn.tid, e
                        );
                        Err(TxnError::CannotBegin)
                    };
                }
                Err(e) => {
                    error!(
                        "Transaction {:?} cannot begin, manager RPC error: {:?}",
                        txn.tid, e
                    );
                    return Err(TxnError::CannotBegin);
                }
            };
            // Erase the txn lifetime so it does not required to be carried with result
            let fn_txn_ref = unsafe { &*((&txn) as *const _) };
            let exec_result = func(fn_txn_ref).await;
            let mut exec_value = None;
            let mut txn_result = Ok(());
            match exec_result {
                Ok(val) => {
                    if txn.state.get() == txn_server::TxnState::Started {
                        txn_result = txn.prepare().await;
                        debug!("PREPARE STATE: {:?}", txn_result);
                    }
                    if txn_result.is_ok() && txn.state.get() == txn_server::TxnState::Prepared {
                        txn_result = txn.commit().await;
                        debug!("COMMIT STATE: {:?}", txn_result);
                    }
                    exec_value = Some(val);
                }
                Err(e) => txn_result = Err(e),
            }
            debug!("TXN CONCLUSION: {:?}", txn_result);
            match txn_result {
                Ok(()) => {
                    return Ok(exec_value.unwrap());
                }
                Err(TxnError::NotRealizable(reason)) => {
                    let reason_type = transaction::ReasonType::from(&reason);
                    *retry_reason_counts.entry(reason_type).or_insert(0) += 1;
                    last_retry_reason = Some(reason.clone());
                    let abort_result = txn.abort().await; // continue the loop to retry
                    debug!(
                        "TXN NOT REALIZABLE ({:?}), ABORT: {:?}",
                        reason, abort_result
                    );
                }
                Err(TxnError::OutcomeUnknown) => {
                    // `end` was sent (D33): the transaction can be committed.
                    // No abort and no retry; the caller reads the data again.
                    return Err(TxnError::OutcomeUnknown);
                }
                Err(e) => {
                    // abort will always be an error to achieve early break
                    let abort_result = txn.abort().await;
                    debug!("TXN ERROR, ABORT: {:?}", abort_result);
                    return Err(e);
                }
            }
            retried += 1;
            debug!("Client retry transaction, {:?} times", retried);
        }

        warn!(
            "Transaction exceeded maximum retry limit ({}). {}",
            TRANSACTION_MAX_RETRY,
            if let Some(ref reason) = last_retry_reason {
                format!("Last failure: {:?}", reason)
            } else {
                "No failure reasons recorded".to_string()
            }
        );

        Err(TxnError::TooManyRetry(RetryInfo {
            attempts: retried,
            reason_counts: retry_reason_counts,
            last_reason: last_retry_reason,
        }))
    }

    pub async fn schema_by_name(&self, name: &String) -> Result<Option<Schema>, ExecError> {
        self.schema_client.get_by_name(name).await
    }

    pub async fn schema_by_id(&self, id: u32) -> Result<Option<Schema>, ExecError> {
        crate::index::query_metrics::metadata();
        self.schema_client.get(&id).await
    }

    pub async fn new_schema_with_id(
        &self,
        schema: Schema,
    ) -> Result<Result<(), NewSchemaError>, ExecError> {
        if let Err(err) = schema.validate_for_registration() {
            return Ok(Err(err));
        }
        let res = self.schema_client.new_schema(&schema).await;
        let schema_id = schema.vid;
        match res {
            Ok(Ok(_)) => {
                // The record is committed here; only derived state remains.
                crate::ram::schema::schema_create_point("neb_after_raft_commit", &schema.name);
                if schema.index_fields.is_empty() && schema.compound_index_fields.is_empty() {
                    // Nothing to process
                    return res;
                }
                if let Some(server_id) = self.conshash.rand_server_id() {
                    match self.client_by_server_id(server_id).await {
                        Ok(client) => {
                            if let Err(e) = client.post_schema_add(schema_id.get()).await {
                                return Ok(Err(NewSchemaError::PostProcessError(format!(
                                    "Post process error: {:?}",
                                    e
                                ))));
                            }
                        }
                        Err(e) => {
                            return Ok(Err(NewSchemaError::PostProcessError(format!(
                                "Connecting error for post process: {:?}",
                                e
                            ))));
                        }
                    }
                } else {
                    return Ok(Err(NewSchemaError::PostProcessError(
                        "Cannot find server for post process".to_string(),
                    )));
                }
            }
            _ => {}
        }
        return res;
    }

    pub async fn new_schema(
        &self,
        mut schema: Schema,
    ) -> Result<Result<u32, NewSchemaError>, ExecError> {
        let schema_id = self.schema_client.next_id().await?;
        schema.assign_identity(schema_id);
        self.new_schema_with_id(schema)
            .await
            .map(|r| r.map(|_| schema_id))
    }
    /// Propose a complete schema as the next generation.
    ///
    /// Private on purpose: a caller who hands over a whole shape silently drops
    /// every field they forget to include, which is exactly why
    /// [`Self::evolve_schema`] takes an edit instead. This is the plumbing
    /// underneath it.
    ///
    /// The proposed record supplies field content only; its identity is
    /// assigned by the cluster, and its name is taken from the family rather
    /// than the request, so an evolution cannot smuggle in a rename.
    async fn propose_schema_generation(
        &self,
        name: String,
        schema: Schema,
        expected_base: Option<SchemaVid>,
    ) -> Result<Result<EvolutionOutcome, EvolveSchemaError>, ExecError> {
        if let Err(err) = schema.validate_for_registration() {
            return Ok(Err(EvolveSchemaError::Illegal(format!("{:?}", err))));
        }
        let res = self
            .schema_client
            .evolve_schema(&name, &schema, &expected_base)
            .await;
        if let Ok(Ok(outcome)) = &res {
            // Reconcile index namespaces against the generation this one
            // supersedes, so an index both generations declare is left alone
            // rather than recreated empty.
            if let Some(server_id) = self.conshash.rand_server_id() {
                match self.client_by_server_id(server_id).await {
                    Ok(client) => {
                        if let Err(e) = client
                            .post_schema_evolve(outcome.previous_vid.get(), outcome.new_vid.get())
                            .await
                        {
                            return Ok(Err(EvolveSchemaError::PostProcessError(format!(
                                "Post process error: {:?}",
                                e
                            ))));
                        }
                    }
                    Err(e) => {
                        return Ok(Err(EvolveSchemaError::PostProcessError(format!(
                            "Connecting error for post process: {:?}",
                            e
                        ))))
                    }
                }
            } else {
                return Ok(Err(EvolveSchemaError::PostProcessError(
                    "Cannot find server for post process".to_string(),
                )));
            }
        }
        res
    }

    /// Evolve a schema into a new generation, by describing the CHANGE.
    ///
    /// Fetches the current generation, applies the edit to it, and proposes the
    /// result -- carrying every field the edit does not mention, so a column
    /// cannot be lost by being left off a list. The generation it read goes
    /// along as a precondition, so an evolution landing in between is reported
    /// as `StaleBase` rather than silently undone.
    ///
    /// Existing cells are not rewritten. They stay readable through the
    /// generation that encoded them, and drain into the new one as they are
    /// updated.
    ///
    /// The edit still has to describe an expressible evolution; this is
    /// ergonomics, not a way around admission.
    pub async fn evolve_schema(
        &self,
        name: &str,
        edit: SchemaEdit,
    ) -> Result<Result<EvolutionOutcome, EvolveSchemaError>, ExecError> {
        self.evolve_schema_if(name, edit, None).await
    }

    /// Evolve, refusing if the family has moved on from `expected_base`.
    ///
    /// The caller supplies the generation its edit was reasoned against -- one
    /// it read earlier, perhaps in a different request -- and the state machine
    /// refuses if that is no longer current. This is the whole point of taking
    /// it as a parameter rather than reading it here: an edit applied to a base
    /// the caller never saw can quietly undo whatever moved in between, and a
    /// precondition computed inside this function would only ever describe a
    /// window this function controls.
    ///
    /// `None` keeps the simple behaviour: read the current generation, apply,
    /// and use what was read as the precondition. Still safe -- it just closes
    /// a shorter window.
    pub async fn evolve_schema_if(
        &self,
        name: &str,
        edit: SchemaEdit,
        expected_base: Option<SchemaVid>,
    ) -> Result<Result<EvolutionOutcome, EvolveSchemaError>, ExecError> {
        let Some(base) = self.schema_by_name(&name.to_owned()).await? else {
            return Ok(Err(EvolveSchemaError::SchemaDoesNotExist));
        };
        // Checked here as well as in the state machine so a caller learns its
        // base is stale before the edit is even applied -- `apply` against the
        // wrong base can fail for reasons that would only confuse.
        if let Some(expected) = expected_base {
            if base.vid != expected {
                return Ok(Err(EvolveSchemaError::StaleBase {
                    expected,
                    actual: base.vid,
                }));
            }
        }
        let evolved = match edit.apply(&base) {
            Ok(schema) => schema,
            Err(e) => return Ok(Err(EvolveSchemaError::BadEdit(format!("{:?}", e)))),
        };
        self.propose_schema_generation(name.to_owned(), evolved, Some(base.vid))
            .await
    }

    /// Rename a schema.
    ///
    /// Metadata only: no cell is rewritten and no index entry moves, because
    /// neither has ever been keyed by the name -- cells name a generation and
    /// index entries name a family, and a rename changes neither.
    pub async fn rename_schema(
        &self,
        old_name: String,
        new_name: String,
    ) -> Result<Result<(), RenameSchemaError>, ExecError> {
        self.schema_client.rename_schema(&old_name, &new_name).await
    }

    /// Replace the owner metadata stored in the family's schema record. One
    /// raft command: it is applied in full or not at all.
    pub async fn set_schema_app_meta(
        &self,
        name: &str,
        app_meta: Vec<u8>,
    ) -> Result<Result<(), SetAppMetaError>, ExecError> {
        self.schema_client
            .set_app_meta(&name.to_string(), &app_meta)
            .await
    }

    pub async fn del_schema(&self, name: String) -> Result<Result<(), DelSchemaError>, ExecError> {
        let schema = self.schema_client.get_by_name(&name).await?;
        let has_index_fields;
        let schema_id = if let Some(schema) = schema {
            has_index_fields =
                !schema.index_fields.is_empty() || !schema.compound_index_fields.is_empty();
            schema.vid
        } else {
            return Ok(Err(DelSchemaError::SchemaDoesNotExisted));
        };
        if has_index_fields {
            if let Some(server_id) = self.conshash.rand_server_id() {
                match self.client_by_server_id(server_id).await {
                    Ok(client) => {
                        if let Err(e) = client.post_schema_delete(schema_id.get()).await {
                            return Ok(Err(DelSchemaError::PostProcessError(format!(
                                "Post process error: {:?}",
                                e
                            ))));
                        }
                    }
                    Err(e) => {
                        return Ok(Err(DelSchemaError::PostProcessError(format!(
                            "Connecting error for post process: {:?}",
                            e
                        ))));
                    }
                }
            } else {
                return Ok(Err(DelSchemaError::PostProcessError(
                    "Cannot find server for post process".to_string(),
                )));
            }
        }
        // Need to do the post processing before deleting the schema from the schema client
        return self.schema_client.del_schema(&name).await;
    }
    /// Every schema, one row per FAMILY.
    ///
    /// The state machine stores one record per GENERATION, because a stale
    /// generation is still needed to decode the cells that were written under
    /// it. That is the right thing for a snapshot and the wrong thing for a
    /// listing: without this filter, one family appears N times after N
    /// evolutions, in every list route, in search, and in anything that walks
    /// "all schemas" to do something once per schema.
    ///
    /// Use [`Self::get_all_schema_generations`] where every generation is
    /// genuinely wanted.
    pub async fn get_all_schema(&self) -> Result<Vec<Schema>, ExecError> {
        Ok(self
            .schema_client
            .get_all()
            .await?
            .into_iter()
            .filter(|schema| schema.status.is_current())
            .collect())
    }

    /// Every schema GENERATION, stale ones included.
    ///
    /// For callers that must see superseded layouts -- diagnostics, migration
    /// tooling, anything decoding cells written before an evolution.
    pub async fn get_all_schema_generations(&self) -> Result<Vec<Schema>, ExecError> {
        crate::index::query_metrics::metadata();
        self.schema_client.get_all().await
    }

    /// Get a full-text search client
    ///
    /// Returns a `FullTextClient` that can be used for distributed full-text search.
    ///
    /// # Example
    /// ```ignore
    /// let ft = client.full_text();
    /// let hits = ft.search(schema_id, field_id, "rust programming", 10).await?;
    /// ```
    pub fn full_text(&self) -> FullTextClient {
        FullTextClient::new_for_database(
            self.conshash.clone(),
            self.group_name(),
            self.database_name(),
        )
    }

    /// Get a ranged index query client
    ///
    /// Returns a `RangedClient` that can be used for distributed range queries.
    ///
    /// # Example
    /// ```ignore
    /// let ranged = client.ranged();
    ///
    /// // Scan all documents in a schema
    /// if let Some(mut cursor) = ranged.scan_schema(schema_id, 100, crate::index::verification::Verify::Predicate).await? {
    ///     while let Some(id) = cursor.next().await? {
    ///         let doc = client.read_cell(id).await?;
    ///     }
    /// }
    /// ```
    pub fn ranged(&self) -> RangedClient {
        let plane_client = self.raft_client.plane(database_meta_plane_id(
            self.group_name(),
            self.database_name(),
        ));
        RangedClient::new_for_database(
            self.conshash.clone(),
            plane_client,
            self.group_name(),
            self.database_name(),
        )
    }
}

impl AsyncClient {
    async fn refresh_owner_schema_cache_for_retry(
        &self,
        client: &Arc<plain_server::AsyncServiceClient>,
        schema_id: u32,
    ) -> Result<(), RPCError> {
        match client.post_schema_add(schema_id).await? {
            Ok(()) => Ok(()),
            Err(error) => {
                warn!(
                    "Schema cache refresh before write retry failed for schema {}: {}",
                    schema_id, error
                );
                Ok(())
            }
        }
    }
}

pub fn client_by_rpc_client(rpc: &Arc<RPCClient>) -> Arc<plain_server::AsyncServiceClient> {
    client_by_rpc_client_for_database(rpc, "", "")
}

pub fn client_by_rpc_client_for_database(
    rpc: &Arc<RPCClient>,
    group_name: &str,
    database_name: &str,
) -> Arc<plain_server::AsyncServiceClient> {
    let service_id = if group_name.is_empty() && database_name.is_empty() {
        plain_server::DEFAULT_SERVICE_ID
    } else {
        plain_server::generate_scoped_service_id(group_name, database_name)
    };
    plain_server::AsyncServiceClient::new_with_service_id(service_id, rpc)
}

pub async fn client_by_server_id(
    conshash: &Arc<ConsistentHashing>,
    server_id: u64,
) -> Result<Arc<plain_server::AsyncServiceClient>, RPCError> {
    client_by_server_id_for_database(conshash, server_id, "", "").await
}

pub async fn client_by_server_id_for_database(
    conshash: &Arc<ConsistentHashing>,
    server_id: u64,
    group_name: &str,
    database_name: &str,
) -> Result<Arc<plain_server::AsyncServiceClient>, RPCError> {
    DEFAULT_CLIENT_POOL
        .get_by_id(server_id, move |sid| conshash.try_server_name(sid))
        .await
        .map_err(|e| RPCError::IOError(e))
        .map(|c| client_by_rpc_client_for_database(&c, group_name, database_name))
}

pub async fn client_by_server_name(
    server_id: u64,
    server_name: String,
) -> Result<Arc<plain_server::AsyncServiceClient>, RPCError> {
    client_by_server_name_for_database(server_id, server_name, "", "").await
}

pub async fn client_by_server_name_for_database(
    server_id: u64,
    server_name: String,
    group_name: &str,
    database_name: &str,
) -> Result<Arc<plain_server::AsyncServiceClient>, RPCError> {
    DEFAULT_CLIENT_POOL
        .get_by_id(server_id, move |_sid| Some(server_name))
        .await
        .map_err(|e| RPCError::IOError(e))
        .map(|c| client_by_rpc_client_for_database(&c, group_name, database_name))
}

/// A walk over the live cell ids of a database. See [`AsyncClient::cell_id_scan`].
///
/// A walk dropped part-way leaves its server cursor to the server's idle expiry.
pub struct CellIdScan<'a> {
    client: &'a AsyncClient,
    servers: Vec<u64>,
    next_server: usize,
    /// The server being walked and its open cursor.
    current: Option<(u64, Arc<plain_server::AsyncServiceClient>, u64)>,
    not_owned: u64,
}

impl CellIdScan<'_> {
    /// Deferred donor copies skipped so far (ids a server holds but does not own).
    pub fn not_owned_skipped(&self) -> u64 {
        self.not_owned
    }

    /// The next page of at most `max` ids, or `None` when every server has been
    /// walked. A page is never empty. It can be short without the walk being
    /// over.
    pub async fn next_page(&mut self, max: u32) -> Result<Option<Vec<Id>>, RPCError> {
        loop {
            if self.current.is_none() {
                let Some(&server_id) = self.servers.get(self.next_server) else {
                    return Ok(None);
                };
                self.next_server += 1;
                let rpc = self.client.client_by_server_id(server_id).await?;
                let cursor = rpc.cell_scan_open().await?;
                self.current = Some((server_id, rpc, cursor));
            }
            let (server_id, rpc, cursor) = self.current.clone().expect("just set");
            let page = rpc.cell_scan_next(cursor, max).await?.map_err(|reason| {
                RPCError::IOError(io::Error::new(io::ErrorKind::Other, reason))
            })?;
            if page.done {
                self.current = None;
            }
            let mut ids = Vec::with_capacity(page.ids.len());
            for id in page.ids {
                if self.client.locate_server_id(&id)? == server_id {
                    ids.push(id);
                } else {
                    self.not_owned += 1;
                }
            }
            if !ids.is_empty() || !page.done {
                if ids.is_empty() {
                    continue;
                }
                return Ok(Some(ids));
            }
        }
    }
}

/// Write fences engaged on every member. See [`AsyncClient::engage_write_fence`].
pub struct ClusterFence {
    holds: Vec<(Arc<plain_server::AsyncServiceClient>, u64)>,
    lease_ms: u64,
    stop: Arc<tokio::sync::Notify>,
    renewal: Option<tokio::task::JoinHandle<()>>,
    lost: Arc<std::sync::atomic::AtomicBool>,
}

impl ClusterFence {
    fn start(holds: Vec<(Arc<plain_server::AsyncServiceClient>, u64)>, lease_ms: u64) -> Self {
        let stop = Arc::new(tokio::sync::Notify::new());
        let lost = Arc::new(std::sync::atomic::AtomicBool::new(false));
        let renewal = {
            let holds = holds.clone();
            let stop = stop.clone();
            let lost = lost.clone();
            let every = std::time::Duration::from_millis((lease_ms / 3).max(1));
            tokio::spawn(async move {
                loop {
                    tokio::select! {
                        _ = stop.notified() => return,
                        _ = tokio::time::sleep(every) => {}
                    }
                    for (rpc, token) in &holds {
                        match rpc.fence_renew(*token, lease_ms).await {
                            Ok(true) => {}
                            // A member that no longer holds the fence means the
                            // copy is no longer protected on it.
                            _ => lost.store(true, std::sync::atomic::Ordering::SeqCst),
                        }
                    }
                }
            })
        };
        ClusterFence {
            holds,
            lease_ms,
            stop,
            renewal: Some(renewal),
            lost,
        }
    }

    /// Prove, right now, that every member has held the fence continuously
    /// since it was engaged.
    ///
    /// [`Self::is_lost`] only knows what the background renewals have seen so
    /// far, and a lease can lapse between two of them (a stalled renewal task,
    /// a slow network) with nobody told yet. A member drops a lease's token
    /// when, and only when, it lifts that fence, so a renewal answered `true`
    /// means the fence has been up there the whole time. A copy calls this
    /// after its last read and before it declares itself complete.
    pub async fn confirm(&self) -> Result<(), String> {
        let lost = |reason: String| {
            self.lost.store(true, std::sync::atomic::Ordering::SeqCst);
            Err(reason)
        };
        if self.is_lost() {
            return Err("a renewal already found the fence lifted on a member".to_string());
        }
        for (rpc, token) in &self.holds {
            match rpc.fence_renew(*token, self.lease_ms).await {
                Ok(true) => {}
                Ok(false) => {
                    return lost(format!(
                        "the fence lease {token} lapsed on a member and the fence was lifted there"
                    ))
                }
                Err(e) => {
                    return lost(format!(
                        "cannot confirm the fence lease {token} on a member: {e:?}"
                    ))
                }
            }
        }
        Ok(())
    }

    /// Whether any member lost the fence (a missed renewal) since it was engaged.
    /// A copy taken while this is true is not consistent.
    pub fn is_lost(&self) -> bool {
        self.lost.load(std::sync::atomic::Ordering::SeqCst)
    }

    /// Stop renewing without releasing, as a holder whose renewal task stalls
    /// (a starved runtime, a slow network) would.
    #[cfg(test)]
    pub(crate) fn stop_renewing_for_test(&mut self) {
        if let Some(task) = self.renewal.take() {
            task.abort();
        }
    }

    pub async fn release(mut self) {
        self.stop.notify_one();
        if let Some(task) = self.renewal.take() {
            let _ = task.await;
        }
        for (rpc, token) in &self.holds {
            let _ = rpc.fence_release(*token).await;
        }
        self.holds.clear();
    }
}

impl Drop for ClusterFence {
    fn drop(&mut self) {
        // Not released explicitly (a panic, a cancelled task): stop renewing and
        // let the members' leases lapse.
        self.stop.notify_one();
        if let Some(task) = self.renewal.take() {
            task.abort();
        }
    }
}
