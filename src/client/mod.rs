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
use crate::ram::schema::{DelSchemaError, NewSchemaError, Schema};
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

pub struct AsyncClient {
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
        self.conshash.note_slot_owner(
            slot as u64,
            owner,
            crate::slots::SLOT_COUNT,
            applied_index,
        )
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
        client.read_cell(id).await
    }

    pub async fn read_cell_select(
        &self,
        id: Id,
        fields: &Vec<u64>,
        need_header: bool,
    ) -> Result<Result<OwnedCell, ReadError>, RPCError> {
        let client = self.locate_plain_server(id).await?;
        client.read_cell_select(id, fields, need_header).await
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
        let mut out: Vec<Option<Result<CellHeader, WriteError>>> = (0..total).map(|_| None).collect();
        while let Some(res) = batches.next().await {
            let (indices, results) = res?;
            for (idx, r) in indices.into_iter().zip(results) {
                out[idx] = Some(r);
            }
        }
        Ok(out.into_iter().map(|r| r.unwrap()).collect())
    }
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
    pub async fn count(&self) -> Result<u64, RPCError> {
        let (members, _) = self.conshash.membership().all_members(true).await.unwrap();
        let mut member_futs: FuturesUnordered<_> = members
            .into_iter()
            .map(|m| async move {
                let client = self.client_by_server_id(m.id).await?;
                Ok(client.count().await?)
            })
            .collect();
        let mut sum = 0;
        while let Some(res) = member_futs.next().await {
            sum += res?;
        }
        Ok(sum)
    }
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
        let schema_id = schema.id;
        match res {
            Ok(Ok(_)) => {
                if schema.index_fields.is_empty() && schema.compound_index_fields.is_empty() {
                    // Nothing to process
                    return res;
                }
                if let Some(server_id) = self.conshash.rand_server_id() {
                    match self.client_by_server_id(server_id).await {
                        Ok(client) => {
                            if let Err(e) = client.post_schema_add(schema_id).await {
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
        schema.id = schema_id;
        self.new_schema_with_id(schema)
            .await
            .map(|r| r.map(|_| schema_id))
    }
    pub async fn del_schema(&self, name: String) -> Result<Result<(), DelSchemaError>, ExecError> {
        let schema = self.schema_client.get_by_name(&name).await?;
        let has_index_fields;
        let schema_id = if let Some(schema) = schema {
            has_index_fields =
                !schema.index_fields.is_empty() || !schema.compound_index_fields.is_empty();
            schema.id
        } else {
            return Ok(Err(DelSchemaError::SchemaDoesNotExisted));
        };
        if has_index_fields {
            if let Some(server_id) = self.conshash.rand_server_id() {
                match self.client_by_server_id(server_id).await {
                    Ok(client) => {
                        if let Err(e) = client.post_schema_delete(schema_id).await {
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
    pub async fn get_all_schema(&self) -> Result<Vec<Schema>, ExecError> {
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
    /// if let Some(mut cursor) = ranged.scan_schema(schema_id, 100).await? {
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
