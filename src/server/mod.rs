use crate::client::{AsyncClient, NebClientError};
use crate::query::data_client::IndexedDataClient;
use crate::server::transactions::manager::TransactionManager;
use crate::{client, index::builder::IndexBuilder};
use bifrost::conshash::weights::Weights;
use bifrost::conshash::ConsistentHashing;
use bifrost::membership::client::ObserverClient;
use bifrost::membership::member::MemberService;
use bifrost::membership::server::Membership;
use bifrost::raft;
use bifrost::raft::client::RaftClient;
use bifrost::raft::disk::DiskOptions;
use bifrost::raft::state_machine::master as sm_master;
use bifrost::rpc::DEFAULT_CLIENT_POOL;
use bifrost::rpc::{self, ClientPool};
use bifrost::rpc::{RPCClient, RPCError, Server};
use bifrost::vector_clock::ServerVectorClock;
use bifrost_plugins::hash_ident;
// use crate::index::lsmtree;
use crate::index::ranged;
use crate::ram::chunk::Chunks;
use crate::ram::cleaner::Cleaner;
use crate::ram::schema::sm as schema_sm;
use crate::ram::schema::LocalSchemasCache;
use crate::ram::types::Id;
use std::collections::HashSet;
use std::io;
use std::path::{Path, PathBuf};
use std::sync::Arc;

pub mod cell_rpc;
pub mod database;
pub mod status;
#[cfg(test)]
mod tests;
pub mod transactions;

// Re-export status types for convenience
pub use status::{ChunkMemoryStatus, ServerMemoryStatus};

pub static CONS_HASH_ID: u64 = hash_ident!(NEB_CONSHASH_MEM_WEIGHTS) as u64;

/// Check if Raft state exists on disk at the given path
fn has_existing_raft_state(raft_storage: &Option<String>) -> bool {
    if let Some(ref path) = raft_storage {
        let raft_path = Path::new(path);
        let log_file = raft_path.join("log.dat");
        let snapshot_file = raft_path.join("snapshot.dat");

        // Consider state exists if either log or snapshot file exists and is non-empty
        let has_logs =
            log_file.exists() && log_file.metadata().map(|m| m.len() > 0).unwrap_or(false);
        let has_snapshot = snapshot_file.exists()
            && snapshot_file
                .metadata()
                .map(|m| m.len() > 0)
                .unwrap_or(false);

        has_logs || has_snapshot
    } else {
        false
    }
}

#[derive(Debug)]
pub enum ServerError {
    CannotJoinCluster,
    CannotJoinClusterGroup(sm_master::ExecError),
    CannotInitMemberTable,
    CannotSetServerWeight,
    CannotInitConsistentHashTable,
    CannotLoadMetaClient,
    CannotInitializeDatabaseCatalog(sm_master::ExecError),
    CannotInitializeSchemaServer(sm_master::ExecError),
    StandaloneMustAlsoBeMetaServer,
}

#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct ServerOptions {
    pub chunk_count: usize,
    pub total_size: usize,
    pub tiered_config: Option<crate::ram::tiered::TieredConfig>,
    pub backup_storage: Option<String>,
    pub wal_storage: Option<String>,
    pub undo_log_storage: Option<String>,
    pub raft_storage: Option<String>,
    pub services: Vec<Service>,
    pub index_enabled: bool,
    pub enable_recovery: bool,
}

#[derive(Debug, Clone, Copy, Serialize, Deserialize, Eq, PartialEq, Hash, Ord, PartialOrd)]
pub enum Service {
    Cell = 0,
    Transaction = 1,
    RangedIndexer = 2,
    HashIndexer = 3,
    Query = 4,
}

pub struct ServerMeta {
    pub schemas: LocalSchemasCache,
}

pub struct DatabaseRuntime {
    pub chunks: Arc<Chunks>,
    pub meta: Arc<ServerMeta>,
    pub cleaner: Arc<Cleaner>,
    pub indexer: Option<Arc<IndexBuilder>>,
    pub undo_log: Option<Arc<transactions::undo_log::UndoLogger>>,
    pub txn_manager: Option<Arc<transactions::manager::TransactionManager>>,
}

pub struct NebServer {
    pub database_runtime: Arc<DatabaseRuntime>,
    pub chunks: Arc<Chunks>,
    pub meta: Arc<ServerMeta>,
    pub rpc: Arc<rpc::Server>,
    pub consh: Arc<ConsistentHashing>,
    pub membership: Arc<ObserverClient>,
    pub member_pool: Arc<rpc::ClientPool>,
    pub txn_peer: Peer,
    pub raft_service: Arc<raft::RaftService>,
    pub raft_client: Arc<RaftClient>,
    pub server_id: u64,
    pub cleaner: Arc<Cleaner>,
    pub indexer: Option<Arc<IndexBuilder>>,
    pub group_name: String,
    pub database_name: String,
    pub neb_client: Arc<AsyncClient>,
    pub undo_log: Option<Arc<transactions::undo_log::UndoLogger>>,
    pub txn_manager: Option<Arc<transactions::manager::TransactionManager>>,
}

pub async fn init_conshash(
    group_name: &String,
    address: &String,
    memory_size: u64,
    raft_client: &Arc<RaftClient>,
    membership: &Arc<ObserverClient>,
) -> Result<Arc<ConsistentHashing>, ServerError> {
    match ConsistentHashing::new_with_id(CONS_HASH_ID, group_name, raft_client, membership).await {
        Ok(ch) => {
            ch.set_weight(address, memory_size).await.unwrap();
            if !ch.init_table().await.is_ok() {
                error!("Cannot initialize member table");
                return Err(ServerError::CannotInitMemberTable);
            }
            return Ok(ch);
        }
        _ => {
            error!("Cannot initialize consistent hash table");
            return Err(ServerError::CannotInitConsistentHashTable);
        }
    }
}

impl NebServer {
    pub fn database_runtime(&self) -> &DatabaseRuntime {
        self.database_runtime.as_ref()
    }

    pub fn chunks(&self) -> &Arc<Chunks> {
        &self.database_runtime.chunks
    }

    pub fn meta(&self) -> &Arc<ServerMeta> {
        &self.database_runtime.meta
    }

    pub fn cleaner(&self) -> &Arc<Cleaner> {
        &self.database_runtime.cleaner
    }

    pub fn indexer(&self) -> Option<&Arc<IndexBuilder>> {
        self.database_runtime.indexer.as_ref()
    }

    pub fn undo_log(&self) -> Option<&Arc<transactions::undo_log::UndoLogger>> {
        self.database_runtime.undo_log.as_ref()
    }

    pub fn txn_manager(&self) -> Option<&Arc<transactions::manager::TransactionManager>> {
        self.database_runtime.txn_manager.as_ref()
    }

    /// Gracefully shutdown the server, flushing all data to disk
    pub async fn shutdown(&self) {
        info!("Starting graceful server shutdown");

        // Step 0: Wait for all pending index tasks to complete
        // CRITICAL: Index tasks (especially enumeration index inserts) must complete
        // before we flush LSM trees, otherwise the flush will have incomplete data!
        // This fixes the bug where index tasks spawned on different threads were lost.
        info!("Waiting for all pending index tasks to complete...");
        if self.indexer().is_some() {
            use crate::index::builder::IndexBuilder;
            let _ = IndexBuilder::await_all_indices().await;
            info!("All pending index tasks completed");

            // CRITICAL DELAY: The RPC insert calls complete when sent to Raft,
            // but Raft must commit them before the data appears in LSM mem_tree.
            // Without this delay, flush_all() sees empty mem_tree and persists nothing!
            info!("Waiting for Raft to commit index inserts before flushing...");
            tokio::time::sleep(tokio::time::Duration::from_millis(2000)).await;
            info!("Index Raft commit grace period complete");
        } else {
            debug!("No index builder, skipping index task await");
        }

        // Step 1: Flush LSM trees if ranged indexer is enabled
        // This ensures enumeration indices are persisted before backup creation
        info!("Flushing LSM trees before shutdown");
        if let Ok(lsm_client) = self.get_lsm_tree_service().await {
            info!("LSM client obtained, calling flush_all");
            let _ = lsm_client.flush_all().await;
            info!("LSM trees flushed successfully");

            // CRITICAL: Wait for background write-back of B-tree nodes to complete
            // The flush triggers merge which adds nodes to CHANGED_NODES queue.
            // The write-back task processes this asynchronously.
            // We MUST wait for all nodes to be persisted before archiving!
            info!("Waiting for B-tree nodes write-back to complete...");
            crate::index::ranged::tree::btree::storage::wait_until_updated().await;
            info!("B-tree nodes write-back completed");

            // Reset write-back state for potential restart within same process
            // This is critical for test scenarios where server is restarted multiple times
            crate::index::ranged::tree::btree::storage::reset_write_back_state().await;
        } else {
            debug!("LSM tree service not available (likely not enabled)");
        }

        // Step 1.4.5: Allow time for mark_migration() RPCs to complete
        // mark_migration() updates LSM tree cells via client.update_cell() which is async
        // We need to ensure these RPCs complete and cells are written before archiving
        info!("Waiting for mark_migration() RPCs to complete...");
        tokio::time::sleep(tokio::time::Duration::from_millis(1000)).await;
        info!("Mark migration wait completed");

        // Step 1.5: Ensure all WAL data is synced to disk
        // This is critical after LSM flush to ensure root cells and new pages are persisted
        info!("Syncing WAL for all chunks...");
        self.chunks().sync_all();
        info!("WAL sync completed");

        // Step 1.6: Archive all dirty segments to backup storage
        // This ensures all in-memory data (including LSM B-Tree pages) is written to backup files
        // Recovery reads from backup files, not WAL, so this is critical for proper recovery
        info!("Archiving all dirty segments to backup storage...");
        self.chunks().archive_all();
        info!("Segment archiving completed");

        // Step 2: Shutdown Raft (triggers backup creation)
        info!("Shutting down Raft service (will create backups)");
        let _ = self.raft_service.shutdown().await;
        info!("Raft service shutdown complete");

        // Step 3: Shutdown RPC server
        info!("Shutting down RPC server");
        let _ = self.rpc.shutdown().await;
        info!("RPC server shutdown complete");

        info!("Server shutdown complete");
    }

    /// Get LSM tree service client
    async fn get_lsm_tree_service(
        &self,
    ) -> Result<Arc<ranged::tree::service::AsyncServiceClient>, bifrost::rpc::RPCError> {
        // Use a dummy ID to locate the local LSM tree service via consistent hashing
        let dummy_id = Id::new(0, 1);
        ranged::tree::service::locate_tree_server_from_conshash(&dummy_id, &self.consh).await
    }

    pub async fn new(
        opts: &ServerOptions,
        server_addr: &String,
        meta_members: &Vec<String>,
        group_name: &String,
        database_name: &str,
        rpc_server: &Arc<rpc::Server>,
        raft_service: &Arc<raft::RaftService>,
        raft_client: &Arc<RaftClient>,
        membership_client: &Arc<ObserverClient>,
    ) -> Result<Arc<NebServer>, ServerError> {
        debug!(
            "Creating key-value server instance, group name {}",
            group_name
        );
        // State machines are already registered before RaftService::start()
        // in new_cluster_from_opts() to allow WAL replay during recovery

        // Now we can query the state machine to build the local cache
        let schemas = LocalSchemasCache::new_for_database(group_name, database_name, raft_client)
            .await
            .unwrap();
        let meta_rc = Arc::new(ServerMeta { schemas });
        let conshasing = init_conshash(
            group_name,
            server_addr,
            opts.total_size as u64,
            raft_client,
            membership_client,
        )
        .await?;
        let neb_client = Arc::new(
            client::AsyncClient::new_for_database(
                rpc_server,
                membership_client,
                &meta_members,
                group_name,
                database_name,
            )
            .await
            .unwrap(),
        );
        neb_client
            .ensure_database()
            .await
            .map_err(ServerError::CannotInitializeDatabaseCatalog)?;
        // Create temporary chunks for index builder initialization
        // Note: chunks will be recreated with index_builder below
        // If indexing is enabled, register inverted index schemas BEFORE recovery
        // These schemas are needed for recovery to recognize inverted index cells
        // Note: These internal schemas have fixed, hash-based IDs so all nodes
        // register identical schemas independently without raft consensus
        if opts.index_enabled {
            meta_rc
                .schemas
                .register_internal_schema(crate::index::full_text::shard::inverted_segment_schema());
            meta_rc
                .schemas
                .register_internal_schema(crate::index::full_text::inverted_stats_schema());
            debug!("Registered inverted index schemas before recovery");
        }

        // Create IndexBuilder first (without inverted indexer initialization)
        let index_builder = if opts.index_enabled {
            Some(Arc::new(
                IndexBuilder::new(&neb_client, &conshasing, &raft_client, rpc_server.server_id)
                    .await,
            ))
        } else {
            None
        };

        // Create chunks with index_builder
        let chunks = Chunks::new_with_recovery(
            opts.chunk_count,
            opts.total_size,
            meta_rc.clone(),
            index_builder.clone(),
            opts.backup_storage.clone(),
            opts.wal_storage.clone(),
            opts.tiered_config
                .clone()
                .or_else(|| crate::ram::tiered::TieredConfig::from_env()),
            opts.enable_recovery,
            opts.raft_storage.clone(),
        );

        // Initialize the inverted indexer with chunks (lazy initialization)
        if let Some(ref index_builder) = index_builder {
            index_builder.initialize_inverted_indexer(&chunks);
        }

        // Initialize undo log if storage path is provided and perform rollback
        // This must happen AFTER segment recovery but BEFORE cleaner starts
        let undo_log = if let Some(ref undo_log_path) = opts.undo_log_storage {
            match transactions::undo_log::UndoLogger::new(undo_log_path.clone()) {
                Ok(log) => {
                    // Recover undo log from disk and get incomplete transactions
                    // Only perform recovery if enable_recovery is true
                    if opts.enable_recovery {
                        match log.recover() {
                            Ok(txn_index) => {
                                // Perform rollback for incomplete transactions
                                // Segments are already in memory, so we can read directly from them
                                if let Err(e) =
                                    log.rollback_incomplete_transactions(txn_index, &chunks)
                                {
                                    error!("Failed to rollback incomplete transactions: {:?}", e);
                                }
                            }
                            Err(e) => {
                                error!("Failed to recover undo log: {:?}", e);
                            }
                        }
                    }

                    Some(log)
                }
                Err(e) => {
                    error!("Failed to initialize undo log: {:?}", e);
                    None
                }
            }
        } else {
            None
        };

        // Start cleaner AFTER all recovery (segments + transactions) is complete
        // This ensures segments with old cell data needed for rollback aren't cleaned
        // Note: If recovery is enabled, we start cleaner in PAUSED state to prevent
        // interference/hangs during recovery. It must be explicitly resumed later.
        let cleaner = if opts.enable_recovery {
            debug!("Recovery enabled: Starting cleaner in PAUSED state");
            Arc::new(Cleaner::new_paused(chunks.clone()))
        } else {
            Arc::new(Cleaner::new_and_start(chunks.clone()))
        };
        let mut transaction_manager = None;
        let member_pool = Arc::new(rpc::ClientPool::new());
        let txn_peer = Peer::new(server_addr);
        let clock = txn_peer.clock.clone();

        if opts.services.contains(&Service::Transaction) {
            transaction_manager = Some(
                init_txn_manager(
                    rpc_server,
                    &meta_rc,
                    &clock,
                    rpc_server.server_id,
                    &conshasing,
                    &member_pool,
                )
                .await,
            );
        }

        let database_runtime = Arc::new(DatabaseRuntime {
            chunks: chunks.clone(),
            meta: meta_rc.clone(),
            cleaner,
            indexer: index_builder.clone(),
            undo_log: undo_log.clone(),
            txn_manager: transaction_manager.clone(),
        });

        let server = Arc::new(NebServer {
            database_runtime: database_runtime.clone(),
            chunks,
            meta: meta_rc,
            rpc: rpc_server.clone(),
            consh: conshasing.clone(),
            membership: membership_client.clone(),
            member_pool: member_pool.clone(),
            raft_service: raft_service.clone(),
            raft_client: raft_client.clone(),
            server_id: rpc_server.server_id,
            txn_peer: txn_peer,
            cleaner: database_runtime.cleaner.clone(),
            indexer: database_runtime.indexer.clone(),
            group_name: group_name.clone(),
            database_name: database_name.to_string(),
            neb_client: neb_client.clone(),
            undo_log: database_runtime.undo_log.clone(),
            txn_manager: database_runtime.txn_manager.clone(),
        });
        let servs = proc_services(&opts.services);
        for service in servs {
            match service {
                Service::Cell => init_cell_rpc_service(rpc_server, &server).await,
                Service::Transaction | Service::HashIndexer => {
                    init_txn_data_site_service(rpc_server, &server).await
                }
                Service::RangedIndexer => {
                    // Use raft storage path for tree persistence if available
                    let tree_path = opts
                        .raft_storage
                        .as_ref()
                        .map(|p| format!("{}/master_tree.dat", p));
                    init_ranged_indexer_service(
                        rpc_server,
                        &neb_client,
                        raft_service,
                        raft_client,
                        &conshasing,
                        group_name,
                        database_name,
                        tree_path,
                    )
                    .await
                }
                Service::Query => {
                    // todo!()
                }
            }
        }

        // Register inverted index RPC service if indexing is enabled
        if opts.index_enabled {
            init_inverted_index_rpc_service(rpc_server, &server).await;
        }

        Ok(server)
    }

    pub async fn new_from_opts<'a, F: AsyncFnOnce(&Arc<raft::RaftService>)>(
        opts: &ServerOptions,
        server_addr: &'a str,
        group_name: &'a str,
        prepare_raft_service: F,
    ) -> Arc<NebServer> {
        Self::new_cluster_from_opts_in_database(
            opts,
            server_addr,
            &vec![server_addr.to_owned()],
            group_name,
            group_name,
            prepare_raft_service,
        )
        .await
    }

    pub async fn new_from_opts_in_database<'a, F: AsyncFnOnce(&Arc<raft::RaftService>)>(
        opts: &ServerOptions,
        server_addr: &'a str,
        group_name: &'a str,
        database_name: &'a str,
        prepare_raft_service: F,
    ) -> Arc<NebServer> {
        Self::new_cluster_from_opts_in_database(
            opts,
            server_addr,
            &vec![server_addr.to_owned()],
            group_name,
            database_name,
            prepare_raft_service,
        )
        .await
    }

    pub async fn new_cluster_from_opts<'a, F: AsyncFnOnce(&Arc<raft::RaftService>)>(
        opts: &ServerOptions,
        server_addr: &'a str,
        meta_servers: &Vec<String>,
        group_name: &'a str,
        prepare_raft_service: F,
    ) -> Arc<NebServer> {
        Self::new_cluster_from_opts_in_database(
            opts,
            server_addr,
            meta_servers,
            group_name,
            group_name,
            prepare_raft_service,
        )
        .await
    }

    pub async fn new_cluster_from_opts_in_database<'a, F: AsyncFnOnce(&Arc<raft::RaftService>)>(
        opts: &ServerOptions,
        server_addr: &'a str,
        meta_servers: &Vec<String>,
        group_name: &'a str,
        database_name: &'a str,
        prepare_raft_service: F,
    ) -> Arc<NebServer> {
        debug!("Creating key-value server from options");
        let group_name = &String::from(group_name);
        let server_addr = &String::from(server_addr);
        let storage_layout =
            database::DatabaseStorageLayout::from_options(opts, group_name, database_name);
        debug!("Creating RPC server and listen");
        let rpc_server = rpc::Server::new(server_addr);
        let meta_members: Vec<_> = meta_servers
            .iter()
            .filter(|n| *n != server_addr)
            .cloned()
            .collect();
        let storage = if let Some(ref raft_path) = storage_layout.raft_storage {
            raft::Storage::DISK(DiskOptions {
                path: raft_path.clone(),
                take_snapshots: true,
                append_logs: true,
                trim_logs: true,
                snapshot_log_threshold: 1000,
                log_compaction_threshold: 2000,
            })
        } else {
            raft::Storage::MEMORY
        };
        let raft_service = raft::RaftService::new(raft::Options {
            storage,
            address: server_addr.to_owned(),
            service_id: raft::DEFAULT_SERVICE_ID,
        });

        // Create recovery flag - will be set to false after all services are initialized
        // This prevents callbacks from being sent during Raft recovery
        let recovering_flag = Arc::new(std::sync::atomic::AtomicBool::new(true));

        // Register state machines BEFORE starting Raft so WAL replay can apply to them
        // This is critical: any SM registered after start() won't receive replayed WAL entries
        debug!("Registering state machines before Raft start for WAL replay (recovery mode)");
        raft_service
            .register_state_machine(Box::new(database::DatabaseCatalogSM::new(group_name)))
            .await;
        raft_service
            .register_state_machine(Box::new(
                schema_sm::SchemasSM::new_with_recovery_flag(
                    group_name,
                    database_name,
                    &raft_service,
                    recovering_flag.clone(),
                )
                .await,
            ))
            .await;
        Weights::new_with_id(CONS_HASH_ID, &raft_service).await;

        // TODO: If RangedIndexer service is enabled, MasterTreeSM should also be
        // registered here before start() to enable WAL replay recovery

        rpc_server.register_service(&raft_service).await;
        Server::listen_and_resume(&rpc_server).await;

        // Register Membership service BEFORE Raft start for WAL replay
        debug!("Registering Membership service before Raft start");
        Membership::new(&rpc_server, &raft_service).await;

        debug!("Preparing raft service");
        prepare_raft_service(&raft_service).await;

        debug!("RPC server created, starting Raft service (will replay WAL to registered SMs)");
        raft::RaftService::start(&raft_service, true).await;

        // Clear recovery flag after Raft recovery completes
        // Future schema operations should now send callbacks normally
        recovering_flag.store(false, std::sync::atomic::Ordering::Relaxed);
        debug!("Raft recovery complete, schema callbacks enabled for Neb SchemasSM");

        // Check if we have existing Raft state on disk
        let has_existing_state = has_existing_raft_state(&storage_layout.raft_storage);

        if has_existing_state {
            // Existing state found - Raft will automatically resume from disk
            info!("Resuming from existing Raft state on disk");
            // Don't bootstrap or join - let Raft recover automatically
            // Give Raft time to elect leader if this is a single-server resumed cluster
            if meta_members.is_empty() {
                info!("Single-server resumed cluster, waiting for leader election and state recovery...");
                tokio::time::sleep(tokio::time::Duration::from_secs(5)).await;
            }
        } else if meta_members.is_empty() {
            // Fresh start, no other members - bootstrap a new cluster
            debug!("No existing state and no other members, bootstrapping new cluster");
            raft_service.bootstrap().await;
        } else {
            // Fresh start with other members - join the cluster
            debug!(
                "No existing state, joining cluster with members: {:?}",
                &meta_members
            );
            match raft_service.join(&meta_members).await {
                Err(sm_master::ExecError::CannotConstructClient) => {
                    info!("Cannot join meta cluster, will bootstrap one.");
                    raft_service.bootstrap().await;
                }
                Ok(true) => {
                    info!(
                        "Joined meta cluster, number of members: {}",
                        raft_service.num_members().await
                    );
                }
                e => {
                    error!("Cannot join into cluster: {:?}", e);
                    panic!("{:?}", ServerError::CannotJoinCluster)
                }
            }
        }
        debug!("Joined with members, membership service already started before Raft start");
        debug!("Starting raft client");
        let raft_client = RaftClient::new(meta_servers, raft::DEFAULT_SERVICE_ID)
            .await
            .map_err(|e| {
                error!("Failed to create Raft client: {:?}", e);
                error!("This may happen if resuming from disk without proper cluster state");
                e
            })
            .unwrap();
        debug!("Prepare raft subscription");
        RaftClient::prepare_subscription(&rpc_server).await;
        debug!("Starting member service");
        let member_service = MemberService::new(server_addr, &raft_client, &raft_service).await;
        debug!("Member join group: {}", group_name);
        member_service.join_group(group_name).await.unwrap();
        let membership_client = Arc::new(ObserverClient::new(&raft_client));
        debug!("Creating neb server");
        let effective_opts = ServerOptions {
            backup_storage: storage_layout.backup_storage,
            wal_storage: storage_layout.wal_storage,
            undo_log_storage: storage_layout.undo_log_storage,
            raft_storage: storage_layout.raft_storage,
            ..opts.clone()
        };
        NebServer::new(
            &effective_opts,
            server_addr,
            &meta_servers,
            group_name,
            database_name,
            &rpc_server,
            &raft_service,
            &raft_client,
            &membership_client,
        )
        .await
        .unwrap()
    }

    pub fn get_server_id_by_id(&self, id: &Id) -> Option<u64> {
        self.consh.get_server_id(id.higher)
    }
    pub async fn get_member_by_server_id(&self, server_id: u64) -> io::Result<Arc<rpc::RPCClient>> {
        self.member_pool
            .get_by_id(server_id, |_| self.consh.to_server_name(server_id))
            .await
    }
    pub async fn get_member_by_server_id_async(
        &self,
        server_id: u64,
    ) -> Result<Arc<RPCClient>, io::Error> {
        let cons_hash = self.consh.clone();
        self.member_pool
            .get_by_id(server_id, move |_| cons_hash.to_server_name(server_id))
            .await
    }
    pub fn conshash(&self) -> &ConsistentHashing {
        &*self.consh
    }
    pub fn raft_client(&self) -> &RaftClient {
        &*self.raft_client
    }
    pub fn database_name(&self) -> &str {
        &self.database_name
    }
    pub fn indexed_data_client(&self) -> IndexedDataClient {
        // Use server's indexer clients if available (for BM25 search support)
        if let Some(index_builder) = self.indexer() {
            IndexedDataClient::new_with_indexers(index_builder.clients.clone(), self.consh.clone())
        } else {
            IndexedDataClient::new(&self.neb_client, &self.consh, &self.raft_client)
        }
    }
    pub async fn data_client(&self, members: &Vec<String>) -> Result<AsyncClient, NebClientError> {
        AsyncClient::new_for_database(
            &self.rpc,
            &self.membership,
            members,
            &self.group_name,
            &self.database_name,
        )
        .await
    }
}

// Note: We intentionally do NOT implement Drop for NebServer because:
// 1. Drop is synchronous but shutdown() is async
// 2. Calling block_on from Drop within a tokio runtime causes deadlocks
// 3. Always call server.shutdown().await explicitly before the server goes out of scope
//
// In production: use signal handlers (SIGTERM, SIGINT) to call shutdown()
// In tests: always call server.shutdown().await at the end

pub async fn rpc_client_by_id(id: &Id, neb: &Arc<NebServer>) -> Result<Arc<RPCClient>, RPCError> {
    let server_id = neb.get_server_id_by_id(id).unwrap();
    let neb = neb.clone();
    DEFAULT_CLIENT_POOL
        .get_by_id(server_id, move |sid| neb.conshash().to_server_name(sid))
        .await
        .map_err(|e| RPCError::IOError(e))
}

// Peer have a clock, meant to update with other servers in the cluster
pub struct Peer {
    pub clock: Arc<ServerVectorClock>,
}

impl Peer {
    pub fn new(server_address: &String) -> Peer {
        Peer {
            clock: Arc::new(ServerVectorClock::new(server_address)),
        }
    }
}

pub async fn init_cell_rpc_service(rpc_server: &Arc<Server>, neb_server: &Arc<NebServer>) {
    rpc_server
        .register_service(&cell_rpc::NebRPCService::new(&neb_server))
        .await;
}

pub async fn init_txn_manager(
    rpc_server: &Arc<Server>,
    meta: &Arc<ServerMeta>,
    clock: &Arc<ServerVectorClock>,
    server_id: u64,
    consh: &Arc<ConsistentHashing>,
    member_pool: &Arc<ClientPool>,
) -> Arc<TransactionManager> {
    let deps = Arc::new(transactions::manager::TransactionManagerDeps {
        meta: meta.clone(),
        clock: clock.clone(),
        server_id: server_id,
        consh: consh.clone(),
        member_pool: member_pool.clone(),
    });
    let txn_manager = transactions::manager::TransactionManager::new(deps);
    rpc_server.register_service(&txn_manager).await;
    return txn_manager;
}
pub async fn init_txn_data_site_service(rpc_server: &Arc<Server>, neb_server: &Arc<NebServer>) {
    rpc_server
        .register_service(&transactions::data_site::DataManager::new(&neb_server))
        .await;
}

pub async fn init_inverted_index_rpc_service(
    rpc_server: &Arc<Server>,
    neb_server: &Arc<NebServer>,
) {
    if let Some(index_builder) = neb_server.indexer() {
        if let Some(inverted_indexer) = index_builder.clients.fulltext_indexer() {
            use crate::index::full_text::rpc::InvertedIndexRPCService;
            let service = InvertedIndexRPCService::new(inverted_indexer.clone());
            rpc_server.register_service(&service).await;
            info!("Registered inverted index RPC service");
        }
    }
}

pub async fn init_ranged_indexer_service(
    rpc_server: &Arc<Server>,
    neb_client: &Arc<AsyncClient>,
    raft_svr: &Arc<raft::RaftService>,
    raft_client: &Arc<RaftClient>,
    cons_hash: &Arc<ConsistentHashing>,
    group_name: &str,
    database_name: &str,
    tree_persistence_path: Option<String>,
) {
    info!("Initializing range indexer service");
    // TODO: create the schema only when it does not exists
    let _ = neb_client
        .new_schema_with_id(ranged::tree::tree::RANGED_TREE_SCHEMA.clone())
        .await
        .unwrap();
    let _ = neb_client
        .new_schema_with_id(ranged::tree::btree::page_schema())
        .await
        .unwrap();
    let sm_client = Arc::new(ranged::sm::client::SMClient::new(
        ranged::sm::generate_scoped_sm_id(group_name, database_name),
        raft_client,
    ));
    rpc_server
        .register_service(&Arc::new(ranged::tree::service::TreeService::new(
            neb_client, &sm_client,
        )))
        .await;

    // Create MasterTreeSM with persistence support
    let persistence_path = tree_persistence_path.map(PathBuf::from);
    let mut tree_sm = ranged::sm::MasterTreeSM::new_with_id_and_persistence(
        ranged::sm::generate_scoped_sm_id(group_name, database_name),
        raft_svr,
        cons_hash,
        persistence_path,
    );
    tree_sm.try_initialize().await;
    raft_svr.register_state_machine(Box::new(tree_sm)).await;
}

fn proc_services(svrs: &Vec<Service>) -> Vec<Service> {
    let mut res_set = HashSet::new();
    for svr in svrs {
        res_set.insert(*svr);
    }
    if res_set.contains(&Service::Query) {
        res_set.insert(Service::RangedIndexer);
    }
    let mut res = res_set.into_iter().collect::<Vec<_>>();
    res.sort(); // Sort by service priority
    res
}
