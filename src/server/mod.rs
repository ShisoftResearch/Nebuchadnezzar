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
use bifrost::rpc::{RPCClient, RPCError, Server, ServiceClient};
use bifrost_hasher::hash_str;
use bifrost_plugins::hash_ident;
// use crate::index::lsmtree;
use crate::index::ranged;
use crate::ram::chunk::Chunks;
use crate::ram::cleaner::Cleaner;
use crate::ram::schema::sm as schema_sm;
use crate::ram::schema::LocalSchemasCache;
use crate::ram::types::Id;
use crate::server::storage_lock::StorageDirectoryLocks;
use std::collections::{HashMap, HashSet};
use std::fs;
use std::io;
use std::path::{Path, PathBuf};
use std::sync::{Arc, RwLock};

pub mod cell_rpc;
pub mod database;
pub mod status;
mod storage_lock;
#[cfg(test)]
mod tests;
pub mod transactions;

// Re-export status types for convenience
pub use status::{ChunkMemoryStatus, ServerMemoryStatus};

pub static CONS_HASH_ID: u64 = hash_ident!(NEB_CONSHASH_MEM_WEIGHTS) as u64;
const META_CLUSTER_JOIN_MAX_RETRIES: usize = 100;
const META_CLUSTER_JOIN_RETRY_DELAY_MS: u64 = 100;
const CLUSTER_GROUP_JOIN_MAX_RETRIES: usize = 100;
const CLUSTER_GROUP_JOIN_RETRY_DELAY_MS: u64 = 100;
const DATABASE_SCOPED_CELL_RPC_READY_MAX_RETRIES: usize = 100;
const DATABASE_SCOPED_CELL_RPC_READY_DELAY_MS: u64 = 100;
const LOCAL_SCHEMA_CACHE_INIT_MAX_RETRIES: usize = 100;
const LOCAL_SCHEMA_CACHE_INIT_RETRY_DELAY_MS: u64 = 100;
const META_PLANE_BOOTSTRAP_MAX_RETRIES: usize = 100;
const META_PLANE_BOOTSTRAP_RETRY_DELAY_MS: u64 = 100;

fn has_recoverable_raft_state_at(raft_path: &Path) -> bool {
    let log_file = raft_path.join("log.dat");
    let snapshot_file = raft_path.join("snapshot.dat");

    let has_logs = log_file.exists() && log_file.metadata().map(|m| m.len() > 0).unwrap_or(false);
    let has_snapshot = snapshot_file.exists()
        && snapshot_file
            .metadata()
            .map(|m| m.len() > 0)
            .unwrap_or(false);

    has_logs || has_snapshot
}

/// Check if Raft state exists on disk at the given path
fn has_existing_raft_state(raft_storage: &Option<String>) -> bool {
    if let Some(ref path) = raft_storage {
        has_recoverable_raft_state_at(Path::new(path))
    } else {
        false
    }
}

fn configured_cluster_members(server_addr: &str, meta_members: &[String]) -> Vec<String> {
    let mut members = meta_members.iter().cloned().collect::<HashSet<_>>();
    members.insert(server_addr.to_string());
    let mut members = members.into_iter().collect::<Vec<_>>();
    members.sort();
    members
}

fn routed_cluster_members(
    server_addr: &str,
    meta_members: &[String],
    conshash: &ConsistentHashing,
) -> Vec<String> {
    configured_cluster_members(server_addr, meta_members)
        .into_iter()
        .filter(|member| {
            conshash
                .to_server_name_option(Some(hash_str(member)))
                .as_ref()
                == Some(member)
        })
        .collect()
}

async fn wait_for_scoped_cell_rpc_services(
    server_addr: &str,
    meta_members: &[String],
    conshash: &ConsistentHashing,
    group_name: &str,
    database_name: &str,
) -> Result<(), ServerError> {
    let members = routed_cluster_members(server_addr, meta_members, conshash);
    if members.len() <= 1 {
        return Ok(());
    }

    let service_id = cell_rpc::generate_scoped_service_id(group_name, database_name);
    let mut last_unavailable = Vec::new();

    for attempt in 0..DATABASE_SCOPED_CELL_RPC_READY_MAX_RETRIES {
        let mut unavailable = Vec::new();

        for member in &members {
            if member == server_addr {
                continue;
            }

            let rpc_client = match DEFAULT_CLIENT_POOL.get(member).await {
                Ok(client) => client,
                Err(error) => {
                    unavailable.push(format!("{member}: connect failed: {error:?}"));
                    continue;
                }
            };
            let cell_client =
                cell_rpc::AsyncServiceClient::new_with_service_id(service_id, &rpc_client);

            if let Err(error) = cell_client.count().await {
                unavailable.push(format!("{member}: {error:?}"));
            }
        }

        if unavailable.is_empty() {
            return Ok(());
        }

        last_unavailable = unavailable;
        if attempt + 1 < DATABASE_SCOPED_CELL_RPC_READY_MAX_RETRIES {
            tokio::time::sleep(tokio::time::Duration::from_millis(
                DATABASE_SCOPED_CELL_RPC_READY_DELAY_MS,
            ))
            .await;
        }
    }

    Err(ServerError::CannotInitializeDatabaseServices(format!(
        "database-scoped cell RPC service {service_id} for {group_name}/{database_name} did not become ready on configured members {:?}; last_unavailable={:?}",
        members,
        last_unavailable
    )))
}

fn discover_known_databases_from_raft_storage(
    raft_storage_root: Option<&str>,
    default_database_name: &str,
) -> Vec<String> {
    let mut names = HashSet::new();
    names.insert(default_database_name.to_string());

    if let Some(root) = raft_storage_root {
        let db_root = Path::new(root).join("databases");
        if let Ok(entries) = fs::read_dir(db_root) {
            for entry in entries.flatten() {
                let Ok(file_type) = entry.file_type() else {
                    continue;
                };
                if !file_type.is_dir() {
                    continue;
                }
                if !has_recoverable_raft_state_at(&entry.path()) {
                    continue;
                }
                if let Some(name) = entry.file_name().to_str() {
                    if !name.is_empty() {
                        names.insert(name.to_string());
                    }
                }
            }
        }
    }

    let mut out = names.into_iter().collect::<Vec<_>>();
    out.sort();
    out
}

fn discover_known_databases_from_storage_roots(
    storage_roots: &[Option<&str>],
    default_database_name: &str,
) -> Vec<String> {
    let mut names = HashSet::new();
    names.insert(default_database_name.to_string());

    for root in storage_roots.iter().flatten() {
        let db_root = Path::new(root).join("databases");
        if let Ok(entries) = fs::read_dir(db_root) {
            for entry in entries.flatten() {
                let Ok(file_type) = entry.file_type() else {
                    continue;
                };
                if !file_type.is_dir() {
                    continue;
                }
                if let Some(name) = entry.file_name().to_str() {
                    if !name.is_empty() {
                        names.insert(name.to_string());
                    }
                }
            }
        }
    }

    let mut out = names.into_iter().collect::<Vec<_>>();
    out.sort();
    out
}

fn discover_databases_for_startup_schema_registration(
    raft_storage_root: Option<&str>,
    non_raft_storage_roots: &[Option<&str>],
    default_database_name: &str,
) -> Vec<String> {
    let mut names =
        discover_known_databases_from_raft_storage(raft_storage_root, default_database_name)
            .into_iter()
            .collect::<HashSet<_>>();
    names.extend(discover_known_databases_from_storage_roots(
        non_raft_storage_roots,
        default_database_name,
    ));

    let mut out = names.into_iter().collect::<Vec<_>>();
    out.sort();
    out
}

pub fn database_meta_plane_id(group_name: &str, database_name: &str) -> raft::PlaneId {
    let mut raw =
        hash_str(&format!("MORPHEUS_DB_PLANE-{group_name}-{database_name}")).wrapping_add(2);
    if raw < 2 {
        raw = raw.wrapping_add(2);
    }
    raft::PlaneId::type2(raw).expect("database plane ids must be non-zero")
}

pub fn shared_meta_plane_id(_group_name: &str) -> raft::PlaneId {
    raft::PlaneId::type2(1).expect("shared plane id must be non-zero")
}

pub fn database_meta_plane_seed_nodes(server_addr: &str, seed_nodes: &[String]) -> Vec<String> {
    let mut nodes = seed_nodes.to_vec();
    nodes.push(server_addr.to_string());
    nodes.sort();
    nodes.dedup();
    nodes
}

fn canonical_member_addresses(mut members: Vec<String>) -> Vec<String> {
    members.sort();
    members.dedup();
    members
}

fn includes_all_members(discovered: &[String], expected: &[String]) -> bool {
    expected
        .iter()
        .all(|member| discovered.iter().any(|known| known == member))
}

async fn ensure_type2_meta_plane(
    raft_service: &Arc<raft::RaftService>,
    raft_client: &Arc<RaftClient>,
    server_addr: &str,
    meta_members: &[String],
    plane_id: raft::PlaneId,
    plane_label: &str,
) -> Result<raft::PlaneHandle, String> {
    let requested_seed_nodes =
        canonical_member_addresses(database_meta_plane_seed_nodes(server_addr, meta_members));
    let type1_members = raft_client
        .root_member_addresses()
        .await
        .map(canonical_member_addresses)
        .unwrap_or_else(|_| vec![server_addr.to_string()]);
    let requested_members = canonical_member_addresses(
        requested_seed_nodes
            .into_iter()
            .filter(|member| type1_members.iter().any(|known| known == member))
            .collect(),
    );
    let mut last_transient_error = None;

    for attempt in 0..META_PLANE_BOOTSTRAP_MAX_RETRIES {
        let plane = match raft_service
            .ensure_plane(raft::PlaneSpec { plane_id })
            .await
        {
            Ok(plane) => plane,
            Err(error) => {
                last_transient_error = Some(format!(
                    "failed to materialize local plane runtime: {error}"
                ));
                if attempt + 1 < META_PLANE_BOOTSTRAP_MAX_RETRIES {
                    tokio::time::sleep(tokio::time::Duration::from_millis(
                        META_PLANE_BOOTSTRAP_RETRY_DELAY_MS,
                    ))
                    .await;
                    continue;
                }
                break;
            }
        };
        let current_members = match plane.member_addresses().await {
            Ok(members) => canonical_member_addresses(members),
            Err(error) => {
                last_transient_error = Some(format!("failed to read plane membership: {error:?}"));
                if attempt + 1 < META_PLANE_BOOTSTRAP_MAX_RETRIES {
                    tokio::time::sleep(tokio::time::Duration::from_millis(
                        META_PLANE_BOOTSTRAP_RETRY_DELAY_MS,
                    ))
                    .await;
                    continue;
                }
                break;
            }
        };
        if current_members.iter().any(|member| {
            !requested_members
                .iter()
                .any(|requested| requested == member)
        }) {
            return Err(format!(
                "{plane_label} membership conflict: current={current_members:?}, requested={requested_members:?}"
            ));
        }

        let has_missing_members = requested_members
            .iter()
            .any(|member| !current_members.iter().any(|current| current == member));
        let mut add_error = None;
        if has_missing_members {
            for member in &requested_members {
                let add_result = plane.add_member(member.clone()).await;
                match add_result {
                    Ok(added) => {
                        if !added && !current_members.iter().any(|current| current == member) {
                            add_error = Some(format!(
                                "failed to add {member} to {plane_label} membership: target was rejected or not reachable"
                            ));
                            break;
                        }
                    }
                    Err(error) => {
                        add_error = Some(format!(
                            "failed to add {member} to {plane_label} membership: {error:?}"
                        ));
                        break;
                    }
                }
            }
        }

        if let Some(error) = add_error {
            last_transient_error = Some(error);
            if attempt + 1 < META_PLANE_BOOTSTRAP_MAX_RETRIES {
                tokio::time::sleep(tokio::time::Duration::from_millis(
                    META_PLANE_BOOTSTRAP_RETRY_DELAY_MS,
                ))
                .await;
                continue;
            }
            break;
        }

        match plane.member_addresses().await {
            Ok(members) => {
                let members = canonical_member_addresses(members);
                if includes_all_members(&members, &requested_members) {
                    return Ok(plane);
                }
                last_transient_error = Some(format!(
                    "{plane_label} membership did not converge: current={members:?}, requested={requested_members:?}"
                ));
            }
            Err(error) => {
                last_transient_error = Some(format!(
                    "failed to verify {plane_label} membership: {error:?}"
                ));
            }
        }

        if attempt + 1 < META_PLANE_BOOTSTRAP_MAX_RETRIES {
            tokio::time::sleep(tokio::time::Duration::from_millis(
                META_PLANE_BOOTSTRAP_RETRY_DELAY_MS,
            ))
            .await;
        }
    }

    Err(last_transient_error.unwrap_or_else(|| "unknown transient bootstrap error".to_string()))
}

async fn ensure_shared_meta_plane(
    raft_service: &Arc<raft::RaftService>,
    raft_client: &Arc<RaftClient>,
    server_addr: &str,
    meta_members: &[String],
    group_name: &str,
) -> Result<raft::PlaneHandle, ServerError> {
    ensure_type2_meta_plane(
        raft_service,
        raft_client,
        server_addr,
        meta_members,
        shared_meta_plane_id(group_name),
        "shared meta plane",
    )
    .await
    .map_err(|e| {
        ServerError::CannotInitializeSharedPlane(format!(
            "failed to ensure shared meta plane for {group_name}: {e}"
        ))
    })
}

async fn register_shared_state_machines_on_plane(
    group_name: &str,
    plane: &raft::PlaneHandle,
) -> Result<(), raft::PlaneError> {
    plane
        .register_state_machine(Box::new(database::DatabaseCatalogSM::new(group_name)))
        .await
}

async fn ensure_database_meta_plane(
    raft_service: &Arc<raft::RaftService>,
    raft_client: &Arc<RaftClient>,
    server_addr: &str,
    meta_members: &[String],
    group_name: &str,
    database_name: &str,
) -> Result<raft::PlaneHandle, ServerError> {
    ensure_type2_meta_plane(
        raft_service,
        raft_client,
        server_addr,
        meta_members,
        database_meta_plane_id(group_name, database_name),
        "database meta plane",
    )
    .await
    .map_err(|e| {
        ServerError::CannotInitializeSchemaPlane(format!(
            "failed to ensure database meta plane for {database_name}: {e}"
        ))
    })
}

async fn register_schema_state_machine_on_plane(
    group_name: &str,
    database_name: &str,
    plane: &raft::PlaneHandle,
    recovering_flag: Arc<std::sync::atomic::AtomicBool>,
) -> Result<(), raft::PlaneError> {
    let sm_id = schema_sm::generate_scoped_sm_id(group_name, database_name);
    let schema_state_machine = schema_sm::SchemasSM::with_callback_and_recovery_flag(
        sm_id,
        plane.callback(sm_id).await?,
        recovering_flag,
    );
    plane
        .register_state_machine(Box::new(schema_state_machine))
        .await
}

async fn register_schema_sms_for_known_databases(
    group_name: &str,
    databases: &[String],
    raft_service: &Arc<raft::RaftService>,
    recovering_flag: Arc<std::sync::atomic::AtomicBool>,
) {
    info!(
        "Pre-registering Neb SchemasSM for databases before replay: {:?}",
        databases
    );

    for database_name in databases {
        let plane = raft_service
            .ensure_plane(raft::PlaneSpec {
                plane_id: database_meta_plane_id(group_name, database_name),
            })
            .await
            .expect("database meta plane should materialize locally during startup");
        register_schema_state_machine_on_plane(
            group_name,
            database_name,
            &plane,
            recovering_flag.clone(),
        )
        .await
        .expect("database schema state machine should register during startup");
    }
}

async fn recover_startup_meta_planes(group_name: &str, raft_service: &Arc<raft::RaftService>) {
    let shared_meta_plane = raft_service
        .ensure_plane(raft::PlaneSpec {
            plane_id: shared_meta_plane_id(group_name),
        })
        .await
        .expect("shared meta plane should materialize locally during startup recovery");
    shared_meta_plane
        .recover_after_register()
        .await
        .expect("shared meta plane should recover registered state machines during startup");
}

async fn recover_schema_sms_for_known_databases(
    group_name: &str,
    databases: &[String],
    raft_service: &Arc<raft::RaftService>,
) {
    info!(
        "Recovering pre-registered Neb SchemasSM planes after Raft start: {:?}",
        databases
    );

    for database_name in databases {
        let plane = raft_service
            .ensure_plane(raft::PlaneSpec {
                plane_id: database_meta_plane_id(group_name, database_name),
            })
            .await
            .expect("database meta plane should materialize locally during startup recovery");
        plane
            .recover_after_register()
            .await
            .expect("database schema state machine should recover during startup");
    }
}

#[cfg(test)]
mod startup_discovery_tests {
    use super::{
        database_meta_plane_id, database_meta_plane_seed_nodes,
        discover_databases_for_startup_schema_registration,
        discover_known_databases_from_raft_storage, discover_known_databases_from_storage_roots,
        initialize_recovered_storage, shared_meta_plane_id,
    };
    use crate::ram::chunk::Chunks;
    use crate::ram::recovery::RecoverySummary;

    #[test]
    fn discovers_only_databases_with_recoverable_raft_state() {
        let temp = tempfile::TempDir::new().expect("tempdir should be created");
        let raft_root = temp.path();
        std::fs::create_dir_all(raft_root.join("databases/default"))
            .expect("default db dir should be created");
        std::fs::create_dir_all(raft_root.join("databases/wikidata"))
            .expect("wikidata db dir should be created");
        std::fs::create_dir_all(raft_root.join("databases/analytics"))
            .expect("analytics db dir should be created");
        std::fs::write(raft_root.join("databases/analytics/log.dat"), b"raft log")
            .expect("analytics log should be created");

        let discovered = discover_known_databases_from_raft_storage(raft_root.to_str(), "default");

        assert_eq!(
            discovered,
            vec!["analytics".to_string(), "default".to_string()]
        );
    }

    #[test]
    fn falls_back_to_default_database_when_raft_root_missing() {
        let discovered = discover_known_databases_from_raft_storage(None, "default");
        assert_eq!(discovered, vec!["default".to_string()]);
    }

    #[test]
    fn discovers_databases_from_scoped_storage_roots() {
        let temp = tempfile::TempDir::new().expect("tempdir should be created");
        let backup_root = temp.path().join("backup");
        let wal_root = temp.path().join("wal");
        std::fs::create_dir_all(backup_root.join("databases/default"))
            .expect("default backup dir should be created");
        std::fs::create_dir_all(backup_root.join("databases/wikidata"))
            .expect("wikidata backup dir should be created");
        std::fs::create_dir_all(wal_root.join("databases/analytics"))
            .expect("analytics wal dir should be created");

        let discovered = discover_known_databases_from_storage_roots(
            &[backup_root.to_str(), wal_root.to_str()],
            "default",
        );

        assert_eq!(
            discovered,
            vec![
                "analytics".to_string(),
                "default".to_string(),
                "wikidata".to_string()
            ]
        );
    }

    #[test]
    fn startup_schema_registration_unions_raft_and_non_raft_databases() {
        let temp = tempfile::TempDir::new().expect("tempdir should be created");
        let raft_root = temp.path().join("raft");
        let backup_root = temp.path().join("backup");
        let wal_root = temp.path().join("wal");
        std::fs::create_dir_all(raft_root.join("databases/default"))
            .expect("default raft dir should be created");
        std::fs::create_dir_all(raft_root.join("databases/catalog_only"))
            .expect("catalog_only raft dir should be created");
        std::fs::write(
            raft_root.join("databases/catalog_only/log.dat"),
            b"raft log",
        )
        .expect("catalog_only log should be created");
        std::fs::create_dir_all(backup_root.join("databases/data_only"))
            .expect("data_only backup dir should be created");
        std::fs::create_dir_all(wal_root.join("databases/data_only"))
            .expect("data_only wal dir should be created");

        let discovered = discover_databases_for_startup_schema_registration(
            raft_root.to_str(),
            &[backup_root.to_str(), wal_root.to_str()],
            "default",
        );

        assert_eq!(
            discovered,
            vec![
                "catalog_only".to_string(),
                "data_only".to_string(),
                "default".to_string()
            ]
        );
    }

    #[test]
    fn database_meta_plane_seed_nodes_always_include_local_server() {
        let seeds =
            database_meta_plane_seed_nodes("127.0.0.1:7000", &["127.0.0.1:7001".to_string()]);

        assert_eq!(
            seeds,
            vec!["127.0.0.1:7000".to_string(), "127.0.0.1:7001".to_string()]
        );
    }

    #[test]
    fn shared_meta_plane_is_type2() {
        let plane_id = shared_meta_plane_id("group_a");
        assert!(plane_id.is_type2());
        assert_eq!(plane_id.raw(), 1);
    }

    #[test]
    fn database_meta_plane_never_uses_reserved_shared_id() {
        let plane_id = database_meta_plane_id("group_a", "analytics");
        assert!(plane_id.is_type2());
        assert_ne!(plane_id.raw(), 1);
    }

    #[test]
    fn storage_recovery_observes_clock_before_undo_and_sets_floor_after() {
        let chunks = Chunks::new_dummy(2, crate::ram::segs::SEGMENT_SIZE);
        let recovered_max = chunks.revision_clock().try_now().unwrap().ts + 100;

        let (compensation_ts, floor) = initialize_recovered_storage(
            &chunks,
            RecoverySummary {
                max_revision_ts: recovered_max,
            },
            || {
                assert!(
                    chunks
                        .list
                        .iter()
                        .all(|chunk| chunk.history.recovery_floor() == 0),
                    "the recovery floor must not exist while undo is running"
                );
                let compensation_ts = chunks.revision_clock().try_now().unwrap().ts;
                assert!(compensation_ts > recovered_max);
                Ok(compensation_ts)
            },
        )
        .unwrap();

        assert!(floor > compensation_ts);
        assert!(
            chunks
                .list
                .iter()
                .all(|chunk| chunk.history.recovery_floor() == floor),
            "every chunk must receive the same post-undo recovery floor"
        );
    }

    #[test]
    fn storage_recovery_refuses_an_exhausted_recovered_clock_before_undo() {
        let chunks = Chunks::new_dummy(1, crate::ram::segs::SEGMENT_SIZE);
        let undo_ran = std::sync::atomic::AtomicBool::new(false);

        let result = initialize_recovered_storage(
            &chunks,
            RecoverySummary {
                max_revision_ts: u64::MAX,
            },
            || {
                undo_ran.store(true, std::sync::atomic::Ordering::Release);
                Ok(())
            },
        );

        assert!(matches!(
            result,
            Err(super::ServerError::CannotRecoverStorage(_))
        ));
        assert!(!undo_ran.load(std::sync::atomic::Ordering::Acquire));
        assert_eq!(chunks.list[0].history.recovery_floor(), 0);
    }
}

#[derive(Debug)]
pub enum ServerError {
    CannotJoinCluster,
    CannotJoinClusterRejected(String),
    CannotJoinClusterGroup(sm_master::ExecError),
    CannotJoinClusterGroupRejected(String),
    CannotInitMemberTable,
    CannotSetServerWeight,
    CannotInitConsistentHashTable,
    CannotLoadMetaClient,
    CannotAcquireStorageLock(String),
    CannotInitializeSharedPlane(String),
    CannotInitializeDatabaseCatalog(sm_master::ExecError),
    CannotInitializeDatabaseServices(String),
    CannotInitializeSchemaServer(sm_master::ExecError),
    CannotInitializeSchemaPlane(String),
    CannotRecoverStorage(String),
    StandaloneMustAlsoBeMetaServer,
}

impl std::fmt::Display for ServerError {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        match self {
            ServerError::CannotJoinCluster => write!(f, "cannot join cluster"),
            ServerError::CannotJoinClusterRejected(error) => {
                write!(f, "cannot join cluster: {error}")
            }
            ServerError::CannotJoinClusterGroup(error) => {
                write!(f, "cannot join cluster group: {error:?}")
            }
            ServerError::CannotJoinClusterGroupRejected(error) => {
                write!(f, "cannot join cluster group: {error}")
            }
            ServerError::CannotInitMemberTable => write!(f, "cannot initialize member table"),
            ServerError::CannotSetServerWeight => write!(f, "cannot set server weight"),
            ServerError::CannotInitConsistentHashTable => {
                write!(f, "cannot initialize consistent hash table")
            }
            ServerError::CannotLoadMetaClient => write!(f, "cannot load meta client"),
            ServerError::CannotAcquireStorageLock(error) => write!(f, "{error}"),
            ServerError::CannotInitializeSharedPlane(error) => {
                write!(f, "cannot initialize shared plane: {error}")
            }
            ServerError::CannotInitializeDatabaseCatalog(error) => {
                write!(f, "cannot initialize database catalog: {error:?}")
            }
            ServerError::CannotInitializeDatabaseServices(error) => {
                write!(f, "cannot initialize database services: {error}")
            }
            ServerError::CannotInitializeSchemaServer(error) => {
                write!(f, "cannot initialize schema server: {error:?}")
            }
            ServerError::CannotInitializeSchemaPlane(error) => {
                write!(f, "cannot initialize schema plane: {error}")
            }
            ServerError::CannotRecoverStorage(error) => {
                write!(f, "cannot recover storage: {error}")
            }
            ServerError::StandaloneMustAlsoBeMetaServer => {
                write!(f, "standalone server must also be a meta server")
            }
        }
    }
}

impl std::error::Error for ServerError {}

fn recovery_clock_exhausted() -> ServerError {
    ServerError::CannotRecoverStorage("recovered revision clock is exhausted".to_string())
}

fn initialize_recovered_storage<T, F>(
    chunks: &Arc<Chunks>,
    recovery: crate::ram::recovery::RecoverySummary,
    recover_undo: F,
) -> Result<(T, u64), ServerError>
where
    F: FnOnce() -> Result<T, ServerError>,
{
    chunks
        .revision_clock()
        .try_observe(bifrost::hlc::Hlc {
            ts: recovery.max_revision_ts,
            node: chunks.revision_clock().node(),
        })
        .map_err(|_| recovery_clock_exhausted())?;

    let undo = recover_undo()?;
    let recovery_floor = chunks
        .establish_recovery_floor()
        .map_err(|_| recovery_clock_exhausted())?;
    info!("MVCC recovery snapshot floor: {}", recovery_floor);
    Ok((undo, recovery_floor))
}

#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct ServerOptions {
    pub chunk_size: usize,
    pub db_size: usize,
    #[serde(default = "default_history_retention_ms")]
    pub history_retention_ms: u64,
    pub tiered_config: Option<crate::ram::tiered::TieredConfig>,
    pub backup_storage: Option<String>,
    pub wal_storage: Option<String>,
    pub undo_log_storage: Option<String>,
    pub raft_storage: Option<String>,
    pub services: Vec<Service>,
    pub index_enabled: bool,
    pub enable_recovery: bool,
    #[serde(default)]
    pub disable_storage_locks: bool,
}

fn default_history_retention_ms() -> u64 {
    300_000
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
    pub group_name: String,
    pub database_name: String,
    _storage_locks: Arc<StorageDirectoryLocks>,
    pub chunks: Arc<Chunks>,
    pub meta: Arc<ServerMeta>,
    pub cleaner: Arc<Cleaner>,
    pub indexer: Option<Arc<IndexBuilder>>,
    pub undo_log: Option<Arc<transactions::undo_log::UndoLogger>>,
    pub txn_manager: Option<Arc<transactions::manager::TransactionManager>>,
    pub rpc: Arc<rpc::Server>,
    pub consh: Arc<ConsistentHashing>,
    pub membership: Arc<ObserverClient>,
    pub raft_client: Arc<RaftClient>,
    pub neb_client: Arc<AsyncClient>,
}

impl DatabaseRuntime {
    #[allow(clippy::too_many_arguments)]
    pub fn new(
        group_name: &str,
        database_name: &str,
        storage_locks: Arc<StorageDirectoryLocks>,
        chunks: Arc<Chunks>,
        meta: Arc<ServerMeta>,
        cleaner: Arc<Cleaner>,
        indexer: Option<Arc<IndexBuilder>>,
        undo_log: Option<Arc<transactions::undo_log::UndoLogger>>,
        txn_manager: Option<Arc<transactions::manager::TransactionManager>>,
        rpc: Arc<rpc::Server>,
        consh: Arc<ConsistentHashing>,
        membership: Arc<ObserverClient>,
        raft_client: Arc<RaftClient>,
        neb_client: Arc<AsyncClient>,
    ) -> Self {
        Self {
            group_name: group_name.to_string(),
            database_name: database_name.to_string(),
            _storage_locks: storage_locks,
            chunks,
            meta,
            cleaner,
            indexer,
            undo_log,
            txn_manager,
            rpc,
            consh,
            membership,
            raft_client,
            neb_client,
        }
    }

    pub fn group_name(&self) -> &str {
        &self.group_name
    }

    pub fn database_name(&self) -> &str {
        &self.database_name
    }

    pub fn chunks(&self) -> &Arc<Chunks> {
        &self.chunks
    }

    pub fn meta(&self) -> &Arc<ServerMeta> {
        &self.meta
    }

    pub fn schemas(&self) -> &LocalSchemasCache {
        &self.meta.schemas
    }

    pub fn cleaner(&self) -> &Arc<Cleaner> {
        &self.cleaner
    }

    pub fn indexer(&self) -> Option<&Arc<IndexBuilder>> {
        self.indexer.as_ref()
    }

    pub fn undo_log(&self) -> Option<&Arc<transactions::undo_log::UndoLogger>> {
        self.undo_log.as_ref()
    }

    pub fn txn_manager(&self) -> Option<&Arc<transactions::manager::TransactionManager>> {
        self.txn_manager.as_ref()
    }

    pub fn indexed_data_client(&self) -> IndexedDataClient {
        if let Some(index_builder) = self.indexer() {
            IndexedDataClient::new_with_indexers(index_builder.clients.clone(), self.consh.clone())
        } else {
            let meta_plane_client = self.raft_client.plane(database_meta_plane_id(
                &self.group_name,
                &self.database_name,
            ));
            IndexedDataClient::new(&self.neb_client, &self.consh, &meta_plane_client)
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

pub struct NebServer {
    pub database_runtime: Arc<DatabaseRuntime>,
    database_runtimes: RwLock<HashMap<String, Arc<DatabaseRuntime>>>,
    registered_schema_services: RwLock<HashSet<String>>,
    runtime_init_lock: tokio::sync::Mutex<()>,
    host_options: ServerOptions,
    /// Shared physical-memory budget for all databases on this server.
    /// `None` when tiered memory is disabled.
    shared_memory_pool: Option<Arc<crate::ram::tiered::SharedMemoryPool>>,
    /// Shared tiered-memory manager coordinating global eviction.
    shared_tiered_manager: Option<Arc<crate::ram::tiered::manager::TieredMemoryManager>>,
    meta_servers: Vec<String>,
    pub rpc: Arc<rpc::Server>,
    pub consh: Arc<ConsistentHashing>,
    pub membership: Arc<ObserverClient>,
    pub member_pool: Arc<rpc::ClientPool>,
    /// Per-server Hybrid Logical Clock source (node = server_id), shared by
    /// every database's transaction manager (coordinator) and data manager
    /// (participant) hosted on this server. Sources transaction ids and the
    /// clock stamps carried on the transaction layer.
    pub hlc: Arc<bifrost::hlc::HlcSource>,
    pub raft_service: Arc<raft::RaftService>,
    pub raft_client: Arc<RaftClient>,
    pub server_id: u64,
    pub group_name: String,
    pub database_name: String,
    pub neb_client: Arc<AsyncClient>,
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

fn is_meta_cluster_bootstrap_seed(server_addr: &str, meta_servers: &[String]) -> bool {
    meta_servers
        .first()
        .map(|candidate| candidate == server_addr)
        .unwrap_or(true)
}

async fn join_or_bootstrap_meta_cluster(
    raft_service: &Arc<raft::RaftService>,
    server_addr: &String,
    meta_members: &Vec<String>,
    meta_servers: &Vec<String>,
) -> Result<(), ServerError> {
    if meta_members.is_empty() {
        debug!("No existing state and no other members, bootstrapping new cluster");
        raft_service.bootstrap().await;
        return Ok(());
    }

    let bootstrap_seed = is_meta_cluster_bootstrap_seed(server_addr, meta_servers);
    if bootstrap_seed {
        info!(
            "No existing state, bootstrapping fresh meta cluster as configured seed {}",
            server_addr
        );
        raft_service.bootstrap().await;
        return Ok(());
    }

    let max_retries = META_CLUSTER_JOIN_MAX_RETRIES;
    let mut last_error = None;

    for attempt in 0..max_retries {
        debug!(
            "No existing state, joining cluster with members: {:?}",
            meta_members
        );
        let join_result = raft_service.join(meta_members).await;
        match join_result {
            Ok(true) => {
                info!(
                    "Joined meta cluster, number of members: {}",
                    raft_service.num_members().await
                );
                return Ok(());
            }
            Ok(false) => {
                last_error = Some("join returned false".to_string());
            }
            Err(error) => {
                last_error = Some(format!("{error:?}"));
            }
        }

        if attempt + 1 < max_retries {
            tokio::time::sleep(tokio::time::Duration::from_millis(
                META_CLUSTER_JOIN_RETRY_DELAY_MS,
            ))
            .await;
        }
    }

    Err(ServerError::CannotJoinClusterRejected(format!(
        "non-seed member {server_addr:?} refused to bootstrap a fresh cluster after {max_retries} join attempts; last_error={}",
        last_error.unwrap_or_else(|| "none".to_string())
    )))
}

async fn join_cluster_group_with_retry(
    member_service: &Arc<MemberService>,
    server_addr: &String,
    group_name: &String,
) -> Result<(), ServerError> {
    let mut last_error = None;
    for attempt in 0..CLUSTER_GROUP_JOIN_MAX_RETRIES {
        match member_service.join(server_addr).await {
            Ok(_) => match member_service.join_group(group_name).await {
                Ok(true) => return Ok(()),
                Ok(false) => {
                    last_error = Some(format!(
                        "membership state rejected group {group_name:?} for {server_addr:?}"
                    ));
                }
                Err(error) => {
                    last_error = Some(format!("{error:?}"));
                }
            },
            Err(error) => {
                last_error = Some(format!("{error:?}"));
            }
        }

        if attempt + 1 < CLUSTER_GROUP_JOIN_MAX_RETRIES {
            tokio::time::sleep(tokio::time::Duration::from_millis(
                CLUSTER_GROUP_JOIN_RETRY_DELAY_MS,
            ))
            .await;
        }
    }

    Err(ServerError::CannotJoinClusterGroupRejected(format!(
        "group {group_name:?} did not accept member {server_addr:?} after {CLUSTER_GROUP_JOIN_MAX_RETRIES} attempts; last_error={}",
        last_error.unwrap_or_else(|| "none".to_string())
    )))
}

impl NebServer {
    pub async fn ensure_database_meta_plane_membership(
        &self,
        database_name: &str,
    ) -> Result<(), ServerError> {
        ensure_shared_meta_plane(
            &self.raft_service,
            &self.raft_client,
            &self.rpc.address,
            &self.meta_servers,
            &self.group_name,
        )
        .await?;
        ensure_database_meta_plane(
            &self.raft_service,
            &self.raft_client,
            &self.rpc.address,
            &self.meta_servers,
            &self.group_name,
            database_name,
        )
        .await?;
        Ok(())
    }

    async fn register_schema_state_machine(
        group_name: &str,
        database_name: &str,
        plane: &raft::PlaneHandle,
    ) -> Result<(), ServerError> {
        register_schema_state_machine_on_plane(
            group_name,
            database_name,
            plane,
            Arc::new(std::sync::atomic::AtomicBool::new(false)),
        )
        .await
        .map_err(|e| {
            ServerError::CannotInitializeSchemaPlane(format!(
                "failed to register schema state machine for {database_name}: {e}"
            ))
        })
    }

    #[allow(clippy::too_many_arguments)]
    async fn build_database_runtime(
        opts: &ServerOptions,
        server_addr: &String,
        meta_members: &Vec<String>,
        group_name: &String,
        database_name: &str,
        tiered_manager: Option<Arc<crate::ram::tiered::manager::TieredMemoryManager>>,
        rpc_server: &Arc<rpc::Server>,
        raft_service: &Arc<raft::RaftService>,
        raft_client: &Arc<RaftClient>,
        membership_client: &Arc<ObserverClient>,
        conshasing: &Arc<ConsistentHashing>,
        member_pool: &Arc<rpc::ClientPool>,
        hlc: &Arc<bifrost::hlc::HlcSource>,
        register_schema_state_machine: bool,
        pre_acquired_storage_locks: Option<Arc<StorageDirectoryLocks>>,
    ) -> Result<Arc<DatabaseRuntime>, ServerError> {
        let _shared_meta_plane = ensure_shared_meta_plane(
            raft_service,
            raft_client,
            server_addr,
            meta_members,
            group_name,
        )
        .await?;
        let schema_plane = ensure_database_meta_plane(
            raft_service,
            raft_client,
            server_addr,
            meta_members,
            group_name,
            database_name,
        )
        .await?;
        if register_schema_state_machine {
            Self::register_schema_state_machine(group_name, database_name, &schema_plane).await?;
            schema_plane.recover_after_register().await.map_err(|e| {
                ServerError::CannotInitializeSchemaPlane(format!(
                    "failed to recover schema state machine for {database_name}: {e}"
                ))
            })?;
        }

        let storage_layout =
            database::DatabaseStorageLayout::from_options(opts, group_name, database_name);
        let storage_locks = match pre_acquired_storage_locks {
            Some(locks) => locks,
            None => Arc::new(
                StorageDirectoryLocks::acquire(&storage_layout, opts.disable_storage_locks)
                    .map_err(|e| ServerError::CannotAcquireStorageLock(e.to_string()))?,
            ),
        };
        let effective_opts = ServerOptions {
            backup_storage: storage_layout.backup_storage,
            wal_storage: storage_layout.wal_storage,
            undo_log_storage: storage_layout.undo_log_storage,
            raft_storage: storage_layout.raft_storage,
            ..opts.clone()
        };

        let schema_plane_client =
            raft_client.plane(database_meta_plane_id(group_name, database_name));
        let mut last_schema_cache_error = None;
        let schemas = 'schema_cache: loop {
            for attempt in 0..LOCAL_SCHEMA_CACHE_INIT_MAX_RETRIES {
                match LocalSchemasCache::new_for_database(
                    group_name,
                    database_name,
                    &schema_plane_client,
                )
                .await
                {
                    Ok(schemas) => break 'schema_cache schemas,
                    Err(error) => {
                        last_schema_cache_error = Some(format!("{error:?}"));
                        if attempt + 1 < LOCAL_SCHEMA_CACHE_INIT_MAX_RETRIES {
                            tokio::time::sleep(tokio::time::Duration::from_millis(
                                LOCAL_SCHEMA_CACHE_INIT_RETRY_DELAY_MS,
                            ))
                            .await;
                        }
                    }
                }
            }
            return Err(ServerError::CannotInitializeSchemaPlane(format!(
                "failed to initialize local schema cache for {group_name}/{database_name}: {}",
                last_schema_cache_error.unwrap_or_else(|| "unknown error".to_string())
            )));
        };
        let meta_rc = Arc::new(ServerMeta { schemas });
        let neb_client = Arc::new(
            client::AsyncClient::new_for_database(
                rpc_server,
                membership_client,
                meta_members,
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

        if effective_opts.index_enabled {
            meta_rc
                .schemas
                .register_internal_schema(crate::index::full_text::shard::inverted_segment_schema());
            meta_rc
                .schemas
                .register_internal_schema(crate::index::full_text::inverted_stats_schema());
            debug!(
                "Registered inverted index schemas before recovery for database {}",
                database_name
            );
        }

        let index_builder = if effective_opts.index_enabled {
            Some(Arc::new(
                IndexBuilder::new(
                    &neb_client,
                    conshasing,
                    &schema_plane_client,
                    rpc_server.server_id,
                )
                .await,
            ))
        } else {
            None
        };

        let (chunks, recovery) = Chunks::try_new_with_recovery_and_clock(
            effective_opts.db_size / effective_opts.chunk_size,
            effective_opts.chunk_size,
            meta_rc.clone(),
            index_builder.clone(),
            effective_opts.backup_storage.clone(),
            effective_opts.wal_storage.clone(),
            tiered_manager,
            effective_opts.enable_recovery,
            effective_opts.raft_storage.clone(),
            hlc.clone(),
            effective_opts.history_retention_ms,
        )
        .map_err(|error| ServerError::CannotRecoverStorage(error.to_string()))?;

        let undo_log = if effective_opts.enable_recovery {
            initialize_recovered_storage(&chunks, recovery, || {
                let Some(undo_log_path) = effective_opts.undo_log_storage.as_ref() else {
                    return Ok(None);
                };
                let log = transactions::undo_log::UndoLogger::new(undo_log_path.clone())
                    .map_err(|error| ServerError::CannotRecoverStorage(error.to_string()))?;
                let txn_index = log
                    .recover()
                    .map_err(|error| ServerError::CannotRecoverStorage(error.to_string()))?;
                log.rollback_incomplete_transactions(txn_index, &chunks)
                    .map_err(|error| ServerError::CannotRecoverStorage(error.to_string()))?;
                Ok(Some(log))
            })?
            .0
        } else if let Some(ref undo_log_path) = effective_opts.undo_log_storage {
            match transactions::undo_log::UndoLogger::new(undo_log_path.clone()) {
                Ok(log) => Some(log),
                Err(error) => {
                    error!("Failed to initialize undo log: {:?}", error);
                    None
                }
            }
        } else {
            None
        };

        if let Some(ref index_builder) = index_builder {
            index_builder.initialize_inverted_indexer(&chunks);
        }

        let cleaner = if effective_opts.enable_recovery {
            debug!(
                "Recovery enabled: Starting cleaner in PAUSED state for database {}",
                database_name
            );
            Arc::new(Cleaner::new_paused(chunks.clone()))
        } else {
            Arc::new(Cleaner::new_and_start(chunks.clone()))
        };

        let transaction_runtime = Arc::new(DatabaseRuntime::new(
            group_name,
            database_name,
            storage_locks.clone(),
            chunks.clone(),
            meta_rc.clone(),
            cleaner.clone(),
            index_builder.clone(),
            undo_log.clone(),
            None,
            rpc_server.clone(),
            conshasing.clone(),
            membership_client.clone(),
            raft_client.clone(),
            neb_client.clone(),
        ));

        let transaction_manager = if effective_opts.services.contains(&Service::Transaction) {
            Some(
                init_txn_manager(
                    rpc_server,
                    &transaction_runtime,
                    rpc_server.server_id,
                    conshasing,
                    member_pool,
                    hlc,
                )
                .await,
            )
        } else {
            None
        };

        let database_runtime = Arc::new(DatabaseRuntime::new(
            group_name,
            database_name,
            storage_locks,
            chunks.clone(),
            meta_rc.clone(),
            cleaner,
            index_builder.clone(),
            undo_log,
            transaction_manager,
            rpc_server.clone(),
            conshasing.clone(),
            membership_client.clone(),
            raft_client.clone(),
            neb_client.clone(),
        ));

        let servs = proc_services(&effective_opts.services);
        for service in servs {
            match service {
                Service::Cell => {
                    init_cell_rpc_service(
                        rpc_server,
                        database_runtime.clone(),
                        database_runtime.neb_client.clone(),
                    )
                    .await;
                }
                Service::Transaction | Service::HashIndexer => {
                    init_txn_data_site_service(rpc_server, database_runtime.clone(), hlc.clone())
                        .await
                }
                Service::RangedIndexer => {
                    wait_for_scoped_cell_rpc_services(
                        server_addr,
                        meta_members,
                        conshasing,
                        group_name,
                        database_name,
                    )
                    .await?;
                    let tree_path = effective_opts
                        .raft_storage
                        .as_ref()
                        .map(|p| format!("{}/master_tree.dat", p));
                    init_ranged_indexer_service(
                        rpc_server,
                        &database_runtime.neb_client,
                        raft_service,
                        &schema_plane,
                        &schema_plane_client,
                        conshasing,
                        group_name,
                        database_name,
                        tree_path,
                    )
                    .await
                }
                Service::Query => {}
            }
        }

        if effective_opts.index_enabled {
            init_inverted_index_rpc_service(
                rpc_server,
                database_runtime.group_name(),
                database_runtime.database_name(),
                database_runtime.indexer.clone(),
            )
            .await;
        }

        debug!(
            "Built database runtime {} for group {} on host {}",
            database_name, group_name, server_addr
        );
        Ok(database_runtime)
    }

    pub fn database(&self, database_name: &str) -> Option<Arc<DatabaseRuntime>> {
        self.database_runtimes
            .read()
            .ok()
            .and_then(|runtimes| runtimes.get(database_name).cloned())
    }

    pub fn current_database(&self) -> Arc<DatabaseRuntime> {
        self.database_runtime.clone()
    }

    pub async fn ensure_database_runtime(
        &self,
        database_name: &str,
    ) -> Result<Arc<DatabaseRuntime>, ServerError> {
        if let Some(runtime) = self.database(database_name) {
            return Ok(runtime);
        }

        let _guard = self.runtime_init_lock.lock().await;
        if let Some(runtime) = self.database(database_name) {
            return Ok(runtime);
        }

        let needs_schema_registration = !self
            .registered_schema_services
            .read()
            .expect("schema service registry lock poisoned")
            .contains(database_name);

        let database_runtime = Self::build_database_runtime(
            &self.host_options,
            &self.rpc.address,
            &self.meta_servers,
            &self.group_name,
            database_name,
            self.shared_tiered_manager.clone(),
            &self.rpc,
            &self.raft_service,
            &self.raft_client,
            &self.membership,
            &self.consh,
            &self.member_pool,
            &self.hlc,
            needs_schema_registration,
            None,
        )
        .await?;

        if needs_schema_registration {
            self.registered_schema_services
                .write()
                .expect("schema service registry lock poisoned")
                .insert(database_name.to_string());
        }

        self.database_runtimes
            .write()
            .expect("database runtime registry lock poisoned")
            .insert(database_name.to_string(), database_runtime.clone());

        Ok(database_runtime)
    }

    pub async fn unload_database_runtime(&self, database_name: &str) -> bool {
        if database_name == self.database_name() {
            return false;
        }
        self.unload_database_runtime_unchecked(database_name).await
    }

    /// Unload a database runtime, bypassing the default-database protection.
    /// Used when intentionally resetting the default database.
    pub async fn unload_database_runtime_unchecked(&self, database_name: &str) -> bool {
        let runtime = self
            .database_runtimes
            .write()
            .expect("database runtime registry lock poisoned")
            .remove(database_name);

        let Some(runtime) = runtime else {
            return false;
        };

        if let Some(ref manager) = runtime.chunks().tiered_manager {
            manager.unregister_chunks(runtime.chunks());
        }

        if runtime.indexer().is_some() {
            let _ = IndexBuilder::await_all_indices().await;

            if let Some(indexer) = runtime.indexer() {
                if let Err(e) = indexer.graceful_shutdown().await {
                    warn!(
                        "Failed to gracefully shut down fulltext indexer for {}/{}: {:?}",
                        runtime.group_name(),
                        runtime.database_name(),
                        e
                    );
                }
            }
        }

        runtime.cleaner().stop();

        self.rpc
            .remove_service(cell_rpc::generate_scoped_service_id(
                runtime.group_name(),
                runtime.database_name(),
            ))
            .await;

        self.rpc
            .remove_service(transactions::manager::generate_scoped_service_id(
                runtime.group_name(),
                runtime.database_name(),
            ))
            .await;

        self.rpc
            .remove_service(transactions::data_site::generate_scoped_service_id(
                runtime.group_name(),
                runtime.database_name(),
            ))
            .await;

        self.rpc
            .remove_service(ranged::tree::service::generate_scoped_service_id(
                runtime.group_name(),
                runtime.database_name(),
            ))
            .await;

        self.rpc
            .remove_service(crate::index::full_text::rpc::generate_scoped_service_id(
                runtime.group_name(),
                runtime.database_name(),
            ))
            .await;

        true
    }

    pub fn delete_database_storage(&self, database_name: &str) -> Result<(), String> {
        if database_name == self.database_name() {
            return Err("cannot delete storage for the default database runtime".to_string());
        }
        self.delete_database_storage_unchecked(database_name)
    }

    /// Delete database storage, bypassing the default-database protection.
    /// Used when intentionally resetting the default database.
    pub fn delete_database_storage_unchecked(&self, database_name: &str) -> Result<(), String> {
        let layout = database::DatabaseStorageLayout::from_options(
            &self.host_options,
            &self.group_name,
            database_name,
        );
        let mut storage_roots = HashSet::new();
        storage_roots.extend(layout.backup_storage);
        storage_roots.extend(layout.wal_storage);
        storage_roots.extend(layout.undo_log_storage);
        storage_roots.extend(layout.raft_storage);

        for storage_root in storage_roots {
            if !Path::new(&storage_root).exists() {
                continue;
            }

            std::fs::remove_dir_all(&storage_root)
                .map_err(|e| format!("failed to remove database storage {storage_root}: {e}"))?;
        }

        Ok(())
    }

    pub fn database_names(&self) -> Vec<String> {
        self.database_runtimes
            .read()
            .map(|runtimes| runtimes.keys().cloned().collect())
            .unwrap_or_default()
    }

    pub fn database_runtime(&self) -> &DatabaseRuntime {
        self.database_runtime.as_ref()
    }

    pub fn chunks(&self) -> &Arc<Chunks> {
        self.database_runtime.chunks()
    }

    pub fn meta(&self) -> &Arc<ServerMeta> {
        self.database_runtime.meta()
    }

    pub fn cleaner(&self) -> &Arc<Cleaner> {
        self.database_runtime.cleaner()
    }

    pub fn indexer(&self) -> Option<&Arc<IndexBuilder>> {
        self.database_runtime.indexer()
    }

    pub fn undo_log(&self) -> Option<&Arc<transactions::undo_log::UndoLogger>> {
        self.database_runtime.undo_log()
    }

    pub fn txn_manager(&self) -> Option<&Arc<transactions::manager::TransactionManager>> {
        self.database_runtime.txn_manager()
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

            // IndexBuilder::await_all_indices() joins the spawned index tasks, and ranged
            // tree inserts are applied directly by TreeService before the RPC resolves.
            // There is no separate Raft commit barrier to wait on here, so a fixed sleep
            // only makes shutdown liveness depend on timer scheduling.
            info!("Index tasks drained; proceeding to LSM flush");

            if let Some(indexer) = self.indexer() {
                if let Err(e) = indexer.graceful_shutdown().await {
                    warn!(
                        "Failed to gracefully shut down fulltext indexer for {}/{}: {:?}",
                        self.group_name, self.database_name, e
                    );
                }
            }
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
        } else {
            debug!("LSM tree service not available (likely not enabled)");
        }

        // flush_all() already awaits mark_migration() for each tree before returning, so
        // there is no additional asynchronous migration work to wait for here.

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
        ranged::tree::service::locate_tree_server_from_conshash(
            &dummy_id,
            &self.consh,
            &self.group_name,
            &self.database_name,
        )
        .await
    }

    pub async fn new(
        opts: &ServerOptions,
        server_addr: &String,
        meta_members: &Vec<String>,
        group_name: &String,
        database_name: &str,
        startup_storage_locks: Arc<StorageDirectoryLocks>,
        rpc_server: &Arc<rpc::Server>,
        raft_service: &Arc<raft::RaftService>,
        raft_client: &Arc<RaftClient>,
        membership_client: &Arc<ObserverClient>,
    ) -> Result<Arc<NebServer>, ServerError> {
        debug!(
            "Creating key-value server instance, group name {}",
            group_name
        );
        let conshasing = init_conshash(
            group_name,
            server_addr,
            opts.db_size as u64,
            raft_client,
            membership_client,
        )
        .await?;
        let member_pool = Arc::new(rpc::ClientPool::new());
        // One HLC source per server process (node = server_id), shared by every
        // database's transaction manager (coordinator) and data manager
        // (participant) hosted on this server.
        let hlc = Arc::new(bifrost::hlc::HlcSource::new(rpc_server.server_id));
        let shared_memory_pool = opts
            .tiered_config
            .as_ref()
            .or_else(|| None) // placeholder so or_else chain compiles cleanly
            .map(|c| crate::ram::tiered::SharedMemoryPool::new(c))
            .or_else(|| {
                crate::ram::tiered::TieredConfig::from_env()
                    .map(|c| crate::ram::tiered::SharedMemoryPool::new(&c))
            });
        let shared_tiered_manager = shared_memory_pool.as_ref().map(|pool| {
            Arc::new(crate::ram::tiered::manager::TieredMemoryManager::new(
                pool.clone(),
            ))
        });

        let database_runtime = Self::build_database_runtime(
            opts,
            server_addr,
            &meta_members,
            group_name,
            database_name,
            shared_tiered_manager.clone(),
            rpc_server,
            raft_service,
            raft_client,
            membership_client,
            &conshasing,
            &member_pool,
            &hlc,
            false,
            Some(startup_storage_locks),
        )
        .await?;

        let server = Arc::new(NebServer {
            database_runtime: database_runtime.clone(),
            database_runtimes: RwLock::new(HashMap::from([(
                database_name.to_string(),
                database_runtime.clone(),
            )])),
            registered_schema_services: RwLock::new(
                discover_databases_for_startup_schema_registration(
                    opts.raft_storage.as_deref(),
                    &[
                        opts.backup_storage.as_deref(),
                        opts.wal_storage.as_deref(),
                        opts.undo_log_storage.as_deref(),
                    ],
                    database_name,
                )
                .into_iter()
                .collect(),
            ),
            runtime_init_lock: tokio::sync::Mutex::new(()),
            host_options: opts.clone(),
            shared_memory_pool,
            shared_tiered_manager,
            meta_servers: meta_members.clone(),
            rpc: rpc_server.clone(),
            consh: conshasing.clone(),
            membership: membership_client.clone(),
            member_pool: member_pool.clone(),
            raft_service: raft_service.clone(),
            raft_client: raft_client.clone(),
            server_id: rpc_server.server_id,
            hlc,
            group_name: group_name.clone(),
            database_name: database_name.to_string(),
            neb_client: database_runtime.neb_client.clone(),
        });

        Ok(server)
    }

    pub async fn new_from_opts<'a, F: AsyncFnOnce(&Arc<raft::RaftService>)>(
        opts: &ServerOptions,
        server_addr: &'a str,
        group_name: &'a str,
        prepare_raft_service: F,
    ) -> Result<Arc<NebServer>, ServerError> {
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
    ) -> Result<Arc<NebServer>, ServerError> {
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
    ) -> Result<Arc<NebServer>, ServerError> {
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
    ) -> Result<Arc<NebServer>, ServerError> {
        debug!("Creating key-value server from options");
        let group_name = &String::from(group_name);
        let server_addr = &String::from(server_addr);
        let storage_layout =
            database::DatabaseStorageLayout::from_options(opts, group_name, database_name);
        let startup_storage_locks = Arc::new(
            StorageDirectoryLocks::acquire(&storage_layout, opts.disable_storage_locks)
                .map_err(|e| ServerError::CannotAcquireStorageLock(e.to_string()))?,
        );
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
        let startup_schema_databases = discover_databases_for_startup_schema_registration(
            opts.raft_storage.as_deref(),
            &[
                opts.backup_storage.as_deref(),
                opts.wal_storage.as_deref(),
                opts.undo_log_storage.as_deref(),
            ],
            database_name,
        );
        Weights::new_with_id(CONS_HASH_ID, &raft_service).await;

        // TODO: If RangedIndexer service is enabled, MasterTreeSM should also be
        // registered here before start() to enable WAL replay recovery

        rpc_server.register_service(&raft_service).await;
        Server::listen_and_resume(&rpc_server).await;

        // Register Membership service BEFORE Raft start for WAL replay
        debug!("Registering Membership service before Raft start");
        Membership::new(&rpc_server, &raft_service).await;

        let shared_meta_plane = raft_service
            .ensure_plane(raft::PlaneSpec {
                plane_id: shared_meta_plane_id(group_name),
            })
            .await
            .expect("shared meta plane should materialize locally during startup");
        register_shared_state_machines_on_plane(group_name, &shared_meta_plane)
            .await
            .expect("shared state machines should register during startup");

        register_schema_sms_for_known_databases(
            group_name,
            &startup_schema_databases,
            &raft_service,
            recovering_flag.clone(),
        )
        .await;

        debug!("Preparing raft service");
        prepare_raft_service(&raft_service).await;

        debug!("RPC server created, starting Raft service (will replay WAL to registered SMs)");
        raft::RaftService::start(&raft_service, true).await;

        recover_startup_meta_planes(group_name, &raft_service).await;
        recover_schema_sms_for_known_databases(
            group_name,
            &startup_schema_databases,
            &raft_service,
        )
        .await;

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
                info!(
                    "Single-server resumed cluster, waiting for leader election and state recovery..."
                );
                tokio::time::sleep(tokio::time::Duration::from_secs(5)).await;
            }
        } else {
            join_or_bootstrap_meta_cluster(&raft_service, server_addr, &meta_members, meta_servers)
                .await?;
        }
        debug!("Joined with members, membership service already started before Raft start");
        debug!("Starting raft client");
        let raft_client = RaftClient::new(meta_servers, raft::DEFAULT_SERVICE_ID)
            .await
            .map_err(|e| {
                error!("Failed to create Raft client: {:?}", e);
                error!("This may happen if resuming from disk without proper cluster state");
                ServerError::CannotLoadMetaClient
            })?;
        debug!("Prepare raft subscription");
        RaftClient::prepare_subscription(&rpc_server).await;
        debug!("Starting member service");
        let member_service = MemberService::new(server_addr, &raft_client, &raft_service).await;
        debug!("Member join group: {}", group_name);
        join_cluster_group_with_retry(&member_service, server_addr, group_name).await?;
        let membership_client = Arc::new(ObserverClient::new(&raft_client));
        debug!("Creating neb server");
        NebServer::new(
            opts,
            server_addr,
            &meta_servers,
            group_name,
            database_name,
            startup_storage_locks,
            &rpc_server,
            &raft_service,
            &raft_client,
            &membership_client,
        )
        .await
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
        self.database_runtime.indexed_data_client()
    }
    pub async fn data_client(&self, members: &Vec<String>) -> Result<AsyncClient, NebClientError> {
        self.database_runtime.data_client(members).await
    }
}

// Note: We intentionally do NOT implement Drop for NebServer because:
// 1. Drop is synchronous but shutdown() is async
// 2. Calling block_on from Drop within a tokio runtime causes deadlocks
// 3. Always call server.shutdown().await explicitly before the server goes out of scope
//
// In production: use signal handlers (SIGTERM, SIGINT) to call shutdown()
// In tests: always call server.shutdown().await at the end

pub async fn rpc_client_by_id(
    id: &Id,
    conshash: &Arc<ConsistentHashing>,
) -> Result<Arc<RPCClient>, RPCError> {
    let server_id = conshash.get_server_id(id.higher).unwrap();
    let conshash = conshash.clone();
    DEFAULT_CLIENT_POOL
        .get_by_id(server_id, move |sid| conshash.to_server_name(sid))
        .await
        .map_err(|e| RPCError::IOError(e))
}

pub async fn init_cell_rpc_service(
    rpc_server: &Arc<Server>,
    database_runtime: Arc<DatabaseRuntime>,
    neb_client: Arc<AsyncClient>,
) {
    rpc_server
        .register_service_with_id(
            cell_rpc::generate_scoped_service_id(
                database_runtime.group_name(),
                database_runtime.database_name(),
            ),
            &cell_rpc::NebRPCService::new(database_runtime, neb_client),
        )
        .await;
}

pub async fn init_txn_manager(
    rpc_server: &Arc<Server>,
    database_runtime: &Arc<DatabaseRuntime>,
    server_id: u64,
    consh: &Arc<ConsistentHashing>,
    member_pool: &Arc<ClientPool>,
    hlc: &Arc<bifrost::hlc::HlcSource>,
) -> Arc<TransactionManager> {
    let deps = Arc::new(transactions::manager::TransactionManagerDeps {
        database_runtime: database_runtime.clone(),
        server_id: server_id,
        consh: consh.clone(),
        member_pool: member_pool.clone(),
        hlc: hlc.clone(),
    });
    let txn_manager = transactions::manager::TransactionManager::new(deps);
    rpc_server
        .register_service_with_id(
            transactions::manager::generate_scoped_service_id(
                database_runtime.group_name(),
                database_runtime.database_name(),
            ),
            &txn_manager,
        )
        .await;
    return txn_manager;
}
pub async fn init_txn_data_site_service(
    rpc_server: &Arc<Server>,
    database_runtime: Arc<DatabaseRuntime>,
    hlc: Arc<bifrost::hlc::HlcSource>,
) {
    rpc_server
        .register_service_with_id(
            transactions::data_site::generate_scoped_service_id(
                database_runtime.group_name(),
                database_runtime.database_name(),
            ),
            &transactions::data_site::DataManager::new(database_runtime, hlc),
        )
        .await;
}

pub async fn init_inverted_index_rpc_service(
    rpc_server: &Arc<Server>,
    group_name: &str,
    database_name: &str,
    index_builder: Option<Arc<IndexBuilder>>,
) {
    if let Some(index_builder) = index_builder.as_ref() {
        if let Some(inverted_indexer) = index_builder.clients.fulltext_indexer() {
            use crate::index::full_text::rpc::InvertedIndexRPCService;
            let service = InvertedIndexRPCService::new(inverted_indexer.clone());
            rpc_server
                .register_service_with_id(
                    crate::index::full_text::rpc::generate_scoped_service_id(
                        group_name,
                        database_name,
                    ),
                    &service,
                )
                .await;
            info!("Registered inverted index RPC service");
        }
    }
}

pub async fn init_ranged_indexer_service<C>(
    rpc_server: &Arc<Server>,
    neb_client: &Arc<AsyncClient>,
    raft_svr: &Arc<raft::RaftService>,
    meta_plane: &raft::PlaneHandle,
    raft_client: &Arc<C>,
    cons_hash: &Arc<ConsistentHashing>,
    group_name: &str,
    database_name: &str,
    tree_persistence_path: Option<String>,
) where
    C: bifrost::raft::client::AsRaftPlaneClient + 'static,
{
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
        .register_service_with_id(
            ranged::tree::service::generate_scoped_service_id(group_name, database_name),
            &Arc::new(ranged::tree::service::TreeService::new(
                neb_client, &sm_client,
            )),
        )
        .await;

    // Create MasterTreeSM with persistence support
    let persistence_path = tree_persistence_path.map(PathBuf::from);
    let mut tree_sm = ranged::sm::MasterTreeSM::new_with_id_and_persistence_on_plane(
        ranged::sm::generate_scoped_sm_id(group_name, database_name),
        ranged::tree::service::generate_scoped_service_id(group_name, database_name),
        raft_svr,
        meta_plane.id(),
        cons_hash,
        persistence_path,
    );
    tree_sm.try_initialize().await;
    meta_plane
        .register_state_machine(Box::new(tree_sm))
        .await
        .unwrap();
    meta_plane.recover_after_register().await.unwrap();
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
