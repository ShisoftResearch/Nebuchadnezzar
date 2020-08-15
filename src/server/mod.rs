use bifrost::conshash::weights::Weights;
use bifrost::conshash::ConsistentHashing;
use bifrost::membership::member::MemberService;
use bifrost::membership::client::ObserverClient;
use bifrost::membership::server::Membership;
use bifrost::raft;
use bifrost::raft::client::RaftClient;
use bifrost::raft::state_machine::master as sm_master;
use bifrost::rpc;
use bifrost::rpc::DEFAULT_CLIENT_POOL;
use bifrost::rpc::{RPCClient, RPCError, Server};
use bifrost::vector_clock::ServerVectorClock;
use bifrost_plugins::hash_ident;
use crate::client;
use crate::client::AsyncClient;
use crate::index::lsmtree;
use crate::ram::chunk::Chunks;
use crate::ram::cleaner::Cleaner;
use crate::ram::schema::sm as schema_sm;
use crate::ram::schema::LocalSchemasCache;
use crate::ram::types::Id;
use std::io;
use std::sync::Arc;

pub mod cell_rpc;
pub mod transactions;
#[cfg(test)]
mod tests;

pub static CONS_HASH_ID: u64 = hash_ident!(NEB_CONSHASH_MEM_WEIGHTS) as u64;

#[derive(Debug)]
pub enum ServerError {
    CannotJoinCluster,
    CannotJoinClusterGroup(sm_master::ExecError),
    CannotInitMemberTable,
    CannotSetServerWeight,
    CannotInitConsistentHashTable,
    CannotLoadMetaClient,
    CannotInitializeSchemaServer(sm_master::ExecError),
    StandaloneMustAlsoBeMetaServer,
}

#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct ServerOptions {
    pub chunk_count: usize,
    pub memory_size: usize,
    pub backup_storage: Option<String>,
    pub wal_storage: Option<String>,
    pub services: Vec<Service>,
}

#[derive(Debug, Clone, Serialize, Deserialize)]
pub enum Service {
    Cell,
    Transaction,
    LSMTreeIndex,
}

pub struct ServerMeta {
    pub schemas: LocalSchemasCache,
}

pub struct NebServer {
    pub chunks: Arc<Chunks>,
    pub meta: Arc<ServerMeta>,
    pub rpc: Arc<rpc::Server>,
    pub consh: Arc<ConsistentHashing>,
    pub membership: Arc<ObserverClient>,
    pub member_pool: rpc::ClientPool,
    pub txn_peer: Peer,
    pub raft_service: Arc<raft::RaftService>,
    pub server_id: u64,
    pub cleaner: Cleaner,
}

pub async fn init_conshash(
    group_name: &String,
    address: &String,
    memory_size: u64,
    raft_client: &Arc<RaftClient>,
    membership: &Arc<ObserverClient>
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
    pub async fn new(
        opts: &ServerOptions,
        server_addr: &String,
        meta_members: &Vec<String>,
        group_name: &String,
        rpc_server: &Arc<rpc::Server>,
        raft_service: &Arc<raft::RaftService>,
        raft_client: &Arc<RaftClient>,
        membership_client: &Arc<ObserverClient>
    ) -> Result<Arc<NebServer>, ServerError> {
        debug!(
            "Creating key-value server instance, group name {}",
            group_name
        );
        raft_service.register_state_machine(Box::new(schema_sm::SchemasSM::new(
            group_name,
            raft_service,
        ).await)).await;
        Weights::new_with_id(CONS_HASH_ID, raft_service).await;
        let schemas = LocalSchemasCache::new(group_name, raft_client).await.unwrap();
        let meta_rc = Arc::new(ServerMeta { schemas });
        let chunks = Chunks::new(
            opts.chunk_count,
            opts.memory_size,
            meta_rc.clone(),
            opts.backup_storage.clone(),
            opts.wal_storage.clone(),
        );
        let cleaner = Cleaner::new_and_start(chunks.clone());
        let conshasing = init_conshash(
            group_name,
            server_addr,
            opts.memory_size as u64,
            raft_client,
            membership_client
        ).await?;
        let server = Arc::new(NebServer {
            chunks,
            cleaner,
            meta: meta_rc,
            rpc: rpc_server.clone(),
            consh: conshasing.clone(),
            membership: membership_client.clone(),
            member_pool: rpc::ClientPool::new(),
            txn_peer: Peer::new(server_addr),
            raft_service: raft_service.clone(),
            server_id: rpc_server.server_id,
        });
        let client =
            Arc::new(client::AsyncClient::new(&server.rpc, membership_client, &meta_members, group_name).await.unwrap());
        for service in &opts.services {
            match service {
                &Service::Cell => init_cell_rpc_service(rpc_server, &server).await,
                &Service::Transaction => init_txn_service(rpc_server, &server).await,
                &Service::LSMTreeIndex => init_lsm_tree_index_service(
                    rpc_server,
                    &server,
                    &client,
                    raft_service,
                    raft_client,
                    &conshasing,
                ).await,
            }
        }

        Ok(server)
    }

    pub async fn new_from_opts<'a>(
        opts: &ServerOptions,
        server_addr: &'a str,
        group_name: &'a str,
    ) -> Arc<NebServer> {
        Self::new_cluster_from_opts(opts, server_addr, &vec![server_addr.to_owned()], group_name).await
    }

    pub async fn new_cluster_from_opts<'a>(
        opts: &ServerOptions,
        server_addr: &'a str,
        meta_servers: &Vec<String>,
        group_name: &'a str,
    ) -> Arc<NebServer> {
        debug!("Creating key-value server from options");
        let group_name = &String::from(group_name);
        let server_addr = &String::from(server_addr);
        debug!("Creating RPC server and listen");
        let rpc_server = rpc::Server::new(server_addr);
        let meta_members: Vec<_> = meta_servers.iter().filter(|n| *n != server_addr).cloned().collect();
        let raft_service = raft::RaftService::new(raft::Options {
            storage: raft::Storage::MEMORY,
            address: server_addr.to_owned(),
            service_id: raft::DEFAULT_SERVICE_ID,
        });
        rpc_server.register_service(raft::DEFAULT_SERVICE_ID, &raft_service).await;
        Server::listen_and_resume(&rpc_server).await;
        debug!("RPC server created, starting Raft service");
        raft::RaftService::start(&raft_service).await;
        if meta_members.is_empty() {
            debug!("No other members in the cluster, will bootstrap");
            raft_service.bootstrap().await;
        } else {
            debug!("Raft service started, joining with members: {:?}", &meta_members);
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
        debug!("Joined with members, starting membership services");
        Membership::new(&rpc_server, &raft_service).await;
        debug!("Starting raft client");
        let raft_client = RaftClient::new(meta_servers, raft::DEFAULT_SERVICE_ID).await.unwrap();
        debug!("Prepare raft subscription");
        RaftClient::prepare_subscription(&rpc_server).await;
        debug!("Starting member service");
        let member_service = MemberService::new(server_addr, &raft_client).await;
        debug!("Member join group: {}", group_name);
        member_service.join_group(group_name).await.unwrap();
        let membership_client = Arc::new(ObserverClient::new(&raft_client));
        debug!("Creating neb server");
        NebServer::new(
            opts,
            server_addr,
            &meta_servers,
            group_name,
            &rpc_server,
            &raft_service,
            &raft_client,
            &membership_client
        )
        .await
        .unwrap()
    }

    pub fn get_server_id_by_id(&self, id: &Id) -> Option<u64> {
        self.consh.get_server_id(id.higher)
    }   
    pub async fn get_member_by_server_id(&self, server_id: u64) -> io::Result<Arc<rpc::RPCClient>> {
        self.member_pool
            .get_by_id(server_id, |_| self.consh.to_server_name(server_id)).await
    }
    pub async fn get_member_by_server_id_async(
        &self,
        server_id: u64,
    ) -> Result<Arc<RPCClient>, io::Error> {
        let cons_hash = self.consh.clone();
        self.member_pool
            .get_by_id(server_id, move |_| cons_hash.to_server_name(server_id)).await
    }
    pub fn conshash(&self) -> &ConsistentHashing {
        &*self.consh
    }
}

pub async fn rpc_client_by_id(
    id: &Id,
    neb: &Arc<NebServer>,
) -> Result<Arc<RPCClient>, RPCError> {
    let server_id = neb.get_server_id_by_id(id).unwrap();
    let neb = neb.clone();
    DEFAULT_CLIENT_POOL
        .get_by_id(server_id, move |sid| neb.conshash().to_server_name(sid)).await
        .map_err(|e| RPCError::IOError(e))
}

// Peer have a clock, meant to update with other servers in the cluster
pub struct Peer {
    pub clock: ServerVectorClock,
}

impl Peer {
    pub fn new(server_address: &String) -> Peer {
        Peer {
            clock: ServerVectorClock::new(server_address),
        }
    }
}

pub async fn init_cell_rpc_service(rpc_server: &Arc<Server>, neb_server: &Arc<NebServer>) {
    rpc_server.register_service(
        cell_rpc::DEFAULT_SERVICE_ID,
        &cell_rpc::NebRPCService::new(&neb_server),
    ).await;
}

pub async fn init_txn_service(rpc_server: &Arc<Server>, neb_server: &Arc<NebServer>) {
    rpc_server.register_service(
        transactions::manager::DEFAULT_SERVICE_ID,
        &transactions::manager::TransactionManager::new(&neb_server),
    ).await;
    rpc_server.register_service(
        transactions::data_site::DEFAULT_SERVICE_ID,
        &transactions::data_site::DataManager::new(&neb_server),
    ).await;
}

pub async fn init_lsm_tree_index_service(
    rpc_server: &Arc<Server>,
    neb_server: &Arc<NebServer>,
    neb_client: &Arc<AsyncClient>,
    raft_svr: &Arc<raft::RaftService>,
    raft_client: &Arc<RaftClient>,
    cons_hash: &Arc<ConsistentHashing>,
) {
    raft_svr.register_state_machine(box lsmtree::placement::sm::PlacementSM::new(cons_hash)).await;
    let sm_client = Arc::new(lsmtree::placement::sm::client::SMClient::new(
        lsmtree::placement::sm::SM_ID,
        raft_client,
    ));
    rpc_server.register_service(
        lsmtree::service::DEFAULT_SERVICE_ID,
        &lsmtree::service::LSMTreeService::new(neb_server, neb_client, &sm_client).await,
    ).await;
}