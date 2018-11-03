use bifrost::conshash::weights::Weights;
use bifrost::conshash::ConsistentHashing;
use bifrost::membership::member::MemberService;
use bifrost::membership::server::Membership;
use bifrost::raft;
use bifrost::raft::client::RaftClient;
use bifrost::raft::state_machine::master as sm_master;
use bifrost::rpc;
use bifrost::rpc::Server;
use bifrost::tcp::STANDALONE_ADDRESS_STRING;
use bifrost::utils::fut_exec::wait;
use futures::Future;
use ram::chunk::Chunks;
use ram::cleaner::Cleaner;
use ram::schema::sm as schema_sm;
use ram::schema::LocalSchemasCache;
use ram::types::Id;
use std::io;
use std::sync::Arc;

pub mod cell_rpc;
pub mod transactions;

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
}

pub struct ServerMeta {
    pub schemas: LocalSchemasCache,
}

pub struct NebServer {
    pub chunks: Arc<Chunks>,
    pub meta: Arc<ServerMeta>,
    pub rpc: Arc<rpc::Server>,
    pub consh: Arc<ConsistentHashing>,
    pub member_pool: rpc::ClientPool,
    pub txn_peer: transactions::Peer,
    pub raft_service: Option<Arc<raft::RaftService>>,
    pub server_id: u64,
    pub cleaner: Cleaner,
}

pub fn init_conshash(
    group_name: &String,
    address: &String,
    memory_size: u64,
    raft_client: &Arc<RaftClient>,
) -> Result<Arc<ConsistentHashing>, ServerError> {
    match ConsistentHashing::new_with_id(CONS_HASH_ID, group_name, raft_client) {
        Ok(ch) => {
            wait(ch.set_weight(address, memory_size));
            if !ch.init_table().is_ok() {
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
    pub fn new(
        opts: &ServerOptions,
        server_addr: &String,
        group_name: &String,
        rpc_server: &Arc<rpc::Server>,
        raft_service: &Option<Arc<raft::RaftService>>,
        raft_client: &Arc<RaftClient>,
    ) -> Result<Arc<NebServer>, ServerError> {
        debug!(
            "Creating key-value server instance, group name {}",
            group_name
        );
        if let &Some(ref raft_service) = raft_service {
            raft_service.register_state_machine(Box::new(schema_sm::SchemasSM::new(
                group_name,
                raft_service,
            )));
            Weights::new_with_id(CONS_HASH_ID, raft_service);
        }
        let schemas = LocalSchemasCache::new(group_name, Some(raft_client)).unwrap();
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
        )?;
        let server = Arc::new(NebServer {
            chunks,
            cleaner,
            meta: meta_rc,
            rpc: rpc_server.clone(),
            consh: conshasing.clone(),
            member_pool: rpc::ClientPool::new(),
            txn_peer: transactions::Peer::new(server_addr),
            raft_service: raft_service.clone(),
            server_id: rpc_server.server_id,
        });
        rpc_server.register_service(
            cell_rpc::DEFAULT_SERVICE_ID,
            &cell_rpc::NebRPCService::new(&server),
        );
        rpc_server.register_service(
            transactions::manager::DEFAULT_SERVICE_ID,
            &transactions::manager::TransactionManager::new(&server),
        );
        rpc_server.register_service(
            transactions::data_site::DEFAULT_SERVICE_ID,
            &transactions::data_site::DataManager::new(&server),
        );
        Ok(server)
    }

    pub fn new_from_opts<'a>(
        opts: &ServerOptions,
        server_addr: &'a str,
        group_name: &'a str,
    ) -> Arc<NebServer> {
        debug!("Creating key-value server from options");
        let group_name = &String::from(group_name);
        let server_addr = &String::from(server_addr);
        let rpc_server = rpc::Server::new(server_addr);
        let meta_mambers = &vec![server_addr.to_owned()];
        let raft_service = raft::RaftService::new(raft::Options {
            storage: raft::Storage::MEMORY,
            address: server_addr.to_owned(),
            service_id: raft::DEFAULT_SERVICE_ID,
        });
        Server::listen_and_resume(&rpc_server);
        rpc_server.register_service(raft::DEFAULT_SERVICE_ID, &raft_service);
        raft::RaftService::start(&raft_service);
        match raft_service.join(meta_mambers) {
            Err(sm_master::ExecError::CannotConstructClient) => {
                info!("Cannot join meta cluster, will bootstrap one.");
                raft_service.bootstrap();
            }
            Ok(Ok(())) => {
                info!(
                    "Joined meta cluster, number of members: {}",
                    raft_service.num_members()
                );
            }
            e => {
                error!("Cannot join into cluster: {:?}", e);
                panic!("{:?}", ServerError::CannotJoinCluster)
            }
        }
        Membership::new(&rpc_server, &raft_service);
        let raft_client = RaftClient::new(meta_mambers, raft::DEFAULT_SERVICE_ID).unwrap();
        RaftClient::prepare_subscription(&rpc_server);
        let member_service = MemberService::new(server_addr, &raft_client);
        wait(member_service.join_group(group_name)).unwrap();
        NebServer::new(
            opts,
            server_addr,
            group_name,
            &rpc_server,
            &Some(raft_service),
            &raft_client,
        )
        .unwrap()
    }

    pub fn get_server_id_by_id(&self, id: &Id) -> Option<u64> {
        if let Some(server_id) = self.consh.get_server_id(id.higher) {
            Some(server_id)
        } else {
            None
        }
    }
    pub fn get_member_by_server_id(&self, server_id: u64) -> io::Result<Arc<rpc::RPCClient>> {
        self.member_pool
            .get_by_id(server_id, |_| self.consh.to_server_name(server_id))
    }
}
