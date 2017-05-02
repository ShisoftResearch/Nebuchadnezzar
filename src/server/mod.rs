use bifrost::rpc;
use bifrost::conshash::ConsistentHashing;
use bifrost::raft;
use bifrost::raft::client::RaftClient;
use bifrost::raft::state_machine::{master as sm_master};
use bifrost::raft::state_machine::callback::client::SubscriptionService;
use bifrost::membership::server::Membership;
use bifrost::membership::member::MemberService;
use bifrost::membership::client::ObserverClient;
use bifrost::conshash::weights::Weights;
use bifrost::tcp::STANDALONE_ADDRESS_STRING;
use ram::chunk::Chunks;
use ram::schema::Schemas;
use ram::schema::{sm as schema_sm};
use ram::types::Id;
use std::rc::Rc;
use std::sync::Arc;
use std::io;

mod cell_rpc;
mod transactions;

#[derive(Debug)]
pub enum ServerError {
    CannotJoinCluster,
    CannotJoinClusterGroup(sm_master::ExecError),
    CannotInitMemberTable,
    CannotSetServerWeight,
    CannotInitConsistentHashTable,
    CannotLoadMetaClient,
    MetaServerCannotBeStandalone,
}

pub struct ServerOptions {
    pub chunk_count: usize,
    pub memory_size: usize,
    pub standalone: bool,
    pub is_meta: bool,
    pub meta_members: Vec<String>,
    pub address: String,
    pub backup_storage: Option<String>,
    pub meta_storage: Option<String>,
    pub group_name: String,
}

pub struct ServerMeta {
    pub schemas: Arc<Schemas>
}

pub struct NebServer {
    pub chunks: Arc<Chunks>,
    pub meta: Arc<ServerMeta>,
    pub rpc: Arc<rpc::Server>,
    pub consh: Option<Arc<ConsistentHashing>>,
    pub member_pool: rpc::ClientPool,
    pub tnx_peer: transactions::Peer
}

impl NebServer {
    fn load_meta_server(opt: &ServerOptions, rpc_server: &Arc<rpc::Server>) -> Result<(), ServerError> {
        let server_addr = &opt.address;
        let raft_service = raft::RaftService::new(raft::Options {
            storage: match opt.meta_storage {
                Some(ref path) => raft::Storage::DISK(path.clone()),
                None => raft::Storage::MEMORY,
            },
            address: server_addr.clone(),
            service_id: raft::DEFAULT_SERVICE_ID,
        });
        rpc_server.register_service(
            raft::DEFAULT_SERVICE_ID,
            &raft_service
        );
        raft_service.register_state_machine(Box::new(schema_sm::SchemasSM::new(&raft_service)));
        raft::RaftService::start(&raft_service);
        match raft_service.join(&opt.meta_members) {
            Err(sm_master::ExecError::CannotConstructClient) => {
                info!("Cannot join meta cluster, will bootstrap one.");
                raft_service.bootstrap();
            },
            Ok(Ok(())) => {
                info!("Joined meta cluster, number of members: {}", raft_service.num_members());
            },
            e => {
                error!("Cannot join into cluster: {:?}", e);
                return Err(ServerError::CannotJoinCluster)
            }
        }
        Membership::new(rpc_server, &raft_service);
        Weights::new(&raft_service);
        return Ok(());
    }
    fn load_subscription(rpc_server: &Arc<rpc::Server>, raft_client: &RaftClient) {
        let subs_service = SubscriptionService::initialize(rpc_server);
        raft_client.set_subscription(&subs_service);
    }
    fn join_group(opt: &ServerOptions, raft_client: &Arc<RaftClient>) -> Result<(), ServerError> {
        let member_service = MemberService::new(&opt.address, raft_client);
        match member_service.join_group(&opt.group_name) {
            Ok(_) => {Ok(())},
            Err(e) => {
                error!("Cannot join cluster group");
                Err(ServerError::CannotJoinClusterGroup(e))
            }
        }
    }
    fn init_conshash(opt: &ServerOptions, raft_client: &Arc<RaftClient>)
        -> Result<Arc<ConsistentHashing>, ServerError> {
        match ConsistentHashing::new(&opt.group_name, raft_client) {
            Ok(ch) => {
                ch.set_weight(&opt.address, opt.memory_size as u64);
                if !ch.init_table().is_ok() {
                    error!("Cannot initialize member table");
                    return Err(ServerError::CannotInitMemberTable);
                }
                return Ok(ch);
            },
            _ => {
                error!("Cannot initialize consistent hash table");
                return Err(ServerError::CannotInitConsistentHashTable);
            }
        }
    }
    fn load_cluster_clients
    (opt: &ServerOptions, schemas: &mut Arc<Schemas>, rpc_server: &Arc<rpc::Server>)
    -> Result<Arc<ConsistentHashing>, ServerError>{
        let raft_client = RaftClient::new(&opt.meta_members, raft::DEFAULT_SERVICE_ID);
        match raft_client {
            Ok(raft_client) => {
                *schemas = Schemas::new(Some(&raft_client));
                NebServer::load_subscription(rpc_server, &raft_client);
                NebServer::join_group(opt, &raft_client)?;
                Ok(NebServer::init_conshash(opt, &raft_client)?)
            },
            Err(e) => {
                error!("Cannot load meta client: {:?}", e);
                Err(ServerError::CannotLoadMetaClient)
            }
        }
    }
    pub fn new(opt: ServerOptions) -> Result<Arc<NebServer>, ServerError> {
        let server_addr = if opt.standalone {&STANDALONE_ADDRESS_STRING} else {&opt.address};
        let rpc_server = rpc::Server::new(server_addr);
        let mut conshasing = None;
        let mut schemas = Schemas::new(None);
        rpc::Server::listen_and_resume(&rpc_server);
        if !opt.standalone {
            if opt.is_meta {
                NebServer::load_meta_server(&opt, &rpc_server)?;
            }
            conshasing = Some(NebServer::load_cluster_clients(&opt, &mut schemas, &rpc_server)?);
        } else if opt.is_meta {
            error!("Meta server cannot be standalone");
            return Err(ServerError::MetaServerCannotBeStandalone)
        }
        let meta_rc = Arc::new(ServerMeta {
            schemas: schemas.clone()
        });
        let chunks = Chunks::new(
            opt.chunk_count,
            opt.memory_size,
            meta_rc.clone(),
            opt.backup_storage.clone(),
        );
        let server = Arc::new(NebServer {
            chunks: chunks,
            meta: meta_rc,
            rpc: rpc_server.clone(),
            consh: conshasing,
            member_pool: rpc::ClientPool::new(),
            tnx_peer: transactions::Peer::new(server_addr)
        });
        rpc_server.register_service(
            cell_rpc::DEFAULT_SERVICE_ID,
            &cell_rpc::NebRPCService::new(&server)
        );
        rpc_server.register_service(
           transactions::manager::DEFAULT_SERVICE_ID,
           &transactions::manager::TransactionManager::new(&server)
        );
        rpc_server.register_service(
            transactions::data_site::DEFAULT_SERVICE_ID,
            &transactions::data_site::DataManager::new(&server)
        );
        Ok(server)
    }
    pub fn get_member_by_id(&self, id: &Id) -> io::Result<Arc<rpc::RPCClient>> {
        match self.consh {
            Some(ref consh) => {
                if let Some(hashed_address) = consh.get_server(id.higher) {
                    self.member_pool.get(&hashed_address)
                } else {
                    Err(io::Error::new(io::ErrorKind::Other, "Cannot get server from consistent hashing"))
                }
            },
            _ => self.member_pool.get(&STANDALONE_ADDRESS_STRING)
        }
    }
}