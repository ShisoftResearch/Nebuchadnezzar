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
use ram::chunk::Chunks;
use ram::schema::Schemas;
use ram::schema::{sm as schema_sm};
use std::rc::Rc;
use std::sync::Arc;

mod cell_rpc;

pub enum ServerError {
    CannotJoinCluster,
    CannotJoinClusterGroup,
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

pub struct Server {
    pub chunks: Arc<Chunks>,
    pub meta: Arc<ServerMeta>,
    pub rpc: Arc<rpc::Server>
}

impl Server {
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
            raft_service.clone()
        );
        raft_service.register_state_machine(Box::new(schema_sm::SchemasSM::new(&raft_service)));
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
            Ok(Ok(_)) => {Ok(())},
            _ => {
                error!("Cannot join cluster group");
                Err(ServerError::CannotJoinClusterGroup)
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
    -> Result<(), ServerError>{
        let raft_client = RaftClient::new(&opt.meta_members, raft::DEFAULT_SERVICE_ID);
        match raft_client {
            Ok(raft_client) => {
                *schemas = Schemas::new(Some(&raft_client));
                Server::load_subscription(rpc_server, &raft_client);
                Server::join_group(opt, &raft_client)?;
                Server::init_conshash(opt, &raft_client)?;
                Ok(())
            },
            Err(e) => {
                error!("Cannot load meta client: {:?}", e);
                Err(ServerError::CannotLoadMetaClient)
            }
        }
    }
    pub fn new(opt: ServerOptions) -> Result<Server, ServerError> {
        let server_addr = &opt.address;
        let rpc_server = rpc::Server::new(vec!());
        let mut schemas = Schemas::new(None);
        rpc::Server::listen_and_resume(&rpc_server, server_addr);
        if !opt.standalone {
            if opt.is_meta {
                Server::load_meta_server(&opt, &rpc_server)?;
            }
            Server::load_cluster_clients(&opt, &mut schemas, &rpc_server)?;
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
        rpc_server.register_service(
            cell_rpc::DEFAULT_SERVICE_ID,
            cell_rpc::NebRPCService::new(&chunks)
        );
        Ok(Server {
            chunks: chunks,
            meta: meta_rc,
            rpc: rpc_server
        })
    }
}