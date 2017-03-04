use bifrost::rpc;
use bifrost::conshash::ConsistentHashing;
use bifrost::raft;
use bifrost::raft::client::RaftClient;
use bifrost::raft::state_machine::{master as sm_master};
use ram::chunk::Chunks;
use ram::schema::Schemas;
use ram::schema::{sm as schema_sm};
use std::rc::Rc;
use std::sync::Arc;

pub struct ServerOptions {
    pub chunk_count: usize,
    pub memory_size: usize,
    pub standalone: bool,
    pub is_meta: bool,
    pub meta_members: Vec<String>,
    pub address: String,
    pub backup_storage: Option<String>,
    pub meta_storage: Option<String>,
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
    pub fn new(opt: ServerOptions) -> Server {
        let server_addr = &opt.address;
        let rpc_server = rpc::Server::new(vec!());
        let mut schemas = Schemas::new(None);
        rpc::Server::listen_and_resume(&rpc_server, server_addr);
        if !opt.standalone {
            if opt.is_meta {
                let raft_service = raft::RaftService::new(raft::Options {
                    storage: match opt.meta_storage {
                        Some(path) => raft::Storage::DISK(path.clone()),
                        None => raft::Storage::MEMORY,
                    },
                    address: server_addr.clone(),
                    service_id: raft::DEFAULT_SERVICE_ID,
                });
                rpc_server.register_service(raft::DEFAULT_SERVICE_ID, raft_service.clone());
                raft_service.register_state_machine(Box::new(schema_sm::SchemasSM::new(&raft_service)));
                match raft_service.join(&opt.meta_members) {
                    Err(sm_master::ExecError::CannotConstructClient) => {
                        info!("Cannot join meta cluster, will bootstrap one.");
                        raft_service.bootstrap();
                    },
                    Ok(Ok(())) => {
                        info!("Joined meta cluster, number of members: {}", raft_service.num_members());
                    },
                    e => {error!("Cannot join into cluster: {:?}", e);}
                }
            }
            let raft_client = RaftClient::new(&opt.meta_members, raft::DEFAULT_SERVICE_ID);
            match raft_client {
                Ok(raft_client) => {
                    schemas = Schemas::new(Some(&raft_client));
                },
                Err(e) => {error!("Cannot generate meta client: {:?}", e);}
            }
        } else if opt.is_meta {error!("Meta server cannot be standalone");}
        let meta_rc = Arc::new(ServerMeta {
            schemas: schemas.clone()
        });
        let chunks = Chunks::new(
            opt.chunk_count,
            opt.memory_size,
            meta_rc.clone(),
            opt.backup_storage.clone(),
        );
        Server {
            chunks: chunks,
            meta: meta_rc,
            rpc: rpc_server
        }
    }
}