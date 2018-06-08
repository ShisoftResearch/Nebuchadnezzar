use bifrost::rpc;
use bifrost::conshash::ConsistentHashing;
use bifrost::raft;
use bifrost::raft::client::RaftClient;
use bifrost::raft::state_machine::{master as sm_master};
use bifrost::tcp::{STANDALONE_ADDRESS_STRING};
use bifrost::conshash::weights::Weights;
use ram::chunk::Chunks;
use ram::schema::LocalSchemasCache;
use ram::schema::{sm as schema_sm};
use ram::types::Id;
use ram::cleaner::Cleaner;
use std::sync::Arc;
use std::io;
use futures::Future;

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

}

pub struct ServerMeta {
    pub schemas: LocalSchemasCache
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
    pub cleaner: Cleaner
}

pub fn init_conshash(
    group_name: &String,
    address: &String,
    memory_size: u64,
    raft_client: &Arc<RaftClient>)
    -> Result<Arc<ConsistentHashing>, ServerError>
{
    match ConsistentHashing::new_with_id(CONS_HASH_ID,group_name, raft_client) {
        Ok(ch) => {
            ch.set_weight(address, memory_size).wait();
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

impl NebServer {
    pub fn new(
        opts: &ServerOptions,
        server_addr: &String,
        group_name: &String,
        rpc_server: &Arc<rpc::Server>,
        raft_service: &Option<Arc<raft::RaftService>>,
        raft_client: &Arc<RaftClient>
    ) -> Result<Arc<NebServer>, ServerError> {
        if let &Some(ref raft_service) = raft_service {
            raft_service.register_state_machine(Box::new(schema_sm::SchemasSM::new(group_name, raft_service)));
            Weights::new_with_id(CONS_HASH_ID, raft_service);
        }
        let schemas = LocalSchemasCache::new(group_name, Some(raft_client)).unwrap();
        let meta_rc = Arc::new(ServerMeta {
            schemas
        });
        let chunks = Chunks::new(
            opts.chunk_count,
            opts.memory_size,
            meta_rc.clone(),
            opts.backup_storage.clone(),
        );
        let cleaner = Cleaner::new_and_start(chunks.clone());
        let conshasing = init_conshash(
            group_name,
            server_addr,
            opts.memory_size as u64,
            raft_client)?;
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
    pub fn get_server_id_by_id(&self, id: &Id) -> Option<u64> {
        if let Some(server_id) = self.consh.get_server_id(id.higher) {
            Some(server_id)
        } else {
            None
        }
    }
    pub fn get_member_by_server_id(&self, server_id: u64) -> io::Result<Arc<rpc::RPCClient>> {
        if let Some(ref server_name) = self.consh.to_server_name(Some(server_id)) {
            self.member_pool.get(server_name)
        } else {
            Err(io::Error::new(io::ErrorKind::Other, "Cannot find server id in consistent hash table"))
        }
    }
}