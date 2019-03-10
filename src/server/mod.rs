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
use bifrost::vector_clock::ServerVectorClock;
use bifrost_plugins::hash_ident;
use futures::Future;
use ram::chunk::Chunks;
use ram::cleaner::Cleaner;
use ram::schema::sm as schema_sm;
use ram::schema::LocalSchemasCache;
use ram::types::Id;
use std::io;
use std::sync::Arc;
use index::lsmtree;
use client;

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
    pub member_pool: rpc::ClientPool,
    pub txn_peer: Peer,
    pub raft_service: Arc<raft::RaftService>,
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
            ch.set_weight(address, memory_size).wait();
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
        raft_service: &Arc<raft::RaftService>,
        raft_client: &Arc<RaftClient>,
    ) -> Result<Arc<NebServer>, ServerError> {
        debug!(
            "Creating key-value server instance, group name {}",
            group_name
        );
        raft_service.register_state_machine(Box::new(schema_sm::SchemasSM::new(
            group_name,
            raft_service,
        )));
        Weights::new_with_id(CONS_HASH_ID, raft_service);
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
            txn_peer: Peer::new(server_addr),
            raft_service: raft_service.clone(),
            server_id: rpc_server.server_id,
        });
        for service in &opts.services {
            match service {
                &Service::Cell => init_cell_rpc_service(rpc_server, &server),
                &Service::Transaction => init_txn_service(rpc_server, &server),
                &Service::LSMTreeIndex => {
                    init_lsm_tree_index_serevice(rpc_server, &server, raft_service, raft_client)
                }
            }
        }

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
        let meta_members = &vec![server_addr.to_owned()];
        let raft_service = raft::RaftService::new(raft::Options {
            storage: raft::Storage::MEMORY,
            address: server_addr.to_owned(),
            service_id: raft::DEFAULT_SERVICE_ID,
        });
        Server::listen_and_resume(&rpc_server);
        rpc_server.register_service(raft::DEFAULT_SERVICE_ID, &raft_service);
        raft::RaftService::start(&raft_service);
        match raft_service.join(meta_members) {
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
        let raft_client = RaftClient::new(meta_members, raft::DEFAULT_SERVICE_ID).unwrap();
        RaftClient::prepare_subscription(&rpc_server);
        let member_service = MemberService::new(server_addr, &raft_client);
        member_service.join_group(group_name).wait().unwrap();
        NebServer::new(
            opts,
            server_addr,
            group_name,
            &rpc_server,
            &raft_service,
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
    pub fn conshash(&self) -> &ConsistentHashing {
        &*self.consh
    }
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

pub fn init_cell_rpc_service(rpc_server: &Arc<Server>, neb_server: &Arc<NebServer>) {
    rpc_server.register_service(
        cell_rpc::DEFAULT_SERVICE_ID,
        &cell_rpc::NebRPCService::new(&neb_server),
    );
}

pub fn init_txn_service(rpc_server: &Arc<Server>, neb_server: &Arc<NebServer>) {
    rpc_server.register_service(
        transactions::manager::DEFAULT_SERVICE_ID,
        &transactions::manager::TransactionManager::new(&neb_server),
    );
    rpc_server.register_service(
        transactions::data_site::DEFAULT_SERVICE_ID,
        &transactions::data_site::DataManager::new(&neb_server),
    );
}

pub fn init_lsm_tree_index_serevice(
    rpc_server: &Arc<Server>,
    neb_server: &Arc<NebServer>,
    raft_svr: &Arc<raft::RaftService>,
    raft_client: &Arc<RaftClient>,
) {
    raft_svr.register_state_machine(box lsmtree::placement::sm::PlacementSM::new());
    let sm_client = Arc::new(lsmtree::placement::sm::client::SMClient::new(lsmtree::placement::sm::SM_ID, raft_client));
    rpc_server.register_service(
        lsmtree::service::DEFAULT_SERVICE_ID,
        &lsmtree::service::LSMTreeService::new(neb_server, &sm_client)
    );
}

#[cfg(test)]
mod tests {
    extern crate test;

    use client;
    use client::AsyncClient;
    use dovahkiin::types::custom_types::id::Id;
    use dovahkiin::types::custom_types::map::Map;
    use dovahkiin::types::type_id_of;
    use futures::Future;
    use ram::cell::Cell;
    use ram::schema::Field;
    use ram::schema::Schema;
    use ram::types::*;
    use server::NebServer;
    use server::ServerOptions;
    use server::Service;
    use std::sync::Arc;
    use test::Bencher;

    const DATA: &'static str = "DATA";

    fn init_service(port: usize) -> (Arc<NebServer>, Arc<AsyncClient>, u32) {
        env_logger::init();
        let server_addr = String::from(format!("127.0.0.1:{}", port));
        let server_group = String::from("bench_test");
        let server = NebServer::new_from_opts(
            &ServerOptions {
                chunk_count: 1,
                memory_size: 512 * 1024 * 1024,
                backup_storage: Some(format!("test-data/{}-bak", port)),
                wal_storage: Some(format!("test-data/{}-wal", port)),
                services: vec![Service::Cell, Service::Transaction, Service::LSMTreeIndex],
            },
            &server_addr,
            &server_group,
        );
        let schema_id = 123;
        let schema = Schema {
            id: schema_id,
            name: String::from("schema"),
            key_field: None,
            str_key_field: None,
            is_dynamic: false,
            fields: Field::new(
                "*",
                0,
                false,
                false,
                Some(vec![Field::new(
                    DATA,
                    type_id_of(Type::U64),
                    false,
                    false,
                    None,
                )]),
            ),
        };

        let client = Arc::new(
            client::AsyncClient::new(&server.rpc, &vec![server_addr], &server_group).unwrap(),
        );
        client.new_schema_with_id(schema).wait();
        (server, client, schema_id)
    }

    #[bench]
    fn wr(b: &mut Bencher) {
        let (_, client, schema_id) = init_service(5302);
        let id = Id::new(0, 1);
        let mut value = Value::Map(Map::new());
        value[DATA] = Value::U64(2);
        let cell = Cell::new_with_id(schema_id, &id, value);
        b.iter(|| {
            client.upsert_cell(cell.clone()).wait();
        })
    }

    #[bench]
    fn w(b: &mut Bencher) {
        let (_, client, schema_id) = init_service(5306);
        let id = Id::new(0, 1);
        let mut value = Value::Map(Map::new());
        value[DATA] = Value::U64(2);
        let cell = Cell::new_with_id(schema_id, &id, value);
        let mut i = 0;
        b.iter(|| {
            let mut cell = cell.clone();
            cell.header.hash = i;
            client.write_cell(cell).wait();
            i += 1
        })
    }

    #[bench]
    fn txn_upsert(b: &mut Bencher) {
        let (_, client, schema_id) = init_service(5303);
        b.iter(|| {
            client
                .transaction(move |txn| {
                    let id = Id::new(0, 1);
                    let mut value = Value::Map(Map::new());
                    value[DATA] = Value::U64(2);
                    let cell = Cell::new_with_id(schema_id, &id, value);
                    txn.upsert(cell)
                })
                .wait()
        })
    }

    #[bench]
    fn txn_insert(b: &mut Bencher) {
        let (_, client, schema_id) = init_service(5305);
        let mut i = 0;
        b.iter(|| {
            client
                .transaction(move |txn| {
                    let id = Id::new(0, i);
                    let mut value = Value::Map(Map::new());
                    value[DATA] = Value::U64(i);
                    let cell = Cell::new_with_id(schema_id, &id, value);
                    txn.write(cell)
                })
                .wait();
            i += 1;
        })
    }

    #[bench]
    fn noop_txn(b: &mut Bencher) {
        let (_, client, _schema_id) = init_service(5304);
        b.iter(|| client.transaction(move |_txn| Ok(())).wait())
    }

    #[bench]
    fn cell_construct(b: &mut Bencher) {
        b.iter(|| {
            let id = Id::new(0, 1);
            let mut value = Value::Map(Map::new());
            value["DATA"] = Value::U64(2);
            Cell::new_with_id(1, &id, value);
        })
    }

    #[bench]
    fn cell_clone(b: &mut Bencher) {
        let id = Id::new(0, 1);
        let mut value = Value::Map(Map::new());
        value["DATA"] = Value::U64(2);
        let cell = Cell::new_with_id(1, &id, value);
        b.iter(|| {
            cell.clone();
        })
    }
}
