use neb::server::*;
use neb::ram::schema::LocalSchemasCache;
use bifrost::rpc;
use bifrost::raft;
use bifrost::membership::server::Membership;
use bifrost::raft::client::RaftClient;
use bifrost::membership::member::MemberService;
use bifrost::raft::state_machine::{master as sm_master};
use bifrost::rpc::Server;
use std::rc::Rc;
use std::sync::Arc;
use futures::prelude::*;

pub fn new_from_opts<'a>(
    opts: &ServerOptions,
    server_addr: &'a str,
    group_name: &'a str
) -> Arc<NebServer> {
    let group_name = &String::from(group_name);
    let server_addr = &String::from(server_addr);
    let rpc_server = rpc::Server::new(server_addr);
    let meta_mambers = &vec![server_addr.to_owned()];
    let raft_service = raft::RaftService::new(raft::Options {
        storage: raft::Storage::MEMORY,
        address: server_addr.to_owned(),
        service_id: raft::DEFAULT_SERVICE_ID
    });
    Server::listen_and_resume(&rpc_server);
    rpc_server.register_service(
        raft::DEFAULT_SERVICE_ID,
        &raft_service
    );
    raft::RaftService::start(&raft_service);
    match raft_service.join(meta_mambers) {
        Err(sm_master::ExecError::CannotConstructClient) => {
            info!("Cannot join meta cluster, will bootstrap one.");
            raft_service.bootstrap();
        },
        Ok(Ok(())) => {
            info!("Joined meta cluster, number of members: {}", raft_service.num_members());
        },
        e => {
            error!("Cannot join into cluster: {:?}", e);
            panic!("{:?}", ServerError::CannotJoinCluster)
        }
    }
    Membership::new(&rpc_server, &raft_service);
    let raft_client =
        RaftClient::new(meta_mambers, raft::DEFAULT_SERVICE_ID).unwrap();
    RaftClient::prepare_subscription(&rpc_server);
    let member_service = MemberService::new(server_addr, &raft_client);
    member_service.join_group(group_name).wait().unwrap();
    NebServer::new(
        opts,
        server_addr,
        group_name,
        &rpc_server,
        &Some(raft_service),
        &raft_client).unwrap()
}

#[test]
pub fn init () {
    new_from_opts(&ServerOptions {
        chunk_count: 1,
        memory_size: 16 * 1024,
        backup_storage: None,
        wal_storage: None
    },
    &String::from("127.0.0.1:5100"),
    &String::from("test"));
}
