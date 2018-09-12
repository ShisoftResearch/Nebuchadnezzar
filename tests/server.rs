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

#[test]
pub fn init () {
    NebServer::new_from_opts(&ServerOptions {
        chunk_count: 1,
        memory_size: 16 * 1024,
        backup_storage: None,
        wal_storage: None
    },
    &String::from("127.0.0.1:5100"),
    &String::from("test"));
}
