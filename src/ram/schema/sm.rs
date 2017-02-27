use super::*;

use bifrost::raft::state_machine::StateMachineCtl;
use bifrost::raft::state_machine::callback::server::SMCallback;
use bifrost::raft::RaftService;

use std::collections::HashMap;

pub struct SchemasSM {
    pub schema_map: HashMap<u32, Schema>,
    pub name_map: HashMap<String, u32>,
    pub callback: Option<SMCallback>,
}

raft_state_machine! {
    def qry get_all() -> Schema;

}