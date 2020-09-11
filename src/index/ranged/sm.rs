use super::trees::*;
use std::collections::BTreeMap;
use crate::ram::types::Id;
use super::lsm::service::{DEFAULT_SERVICE_ID, AsyncServiceClient as LSMServiceClient, Boundary};
use futures::prelude::*;
use bifrost::raft::state_machine::StateMachineCtl;
use std::sync::Arc;
use bifrost::utils;
use bifrost::raft::RaftService;
use bifrost::conshash::ConsistentHashing;
use bifrost::rpc::{RPCClient, RPCError, DEFAULT_CLIENT_POOL};
use crate::ram::types::RandValue;

pub struct MasterTreeSM {
    tree: BTreeMap<EntryKey, Id>,
    raft_svr: Arc<RaftService>,
    conshash: Arc<ConsistentHashing>,
    sm_id: u64
}

raft_state_machine! {
    def qry locate_key(entry: EntryKey) -> Id;
    def cmd split(new_tree: Id, pivot: EntryKey);
    // No subscription for clients
}

impl StateMachineCmds for MasterTreeSM {
    fn locate_key(&self, entry: EntryKey) -> BoxFuture<Id> {
        future::ready(
            self.tree.range(entry..)
            .next()
            .map(|(_, id)| *id)
            .unwrap()
        ).boxed()
    }

    fn split(&mut self, new_tree: Id, pivot: EntryKey) -> BoxFuture<()> {
        // Call this after the tree have been split and persisted
        let upper_bound = self.tree.range(pivot.clone()..).next().unwrap().0.clone();
        self.tree.insert(pivot.clone(), new_tree);
        self.load_sub_tree(new_tree, pivot, upper_bound).boxed()
    }
}

impl StateMachineCtl for MasterTreeSM {
    raft_sm_complete!();
    fn id(&self) -> u64 {
        self.sm_id
    }
    fn snapshot(&self) -> Option<Vec<u8>> {
        Some(utils::serde::serialize(&self.tree))
    }
    fn recover(&mut self, data: Vec<u8>) -> BoxFuture<()> {
        let tree = utils::serde::deserialize(&data).unwrap();
        self.tree = tree; 
        future::ready(()).boxed()
    }
}

impl MasterTreeSM {
    pub async fn new(raft_svr: &Arc<RaftService>, conshash: &Arc<ConsistentHashing>, sm_id: u64) -> Self {
        let mut tree = BTreeMap::new();
        let genesis_id = Id::rand();
        tree.insert(min_entry_key(), genesis_id);
        locate_tree_server_from_conshash(&genesis_id, conshash).await
            .unwrap()
            .crate_tree(genesis_id, Boundary::new(min_entry_key(), max_entry_key()))
            .await
            .unwrap();
        Self {
            tree,
            raft_svr: raft_svr.clone(),
            conshash: conshash.clone(),
            sm_id
        }
    }
    async fn load_sub_tree(&self, id: Id, lower: EntryKey, upper: EntryKey) {
        if self.raft_svr.is_leader() {
            // Only the leader can initiate the request to load the sub tree
            let client = self.locate_tree_server(&id).await.unwrap();
            client.load_tree(id, Boundary::new(lower, upper)).await.unwrap()
        } 
    }

    async fn locate_tree_server(&self, id: &Id) -> Result<Arc<LSMServiceClient>, RPCError> {
        locate_tree_server_from_conshash(id, &self.conshash).await
    }
}
pub fn client_by_rpc_client(rpc: &Arc<RPCClient>) -> Arc<LSMServiceClient> {
    LSMServiceClient::new(DEFAULT_SERVICE_ID, rpc)
}

async fn locate_tree_server_from_conshash(id: &Id, conshash: &Arc<ConsistentHashing>) -> Result<Arc<LSMServiceClient>, RPCError> {
    let server_id = conshash.get_server_id_by(id).unwrap();
    DEFAULT_CLIENT_POOL
        .get_by_id(server_id, move |sid| conshash.to_server_name(sid))
        .await
        .map_err(|e| RPCError::IOError(e))
        .map(|c| client_by_rpc_client(&c))
}