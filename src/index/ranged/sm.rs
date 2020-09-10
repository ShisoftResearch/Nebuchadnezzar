use super::trees::*;
use std::collections::BTreeMap;
use crate::ram::types::Id;
use futures::prelude::*;
use bifrost::raft::state_machine::StateMachineCtl;
use bifrost::utils;
use bifrost::raft::RaftService;

pub struct MasterTreeSM {
    tree: BTreeMap<EntryKey, Id>,
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
        self.tree.insert(pivot, new_tree);
        future::ready(()).boxed()
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