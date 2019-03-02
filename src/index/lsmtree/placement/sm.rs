use std::collections::btree_map::BTreeMap;
use index::EntryKey;
use dovahkiin::types::Id;
use std::collections::HashMap;
use serde::Serialize;
use serde::Deserialize;
use serde::de::DeserializeOwned;
use bifrost::raft::state_machine::StateMachineCtl;
use bifrost::utils::bincode::serialize;
use bincode::deserialize;
use bifrost_plugins::hash_ident;

pub static SM_ID: u64 = hash_ident!(LSM_TREE_PLACEMENT_SM) as u64;

#[derive(Serialize, Deserialize)]
pub enum SplitError {
    AnotherSplitInProgress(Id),
    MidOutOfRange,
    PlacementNotFound
}

pub struct InSplitStatus {
    part_2: Id,
    mid: EntryKey
}

pub struct Placement {
    starts: EntryKey,
    ends: EntryKey,
    in_split: Option<InSplitStatus>,
    id: Id,
}

pub struct PlacementSM {
    placements: HashMap<Id, Placement>,
    starts: BTreeMap<EntryKey, Id>,

}

raft_state_machine! {
    def cmd prepare_split(source: Id)  -> Id | SplitError;
    def cmd start_split(source: Id, mid: Vec<u8>) -> bool | SplitError;
    def cmd complete_split(part_1: Id, part_2: Id) -> bool | SplitError;
    def qry locate(id: Id) -> Id | SplitError;
}

impl StateMachineCmds for PlacementSM {
    fn prepare_split(&mut self, source: Id) -> Result<Id, SplitError> {
        unimplemented!()
    }

    fn start_split(&mut self, source: Id, mid: Vec<u8>) -> Result<bool, SplitError> {
        unimplemented!()
    }

    fn complete_split(&mut self, part_1: Id, part_2: Id) -> Result<bool, SplitError> {
        unimplemented!()
    }

    fn locate(&self, id: Id) -> Result<Id, SplitError> {
        unimplemented!()
    }
}

impl StateMachineCtl for PlacementSM {
    raft_sm_complete!();
    fn id(&self) -> u64 {
        SM_ID
    }
    fn snapshot(&self) -> Option<Vec<u8>> {
        unimplemented!()
    }
    fn recover(&mut self, data: Vec<u8>) {
        unimplemented!()
    }
}