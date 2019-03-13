use bifrost::raft::state_machine::StateMachineCtl;
use bifrost::utils::bincode::serialize;
use bifrost_plugins::hash_ident;
use bincode::deserialize;
use dovahkiin::types::Id;
use index::EntryKey;
use parking_lot::RwLock;
use ram::types::RandValue;
use serde::de::DeserializeOwned;
use serde::Deserialize;
use serde::Serialize;
use serde_json::ser::CharEscape::Quote;
use smallvec::SmallVec;
use std::collections::btree_map::BTreeMap;
use std::collections::btree_set::BTreeSet;
use std::collections::HashMap;
use std::collections::HashSet;

pub static SM_ID: u64 = hash_ident!(LSM_TREE_PLACEMENT_SM) as u64;

#[derive(Serialize, Deserialize, Debug)]
pub enum CmdError {
    AnotherSplitInProgress(InSplitStatus),
    CannotFindSplitMeta,
    SplitUnmatchSource(Id),
    NoSplitInProgress,
    MidOutOfRange,
    PlacementNotFound,
    PlacementExists,
}

#[derive(Serialize, Deserialize, Debug)]
pub enum QueryError {
    OutOfRange,
    PlacementNotFound,
}

#[derive(Serialize, Deserialize)]
pub struct QueryResult {
    id: Id,
    split: Option<(Vec<u8>, Id)>,
    epoch: u64,
}

#[derive(Serialize, Deserialize, Clone, Debug)]
pub struct InSplitStatus {
    pub dest: Id,
    pub pivot: Vec<u8>,
}

#[derive(Serialize, Deserialize, Clone, Debug)]
pub struct Placement {
    pub starts: Vec<u8>,
    pub ends: Vec<u8>,
    pub in_split: Option<InSplitStatus>,
    pub epoch: u64,
    pub id: Id,
}

pub struct PlacementSM {
    placements: HashMap<Id, Placement>,
    starts: BTreeMap<Vec<u8>, Id>,
}

raft_state_machine! {
    def cmd prepare_split(source: Id)  -> () | CmdError;
    def cmd start_split(source: Id, dest: Id, mid: Vec<u8>, src_epoch: u64) -> u64 | CmdError;
    def cmd complete_split(source: Id, dest: Id, src_epoch: u64) -> u64 | CmdError;
    def cmd update_epoch(source: Id, epoch: u64) -> u64 | CmdError;
    def cmd upsert(placement: Placement) -> () | CmdError;
    def qry locate(id: Vec<u8>) -> QueryResult | QueryError;
    def qry all() -> Vec<Placement>;
    def qry get(id: Id) -> Placement | QueryError;
}

impl StateMachineCmds for PlacementSM {
    fn prepare_split(&mut self, source: Id) -> Result<(), CmdError> {
        if let Some(src_placement) = self.placements.get(&source) {
            if let &Some(ref split) = &src_placement.in_split {
                return Err(CmdError::AnotherSplitInProgress(split.clone()));
            } else {
                return Ok(());
            }
        } else {
            return Err(CmdError::PlacementNotFound);
        }
    }

    fn start_split(
        &mut self,
        source: Id,
        dest: Id,
        mid: Vec<u8>,
        src_epoch: u64,
    ) -> Result<u64, CmdError> {
        if let Some(mut source_placement) = self.placements.get_mut(&source) {
            if let &Some(ref in_progress) = &source_placement.in_split {
                return Err(CmdError::AnotherSplitInProgress(in_progress.clone()));
            }
            if mid < source_placement.starts || mid >= source_placement.ends {
                return Err(CmdError::MidOutOfRange);
            }

            source_placement.in_split = Some(InSplitStatus { dest, pivot: mid });
            source_placement.epoch = src_epoch;
            Ok(source_placement.epoch)
        } else {
            Err(CmdError::PlacementNotFound)
        }
    }

    fn complete_split(&mut self, source: Id, dest: Id, src_epoch: u64) -> Result<u64, CmdError> {
        let (dest_placement, src_epoch) =
            if let Some(mut source_placement) = self.placements.get_mut(&source) {
                let dest_placement = if let &Some(ref in_progress) = &source_placement.in_split {
                    if in_progress.dest != dest {
                        return Err(CmdError::AnotherSplitInProgress(in_progress.clone()));
                    }
                    let dest_ends = source_placement.ends.clone();
                    source_placement.ends = in_progress.pivot.clone();
                    Placement {
                        starts: source_placement.ends.clone(),
                        ends: dest_ends,
                        in_split: None,
                        epoch: 0,
                        id: dest,
                    }
                } else {
                    return Err(CmdError::NoSplitInProgress);
                };
                source_placement.in_split = None;
                source_placement.epoch = src_epoch;
                (dest_placement, source_placement.epoch)
            } else {
                return Err(CmdError::PlacementNotFound);
            };
        self.starts.insert(dest_placement.starts.clone(), dest);
        self.placements.insert(dest, dest_placement);
        Ok(src_epoch)
    }

    fn update_epoch(&mut self, source: Id, epoch: u64) -> Result<u64, CmdError> {
        // unconditionally update placement epoch in state machine and return its original value
        if let Some(mut source_placement) = self.placements.get_mut(&source) {
            let original = source_placement.epoch;
            source_placement.epoch = epoch;
            Ok(original)
        } else {
            Err(CmdError::PlacementNotFound)
        }
    }

    fn upsert(&mut self, placement: Placement) -> Result<(), CmdError> {
        if let Some(p) = self.placements.get(&placement.id) {
            if let Some(id) = self.starts.get(&placement.starts) {
                if id != &placement.id {
                    // return error if existed id at this position is not we are inserting
                    return Err(CmdError::PlacementExists);
                }
            }
        }
        self.starts.insert(placement.starts.clone(), placement.id);
        self.placements.insert(placement.id, placement);
        Ok(())
    }

    fn locate(&self, entry: Vec<u8>) -> Result<QueryResult, QueryError> {
        if let Some((_, placement_id)) = self.starts.range(..=entry).last() {
            let placement = self.placements.get(placement_id).unwrap();
            let split = placement
                .in_split
                .as_ref()
                .map(|s| (s.pivot.clone(), s.dest));
            return Ok(QueryResult {
                id: placement.id,
                split,
                epoch: placement.epoch,
            });
        } else {
            return Err(QueryError::OutOfRange);
        }
    }

    fn all(&self) -> Result<Vec<Placement>, ()> {
        Ok(self.placements.values().cloned().collect())
    }

    fn get(&self, id: Id) -> Result<Placement, QueryError> {
        self.placements
            .get(&id)
            .ok_or(QueryError::PlacementNotFound)
            .map(|p| p.clone())
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

impl PlacementSM {
    pub fn new() -> Self {
        Self {
            placements: HashMap::new(),
            starts: BTreeMap::new(),
        }
    }
}
