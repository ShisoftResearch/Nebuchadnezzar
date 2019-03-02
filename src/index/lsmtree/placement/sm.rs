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

#[derive(Serialize, Deserialize)]
pub enum SplitError {
    AnotherSplitInProgress(InSplitStatus),
    CannotFindSplitMeta,
    SplitUnmatchSource,
    NoSplitInProgress,
    MidOutOfRange,
    PlacementNotFound,
}

#[derive(Serialize, Deserialize)]
pub enum QueryError {
    OutOfRange,
}

#[derive(Serialize, Deserialize)]
pub struct QueryResult {
    id: Id,
    split: Option<Id>,
    epoch: u64,
}

#[derive(Serialize, Deserialize, Clone)]
pub struct InSplitStatus {
    dest: Id,
    mid: Vec<u8>,
}

pub struct Placement {
    starts: EntryKey,
    ends: EntryKey,
    in_split: Option<InSplitStatus>,
    epoch: u64,
    id: Id,
}

pub struct PlacementSM {
    placements: HashMap<Id, Placement>,
    starts: BTreeMap<EntryKey, Id>,
    pending_new_ids: BTreeMap<Id, Id>,
}

raft_state_machine! {
    def cmd prepare_split(source: Id)  -> Id | SplitError;
    def cmd start_split(source: Id, dest: Id, mid: Vec<u8>) -> u64 | SplitError;
    def cmd complete_split(source: Id, dest: Id) -> u64 | SplitError;
    def qry locate(id: Vec<u8>) -> QueryResult | QueryError;
}

impl StateMachineCmds for PlacementSM {
    fn prepare_split(&mut self, source: Id) -> Result<Id, SplitError> {
        if !self.placements.contains_key(&source) {
            return Err(SplitError::PlacementNotFound);
        }
        let new_id = Id::rand();
        self.pending_new_ids.insert(new_id, source);
        Ok(new_id)
    }

    fn start_split(&mut self, source: Id, dest: Id, mid: Vec<u8>) -> Result<u64, SplitError> {
        if let Some(mut source_placement) = self.placements.get_mut(&source) {
            if let &Some(ref in_progress) = &source_placement.in_split {
                return Err(SplitError::AnotherSplitInProgress(in_progress.clone()));
            }

            let mid_key: EntryKey = SmallVec::from(mid);
            if mid_key < source_placement.starts || mid_key >= source_placement.ends {
                return Err(SplitError::MidOutOfRange);
            }

            source_placement.in_split = Some(InSplitStatus {
                dest,
                mid: mid_key.into_iter().collect(),
            });
            source_placement.epoch += 1;
            Ok(source_placement.epoch)
        } else {
            Err(SplitError::PlacementNotFound)
        }
    }

    fn complete_split(&mut self, source: Id, dest: Id) -> Result<u64, SplitError> {
        let (dest_placement, src_epoch) =
            if let Some(mut source_placement) = self.placements.get_mut(&source) {
                if let Some(pending_src) = self.pending_new_ids.get(&dest) {
                    if pending_src != &source {
                        return Err(SplitError::SplitUnmatchSource);
                    }
                } else {
                    return Err(SplitError::CannotFindSplitMeta);
                }
                let dest_placement = if let &Some(ref in_progress) = &source_placement.in_split {
                    if in_progress.dest != dest {
                        return Err(SplitError::AnotherSplitInProgress(in_progress.clone()));
                    }
                    let dest_ends = source_placement.ends.clone();
                    let source_ends = SmallVec::from(in_progress.mid.clone());
                    source_placement.ends = source_ends.clone();
                    Placement {
                        starts: source_ends,
                        ends: dest_ends,
                        in_split: None,
                        epoch: 0,
                        id: dest,
                    }
                } else {
                    return Err(SplitError::NoSplitInProgress);
                };
                source_placement.in_split = None;
                source_placement.epoch += 1;
                (dest_placement, source_placement.epoch)
            } else {
                return Err(SplitError::PlacementNotFound);
            };
        self.starts.insert(dest_placement.starts.clone(), dest);
        self.placements.insert(dest, dest_placement);
        self.pending_new_ids.remove(&dest);
        Ok(src_epoch)
    }

    fn locate(&self, entry: Vec<u8>) -> Result<QueryResult, QueryError> {
        let search_key = SmallVec::from(entry);
        if let Some((_, placement_id)) = self.starts.range(..=search_key).last() {
            let placement = self.placements.get(placement_id).unwrap();
            let split = placement.in_split.as_ref().map(|s| s.dest);
            return Ok(QueryResult {
                id: placement.id,
                split,
                epoch: placement.epoch,
            });
        } else {
            return Err(QueryError::OutOfRange);
        }
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
