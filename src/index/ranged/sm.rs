use super::lsm::{service::AsyncServiceClient as LSMServiceClient, tree::INITIAL_TREE_EPOCH};
use super::lsm::service::*;
use super::trees::*;
use crate::ram::types::Id;
use crate::ram::types::RandValue;
use bifrost::conshash::ConsistentHashing;
use bifrost::raft::state_machine::StateMachineCtl;
use bifrost::raft::RaftService;
use bifrost::rpc::RPCError;
use bifrost::utils;
use bifrost_plugins::hash_ident;
use futures::prelude::*;
use std::collections::BTreeMap;
use std::sync::Arc;
use std::ops::Bound::*;

pub const DEFAULT_SM_ID: u64 = hash_ident!("RANGED_INDEX_SM_ID") as u64;

#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct TreePlacement {
    pub id: Id,
    pub epoch: u64
}

#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct TreeInfo {
    pub lower: EntryKey,
    pub upper: EntryKey,
    pub placement: TreePlacement,
}

pub struct MasterTreeSM {
    tree: BTreeMap<EntryKey, TreePlacement>,
    raft_svr: Arc<RaftService>,
    conshash: Arc<ConsistentHashing>,
}

raft_state_machine! {
    def qry locate_key(entry: EntryKey) -> (EntryKey, TreePlacement, EntryKey);
    def qry next_tree(tree_lower: EntryKey, ordering: Ordering) -> Option<TreeInfo>;
    def cmd split(src_tree: Id, new_tree: Id, pivot: EntryKey);
    // No subscription for clients
}

impl StateMachineCmds for MasterTreeSM {
    fn locate_key(&self, entry: EntryKey) -> BoxFuture<(EntryKey, TreePlacement, EntryKey)> {
        let (lower, tree) = self
            .tree
            .range(..=entry.clone())
            .last()
            .map(|(key, tree)| (key.clone(), tree.clone()))
            .unwrap();
        let upper = self
            .tree
            .range((Excluded(entry), Unbounded))
            .next()
            .map(|(key, _)| key.clone())
            .unwrap_or_else(|| EntryKey::max());
        future::ready((lower, tree, upper)).boxed()
    }

    fn next_tree(&self, tree_lower: EntryKey, ordering: Ordering) -> BoxFuture<Option<TreeInfo>> {
        future::ready(match ordering {
            Ordering::Forward => {
                let mut iter = self.tree.range((Excluded(&tree_lower), Unbounded));
                if let Some((next_lower, next_place)) = iter.next() {
                    let next_upper = iter.next().map(|(k, _)| k).unwrap_or_else(|| &*MAX_ENTRY_KEY);
                    Some(TreeInfo {
                        lower: next_lower.clone(),
                        upper: next_upper.clone(),
                        placement: next_place.clone()
                    })
                } else {
                    None
                }
            },
            Ordering::Backward => {
                let mut iter = self.tree.range(..&tree_lower).rev();
                if let Some((next_lower, next_place)) = iter.next() {
                    Some(TreeInfo {
                        lower: next_lower.clone(),
                        upper: tree_lower.clone(),
                        placement: next_place.clone()
                    })
                } else {
                    None
                }
            },
        }).boxed()
    }

    fn split(&mut self, src_tree: Id, new_tree: Id, pivot: EntryKey) -> BoxFuture<()> {
        // Call this after the tree have been split and persisted
        async move {
            let upper_bound = match self.tree.range(&pivot..).next() {
                Some((k, _id)) => k.clone(),
                None => max_entry_key()
            };
            debug_assert!(pivot < upper_bound);
            debug!("Splitted to new tree {:?}, starts at {:?}, ends at {:?}", new_tree, pivot, upper_bound);
            self.tree.insert(pivot.clone(), TreePlacement::new(new_tree));
            self.load_sub_tree(new_tree, &pivot, &upper_bound, INITIAL_TREE_EPOCH).await;
            if let Some((_, mut prev_tree)) = self.tree.range_mut(..&pivot).last() {
                assert_eq!(prev_tree.id, src_tree);
                prev_tree.epoch += 1;
            }
        }.boxed()
    }
}

impl StateMachineCtl for MasterTreeSM {
    raft_sm_complete!();
    fn id(&self) -> u64 {
        DEFAULT_SM_ID
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
    pub fn new(raft_svr: &Arc<RaftService>, conshash: &Arc<ConsistentHashing>) -> Self {
        Self {
            tree: BTreeMap::new(),
            raft_svr: raft_svr.clone(),
            conshash: conshash.clone(),
        }
    }
    pub async fn try_initialize(&mut self) -> bool {
        let genesis_id = Id::rand();
        self.tree.insert(min_entry_key(), TreePlacement::new(genesis_id));
        locate_tree_server_from_conshash(&genesis_id, &self.conshash)
            .await
            .unwrap()
            .crate_tree(genesis_id, Boundary::new(min_entry_key(), max_entry_key()), INITIAL_TREE_EPOCH)
            .await
            .unwrap();
        true
    }
    async fn load_sub_tree(&mut self, id: Id, lower: &EntryKey, upper: &EntryKey, epoch: u64) {
        if self.raft_svr.is_leader() {
            // Only the leader can initiate the request to load the sub tree
            info!("Placement leader calling to load sub tree {:?} with lower key {:?}, upper key {:?}", id, lower, upper);
            let client = self.locate_tree_server(&id).await.unwrap();
            debug!("Located {:?} at server {:?}", id, client.server_id());
            client
                .load_tree(id, Boundary::new(lower.clone(), upper.clone()), epoch)
                .await
                .unwrap();
            debug!("Tree loaded for {:?}", id);
        }
    }

    async fn locate_tree_server(&self, id: &Id) -> Result<Arc<LSMServiceClient>, RPCError> {
        locate_tree_server_from_conshash(id, &self.conshash).await
    }
}

impl TreePlacement {
    pub fn new(id: Id) -> Self {
        TreePlacement {
            id, epoch: INITIAL_TREE_EPOCH
        }
    }
}