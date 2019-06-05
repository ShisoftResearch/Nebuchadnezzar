use super::placement::sm::client::SMClient;
use bifrost::rpc::*;
use bifrost_plugins::hash_ident;
use client::AsyncClient;
use dovahkiin::types::custom_types::id::Id;
use index::lsmtree::service::inner::LSMTreeIns;
use index::lsmtree::split::check_and_split;
use index::lsmtree::tree::{LSMTree, LSMTreeResult};
use index::{EntryKey, Ordering};
use itertools::Itertools;
use parking_lot::RwLock;
use rayon::iter::IntoParallelRefIterator;
use rayon::iter::ParallelIterator;
use server::NebServer;
use smallvec::SmallVec;
use std::collections::HashMap;
use std::path::Component::CurDir;
use std::sync::atomic;
use std::sync::atomic::AtomicU64;
use std::thread;
use index::btree::storage::store_changed_nodes;

mod inner;
#[cfg(test)]
mod test;

pub static DEFAULT_SERVICE_ID: u64 = hash_ident!(LSM_TREE_RPC_SERVICE) as u64;

#[derive(Serialize, Deserialize, Debug)]
pub enum LSMTreeSvrError {
    TreeNotFound,
    SMError,
}

#[derive(Serialize, Deserialize)]
pub struct LSMTreeSummary {
    id: Id,
    count: u64,
    epoch: u64,
    range: (Vec<u8>, Vec<u8>),
}

service! {
    rpc seek(tree_id: Id, key: Vec<u8>, ordering: Ordering, epoch: u64) -> LSMTreeResult<u64> | LSMTreeSvrError;
    rpc next(tree_id: Id, cursor_id: u64) -> Option<bool> | LSMTreeSvrError;
    rpc current(tree_id: Id, cursor_id: u64) -> Option<Option<Vec<u8>>> | LSMTreeSvrError;
    rpc complete(tree_id: Id, cursor_id: u64) -> bool | LSMTreeSvrError;
    rpc new_tree(start: Vec<u8>, end: Vec<u8>, id: Id) -> bool;
    rpc summary() -> Vec<LSMTreeSummary>;

    rpc insert(tree_id: Id, key: Vec<u8>, epoch: u64) -> LSMTreeResult<bool> | LSMTreeSvrError;
    rpc merge(tree_id: Id, keys: Vec<Vec<u8>>, epoch: u64) -> LSMTreeResult<()> | LSMTreeSvrError;
    rpc set_epoch(tree_id: Id, epoch: u64) -> u64 | LSMTreeSvrError;
}

pub struct LSMTreeService {
    neb_server: Arc<NebServer>,
    sm: Arc<SMClient>,
    counter: AtomicU64,
    trees: Arc<RwLock<HashMap<Id, Arc<LSMTreeIns>>>>,
}

dispatch_rpc_service_functions!(LSMTreeService);

impl Service for LSMTreeService {
    fn seek(
        &self,
        tree_id: Id,
        key: Vec<u8>,
        ordering: Ordering,
        epoch: u64,
    ) -> Box<Future<Item = LSMTreeResult<u64>, Error = LSMTreeSvrError>> {
        let trees = self.trees.read();
        box future::result(
            trees
                .get(&tree_id)
                .ok_or(LSMTreeSvrError::TreeNotFound)
                .map(|tree| {
                    tree.with_epoch_check(epoch, || tree.seek(&SmallVec::from(key), ordering))
                }),
        )
    }

    fn next(
        &self,
        tree_id: Id,
        cursor_id: u64,
    ) -> Box<Future<Item = Option<bool>, Error = LSMTreeSvrError>> {
        let trees = self.trees.read();
        box future::result(
            trees
                .get(&tree_id)
                .ok_or(LSMTreeSvrError::TreeNotFound)
                .map(|tree| tree.next(&cursor_id)),
        )
    }

    fn current(
        &self,
        tree_id: Id,
        cursor_id: u64,
    ) -> Box<Future<Item = Option<Option<Vec<u8>>>, Error = LSMTreeSvrError>> {
        let trees = self.trees.read();
        box future::result(
            trees
                .get(&tree_id)
                .ok_or(LSMTreeSvrError::TreeNotFound)
                .map(|tree| tree.current(&cursor_id)),
        )
    }

    fn complete(
        &self,
        tree_id: Id,
        cursor_id: u64,
    ) -> Box<Future<Item = bool, Error = LSMTreeSvrError>> {
        let trees = self.trees.read();
        box future::result(
            trees
                .get(&tree_id)
                .ok_or(LSMTreeSvrError::TreeNotFound)
                .map(|tree| tree.complete(&cursor_id)),
        )
    }

    fn new_tree(
        &self,
        start: Vec<u8>,
        end: Vec<u8>,
        id: Id,
    ) -> Box<Future<Item = bool, Error = ()>> {
        let mut trees = self.trees.write();
        let succeed = if trees.contains_key(&id) {
            false
        } else {
            trees.insert(
                id,
                Arc::new(LSMTreeIns::new(
                    (EntryKey::from(start), EntryKey::from(end)),
                    id,
                )),
            );
            true
        };
        box future::ok(succeed)
    }

    fn summary(&self) -> Box<Future<Item = Vec<LSMTreeSummary>, Error = ()>> {
        let trees = self.trees.read();
        box future::ok(
            trees
                .iter()
                .map(|(id, tree)| LSMTreeSummary {
                    id: *id,
                    count: tree.count(),
                    epoch: tree.epoch(),
                    range: tree.range(),
                })
                .sorted_by_key(|tree| tree.range.0.clone())
                .collect(),
        )
    }

    fn insert(
        &self,
        tree_id: Id,
        key: Vec<u8>,
        epoch: u64,
    ) -> Box<Future<Item = LSMTreeResult<bool>, Error = LSMTreeSvrError>> {
        let trees = self.trees.read();
        let neb = self.neb_server.clone();
        box future::result(
            trees
                .get(&tree_id)
                .ok_or(LSMTreeSvrError::TreeNotFound)
                .map(|tree| tree.with_epoch_check(epoch, || tree.insert(SmallVec::from(key))))
        ).and_then(|o| persist(neb, o))
    }

    fn merge(
        &self,
        tree_id: Id,
        keys: Vec<Vec<u8>>,
        epoch: u64,
    ) -> Box<Future<Item = LSMTreeResult<()>, Error = LSMTreeSvrError>> {
        let trees = self.trees.read();
        let neb = self.neb_server.clone();
        box future::result(
            trees
                .get(&tree_id)
                .ok_or(LSMTreeSvrError::TreeNotFound)
                .map(|tree| {
                    tree.with_epoch_check(epoch, || {
                        tree.merge(box keys.into_iter().map(|key| SmallVec::from(key)).collect())
                    })
                }),
        ).and_then(|o| persist(neb, o))
    }

    fn set_epoch(
        &self,
        tree_id: Id,
        epoch: u64,
    ) -> Box<Future<Item = u64, Error = LSMTreeSvrError>> {
        let trees = self.trees.read();
        let sm = self.sm.clone();
        box future::result(
            trees
                .get(&tree_id)
                .ok_or(LSMTreeSvrError::TreeNotFound)
                .map(|tree| tree.set_epoch(epoch)),
        )
        .and_then(move |_| {
            sm.update_epoch(&tree_id, &epoch)
                .map_err(|_| LSMTreeSvrError::SMError)
                .and_then(|r| r.map_err(|_| LSMTreeSvrError::SMError))
        })
    }
}

impl LSMTreeService {
    pub fn new(neb_server: &Arc<NebServer>, sm: &Arc<SMClient>) -> Arc<Self> {
        Arc::new(Self {
            neb_server: neb_server.clone(),
            counter: AtomicU64::new(0),
            trees: Arc::new(RwLock::new(HashMap::new())),
            sm: sm.clone(),
        })
    }
}

fn persist<T, E>(neb: Arc<NebServer>, val: T) -> impl Future<Item = T, Error = E> {
    store_changed_nodes(neb).then(move |_|  future::ok(val))
}
