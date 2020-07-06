use super::placement::sm::client::SMClient;
use bifrost::rpc::*;
use bifrost_plugins::hash_ident;
use crate::client::AsyncClient;
use dovahkiin::types::custom_types::id::Id;
use crate::index::btree::storage::store_changed_nodes;
use crate::index::lsmtree::persistent::level_trees_from_cell;
use crate::index::lsmtree::service::inner::LSMTreeIns;
use crate::index::lsmtree::split::check_and_split;
use crate::index::lsmtree::tree::{LSMTree, LSMTreeResult};
use crate::index::trees::{EntryKey, Ordering};
use itertools::Itertools;
use parking_lot::RwLock;
use crate::ram::cell::{Cell, ReadError};
use rayon::prelude::*;
use crate::server::NebServer;
use smallvec::SmallVec;
use std::collections::HashMap;
use std::path::Component::CurDir;
use std::sync::atomic;
use std::sync::atomic::AtomicU64;
use std::thread;

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

#[derive(Serialize, Deserialize)]
pub struct LSMTreeBlock {
    pub data: Vec<Vec<u8>>,
    pub cursor_id: u64
}

service! {
    rpc seek(tree_id: Id, key: Vec<u8>, ordering: Ordering, epoch: u64, block_size: u32) -> Result<LSMTreeResult<Option<LSMTreeBlock>>, LSMTreeSvrError>;
    rpc next_block(tree_id: Id, cursor_id: u64, size: u32) -> Result<Option<Vec<Vec<u8>>>, LSMTreeSvrError>;
    rpc current(tree_id: Id, cursor_id: u64) -> Result<Option<Option<Vec<u8>>>, LSMTreeSvrError>;
    rpc complete(tree_id: Id, cursor_id: u64) -> Result<bool, LSMTreeSvrError>;
    rpc new_tree(start: Vec<u8>, end: Vec<u8>, id: Id) -> bool;
    rpc summary() -> Vec<LSMTreeSummary>;

    rpc insert(tree_id: Id, key: Vec<u8>, epoch: u64) -> Result<LSMTreeResult<bool>, LSMTreeSvrError>;
    rpc merge(tree_id: Id, keys: Vec<Vec<u8>>, epoch: u64) -> Result<LSMTreeResult<()>, LSMTreeSvrError>;
    rpc set_epoch(tree_id: Id, epoch: u64) -> Result<u64, LSMTreeSvrError>;
}

pub struct LSMTreeService {
    neb_server: Arc<NebServer>,
    sm: Arc<SMClient>,
    counter: AtomicU64,
    trees: Arc<RwLock<HashMap<Id, LSMTreeIns>>>,
}

dispatch_rpc_service_functions!(LSMTreeService);

impl Service for LSMTreeService {
    fn seek(
        &self,
        tree_id: Id,
        key: Vec<u8>,
        ordering: Ordering,
        epoch: u64,
        block_size: u32
    ) -> Box<Future<Item = LSMTreeResult<Option<LSMTreeBlock>>, Error = LSMTreeSvrError>> {
        let trees = self.trees.read();
        box future::result(
            trees
                .get(&tree_id)
                .ok_or(LSMTreeSvrError::TreeNotFound)
                .map(|tree| {
                    tree.with_epoch_check(epoch, || {
                        let cursor_id = tree.seek(&SmallVec::from(key), ordering);
                        let data = tree.next_block(&cursor_id, block_size as usize);
                        data.map(|data| LSMTreeBlock {
                            data,
                            cursor_id
                        })
                    })
                }),
        )
    }

    fn next_block(&self, tree_id: Id, cursor_id: u64, size: u32) -> Box<Future<Item=Option<Vec<Vec<u8>>>, Error=LSMTreeSvrError>> {
        let trees = self.trees.read();
        box future::result(
            trees
                .get(&tree_id)
                .ok_or(LSMTreeSvrError::TreeNotFound)
                .map(|tree| {
                    tree.next_block(&cursor_id, size as usize)
                }),
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
                LSMTreeIns::new((EntryKey::from(start), EntryKey::from(end)), id),
            );
            true
        };
        box persist(self.neb_server.clone(), succeed)
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
                .map(|tree| tree.with_epoch_check(epoch, || tree.insert(SmallVec::from(key)))),
        )
        .and_then(|o| persist(neb, o))
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
        )
        .and_then(|o| persist(neb, o))
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
    pub fn new(
        neb_server: &Arc<NebServer>,
        neb_client: &Arc<AsyncClient>,
        sm: &Arc<SMClient>,
    ) -> Arc<Self> {
        let trees = Arc::new(RwLock::new(HashMap::<Id, LSMTreeIns>::new()));
        let trees_clone = trees.clone();
        let neb = neb_server.clone();
        thread::Builder::new()
            .name("LSM-tree service sentinel".to_string())
            .spawn(move || {
                thread::sleep(Duration::from_millis(500));
                let tree_map = trees_clone.read();
                tree_map
                    .par_iter()
                    .map(|(k, v)| v)
                    .filter(|tree| tree.oversized())
                    .for_each(|tree| {
                        tree.check_and_merge();
                    });
                persist::<_, ()>(neb.clone(), ()).wait().unwrap();
            });
        let service = Self {
            neb_server: neb_server.clone(),
            counter: AtomicU64::new(0),
            sm: sm.clone(),
            trees,
        };
        {
            let mut trees = service.trees.write();
            let placements = service
                .sm
                .all_for_server(&neb_server.server_id)
                .wait()
                .unwrap()
                .unwrap();
            // the placement state machine have all the data except header id for each level, need to get them
            let fetches = placements
                .iter()
                .map(|placement| neb_client.read_cell(placement.id));
            let tree_id_cells: Vec<Result<Cell, ReadError>> = join_all(fetches).wait().unwrap();
            let lsm_trees: Vec<_> = tree_id_cells
                .into_par_iter()
                .zip(placements)
                .map(|(res, p)| {
                    let cell = res.unwrap();
                    let tree_ids = level_trees_from_cell(cell);
                    let starts = EntryKey::from(p.starts);
                    let ends = EntryKey::from(p.ends);
                    LSMTree::new_with_level_ids((starts, ends), p.id, tree_ids, neb_client)
                })
                .collect();
            for lsm_tree in lsm_trees {
                trees.insert(lsm_tree.id, LSMTreeIns::new_from_tree(lsm_tree));
            }
        }
        Arc::new(service)
    }
}

fn persist<T, E>(neb: Arc<NebServer>, val: T) -> impl Future<Item = T, Error = E> {
    store_changed_nodes(neb).then(move |_| future::ok(val))
}
