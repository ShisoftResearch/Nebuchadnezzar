use bifrost::rpc::*;
use bifrost_plugins::hash_ident;
use client::AsyncClient;
use dovahkiin::types::custom_types::id::Id;
use index::lsmtree::service::inner::LSMTreeIns;
use index::lsmtree::tree::{LSMTree, LSMTreeResult};
use index::{EntryKey, Ordering};
use itertools::Itertools;
use parking_lot::RwLock;
use rayon::iter::IntoParallelRefIterator;
use rayon::iter::ParallelIterator;
use smallvec::SmallVec;
use std::collections::HashMap;
use std::path::Component::CurDir;
use std::sync::atomic;
use std::sync::atomic::AtomicU64;
use std::thread;

mod inner;

pub static DEFAULT_SERVICE_ID: u64 = hash_ident!(LSM_TREE_RPC_SERVICE) as u64;

#[derive(Serialize, Deserialize)]
enum LSMTreeSvrError {
    TreeNotFound,
}

#[derive(Serialize, Deserialize)]
struct LSMTreeSummary {
    id: Id,
    count: u64,
    range: (Vec<u8>, Vec<u8>),
}

service! {
    rpc seek(tree_id: Id, key: Vec<u8>, ordering: Ordering) -> u64 | LSMTreeSvrError;
    rpc next(tree_id: Id, cursor_id: u64) -> Option<bool> | LSMTreeSvrError;
    rpc current(tree_id: Id, cursor_id: u64) -> Option<Option<Vec<u8>>> | LSMTreeSvrError;
    rpc complete(tree_id: Id, cursor_id: u64) -> bool | LSMTreeSvrError;
    rpc new_tree(start: Vec<u8>, end: Vec<u8>, id: Id);
    rpc summary() -> Vec<LSMTreeSummary>;

    rpc insert(tree_id: Id, key: Vec<u8>, epoch: u64) -> LSMTreeResult<bool> | LSMTreeSvrError;
    rpc merge(tree: Id, keys: Vec<Vec<u8>>, epoch: u64) -> LSMTreeResult<bool> | LSMTreeSvrError;
}

pub struct LSMTreeService {
    neb_client: Arc<AsyncClient>,
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
    ) -> Box<Future<Item = u64, Error = LSMTreeSvrError>> {
        let trees = self.trees.read();
        box future::result(
            trees
                .get(&tree_id)
                .ok_or(LSMTreeSvrError::TreeNotFound)
                .map(|tree| tree.seek(SmallVec::from(key), ordering)),
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

    fn complete(&self, tree_id: Id, cursor_id: u64) -> Box<Future<Item = bool, Error = LSMTreeSvrError>> {
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
    ) -> Box<Future<Item = (), Error = ()>> {
        let mut trees = self.trees.write();
        trees.insert(
            id,
            Arc::new(LSMTreeIns::new(
                &self.neb_client,
                (EntryKey::from(start), EntryKey::from(end)),
                id,
            )),
        );
        box future::ok(())
    }

    fn summary(&self) -> Box<Future<Item = Vec<LSMTreeSummary>, Error = ()>> {
        let trees = self.trees.read();
        box future::ok(
            trees
                .iter()
                .map(|(id, tree)| LSMTreeSummary {
                    id: *id,
                    count: tree.count(),
                    range: tree.range(),
                })
                .sorted_by_key(|tree| tree.range.0.clone())
                .collect(),
        )
    }

    fn insert(&self, tree_id: Id, key: Vec<u8>, epoch: u64) -> Box<Future<Item=LSMTreeResult<bool>, Error=LSMTreeSvrError>> {
        let trees = self.trees.read();
        box future::result(trees.get(&tree_id).ok_or(LSMTreeSvrError::TreeNotFound).map(|tree| tree.insert(SmallVec::from(key), epoch)))
    }

    fn merge(&self, tree: Id, keys: Vec<Vec<u8>>, epoch: u64) -> Box<Future<Item=LSMTreeResult<bool>, Error=LSMTreeSvrError>> {
        unimplemented!()
    }
}

impl LSMTreeService {
    pub fn new(neb_client: &Arc<AsyncClient>) -> Arc<Self> {
        Arc::new(Self {
            neb_client: neb_client.clone(),
            counter: AtomicU64::new(0),
            trees: Arc::new(RwLock::new(HashMap::new())),
        })
    }

    pub fn start_sentinel(&self) {
        let trees_lock = self.trees.clone();
        thread::Builder::new()
            .name("LSM-Tree Serivce Sentinel".to_string())
            .spawn(move || loop {
                let trees = trees_lock.read().values().cloned().collect_vec();
                trees.par_iter().for_each(|tree| {
                    tree.check_and_merge();
                });
                thread::sleep(Duration::from_millis(50));
            });
    }
}
