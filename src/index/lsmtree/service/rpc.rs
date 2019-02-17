use bifrost::rpc::*;
use client::AsyncClient;
use index::lsmtree::service::inner::LSMTreeIns;
use index::lsmtree::tree::LSMTree;
use index::{EntryKey, Ordering};
use parking_lot::RwLock;
use smallvec::SmallVec;
use std::collections::HashMap;
use std::path::Component::CurDir;
use std::sync::atomic;
use std::sync::atomic::AtomicU64;

#[derive(Serialize, Deserialize)]
enum LSMTreeSvrError {
    TreeNotFound,
}

#[derive(Serialize, Deserialize)]
struct LSMTreeSummary {
    id: u64,
    count: u64,
    range: (Vec<u8>, Vec<u8>),
}

service! {
    rpc seek(tree_id: u64, key: Vec<u8>, ordering: Ordering) -> u64 | LSMTreeSvrError;
    rpc next(tree_id: u64, id: u64) -> Option<bool> | LSMTreeSvrError;
    rpc current(tree_id: u64, id: u64) -> Option<Option<Vec<u8>>> | LSMTreeSvrError;
    rpc complete(tree_id: u64, id: u64) -> bool | LSMTreeSvrError;
    rpc new_tree(start: Vec<u8>, end: Vec<u8>) -> u64;
    rpc summary() -> Vec<LSMTreeSummary>;
}

pub struct LSMTreeService {
    neb_client: Arc<AsyncClient>,
    counter: AtomicU64,
    trees: RwLock<HashMap<u64, LSMTreeIns>>,
}

dispatch_rpc_service_functions!(LSMTreeService);

impl Service for LSMTreeService {
    fn seek(
        &self,
        tree_id: u64,
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
        tree_id: u64,
        id: u64,
    ) -> Box<Future<Item = Option<bool>, Error = LSMTreeSvrError>> {
        let trees = self.trees.read();
        box future::result(
            trees
                .get(&tree_id)
                .ok_or(LSMTreeSvrError::TreeNotFound)
                .map(|tree| tree.next(&id)),
        )
    }

    fn current(
        &self,
        tree_id: u64,
        id: u64,
    ) -> Box<Future<Item = Option<Option<Vec<u8>>>, Error = LSMTreeSvrError>> {
        let trees = self.trees.read();
        box future::result(
            trees
                .get(&tree_id)
                .ok_or(LSMTreeSvrError::TreeNotFound)
                .map(|tree| tree.current(&id)),
        )
    }

    fn complete(&self, tree_id: u64, id: u64) -> Box<Future<Item = bool, Error = LSMTreeSvrError>> {
        let trees = self.trees.read();
        box future::result(
            trees
                .get(&tree_id)
                .ok_or(LSMTreeSvrError::TreeNotFound)
                .map(|tree| tree.complete(&id)),
        )
    }

    fn new_tree(&self, start: Vec<u8>, end: Vec<u8>) -> Box<Future<Item = u64, Error = ()>> {
        let mut trees = self.trees.write();
        let tree_id = self.counter.fetch_add(1, atomic::Ordering::Relaxed);
        trees.insert(
            tree_id,
            LSMTreeIns::new(
                &self.neb_client,
                (EntryKey::from(start), EntryKey::from(end)),
            ),
        );
        box future::ok(tree_id)
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
                .collect(),
        )
    }
}

impl LSMTreeService {
    pub fn new(neb_client: &Arc<AsyncClient>) -> Arc<Self> {
        Arc::new(Self {
            neb_client: neb_client.clone(),
            counter: AtomicU64::new(0),
            trees: RwLock::new(HashMap::new()),
        })
    }
}