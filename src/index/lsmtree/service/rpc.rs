use bifrost::rpc::*;
use index::{EntryKey, Ordering};
use index::lsmtree::service::inner::LSMTreeIns;
use smallvec::SmallVec;
use index::lsmtree::tree::LSMTree;
use client::AsyncClient;
use std::path::Component::CurDir;
use std::collections::HashMap;
use std::sync::atomic::AtomicU64;
use parking_lot::RwLock;
use std::sync::atomic;

#[derive(Serialize, Deserialize)]
enum LSMTreeServiceError {
    TreeNotFound
}

service! {
    rpc seek(tree_id: u64, key: Vec<u8>, ordering: Ordering) -> u64 | LSMTreeServiceError;
    rpc next(tree_id: u64, id: u64) -> Option<bool> | LSMTreeServiceError;
    rpc current(tree_id: u64, id: u64) -> Option<Option<Vec<u8>>> | LSMTreeServiceError;
    rpc complete(tree_id: u64, id: u64) -> bool | LSMTreeServiceError;
    rpc new_tree(start: Vec<u8>, end: Vec<u8>) -> u64;
}


pub struct LSMTreeService {
    neb_client: Arc<AsyncClient>,
    counter: AtomicU64,
    trees: RwLock<HashMap<u64, LSMTreeIns>>
}

dispatch_rpc_service_functions!(LSMTreeService);

impl Service for LSMTreeService {
    fn seek(&self, tree_id: u64, key: Vec<u8>, ordering: Ordering) -> Box<Future<Item=u64, Error=LSMTreeServiceError>> {
        let trees = self.trees.read();
        box future::result(
            trees
                .get(&tree_id)
                .ok_or(LSMTreeServiceError::TreeNotFound)
                .map(|tree|tree.seek(SmallVec::from(key), ordering)))
    }

    fn next(&self, tree_id: u64, id: u64) -> Box<Future<Item=Option<bool>, Error=LSMTreeServiceError>> {
        let trees = self.trees.read();
        box future::result(
            trees
                .get(&tree_id)
                .ok_or(LSMTreeServiceError::TreeNotFound)
                .map(|tree|tree.next(&id)))
    }

    fn current(&self, tree_id: u64, id: u64) -> Box<Future<Item=Option<Option<Vec<u8>>>, Error=LSMTreeServiceError>> {
        let trees = self.trees.read();
        box future::result(
            trees
                .get(&tree_id)
                .ok_or(LSMTreeServiceError::TreeNotFound)
                .map(|tree| tree.current(&id)))
    }

    fn complete(&self, tree_id: u64, id: u64) -> Box<Future<Item=bool, Error=LSMTreeServiceError>> {
        let trees = self.trees.read();
        box future::result(
            trees
                .get(&tree_id)
                .ok_or(LSMTreeServiceError::TreeNotFound)
                .map(|tree| tree.complete(&id)))
    }

    fn new_tree(&self, start: Vec<u8>, end: Vec<u8>) -> Box<Future<Item=u64, Error=()>> {
        let mut trees = self.trees.write();
        let tree_id = self.counter.fetch_add(1, atomic::Ordering::Relaxed);
        trees.insert(tree_id, LSMTreeIns::new(&self.neb_client, (EntryKey::from(start), EntryKey::from(end))));
        box future::ok(tree_id)
    }
}

impl LSMTreeService {
    pub fn new(neb_client: &Arc<AsyncClient>) -> Arc<Self> {
        Arc::new(Self {
            neb_client: neb_client.clone(),
            counter: AtomicU64::new(0),
            trees: RwLock::new(HashMap::new())
        })
    }
}