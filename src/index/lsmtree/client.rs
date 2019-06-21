use index::lsmtree::placement::sm::client::SMClient as PlacementClient;
use index::lsmtree::placement::sm::{Placement as PlacementMeta, QueryError};
use index::lsmtree::service::AsyncServiceClient;
use index::EntryKey;
use linked_hash_map::LinkedHashMap;
use parking_lot::RwLock;
use ram::types::Id;
use server::NebServer;
use std::collections::btree_map::BTreeMap;
use std::sync::atomic::{AtomicUsize, Ordering::Relaxed};
use std::sync::Arc;
use futures::prelude::*;
use index::lsmtree::split::placement_client;

pub struct IndexEntry {
    id: Id,
    val: u64,
}

pub struct Cursor {
    buffer: Vec<IndexEntry>,
    at_end: bool,
    pos: usize,
}

pub struct Placement {
    meta: PlacementMeta,
    client: Arc<AsyncServiceClient>,
}

pub struct LSMTreeClient {
    counter: AtomicUsize,
    placements: RwLock<BTreeMap<Vec<u8>, Placement>>,
    cursors: LinkedHashMap<usize, Cursor>,
    placement_client: PlacementClient,
    neb: Arc<NebServer>,
}

impl LSMTreeClient {
    fn inspect_placement(&self, id: &Id) {
        match self.placement_client.get(id).wait().unwrap() {
            Ok(placement) => {
                let mut placements = self.placements.write();
                let rpc_client = placement_client(id, &self.neb).wait().unwrap();
                placements.insert(placement.starts.clone(), Placement {
                    meta: placement,
                    client: rpc_client
                });
            },
            Err(QueryError::PlacementNotFound) => panic!("cannot find a suitable placement"),
            Err(e) => panic!("{:?}", e)
        }
    }
}
