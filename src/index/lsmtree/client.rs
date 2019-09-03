use index::lsmtree::placement::sm::client::SMClient as PlacementClient;
use index::lsmtree::placement::sm::{Placement as PlacementMeta, QueryError};
use index::lsmtree::service::AsyncServiceClient;
use index::trees::EntryKey;
use linked_hash_map::LinkedHashMap;
use parking_lot::RwLock;
use ram::types::Id;
use server::NebServer;
use std::collections::btree_map::BTreeMap;
use std::sync::atomic::{AtomicUsize, Ordering::Relaxed};
use std::sync::Arc;
use futures::prelude::*;
use index::lsmtree::split::placement_client;
use index::lsmtree::placement;

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
                let rpc_client = placement_client(&placement.id, &self.neb).wait().unwrap();
                placements.insert(placement.starts.clone(), Placement {
                    meta: placement,
                    client: rpc_client
                });
            },
            Err(QueryError::PlacementNotFound) => panic!("cannot find a suitable placement"),
            Err(e) => panic!("{:?}", e)
        }
    }

    fn get_client(&self, key: &Vec<u8>) -> Arc<AsyncServiceClient> {
        // Stage one, early exit if placement founded. Read lock only
        {
            let placements = self.placements.read();
            let candidate = placements.range::<Vec<u8>, _>(key..).next();
            if let Some((_, candidate_placement)) = candidate {
                if &candidate_placement.meta.ends >= key {
                    // in range, return
                    return candidate_placement.client.clone();
                }
            }
        }

        // Stage two, write lock. Also check for placement availability. If not, insert it to the cache.
        {
            let mut placements = self.placements.write();
            let candidate = placements.range::<Vec<u8>, _>(key..).next();
            if let Some((_, candidate_placement)) = candidate {
                if &candidate_placement.meta.ends >= key {
                    // in range, return
                    return candidate_placement.client.clone();
                }
            }
            let placement: PlacementMeta = self.placement_client.locate(&Vec::from(key.as_slice()))
                .wait().unwrap().unwrap();
            let rpc_client = placement_client(&placement.id, &self.neb)
                .wait().unwrap();
            placements.insert(placement.starts.clone(), Placement {
                meta: placement,
                client: rpc_client.clone()
            });
            return rpc_client;
        }
    }
}
