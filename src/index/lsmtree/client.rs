use index::lsmtree::placement::sm::client::SMClient as PlacementClient;
use index::lsmtree::placement::sm::{Placement as PlacementMeta, QueryError};
use index::lsmtree::service::AsyncServiceClient;
use index::trees::{EntryKey, KEY_SIZE};
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
use index::builder::Feature;
use byteorder::{BigEndian, WriteBytesExt};

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

pub struct SubTree {
    client: Arc<AsyncServiceClient>,
    epoch: u64,
    tree_id: Id
}

impl SubTree {
    pub fn new(tree_id: Id, client: Arc<AsyncServiceClient>, epoch: u64) -> Self {
        Self {
            tree_id, client, epoch
        }
    }
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

    fn get_sub_tree(&self, key: &Vec<u8>) -> SubTree {
        // Stage one, early exit if placement founded. Read lock only
        {
            let placements = self.placements.read();
            let candidate = placements.range::<Vec<u8>, _>(key..).next();
            if let Some((_, candidate_placement)) = candidate {
                if &candidate_placement.meta.ends >= key {
                    // in range, return
                    return SubTree::new(
                        candidate_placement.meta.id,
                        candidate_placement.client.clone(),
                        candidate_placement.meta.epoch
                    );
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
                    return SubTree::new(
                        candidate_placement.meta.id,
                        candidate_placement.client.clone(),
                        candidate_placement.meta.epoch
                    );;
                }
            }
            let placement: PlacementMeta = self.placement_client.locate(&Vec::from(key.as_slice()))
                .wait().unwrap().unwrap();
            let rpc_client = placement_client(&placement.id, &self.neb)
                .wait().unwrap();
            let tree_id = placement.id;
            let epoch = placement.epoch;
            placements.insert(placement.starts.clone(), Placement {
                meta: placement,
                client: rpc_client.clone()
            });
            SubTree::new(tree_id, rpc_client, epoch)
        }
    }

    fn essential_key_components(schema_id: u32, field_id: &u64) -> Vec<u8> {
        let mut key = Vec::with_capacity(KEY_SIZE);
        let mut schema_id_fut = [0u8; 4];
        let mut field_id_fut = [0u8; 8];
        (&mut schema_id_fut as &mut [u8]).write_u32::<BigEndian>(schema_id).unwrap();
        (&mut field_id_fut as &mut [u8]).write_u64::<BigEndian>(*field_id).unwrap();
        key.extend_from_slice(&schema_id_fut);  // 4 bytes
        key.extend_from_slice(&field_id_fut);   // 8 bytes
        key
    }

    pub fn insert(&self,schema_id: u32, field_id: &u64, cell_id: &Id, feature: &Feature) {
        let mut key = Self::essential_key_components(schema_id, field_id);
        key.extend_from_slice(feature); // 8 bytes
        key.extend_from_slice(&cell_id.to_binary()); // ID SIZE
        let sub_tree = self.get_sub_tree(&key);

    }
}
