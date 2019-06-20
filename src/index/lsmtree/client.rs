use index::lsmtree::placement::sm::client::SMClient as PlacementClient;
use index::lsmtree::placement::sm::Placement as PlacementMeta;
use index::lsmtree::service::AsyncServiceClient;
use index::EntryKey;
use linked_hash_map::LinkedHashMap;
use parking_lot::RwLock;
use ram::types::Id;
use server::NebServer;
use std::collections::btree_map::BTreeMap;
use std::sync::atomic::{AtomicUsize, Ordering::Relaxed};
use std::sync::Arc;

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
    placements: RwLock<BTreeMap<EntryKey, Placement>>,
    cursors: LinkedHashMap<usize, Cursor>,
    placement_client: PlacementClient,
    neb: Arc<NebServer>,
}
