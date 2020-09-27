use super::super::sm::client::SMClient;
use super::super::trees::*;
pub use super::btree::level::LEVEL_M as BLOCK_SIZE;
use super::btree::storage;
use super::tree::*;
use crate::client::AsyncClient;
use crate::ram::types::Id;
use crate::ram::types::RandValue;
use bifrost::conshash::ConsistentHashing;
use bifrost::utils::time::get_time;
use bifrost_plugins::hash_ident;
use futures::future::BoxFuture;
use futures::prelude::*;
use lightning::map::Map;
use lightning::map::{HashMap, ObjectMap};
use parking_lot::RwLock;
use serde::{Deserialize, Serialize};
use std::cell::RefCell;
use std::sync::atomic::{AtomicUsize, Ordering::Relaxed};
use std::time::Duration;

pub type IdBlock = [Id; BLOCK_SIZE];
pub static DEFAULT_SERVICE_ID: u64 = hash_ident!(LSM_TREE_RPC_SERVICE) as u64;

#[derive(Clone, Serialize, Deserialize)]
pub struct Boundary {
    upper: EntryKey,
    lower: EntryKey,
}

#[derive(Clone, Serialize, Deserialize)]
pub enum OpResult<T> {
    Successful(T),
    NotFound,
    OutOfBound,
    Migrating,
}

#[derive(Clone, Copy, Serialize, Deserialize)]
pub struct ServCursor {
    cursor_id: u64,
}

pub struct DistLSMTree {
    id: Id,
    tree: LSMTree,
    prop: RwLock<DistProp>,
}

struct DistProp {
    boundary: Boundary,
    migration: Option<Migration>,
}

struct Migration {
    pivot: EntryKey,
}

pub struct CursorMemo {
    tree_cursor: LSMTreeCursor,
    expires: i64,
}

service! {
    rpc crate_tree(id: Id, boundary: Boundary);
    rpc load_tree(id: Id, boundary: Boundary);
    rpc insert(id: Id, entry: EntryKey) -> OpResult<bool>;
    rpc delete(id: Id, entry: EntryKey) -> OpResult<bool>;
    rpc seek(id: Id, entry: EntryKey, ordering: Ordering, cursor_lifetime: u16)
        -> OpResult<(ServCursor, Option<Id>)>;
    rpc renew_cursor(cursor: ServCursor, time: u16) -> bool;
    rpc dispose_cursor(cursor: ServCursor) -> bool;
    rpc cursor_next(cursor: ServCursor) -> Option<IdBlock>;
}

pub struct LSMTreeService {
    client: Arc<AsyncClient>,
    cursor_counter: AtomicUsize,
    cursors: ObjectMap<Arc<RefCell<CursorMemo>>>,
    trees: Arc<HashMap<Id, Arc<DistLSMTree>>>,
}

impl Service for LSMTreeService {
    fn crate_tree(&self, id: Id, boundary: Boundary) -> BoxFuture<()> {
        async move {
            if self.trees.contains(&id) {
                return;
            }
            let tree = LSMTree::create(&self.client, &id).await;
            self.trees
                .insert(&id, Arc::new(DistLSMTree::new(id, tree, boundary, None)));
        }
        .boxed()
    }

    fn load_tree(&self, id: Id, boundary: Boundary) -> BoxFuture<()> {
        async move {
            if self.trees.contains(&id) {
                return;
            }
            let tree = LSMTree::recover(&self.client, &id).await;
            self.trees
                .insert(&id, Arc::new(DistLSMTree::new(id, tree, boundary, None)));
        }
        .boxed()
    }

    fn insert(&self, id: Id, entry: EntryKey) -> BoxFuture<OpResult<bool>> {
        self.apply_in_ranged_tree(id, entry, |entry, tree| {
            if tree.insert(&entry) {
                OpResult::Successful(true)
            } else {
                OpResult::Successful(false)
            }
        })
    }

    fn delete(&self, id: Id, entry: EntryKey) -> BoxFuture<OpResult<bool>> {
        self.apply_in_ranged_tree(id, entry, |entry, tree| {
            if tree.delete(&entry) {
                OpResult::Successful(true)
            } else {
                OpResult::Successful(false)
            }
        })
    }

    fn seek(
        &self,
        id: Id,
        entry: EntryKey,
        ordering: Ordering,
        cursor_lifetime: u16,
    ) -> BoxFuture<OpResult<(ServCursor, Option<Id>)>> {
        self.apply_in_ranged_tree(id, entry, |entry, tree| {
            let mut tree_cursor = tree.seek(&entry, ordering);
            let cursor_id = self.cursor_counter.fetch_add(1, Relaxed);
            let expires = get_time() + cursor_lifetime as i64;
            let first_entry = tree_cursor.next();
            let cursor_memo = CursorMemo {
                tree_cursor,
                expires,
            };
            self.cursors
                .insert(&(cursor_id), Arc::new(RefCell::new(cursor_memo)));
            let cursor = ServCursor {
                cursor_id: cursor_id as u64,
            };
            OpResult::Successful((cursor, first_entry.map(|entry| entry.id())))
        })
    }

    fn renew_cursor(&self, cursor: ServCursor, time: u16) -> BoxFuture<bool> {
        future::ready(
            if let Some(cursor) = self.cursors.write(cursor.cursor_id as usize) {
                cursor.borrow_mut().expires = get_time() + time as i64;
                true
            } else {
                false
            },
        )
        .boxed()
    }

    fn dispose_cursor(&self, cursor: ServCursor) -> BoxFuture<bool> {
        future::ready(self.cursors.remove(&(cursor.cursor_id as usize)).is_some()).boxed()
    }

    fn cursor_next(&self, cursor: ServCursor) -> BoxFuture<Option<IdBlock>> {
        future::ready(
            if let Some(cursor) = self.cursors.write(cursor.cursor_id as usize) {
                let mut cursor_ref = cursor.borrow_mut();
                Some(id_block_from_cursor_memo(&mut *cursor_ref))
            } else {
                None
            },
        )
        .boxed()
    }
}

impl LSMTreeService {
    pub fn new(client: &Arc<AsyncClient>, sm_client: &Arc<SMClient>) -> Self {
        let trees_map = Arc::new(HashMap::with_capacity(32));
        super::btree::storage::start_external_nodes_write_back(client);
        Self::start_tree_balancer(&trees_map, client, sm_client);
        Self {
            client: client.clone(),
            cursor_counter: AtomicUsize::new(0),
            cursors: ObjectMap::with_capacity(64),
            trees: trees_map,
        }
    }

    pub fn start_tree_balancer(
        trees_map: &Arc<HashMap<Id, Arc<DistLSMTree>>>,
        client: &Arc<AsyncClient>,
        sm_client: &Arc<SMClient>,
    ) {
        let trees_map = trees_map.clone();
        let client = client.clone();
        let sm_client = sm_client.clone();
        tokio::spawn(async move {
            loop {
                for (_, dist_tree) in trees_map.entries() {
                    let tree = &dist_tree.tree;
                    tree.merge_levels();
                    if tree.oversized() {
                        // Tree oversized, need to migrate
                        let mid_key = tree.mid_key().unwrap();
                        let migration_target_id = Id::rand();
                        let migration_tree = LSMTree::create(&client, &migration_target_id).await;
                        {
                            let mut dist_tree_prop = dist_tree.prop.write();
                            dist_tree_prop.migration = Some(Migration {
                                pivot: mid_key.clone(),
                            });
                        }
                        tree.mark_migration(&dist_tree.id, Some(migration_target_id), &client)
                            .await;
                        let buffer_size = BLOCK_SIZE << 2;
                        let mut cursor = tree.seek(&mid_key, Ordering::Forward);
                        let mut entry_buffer = Vec::with_capacity(buffer_size);
                        // Moving keys
                        while cursor.current().is_some() {
                            if let Some(entry) = cursor.next() {
                                entry_buffer.push(entry);
                                if entry_buffer.len() >= buffer_size {
                                    migration_tree.merge_keys(Box::new(entry_buffer));
                                    entry_buffer = Vec::with_capacity(buffer_size);
                                }
                            }
                        }
                        storage::wait_until_updated().await;
                        sm_client
                            .split(&migration_target_id, &mid_key)
                            .await
                            .unwrap();
                        // Reset state on current tree
                        tree.mark_migration(&dist_tree.id, None, &client).await;
                        {
                            let mut dist_prop = dist_tree.prop.write();
                            dist_prop.boundary.upper = mid_key;
                            dist_prop.migration = None;
                        }
                    }
                }
                // Sleep for a while to check for trees to be merge in levels
                tokio::time::delay_for(Duration::from_secs(5)).await;
            }
        });
    }

    fn apply_in_ranged_tree<F, R>(&self, id: Id, entry: EntryKey, func: F) -> BoxFuture<OpResult<R>>
    where
        F: Fn(&EntryKey, &LSMTree) -> OpResult<R>,
        R: Send + 'static,
    {
        future::ready(if let Some(tree) = self.trees.get(&id) {
            let tree_prop = tree.prop.read();
            if tree_prop.boundary.in_boundary(&entry) {
                if let &Some(ref migration) = &tree_prop.migration {
                    if entry < migration.pivot {
                        // Entries lower than pivot should be safe to work on
                        func(&entry, &tree.tree)
                    } else {
                        OpResult::Migrating
                    }
                } else {
                    func(&entry, &tree.tree)
                }
            } else {
                OpResult::OutOfBound
            }
        } else {
            OpResult::NotFound
        })
        .boxed()
    }
}

fn id_block_from_cursor_memo(cursor_memo: &mut CursorMemo) -> IdBlock {
    let mut res = IdBlock::default();
    for entry in res.iter_mut() {
        if let Some(tree_entry) = cursor_memo.tree_cursor.next() {
            *entry = tree_entry.id();
        } else {
            break;
        }
    }
    res
}

impl DistLSMTree {
    fn new(id: Id, tree: LSMTree, boundary: Boundary, migration: Option<Migration>) -> Self {
        let prop = RwLock::new(DistProp {
            boundary,
            migration,
        });
        Self { id, tree, prop }
    }
}

impl Boundary {
    pub fn new(lower: EntryKey, upper: EntryKey) -> Self {
        Boundary { lower, upper }
    }
    fn in_boundary(&self, entry: &EntryKey) -> bool {
        return entry >= &self.lower && entry < &self.upper;
    }
}

dispatch_rpc_service_functions!(LSMTreeService);

unsafe impl Send for DistLSMTree {}
unsafe impl Sync for DistLSMTree {}

pub fn client_by_rpc_client(rpc: &Arc<RPCClient>) -> Arc<AsyncServiceClient> {
    AsyncServiceClient::new(DEFAULT_SERVICE_ID, rpc)
}

pub async fn locate_tree_server_from_conshash(
    id: &Id,
    conshash: &Arc<ConsistentHashing>,
) -> Result<Arc<AsyncServiceClient>, RPCError> {
    let server_id = conshash.get_server_id_by(id).unwrap();
    DEFAULT_CLIENT_POOL
        .get_by_id(server_id, move |sid| conshash.to_server_name(sid))
        .await
        .map_err(|e| RPCError::IOError(e))
        .map(|c| client_by_rpc_client(&c))
}
