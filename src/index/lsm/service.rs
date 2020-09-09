use crate::index::trees::EntryKey;
use crate::ram::types::Id;
use super::tree::*;
use crate::index::btree::level::LEVEL_M as BLOCK_SIZE;
use crate::client::AsyncClient;
use crate::index::trees::*;
use crate::ram::types::RandValue;
use crate::index::btree::storage;
use parking_lot::RwLock;
use std::cell::RefCell;
use bifrost::utils::time::get_time;
use serde::{Serialize, Deserialize};
use lightning::map::{ObjectMap, HashMap};
use lightning::map::Map;
use std::sync::atomic::{AtomicUsize, Ordering::Relaxed};
use std::time::Duration;
use futures::future::BoxFuture;
use futures::prelude::*;

pub type EntryKeyBlock = [EntryKey; BLOCK_SIZE];

#[derive(Clone, Serialize, Deserialize)]
pub struct Boundary {
    upper: EntryKey,
    lower: EntryKey
}

#[derive(Clone, Serialize, Deserialize)]
pub enum OpResult<T> {
    Successful(T),
    Failed,
    NotFound,
    OutOfBound,
    Migrating
}

#[derive(Clone, Serialize, Deserialize)]
pub struct ServCursor {
    cursor_id: u64
}

pub struct DistLSMTree {
    id: Id,
    tree: LSMTree,
    prop: RwLock<DistProp>
}

struct DistProp {
    boundary: Boundary,
    migration: Option<Migration>
}

struct Migration {
    pivot: EntryKey,
}

pub struct CursorMemo {
    tree_cursor: LSMTreeCursor,
    expires: i64
}

service! {
    rpc crate_tree(id: Id, boundary: Boundary);
    rpc load_tree(id: Id, boundary: Boundary);
    rpc insert(id: Id, entry: EntryKey) -> OpResult<()>;
    rpc delete(id: Id, entry: EntryKey) -> OpResult<()>;
    rpc seek(id: Id, entry: EntryKey, ordering: Ordering, cursor_lifetime: u16) -> OpResult<ServCursor>;
    rpc renew_cursor(cursor: ServCursor, time: u16) -> bool;
    rpc dispose_cursor(cursor: ServCursor) -> bool;
    rpc cursor_next(cursor: ServCursor) -> Option<EntryKeyBlock>;
}

pub struct LSMTreeService {
    client: Arc<AsyncClient>,
    cursor_counter: AtomicUsize,
    cursors: ObjectMap<Arc<RefCell<CursorMemo>>>,
    trees: Arc<HashMap<Id, Arc<DistLSMTree>>>
}

impl Service for LSMTreeService {
    fn crate_tree(&self, id: Id, boundary: Boundary) -> BoxFuture<()> {
        async move {
            let tree = LSMTree::create(&self.client, &id).await;
            self.trees.insert(&id, Arc::new(DistLSMTree::new(id, tree, boundary, None)));
        }.boxed()
    }

    fn load_tree(&self, id: Id, boundary: Boundary) -> BoxFuture<()> {
        async move {
            let tree = LSMTree::recover(&self.client, &id).await;
            self.trees.insert(&id, Arc::new(DistLSMTree::new(id, tree, boundary, None)));
        }.boxed()
    }

    fn insert(&self, id: Id, entry: EntryKey) -> BoxFuture<OpResult<()>> {
        self.apply_in_ranged_tree(
            id, entry, 
            |entry, tree| {
                if tree.insert(&entry) {
                    OpResult::Successful(())  
                } else {
                    OpResult::Failed
                }
            }
        )
    }

    fn delete(&self, id: Id, entry: EntryKey) -> BoxFuture<OpResult<()>> {
        self.apply_in_ranged_tree(
            id, entry, 
            |entry, tree| {
                if tree.delete(&entry) {
                    OpResult::Successful(())
                } else {
                    OpResult::Failed
                }
            }
        )
    }

    fn seek(&self, id: Id, entry: EntryKey, ordering: Ordering, cursor_lifetime: u16) -> BoxFuture<OpResult<ServCursor>> {
        self.apply_in_ranged_tree(
            id, entry, 
            |entry, tree| {
                let tree_cursor = tree.seek(&entry, ordering);
                let cursor_id = self.cursor_counter.fetch_add(1, Relaxed);
                let expires = get_time() + cursor_lifetime as i64;
                let cursor_memo = CursorMemo { tree_cursor, expires };
                self.cursors.insert(&(cursor_id), Arc::new(RefCell::new(cursor_memo)));
                OpResult::Successful(ServCursor { cursor_id: cursor_id as u64 })
            } 
        )
    }

    fn renew_cursor(&self, cursor: ServCursor, time: u16) -> BoxFuture<bool> {
        future::ready(if let Some(cursor) = self.cursors.write(cursor.cursor_id as usize){
            cursor.borrow_mut().expires = get_time() + time as i64;
            true
        } else {
            false
        }).boxed()
    }

    fn dispose_cursor(&self, cursor: ServCursor) -> BoxFuture<bool> {
        future::ready(self.cursors.remove(&(cursor.cursor_id as usize)).is_some()).boxed()
    }

    fn cursor_next(&self, cursor: ServCursor) -> BoxFuture<Option<EntryKeyBlock>> {
        future::ready(if let Some(cursor) = self.cursors.write(cursor.cursor_id as usize){
            let mut res = EntryKeyBlock::default();
            let mut cursor_memo = cursor.borrow_mut();
            for entry in res.iter_mut() {
                if let Some(tree_entry) = cursor_memo.tree_cursor.next() {
                    *entry = tree_entry.clone();
                } else {
                    break;
                }
            } 
            Some(res)
        } else {
            None
        }).boxed()
    }
}

impl LSMTreeService {
    pub fn new(client: &Arc<AsyncClient>) -> Self {
        let trees_map = Arc::new(HashMap::with_capacity(32));
        crate::index::btree::storage::start_external_nodes_write_back(client);
        Self::start_tree_balancer(&trees_map, client);
        Self {
            client: client.clone(),
            cursor_counter: AtomicUsize::new(0),
            cursors: ObjectMap::with_capacity(64),
            trees: trees_map
        }
    }

    pub fn start_tree_balancer(trees_map: &Arc<HashMap<Id, Arc<DistLSMTree>>>, client: &Arc<AsyncClient>) {
        let trees_map = trees_map.clone();
        let client = client.clone();
        tokio::spawn(async move {
            loop {
                for (_, dist_tree) in trees_map.entries() {
                    let tree = &dist_tree.tree;
                    tree.merge_levels();
                    if tree.oversized() {
                        // Tree oversized, need to migrate
                        let mid_key = tree.mid_key().unwrap();
                        let migration_target_id = Id::rand();
                        let target_boundary = Boundary {
                            lower: mid_key.clone(),
                            upper: dist_tree.prop.read().boundary.upper.clone()
                        };
                        let migration_tree = LSMTree::create(&client, &migration_target_id).await;
                        {
                            let mut dist_tree_prop = dist_tree.prop.write();
                            dist_tree_prop.migration = Some(Migration {
                                pivot: mid_key.clone(),
                                id: migration_target_id
                            });
                        }
                        tree.mark_migration(&dist_tree.id, Some(migration_target_id), &client).await;
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
                        // Remove the tree from local first so no new keys will be inserted in the tree
                        trees_map.remove(&migration_target_id);
                        // TODO: Inform the Raft state machine that the tree have splited with new boundary

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

    fn apply_in_ranged_tree<F, R>(
        &self, id: Id, entry: EntryKey, func: F
    ) -> BoxFuture<OpResult<R>>
        where F: Fn(&EntryKey, &LSMTree) -> OpResult<R>,
              R: Send + 'static
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
        }).boxed()
    }
}

impl DistLSMTree {
    fn new(id: Id, tree: LSMTree, boundary: Boundary, migration: Option<Migration>) -> Self {
        let prop = RwLock::new(DistProp {
            boundary, migration
        });
        Self {
            id, tree, prop
        }
    }
}

impl Boundary {
    fn in_boundary(&self, entry: &EntryKey) -> bool {
        return entry >= &self.lower && entry < &self.upper
    }
}

dispatch_rpc_service_functions!(LSMTreeService);

unsafe impl Send for DistLSMTree {}
unsafe impl Sync for DistLSMTree {}