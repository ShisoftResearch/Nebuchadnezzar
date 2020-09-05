use crate::index::trees::EntryKey;
use crate::ram::types::Id;
use super::tree::*;
use crate::index::btree::level::LEVEL_M as BLOCK_SIZE;
use crate::client::AsyncClient;
use crate::index::trees::*;
use std::cell::RefCell;
use bifrost::utils::time::get_time;
use serde::{Serialize, Deserialize};
use lightning::map::{ObjectMap, HashMap};
use lightning::map::Map;
use std::sync::atomic::{AtomicUsize, Ordering::Relaxed};
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
    OutOfBound
}

#[derive(Clone, Serialize, Deserialize)]
pub struct ServCursor {
    cursor_id: u64
}

pub struct DistLSMTree {
    tree: LSMTree,
    boundary: Boundary
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
    trees: HashMap<Id, Arc<DistLSMTree>>
}

impl Service for LSMTreeService {
    fn crate_tree(&self, id: Id, boundary: Boundary) -> BoxFuture<()> {
        async move {
            let tree = LSMTree::create(&self.client, &id).await;
            self.trees.insert(&id, Arc::new(DistLSMTree::new(tree, boundary)));
        }.boxed()
    }

    fn load_tree(&self, id: Id, boundary: Boundary) -> BoxFuture<()> {
        async move {
            let tree = LSMTree::recover(&self.client, &id).await;
            self.trees.insert(&id, Arc::new(DistLSMTree::new(tree, boundary)));
        }.boxed()
    }

    fn insert(&self, id: Id, entry: EntryKey) -> BoxFuture<OpResult<()>> {
        future::ready(if let Some(tree) = self.trees.get(&id) {
            if tree.key_in_boundary(&entry) {
                if tree.tree.insert(&entry) {
                    OpResult::Successful(())
                } else {
                    OpResult::Failed
                }
            } else {
                OpResult::OutOfBound
            }
        } else {
            OpResult::NotFound
        }).boxed()
    }

    fn delete(&self, id: Id, entry: EntryKey) -> BoxFuture<OpResult<()>> {
        future::ready(if let Some(tree) = self.trees.get(&id) {
            if tree.key_in_boundary(&entry) {
                if tree.tree.delete(&entry) {
                    OpResult::Successful(())
                } else {
                    OpResult::Failed
                }
            } else {
                OpResult::OutOfBound
            }
        } else {
            OpResult::NotFound
        }).boxed()
    }

    fn seek(&self, id: Id, entry: EntryKey, ordering: Ordering, cursor_lifetime: u16) -> BoxFuture<OpResult<ServCursor>> {
        future::ready(if let Some(tree) = self.trees.get(&id) {
            if tree.key_in_boundary(&entry) {
                let tree_cursor = tree.tree.seek(&entry, ordering);
                let cursor_id = self.cursor_counter.fetch_add(1, Relaxed);
                let expires = get_time() + cursor_lifetime as i64;
                let cursor_memo = CursorMemo { tree_cursor, expires };
                self.cursors.insert(&(cursor_id), Arc::new(RefCell::new(cursor_memo)));
                OpResult::Successful(ServCursor { cursor_id: cursor_id as u64 })
            } else {
                OpResult::OutOfBound
            }
        } else {
            OpResult::NotFound
        }).boxed()
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
        Self {
            client: client.clone(),
            cursor_counter: AtomicUsize::new(0),
            cursors: ObjectMap::with_capacity(32),
            trees: HashMap::with_capacity(16)
        }
    }
}

impl DistLSMTree {
    fn new(tree: LSMTree, boundary: Boundary) -> Self {
        Self {
            tree, boundary
        }
    }
    fn key_in_boundary(&self, entry: &EntryKey) -> bool {
        self.boundary.in_boundary(entry)
    }
}

impl Boundary {
    fn in_boundary(&self, entry: &EntryKey) -> bool {
        return entry >= &self.lower && entry < &self.upper
    }
}

dispatch_rpc_service_functions!(LSMTreeService);