use crate::client::AsyncClient;
use crate::index::btree::external::ExtNode;
use crate::index::btree::internal::InNode;
use crate::index::btree::node::{write_node, Node, NodeWriteGuard};
use crate::index::btree::{external, max_entry_key, BPlusTree, NodeCellRef};
use crate::index::trees::{EntryKey, Slice};
use crate::ram::types::*;
use std::cell::RefCell;
use std::fmt::Debug;
use std::mem;
use std::rc::Rc;

pub struct TreeConstructor<KS, PS>
where
    KS: Slice<EntryKey> + Debug + 'static,
    PS: Slice<NodeCellRef> + 'static,
{
    level_guards: Vec<Rc<RefCell<NodeWriteGuard<KS, PS>>>>,
}

impl<KS, PS> TreeConstructor<KS, PS>
where
    KS: Slice<EntryKey> + Debug + 'static,
    PS: Slice<NodeCellRef> + 'static,
{
    pub fn new() -> Self {
        TreeConstructor {
            level_guards: vec![],
        }
    }

    pub fn push_extnode(&mut self, node: &NodeCellRef, first_key: EntryKey) {
        self.push(0, node, None, first_key);
    }

    fn push(
        &mut self,
        level: usize,
        node: &NodeCellRef,
        left_node: Option<&mut NodeWriteGuard<KS, PS>>,
        left_bound: EntryKey,
    ) {
        let mut new_tree = false;
        debug!("Push node at {}", level);
        if self.level_guards.len() < level + 1 {
            debug!("Creating new level {}", level);
            let mut new_root_innode = InNode::<KS, PS>::new(0, max_entry_key());
            if level > 0 {
                let left_node = left_node.unwrap();
                new_root_innode.ptrs.as_slice()[0] = left_node.node_ref().clone();
            } else {
                new_tree = true;
            }
            let new_root_ref = NodeCellRef::new(Node::with_internal(new_root_innode));
            self.level_guards
                .push(Rc::new(RefCell::new(write_node::<KS, PS>(&new_root_ref))));
        }
        let parent_page_ref = self.level_guards[level].clone();
        let mut node_guard = parent_page_ref.borrow_mut();
        let cap = KS::slice_len();
        if node_guard.len() >= cap {
            // current page overflowed, need a new page
            debug!("Creating new node at level {}", level);
            let mut new_innode = InNode::<KS, PS>::new(1, max_entry_key());
            let parent_right_bound = node_guard.last_key().clone();
            let new_innode_head_ptr = {
                // take a key and a ptr from current page to new page
                // reset current page right bound to the taken key
                // return the taken ptr
                let mut node_innode = node_guard.innode_mut();
                node_innode.len -= 1;
                node_innode.right_bound = parent_right_bound.clone();
                mem::replace(
                    &mut node_innode.ptrs.as_slice()[cap],
                    NodeCellRef::default(),
                )
            };
            // arrange a valid new page by putting the ptr from current page at 1st
            new_innode.ptrs.as_slice()[0] = new_innode_head_ptr;
            new_innode.ptrs.as_slice()[1] = node.clone();
            new_innode.keys.as_slice()[0] = left_bound;
            let new_node = NodeCellRef::new(Node::with_internal(new_innode));
            self.push(
                level + 1,
                &new_node,
                Some(&mut *node_guard),
                parent_right_bound,
            );
            node_guard.right_ref_mut().map(|rn| *rn = new_node.clone());
            *node_guard = write_node::<KS, PS>(&new_node);
        } else {
            let mut parent_innode = node_guard.innode_mut();
            let new_len = if new_tree {
                0
            } else {
                let len = parent_innode.len;
                parent_innode.keys.as_slice()[len] = left_bound;
                len + 1
            };
            parent_innode.ptrs.as_slice()[new_len] = node.clone();
            parent_innode.len = new_len;
        }
    }

    pub fn root(&self) -> NodeCellRef {
        debug_assert!(self.level_guards.len() > 0, "reconstructed levels is zero");
        debug!("The tree have {} levels", self.level_guards.len());
        let last_ref = self.level_guards.last().unwrap().clone();
        let last_guard = last_ref.borrow();
        if last_guard.len() == 0 {
            debug!("Taking root from first ptr of overprovisioned level root");
            last_guard.innode().ptrs.as_slice_immute()[0].clone()
        } else {
            debug!("Taking level root");
            last_guard.node_ref().clone()
        }
    }
}

pub async fn reconstruct_from_head_id<KS, PS>(head_id: Id, neb: &AsyncClient) -> BPlusTree<KS, PS>
where
    KS: Slice<EntryKey> + Debug + 'static,
    PS: Slice<NodeCellRef> + 'static,
{
    let mut len = 0;
    let mut constructor = TreeConstructor::<KS, PS>::new();
    let mut prev_ref = NodeCellRef::new_none::<KS, PS>();
    let mut id = head_id;
    let mut at_end = false;
    while !at_end {
        let cell = neb.read_cell(id).await.unwrap().unwrap();
        let page = ExtNode::<KS, PS>::from_cell(&cell);
        let next_id = page.next_id;
        let prev_id = page.prev_id;
        let mut node = page.node;
        at_end = next_id.is_unit_id();
        if at_end {
            node.next = NodeCellRef::new_none::<KS, PS>();
        }
        let mut prev_lock = write_node::<KS, PS>(&prev_ref);
        if node.len == 0 {
            // skip this empty node and make it deleted
            external::make_deleted(&node.id);
            if at_end {
                // if the empty node is the last node, assign the right none node to previous node
                *prev_lock.right_ref_mut().unwrap() = node.next.clone();
            }
            continue;
        }
        let first_key = node.keys.as_slice_immute()[0].clone();
        len += node.len;
        node.prev = prev_ref.clone();
        let node_ref = NodeCellRef::new(Node::with_external(box node));
        if !prev_lock.is_none() {
            *prev_lock.right_bound_mut() = first_key.clone();
            *prev_lock.right_ref_mut().unwrap() = node_ref.clone();
        } else {
            assert_eq!(prev_id, Id::unit_id());
        }
        constructor.push_extnode(&node_ref, first_key);
        prev_ref = node_ref;
        id = next_id;
    }
    let root = constructor.root();
    BPlusTree::from_root(root, head_id, len)
}


#[cfg(test)]
mod test {
    use crate::server::*;
    use crate::client;
    use std::sync::Arc;
    use crate::index::btree::external::*;
    use dovahkiin::types::custom_types::id::Id;
    use dovahkiin::types::custom_types::map::Map;
    use dovahkiin::types::Value;
    use crate::ram::types::*;
    use crate::ram::cell::Cell;
    use itertools::Itertools;
    use crate::index::btree::test::*;
    use smallvec::SmallVec;
    use crate::index::trees::{key_with_id, Ordering};
    use crate::index::trees::Cursor;

    #[tokio::test(threaded_scheduler)]
    async fn tree_reconstruct_from_head_cell() {
        let _ = env_logger::try_init();
        let server_group = "btree-reconstruct";
        let server_addr = String::from("127.0.0.1:5600");
        let server = NebServer::new_from_opts(
            &ServerOptions {
                chunk_count: 1,
                memory_size: 16 * 12024 * 1024,
                backup_storage: None,
                wal_storage: None,
                services: vec![Service::Cell]
            }, 
            &server_addr,
            &server_group
        ).await;
        let client = 
            Arc::new(
                client::AsyncClient::new(
                    &server.rpc, 
                    &server.membership, 
                    &vec![server_addr], 
                    server_group)
                    .await
                    .unwrap()
                );
        client.new_schema_with_id(page_schema()).await.unwrap().unwrap();
        let mut last_id = Id::unit_id();
        let cell_limit = 1000;
        let mut counter = 0;
        let mut all_keys = vec![];
        for i in 1..=cell_limit {
            let new_id = Id::new(i, i);
            let mut value = Value::Map(Map::new());
            value[*PREV_PAGE_KEY_HASH] = Value::Id(last_id);
            value[*NEXT_PAGE_KEY_HASH] = if i < cell_limit { 
                Value::Id(Id::new(i + 1, i + 1)) 
            } else {
                Value::Id(Id::unit_id())
            };
            value[*KEYS_KEY_HASH] = (0..PAGE_SIZE).map(|_| {
                counter += 1;
                let key_slice = u64_to_slice(counter);
                let mut key = SmallVec::from_slice(&key_slice);
                key_with_id(&mut key, &new_id);
                all_keys.push(key.clone());
                SmallBytes::from_vec(key.as_slice().to_vec())
            })
            .collect_vec()
            .value();
            client.write_cell(Cell::new_with_id(*PAGE_SCHEMA_ID, &new_id, value)).await.unwrap().unwrap();
            last_id = new_id;
        }
        let tree = LevelBPlusTree::from_head_id(&Id::new(1, 1), &client).await;
        for key in &all_keys {
            assert_eq!(tree.seek(key, Ordering::Forward).current().unwrap(), key);
        }
    }
}