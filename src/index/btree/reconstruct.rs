use client::AsyncClient;
use dovahkiin::types::*;
use futures::prelude::*;
use index::btree::external::ExtNode;
use index::btree::external::*;
use index::btree::internal::InNode;
use index::btree::node::{write_node, Node, NodeWriteGuard};
use index::btree::remove::SubNodeStatus::InNodeEmpty;
use index::btree::{external, max_entry_key, BPlusTree, DeletionSetInneer, NodeCellRef};
use index::{EntryKey, Slice};
use parking_lot::RwLock;
use ram::cell::Cell;
use std::cell::RefCell;
use std::cmp::max;
use std::collections::btree_set::BTreeSet;
use std::fmt::Debug;
use std::marker::PhantomData;
use std::mem;
use std::rc::Rc;
use std::sync::atomic::AtomicUsize;
use std::sync::Arc;

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
        self.push(0, node, first_key);
    }

    fn push(&mut self, level: usize, node: &NodeCellRef, first_key: EntryKey) {
        let mut new_level = false;
        debug!("Push node with first key: {:?}", first_key);
        if self.level_guards.len() < level + 1 {
            let mut new_root_innode = InNode::<KS, PS>::new(0, max_entry_key());
            let new_root_ref = NodeCellRef::new(Node::with_internal(new_root_innode));
            self.level_guards
                .push(Rc::new(RefCell::new(write_node::<KS, PS>(&new_root_ref))));
            new_level = true;
        }
        let parent_page_ref = self.level_guards[level].clone();
        let mut parent_guard = parent_page_ref.borrow_mut();
        let cap = KS::slice_len();
        if parent_guard.len() >= cap {
            // current page overflowed, need a new page
            let mut new_innode = InNode::<KS, PS>::new(1, max_entry_key());
            let parent_right_bound = parent_guard.last_key().clone();
            let new_innode_head_ptr = {
                // take a key and a ptr from current page to new page
                // reset current page right bound to the taken key
                // return the taken ptr
                let mut parent_innode = parent_guard.innode_mut();
                parent_innode.len -= 1;
                parent_innode.right_bound = parent_right_bound.clone();
                mem::replace(
                    &mut parent_innode.ptrs.as_slice()[cap],
                    NodeCellRef::default(),
                )
            };
            // arrange a valid new page by putting the ptr from current page at 1st
            new_innode.ptrs.as_slice()[0] = new_innode_head_ptr;
            new_innode.ptrs.as_slice()[1] = node.clone();
            new_innode.keys.as_slice()[0] = first_key;
            let new_node = NodeCellRef::new(Node::with_internal(new_innode));
            *parent_guard = write_node::<KS, PS>(&new_node);
            self.push(level + 1, &new_node, parent_right_bound)
        } else {
            let mut parent_innode = parent_guard.innode_mut();
            let new_len = if new_level {
                0
            } else {
                let len = parent_innode.len;
                parent_innode.keys.as_slice()[len] = first_key;
                len + 1
            };
            parent_innode.ptrs.as_slice()[new_len] = node.clone();
            parent_innode.len = new_len;
        }
    }

    pub fn root(&self) -> NodeCellRef {
        debug_assert!(self.level_guards.len() > 0, "reconstructed levels is zero");
        let last_ref = self.level_guards.last().unwrap().clone();
        let last_guard = last_ref.borrow();
        if last_guard.len() == 0 {
            last_guard.innode().ptrs.as_slice_immute()[0].clone()
        } else {
            last_guard.node_ref().clone()
        }
    }
}

pub fn reconstruct_from_head_id<KS, PS>(head_id: Id, neb: &AsyncClient) -> BPlusTree<KS, PS>
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
        let cell = neb.read_cell(id).wait().unwrap().unwrap();
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
