use index::Slice;
use index::btree::BPlusTree;
use index::btree::NodeCellRef;
use index::EntryKey;
use index::btree::node::read_node;
use index::btree::node::NodeReadHandler;
use index::btree::node::NodeData;
use std::fmt::Debug;
use index::btree::node::NodeWriteGuard;
use index::btree::node::write_node;
use index::btree::node::write_key_page;

pub enum InsertSearchResult {
    External,
    Internal(NodeCellRef),
    RightNode(NodeCellRef),
}

pub enum InsertToNodeResult<KS, PS>
    where KS: Slice<EntryKey> + Debug + 'static + Debug,
          PS: Slice<NodeCellRef> + 'static
{
    NoSplit,
    Split(NodeWriteGuard<KS, PS>, NodeCellRef, Option<EntryKey>),
    SplitParentChanged,
}

pub struct NodeSplit<KS, PS>
    where KS: Slice<EntryKey> + Debug + 'static,
          PS: Slice<NodeCellRef> + 'static
{
    pub new_right_node: NodeCellRef,
    pub left_node_latch: NodeWriteGuard<KS, PS>,
    pub pivot: EntryKey,
    pub parent_latch: NodeWriteGuard<KS, PS>,
}

pub fn insert_internal<KS, PS>(
    split_res: Option<NodeSplit<KS, PS>>,
    parent: &NodeCellRef
) -> Option<NodeSplit<KS, PS>>
    where KS: Slice<EntryKey> + Debug + 'static,
          PS: Slice<NodeCellRef> + 'static
{
    match split_res {
        None => None,
        Some(split) => {
            debug!(
                "Sub level node split, shall insert new node to current level, pivot {:?}",
                split.pivot
            );
            let pivot = split.pivot;
            debug!("New pivot {:?}", pivot);
            debug!("obtain latch for internal node split");
            let mut self_guard = split.parent_latch;
            let mut target_guard = write_key_page(self_guard, &pivot);
            debug_assert!(
                split.new_right_node.deref::<KS, PS>().read_unchecked().first_key() >= &pivot
            );
            let mut split_result = target_guard.innode_mut().insert(
                pivot,
                split.new_right_node,
                parent
            );
            if let &mut Some(ref mut split) = &mut split_result {
                split.left_node_latch = target_guard;
            }
            split_result
        }
    }
}

pub fn insert_external<KS, PS>(
    tree: &BPlusTree<KS, PS>,
    node_ref: &NodeCellRef,
    parent: &NodeCellRef,
    key: &EntryKey
) -> Option<NodeSplit<KS, PS>>
    where KS: Slice<EntryKey> + Debug + 'static,
          PS: Slice<NodeCellRef> + 'static
{
    // latch nodes from left to right
    debug!("Obtain latch for external node");
    let node_guard = write_node(node_ref);
    let mut searched_guard = write_key_page(node_guard, key);
    debug_assert!(
        searched_guard.is_ext(),
        "{:?}",
        searched_guard.innode().keys
    );
    let mut split_result = searched_guard.extnode_mut().insert(
        key,
        tree,
        node_ref,
        parent
    );
    if let &mut Some(ref mut split) = &mut split_result {
        split.left_node_latch = searched_guard;
    }
    split_result
}

pub fn insert_to_node<KS, PS>(
    tree: &BPlusTree<KS, PS>,
    node_ref: &NodeCellRef,
    parent: &NodeCellRef,
    key: &EntryKey,
    level: usize,
) -> Option<NodeSplit<KS, PS>>
    where KS: Slice<EntryKey> + Debug + 'static,
          PS: Slice<NodeCellRef> + 'static
{
    let mut search = read_node(node_ref, |node_handler: &NodeReadHandler<KS, PS>| {
        debug!(
            "insert to node, len {}, external: {}",
            node_handler.len(),
            node_handler.is_ext()
        );
        match &**node_handler {
            &NodeData::External(ref node) => InsertSearchResult::External,
            &NodeData::Internal(ref node) => {
                let pos = node.search(key);
                let sub_node_ref = &node.ptrs.as_slice_immute()[pos];
                InsertSearchResult::Internal(sub_node_ref.clone())
            }
            &NodeData::Empty(ref node) => InsertSearchResult::RightNode(node.right.clone()),
            &NodeData::None => unreachable!(),
        }
    });
    let modification = match search {
        InsertSearchResult::RightNode(node) => {
            insert_to_node(tree,&node, parent, key, level)
        }
        InsertSearchResult::External => {
            insert_external(tree, node_ref, parent, key)
        }
        InsertSearchResult::Internal(sub_node) => {
            let split_res =
                insert_to_node(tree, &sub_node, node_ref, key, level + 1);
            insert_internal(split_res, parent)
        }
    };
    if level == 0 {
        if let &Some(ref split) = &modification {
            let current_root = tree.get_root();
            if  current_root.deref::<KS, PS>().read_unchecked().first_key() != split.left_node_latch.first_key() &&
                split.left_node_latch.has_vaild_right_node() {
                // at this point, root split occurred when waiting for the latch
                // the new right node should be inserted to any right node of the old root
                // hopefully that node won't split again
                let current_root_guard = write_node(&current_root);
                // at this point, the root may have split again, we need to search for the exact one
                let mut root_level_target = write_key_page(current_root_guard, &split.pivot);
                // assert this even in production
                assert!(!root_level_target.is_ext());
                return root_level_target.innode_mut().insert(split.pivot.clone(), split.new_right_node.clone(), parent);
            }
        }
    }
    modification
}