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
use index::btree::node::read_unchecked;
use index::btree::search::mut_search;
use index::btree::search::MutSearchResult;

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

pub fn insert_internal_tree_node<KS, PS>(
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
            let mut target_guard = write_key_page(split.parent_latch, &pivot);
            debug_assert!(
                read_unchecked::<KS, PS>(&split.new_right_node).first_key() >= &pivot
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

pub fn insert_external_tree_node<KS, PS>(
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
    let mut searched_guard = write_key_page(write_node(node_ref), key);
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

pub fn check_root_modification<KS, PS>(
    tree: &BPlusTree<KS, PS>,
    modification: &Option<NodeSplit<KS, PS>>,
    parent: &NodeCellRef
) -> Option<NodeSplit<KS, PS>>
    where KS: Slice<EntryKey> + Debug + 'static,
          PS: Slice<NodeCellRef> + 'static
{
    if let &Some(ref split) = modification {
        let current_root = tree.get_root();
        if  read_unchecked::<KS, PS>(&current_root).first_key() != split.left_node_latch.first_key() &&
            split.left_node_latch.has_vaild_right_node() {
            // at this point, root split occurred when waiting for the latch
            // the new right node should be inserted to any right node of the old root
            // hopefully that node won't split again
            let current_root_guard = write_node::<KS, PS>(&current_root);
            // at this point, the root may have split again, we need to search for the exact one
            let mut root_level_target = write_key_page(current_root_guard, &split.pivot);
            // assert this even in production
            assert!(!root_level_target.is_ext());
            root_level_target.innode_mut().insert(
                split.pivot.clone(),
                split.new_right_node.clone(),
                parent);
        }
    }
    None
}

pub fn insert_to_tree_node<KS, PS>(
    tree: &BPlusTree<KS, PS>,
    node_ref: &NodeCellRef,
    parent: &NodeCellRef,
    key: &EntryKey,
    level: usize,
) -> Option<NodeSplit<KS, PS>>
    where KS: Slice<EntryKey> + Debug + 'static,
          PS: Slice<NodeCellRef> + 'static
{
    let mut search = mut_search::<KS, PS>(node_ref, key);
    let modification = match search {
        MutSearchResult::RightNode(node) => {
            insert_to_tree_node(tree, &node, parent, key, level)
        }
        MutSearchResult::External => {
            insert_external_tree_node(tree, node_ref, parent, key)
        }
        MutSearchResult::Internal(sub_node) => {
            let split_res =
                insert_to_tree_node(tree, &sub_node, node_ref, key, level + 1);
            insert_internal_tree_node(split_res, parent)
        }
    };
    if level == 0 {
        let root_modified = check_root_modification(tree, &modification, parent);
        if root_modified.is_some() {
            return root_modified;
        }
    }
    modification
}