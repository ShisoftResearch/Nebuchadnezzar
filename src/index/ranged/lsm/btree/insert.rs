use super::node::read_unchecked;
use super::node::write_node;
use super::node::write_targeted;
use super::node::NodeWriteGuard;
use super::search::mut_search;
use super::search::MutSearchResult;
use super::*;
use std::fmt::Debug;

pub struct NodeSplit<KS, PS>
where
    KS: Slice<EntryKey> + Debug + 'static,
    PS: Slice<NodeCellRef> + 'static,
{
    pub new_right_node: NodeCellRef,
    pub left_node_latch: NodeWriteGuard<KS, PS>,
    pub pivot: EntryKey,
    pub parent_latch: NodeWriteGuard<KS, PS>,
}

pub fn insert_internal_tree_node<KS, PS>(
    split_res: Option<NodeSplit<KS, PS>>,
    parent: &NodeCellRef,
) -> Option<NodeSplit<KS, PS>>
where
    KS: Slice<EntryKey> + Debug + 'static,
    PS: Slice<NodeCellRef> + 'static,
{
    match split_res {
        None => None,
        Some(split) => {
            trace!(
                "Sub level node split, shall insert new node to current level, pivot {:?}, node {:?}",
                split.pivot, split.new_right_node
            );
            let pivot = split.pivot;
            let mut target_guard = write_targeted(split.parent_latch, &pivot);
            debug_assert!(read_unchecked::<KS, PS>(&split.new_right_node).first_key() >= &pivot);
            let mut split_result =
                target_guard
                    .innode_mut()
                    .insert(pivot, split.new_right_node, parent);
            if let &mut Some(ref mut split) = &mut split_result {
                split.left_node_latch = target_guard;
            }
            split_result
        }
    }
}

pub fn insert_external_tree_node<KS, PS>(
    tree: &GenericBPlusTree<KS, PS>,
    node_ref: &NodeCellRef,
    parent: &NodeCellRef,
    key: &EntryKey,
) -> Option<Option<NodeSplit<KS, PS>>>
where
    KS: Slice<EntryKey> + Debug + 'static,
    PS: Slice<NodeCellRef> + 'static,
{
    // latch nodes from left to right
    let mut searched_guard = write_targeted(write_node(node_ref), key);
    let self_ref = searched_guard.node_ref().clone();
    debug_assert!(
        searched_guard.is_ext(),
        "{:?}",
        searched_guard.innode().keys
    );
    let mut split_result = searched_guard
        .extnode_mut(tree)
        .insert(key, tree, &self_ref, parent);
    if split_result.is_some() {
        external::make_changed(node_ref, tree);
    }
    if let &mut Some(Some(ref mut split)) = &mut split_result {
        split.left_node_latch = searched_guard;
        external::make_changed(&split.new_right_node, tree);
    }
    split_result
}

pub fn check_root_modification<KS, PS>(
    tree: &GenericBPlusTree<KS, PS>,
    modification: &Option<NodeSplit<KS, PS>>,
    parent: &NodeCellRef,
) -> Option<NodeSplit<KS, PS>>
where
    KS: Slice<EntryKey> + Debug + 'static,
    PS: Slice<NodeCellRef> + 'static,
{
    if let &Some(ref split) = modification {
        let current_root = tree.get_root();
        if read_unchecked::<KS, PS>(&current_root).first_key() != split.left_node_latch.first_key()
            && split.left_node_latch.has_vaild_right_node()
        {
            // at this point, root split occurred when waiting for the latch
            // the new right node should be inserted to any right node of the old root
            // hopefully that node won't split again
            let current_root_guard = write_node::<KS, PS>(&current_root);
            // at this point, the root may have split again, we need to search for the exact one
            let mut root_level_target = write_targeted(current_root_guard, &split.pivot);
            // assert this even in production
            assert!(!root_level_target.is_ext());
            root_level_target.innode_mut().insert(
                split.pivot.clone(),
                split.new_right_node.clone(),
                parent,
            );
        }
    }
    None
}

pub fn insert_to_tree_node<KS, PS>(
    tree: &GenericBPlusTree<KS, PS>,
    node_ref: &NodeCellRef,
    parent: &NodeCellRef,
    key: &EntryKey,
    level: usize,
) -> Option<Option<NodeSplit<KS, PS>>>
where
    KS: Slice<EntryKey> + Debug + 'static,
    PS: Slice<NodeCellRef> + 'static,
{
    let search = mut_search::<KS, PS>(node_ref, key);
    let modification = match search {
        MutSearchResult::External => {
            if let Some(insertion) = insert_external_tree_node(tree, node_ref, parent, key) {
                insertion
            } else {
                return None;
            }
        }
        MutSearchResult::Internal(sub_node) => {
            if let Some(split_res) = insert_to_tree_node(tree, &sub_node, node_ref, key, level + 1)
            {
                insert_internal_tree_node(split_res, parent)
            } else {
                return None;
            }
        }
    };
    if level == 0 {
        let root_modified = check_root_modification(tree, &modification, parent);
        if root_modified.is_some() {
            return Some(root_modified);
        }
    }
    Some(modification)
}
