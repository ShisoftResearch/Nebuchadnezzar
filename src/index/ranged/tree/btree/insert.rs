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
            debug_assert!(read_unchecked::<KS, PS>(&split.new_right_node).first_key() >= pivot);
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
    tree: &BPlusTree<KS, PS>,
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

// Apply a split that bubbled out of the top level of the tree.
//
// The split carries two latches: the node that split (`left_node_latch`) and
// the tree's root-versioning latch (`parent_latch`), which serializes root
// installations. Two cases:
//
// - The split node is still the tree root: install a new root above it.
// - Another thread grew the tree while this one waited for latches: the pivot
//   belongs in the level of the current root instead. Both latches must be
//   released before walking that level, because the walk can reach the very
//   node this thread latched (self-deadlock), and inserting into a full node
//   re-acquires the root-versioning latch. If that insert splits the target,
//   the loop applies the new split the same way.
/// Follow a bypass `Empty` to the node it stands for.
///
/// A split that leaves a level with one child collapses it to a bypass
/// `Empty`, whose `right` is that child. Traversal already forwards through
/// it -- `mut_search` and `write_targeted` both do -- but `is_ext()` cannot:
/// it is a question only External and Internal can answer, and a bypass is
/// neither. Asking a bypass root directly is what panicked at node.rs:86 on
/// the first insert that grew a freshly split tree.
///
/// Bounded rather than a `while`: a cycle here would hang a write path, and
/// a bypass chain deeper than this is a corrupt tree, not a slow one.
fn resolve_bypass<KS, PS>(mut node_ref: NodeCellRef) -> NodeCellRef
where
    KS: Slice<EntryKey> + Debug + 'static,
    PS: Slice<NodeCellRef> + 'static,
{
    const MAX_BYPASS_HOPS: usize = 16;
    for _ in 0..MAX_BYPASS_HOPS {
        let next = {
            let node = read_unchecked::<KS, PS>(&node_ref);
            if !node.is_empty_node() {
                return node_ref;
            }
            match node.right_ref() {
                Some(right) if !right.is_default() => right.clone(),
                // A bypass with nowhere to go. Nothing can be resolved, so
                // hand back what we have and let the caller fail honestly
                // rather than loop.
                _ => return node_ref,
            }
        };
        node_ref = next;
    }
    node_ref
}

pub fn apply_top_level_split<KS, PS>(tree: &BPlusTree<KS, PS>, mut split: NodeSplit<KS, PS>)
where
    KS: Slice<EntryKey> + Debug + 'static,
    PS: Slice<NodeCellRef> + 'static,
{
    loop {
        let current_root = tree.get_root();
        let left_is_root = !split.left_node_latch.is_ref_none()
            && current_root.ptr_eq(split.left_node_latch.node_ref());
        // When the current root is external but is not the node that split,
        // both are on the same (bottom) level — possible when a concurrent
        // bulk merge created siblings of a leaf root. A new root above the
        // current one keeps the tree correct through the B-link pointers.
        // Resolve through any bypass BEFORE asking what kind of node this is.
        // The bypass stands for its child, so growing the tree above the child
        // is the same thing as growing it above the bypass -- and the root ref
        // is replaced below in any case.
        let current_root = resolve_bypass::<KS, PS>(current_root);
        if left_is_root || read_unchecked::<KS, PS>(&current_root).is_ext() {
            trace!("split root with pivot key {:?}", split.pivot);
            let mut new_in_root: Box<InNode<KS, PS>> = InNode::new(1, max_entry_key());
            new_in_root.keys = InternalKeys::from_keys(&[split.pivot.clone()]);
            new_in_root.ptrs.as_slice()[0] = current_root.clone();
            new_in_root.ptrs.as_slice()[1] = split.new_right_node.clone();
            // `parent_latch` (root versioning) is still held here, so no other
            // thread can install a root concurrently.
            *tree.root.write() = NodeCellRef::new(Node::new(NodeData::Internal(new_in_root)));
            tree.height.fetch_add(1, AcqRel);
            return;
        }
        // The tree grew above the split node; place the pivot at the current
        // root's level.
        let NodeSplit {
            pivot,
            new_right_node,
            left_node_latch,
            parent_latch,
        } = split;
        drop(left_node_latch);
        drop(parent_latch);
        let current_root_guard = write_node::<KS, PS>(&current_root);
        let mut target = write_targeted(current_root_guard, &pivot);
        // The walk stays on the current root's level, which is internal here.
        assert!(!target.is_ext());
        let insert_result =
            target
                .innode_mut()
                .insert(pivot, new_right_node, &tree.root_versioning);
        match insert_result {
            None => return,
            Some(mut next_split) => {
                next_split.left_node_latch = target;
                split = next_split;
            }
        }
    }
}

pub fn insert_to_tree_node<KS, PS>(
    tree: &BPlusTree<KS, PS>,
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
    Some(modification)
}
