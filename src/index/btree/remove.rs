use index::EntryKey;
use index::btree::NodeCellRef;
use index::Slice;
use index::btree::node::NodeWriteGuard;
use std::fmt::Debug;
use index::btree::node::write_node;
use index::btree::node::read_node;
use index::btree::node::NodeData;
use std::mem;
use index::btree::node::EmptyNode;
use index::btree::node::write_key_page;
use index::btree::node::NodeReadHandler;
use index::btree::BPlusTree;
use index::btree::node::read_unchecked;
use index::btree::search::mut_search;
use index::btree::search::MutSearchResult;

pub enum RemoveSearchResult {
    External,
    RightNode(NodeCellRef),
    Internal(NodeCellRef),
}

pub enum SubNodeStatus {
    ExtNodeEmpty,
    InNodeEmpty,
    Relocate(usize, usize),
    Merge(usize, usize),
    Ok,
}

pub struct RebalancingNodes<KS, PS>
    where KS: Slice<EntryKey> + Debug + 'static,
          PS: Slice<NodeCellRef> + 'static
{
    pub left_guard: NodeWriteGuard<KS, PS>,
    pub left_ref: NodeCellRef,
    pub right_guard: NodeWriteGuard<KS, PS>,
    pub right_right_guard: NodeWriteGuard<KS, PS>, // for external pointer modification
pub parent: NodeWriteGuard<KS, PS>,
    pub parent_pos: usize,
}

pub struct RemoveResult<KS, PS>
    where KS: Slice<EntryKey> + Debug + 'static,
          PS: Slice<NodeCellRef> + 'static
{
    pub rebalancing: Option<RebalancingNodes<KS, PS>>,
    pub removed: bool,
}

pub fn with_innode_removing<F, KS, PS>(
    key: &EntryKey,
    mut rebalancing: RebalancingNodes<KS, PS>,
    parent: &NodeCellRef,
    parent_parent: &NodeCellRef,
    removed: bool,
    level: usize,
    func: F
) -> RemoveResult<KS, PS>
    where F: Fn(&mut RebalancingNodes<KS, PS>),
          KS: Slice<EntryKey> + Debug + 'static,
          PS: Slice<NodeCellRef> + 'static
{
    let parent_half_full = rebalancing.parent.is_half_full();
    let mut parent_right_guard = write_node(&rebalancing.parent.innode_mut().right);
    let mut parent_remove_result = RemoveResult {
        rebalancing: None,
        removed
    };

    let parent_parent_guard;
    let parent_parent_remove_pos;
    let parent_right_right_guard;
    if level == 0 {
        parent_parent_guard = write_node(parent_parent);
        parent_right_right_guard = None;
        parent_parent_remove_pos = 0;
    } else {
        let pre_locked_parent_right_right_guard = parent_right_guard.right_ref().map(|r| write_node(r));
        let pp_guard = write_key_page(write_node(parent_parent), key);
        parent_parent_guard = pp_guard;
        parent_parent_remove_pos = parent_parent_guard.search(key);
        let parent_right_half_full = parent_right_guard.is_half_full();
        parent_right_right_guard = if parent_parent_guard.len() > parent_parent_remove_pos + 1 && (!parent_half_full || !parent_right_half_full) {
            // indicates whether the upper parent level need to relocated
            pre_locked_parent_right_right_guard
        } else { None };
    }

    func(&mut rebalancing);
    parent_remove_result.rebalancing = parent_right_right_guard.map(|parent_right_right_guard_stripped| {
        RebalancingNodes {
            left_guard: rebalancing.parent,
            left_ref: parent.clone(),
            right_right_guard: parent_right_right_guard_stripped,
            right_guard: parent_right_guard,
            parent: parent_parent_guard,
            parent_pos: parent_parent_remove_pos,
        }
    });
    parent_remove_result
}

pub fn remove_from_node<KS, PS>(
    tree: &BPlusTree<KS, PS>,
    node_ref: &mut NodeCellRef,
    key: &mut EntryKey,
    parent: &NodeCellRef,
    level: usize
) -> RemoveResult<KS, PS>
    where KS: Slice<EntryKey> + Debug + 'static,
          PS: Slice<NodeCellRef> + 'static
{
    debug!("Removing {:?} from node, level {}", key, level);
    let mut search = mut_search::<KS, PS>(node_ref, key);
    match search {
        MutSearchResult::Internal(mut sub_node) => {
            let mut node_remove_res = remove_from_node(tree,&mut sub_node, key, node_ref, level + 1);
            let removed = node_remove_res.removed;
            if let Some(mut rebalancing) = node_remove_res.rebalancing {
                debug!("Need to rebalance sub nodes, key {:?}, level {}", key, level);
                if rebalancing.left_guard.is_empty() && !rebalancing.right_guard.is_empty() {
                    debug!("Remove {:?} sub level left node is empty, level {}", key, level);
                    Some(with_innode_removing(
                        key,
                        rebalancing,
                        &sub_node,
                        parent,
                        removed,
                        level,
                        |rebalancing| {
                            // Remove the empty node that have a right node with the same parent
                            // Because we cannot lock from left to right, we have to move the content
                            // of the right node to the left and remove the right node instead so the
                            // left right pointers can be modified
                            if !rebalancing.left_guard.is_empty() || (level == 0 && read_unchecked::<KS, PS>(&(*tree.root.read())).first_key() != rebalancing.left_guard.first_key()) {
                                return;
                            }

                            let mut left_node = &mut*rebalancing.left_guard;
                            let mut right_node = &mut*rebalancing.right_guard;
                            // swap the content of left and right, then delete right
                            // this procedure will prevent locking left node for changing right reference
                            let left_left_ref = left_node.left_ref_mut().map(|r| r.clone());
                            let right_right_ref = right_node.right_ref_mut().map(|r| r.clone());
                            let left_ref = rebalancing.left_ref.clone();
                            // swap the empty node with the right node. In this case left node holds
                            // content of the right node but pointers need to be corrected.
                            mem::swap(left_node, right_node);
                            *right_node = NodeData::Empty(box EmptyNode { left: Some(left_ref.clone()), right: left_ref.clone() });
                            left_node.left_ref_mut().map(|r| *r = left_left_ref.unwrap());
                            left_node.right_ref_mut().map(|r| *r = right_right_ref.unwrap());
                            rebalancing.right_right_guard.left_ref_mut().map(|r| *r = left_ref);
                            // remove the left ptr
                            rebalancing.parent.remove(rebalancing.parent_pos);
                            // point the right ptr to the replaced sub node
                            debug!("Removed sub level empty node, living node len {}, have {:?}", left_node.len(), left_node.keys());
                            rebalancing.parent.innode_mut().ptrs.as_slice()[rebalancing.parent_pos + 1] = sub_node.clone();
                        }))
                } else if rebalancing.right_guard.is_empty() {
                    debug!("Remove {:?} sub level right node is empty, level {}", key, level);
                    Some(with_innode_removing(
                        key,
                        rebalancing,
                        &sub_node,
                        parent,
                        removed,
                        level,
                        |rebalancing| {
                            // There is a right empty node that can be deleted directly without hassle
                            let mut node_to_remove = &mut rebalancing.right_guard;
                            let owned_left_ref = rebalancing.left_ref.clone();
                            rebalancing.left_guard.right_ref_mut().map(|r| *r = node_to_remove.right_ref_mut().unwrap().clone());
                            rebalancing.right_right_guard.left_ref_mut().map(|r| *r = owned_left_ref);
                            rebalancing.parent.remove(rebalancing.parent_pos + 1);
                        }))
                } else if rebalancing.left_guard.cannot_merge() || rebalancing.right_guard.cannot_merge() {
                    if  rebalancing.right_guard.len() < KS::slice_len() / 3 &&
                        rebalancing.right_guard.len() < rebalancing.left_guard.len() &&
                        rebalancing.left_guard.len() > KS::slice_len() / 2 {
                        // Relocate the nodes with the same parent for balance.
                        // For OLFIT, items can only be located from left to right.
                        debug!("Remove {:?} sub level need relocation, level {}", key, level);
                        let mut left_node = &mut*rebalancing.left_guard;
                        let mut right_node = &mut*rebalancing.right_guard;
                        let left_pos = rebalancing.parent_pos;
                        let right_pos = left_pos + 1;
                        rebalancing.parent.innode_mut().relocate_children(left_pos, right_pos, left_node, right_node);
                    }
                    None
                } else if rebalancing.left_guard.len() + rebalancing.right_guard.len() + 1 <= KS::slice_len() {
                    // Nodes with the same parent can merge
                    debug!("Remove {:?} sub level need to be merged, level {}", key, level);
                    Some(with_innode_removing(
                        key,
                        rebalancing,
                        &sub_node,
                        parent,
                        removed,
                        level,
                        |rebalancing| {
                            let mut left_node = &mut *rebalancing.left_guard;
                            let mut right_node = &mut *rebalancing.right_guard;
                            let mut right_node_next = &mut *rebalancing.right_right_guard;
                            let left_pos = rebalancing.parent_pos;
                            let right_pos = left_pos + 1;
                            if left_node.is_empty() || right_node.is_empty() { return; }
                            rebalancing.parent.innode_mut().merge_children(left_pos, right_pos, left_node, right_node, right_node_next);
                        }))
                    // None
                } else if rebalancing.parent.len() == 1 && rebalancing.left_guard.is_empty() && rebalancing.right_guard.is_empty() {
                    // this node and its children is empty, should be removed
                    debug!("Cleaning up empty level nodes, level {}", level);
                    Some(with_innode_removing(
                        key,
                        rebalancing,
                        &sub_node,
                        parent,
                        removed,
                        level, |rebalancing| {
                            rebalancing.parent.remove(0);
                            rebalancing.parent.innode_mut().ptrs.as_slice()[0] = Default::default();
                        }))
                } else {
                    None
                }
            } else {
                None
            }.unwrap_or_else(|| {
                RemoveResult {
                    rebalancing: None,
                    removed
                }
            })
        }
        MutSearchResult::External => {
            let node_guard: NodeWriteGuard<KS, PS> = write_node(node_ref);
            let mut target_guard = write_key_page(node_guard, key);
            let target_guard_ref = target_guard.node_ref().clone();
            let mut remove_result = RemoveResult {
                rebalancing: None,
                removed: false
            };
            {
                let is_left_half_full = target_guard.is_half_full();
                let mut node = target_guard.extnode_mut();
                let pos = node.search(key);
                let right_guard = write_node(&node.next);
                let right_node_cannot_rebalance = right_guard.is_none() || !right_guard.is_empty_node();
                if !right_guard.is_none() && !is_left_half_full || !right_guard.is_half_full(){
                    let right_right_guard = write_node(&right_guard.right_ref().unwrap());
                    let parent_guard = write_node(parent);
                    let parent_target_guard = write_key_page(parent_guard, key);
                    let parent_pos = parent_target_guard.search(key);
                    // Check if the right node is innode and its parent is the same as the left one
                    // because we have to lock from left to right, there is no way to lock backwards
                    // if the empty non half-full node is at the right most of its parent
                    // So left over imbalanced such nodes will be expected and they can be eliminate
                    // by their left node remove operations.
                    if parent_pos < parent_target_guard.len() - 1 {
                        debug!("removing innode have a rebalance requirement");
                        remove_result.rebalancing = Some(RebalancingNodes {
                            left_ref: target_guard_ref,
                            left_guard: Default::default(),
                            parent: parent_target_guard,
                            parent_pos,
                            right_right_guard,
                            right_guard
                        });
                    } else {
                        debug!("removing innode have a rebalance requirement but left-right node does not with the same parent");
                    }
                }
                if pos >= node.len {
                    debug!(
                        "Removing pos overflows external node, pos {}, len {}, expecting key {:?}, current keys {:?}, right keys {:?}",
                        pos, node.len, key, node.keys, read_unchecked::<KS, PS>(&node.next).keys()
                    );
                    remove_result.removed = false;
                }
                if &node.keys.as_slice()[pos] == key {
                    debug!("Removing key {:?} at {}, keys {:?}", key, pos, &node.keys);
                    node.remove_at(pos);
                    remove_result.removed = true;
                } else {
                    debug!(
                        "Search check failed for remove at pos {}, expecting {:?}, actual {:?}, have {:?}",
                        pos, key, &node.keys.as_slice_immute()[pos], &node.keys
                    );
                    remove_result.removed = false;
                }
            }
            if let Some(ref mut rebalance) = &mut remove_result.rebalancing {
                rebalance.left_guard = target_guard;
            }
            remove_result
        }
    }
}