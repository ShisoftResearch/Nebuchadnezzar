use super::node::write_node;
use super::node::NodeWriteGuard;
use super::search::MutSearchResult;
use super::BPlusTree;
use super::LevelTree;
use super::NodeCellRef;
use super::*;
use itertools::Itertools;
use std::cmp::min;
use std::fmt::Debug;

pub const LEVEL_TREE_DEPTH: u32 = 2;

pub const LEVEL_M: usize = 8;
pub const LEVEL_0: usize = LEVEL_M * LEVEL_M; // Smaller can be faster but more fragmented
pub const LEVEL_1: usize = LEVEL_0 * LEVEL_M;

pub const NUM_LEVELS: usize = 2;

enum NodeSelection<KS, PS>
where
    KS: Slice<EntryKey> + Debug + 'static,
    PS: Slice<NodeCellRef> + 'static,
{
    WholePage(Vec<NodeReadHandler<KS, PS>>, NodeReadHandler<KS, PS>),
    PartialPage(Vec<NodeReadHandler<KS, PS>>, NodeReadHandler<KS, PS>),
}

fn merge_prune<KS, PS>(
    level: usize,
    node: &NodeCellRef,
    src_tree: &BPlusTree<KS, PS>,
    dest_tree: &dyn LevelTree,
    boundary: &EntryKey,
    prune_src: bool,
    lsm: usize,
) -> (usize, Option<NodeCellRef>)
where
    KS: Slice<EntryKey> + Debug + 'static,
    PS: Slice<NodeCellRef> + 'static,
{
    let search = mut_first::<KS, PS>(node);
    match search {
        MutSearchResult::External => {
            debug!("Selecting external nodes in boundary {:?}, tree level {}, lsm {}", boundary, level, lsm);
            if let NodeSelection::WholePage(nodes, new_first) =
                select_nodes_in_boundary::<KS, PS>(node, boundary, level, lsm)
            {
                let head_id = nodes[0].ext_id();
                debug_assert!(
                    !nodes.is_empty(),
                    "Cannot find any keys in range in external with boundary {:?}",
                    boundary
                );
                debug!("Selected {} pages to merge", nodes.len());
                let deleted_keys = nodes
                    .iter()
                    .map(|node| {
                        node
                        .keys()
                        .iter()
                        .filter(|k| src_tree.deletion.contains(k))
                        .cloned()
                    })
                    .flatten();
                let merging_keys = nodes
                    .iter()
                    .map(|node| {
                        node.keys()
                        .iter()
                        .filter(|k| !src_tree.deletion.contains(k))
                        .map(|k| k.clone())
                    })
                    .flatten()
                    .collect_vec();
                let num_keys_merged = merging_keys.len();
                debug!(
                    "Collected {} keys at level {}, merging to destination tree",
                    num_keys_merged, level
                );
                dest_tree.merge_with_keys(box merging_keys);
                debug!("Merge completed in external level");
                if prune_src {
                    debug!("Pruning source tree for {} external pages", nodes.len());
                    for node in nodes.into_iter() {
                        let node_ref = node.node_ref().clone();
                        drop(node); // unlock read
                        let node = write_node::<KS, PS>(&node_ref);
                        clear_node(node, new_first.node_ref());
                    }
                    let new_first_ref = new_first.node_ref().clone();
                    drop(new_first);
                    let mut new_first = write_node::<KS, PS>(&new_first_ref);
                    new_first.extnode_mut(src_tree).id = head_id;
                    *new_first.left_ref_mut().unwrap() = NodeCellRef::default();
                    debug!("Source tree external nodes pruned");
                }
                return (num_keys_merged, None);
            } else {
                error!("Found partial external node in selection");
                unreachable!();
            }
        }
        MutSearchResult::Internal(sub_node) => {
            #[derive(Debug)]
            enum RightCheck
            {
                SinglePtr,
                LevelTerminal(NodeCellRef),
                Normal,
            }
            let check_right = |node: NodeReadHandler<KS, PS>| {
                if node.right_ref().unwrap().is_default() {
                    debug_assert_eq!(node.right_bound(), &EntryKey::max());
                    if node.len() == 0 {
                        RightCheck::SinglePtr
                    } else {
                        RightCheck::LevelTerminal(node.node_ref().clone())
                    }
                } else {
                    debug_assert_ne!(node.right_bound(), &EntryKey::max());
                    debug_assert!(node.len() > 0);
                    RightCheck::Normal
                }
            };
            let (num_keys_merged, sub_level_new_root) = merge_prune(
                level + 1,
                &sub_node,
                src_tree,
                dest_tree,
                boundary,
                prune_src,
                lsm,
            );
            let mut new_root = None;
            if prune_src {
                let right_node =
                    match select_nodes_in_boundary::<KS, PS>(node, boundary, level, lsm) {
                        NodeSelection::WholePage(nodes, right_node) => {
                            clear_nodes(nodes, right_node.node_ref());
                            check_right(right_node)
                        }
                        NodeSelection::PartialPage(nodes, mut terminal_node) => {
                            clear_nodes(nodes, terminal_node.node_ref());
                            let search_pos = terminal_node.keys().binary_search(boundary);
                            let pos = match search_pos {
                                Ok(n) => n + 1,
                                Err(n) => n,
                            };
                            let mut new_keys = KS::init();
                            let mut new_ptrs = PS::init();
                            let mut num_keys = 0;
                            for (i, k) in terminal_node.keys()[pos..].iter().enumerate() {
                                new_keys.as_slice()[i] = k.clone();
                                num_keys += 1;
                            }
                            for (i, p) in terminal_node.ptrs()[pos..].iter().enumerate() {
                                new_ptrs.as_slice()[i] = p.clone();
                            }
                            let terminal_ref = terminal_node.node_ref().clone();
                            let right_stat = check_right(terminal_node);
                            {
                                let mut terminal_node = write_node(&terminal_ref);
                                let innode = terminal_node.innode_mut();
                                innode.keys = new_keys;
                                innode.ptrs = new_ptrs;
                                innode.len = num_keys;
                            }
                            right_stat
                        }
                    };
                new_root = {
                    if let Some(sub_level_new_root) = sub_level_new_root {
                        if cfg!(debug_assertions) {
                            match right_node {
                                RightCheck::SinglePtr => {}
                                _ => panic!("Unexpected node status {:?}", right_node),
                            }
                        }
                        Some(sub_level_new_root)
                    } else {
                        match right_node {
                            RightCheck::Normal => None,
                            RightCheck::LevelTerminal(node) => Some(node),
                            RightCheck::SinglePtr => unreachable!(),
                        }
                    }
                };
            }
            return (num_keys_merged, new_root);
        }
    }
}

fn clear_node<KS, PS>(mut node: NodeWriteGuard<KS, PS>, right_ref: &NodeCellRef)
where
    KS: Slice<EntryKey> + Debug + 'static,
    PS: Slice<NodeCellRef> + 'static,
{
    node.make_empty_node(false);
    *node.right_ref_mut().unwrap() = right_ref.clone();
    node.left_ref_mut().map(|r| *r = right_ref.clone());
}

fn clear_nodes<KS, PS>(nodes: Vec<NodeReadHandler<KS, PS>>, right_ref: &NodeCellRef)
where
    KS: Slice<EntryKey> + Debug + 'static,
    PS: Slice<NodeCellRef> + 'static,
{
    for node in nodes.into_iter() {
        let node_ref = node.node_ref().clone();
        drop(node);
        let node = write_node::<KS, PS>(&node_ref);
        clear_node(node, right_ref);
    }
}

fn select_nodes_in_boundary<KS, PS>(
    first_node: &NodeCellRef,
    right_boundary: &EntryKey,
    level: usize,
    lsm: usize,
) -> NodeSelection<KS, PS>
where
    KS: Slice<EntryKey> + Debug + 'static,
    PS: Slice<NodeCellRef> + 'static,
{
    let first_node = read_unchecked(first_node);
    if first_node.right_bound() > right_boundary {
        return NodeSelection::PartialPage(vec![], first_node);
    }
    let mut next_node = read_unchecked(first_node.right_ref().unwrap());
    let mut collected = vec![first_node];
    loop {
        let node_right = next_node.right_bound();
        if node_right <= right_boundary {
            let on_boundary = node_right == right_boundary;
            let next_right_ref = next_node.right_ref().unwrap();
            if next_right_ref.is_default() {
                debug!("Collected all {} whole pages at level {}", collected.len(), level);
                return NodeSelection::WholePage(collected, next_node);
            }
            let next_right = read_unchecked(next_right_ref);
            trace!("Selection collected page {:?}", next_node.node_ref());
            collected.push(next_node);
            next_node = next_right;
            if on_boundary || next_node.first_key() > right_boundary {
                debug!("Collected {} whole pages at level {}", collected.len(), level);
                return NodeSelection::WholePage(collected, next_node);
            }
        } else {
            debug!("Collected {} pages and partial at level {}", collected.len(), level);
            return NodeSelection::PartialPage(collected, next_node);
        }
    }
}

fn select_boundary<KS, PS>(node: &NodeCellRef) -> EntryKey
where
    KS: Slice<EntryKey> + Debug + 'static,
    PS: Slice<NodeCellRef> + 'static,
{
    let res = read_node(node, |node: &NodeReadHandler<KS, PS>| {
        let node_keys = node.keys();
        let node_len = node_keys.len();
        if node.len() < 1 {
            return Err(node.ptrs()[0].clone());
        }
        // Pick half of the keys in the root
        // Genreally, higher level sub tree in LSM tree will select more keys to merged
        // into next level
        let mid_idx = min(node_len / 2, 8);
        // Return the mid key as the boundary for selection (cut)
        Ok(node_keys[mid_idx].clone())
    });
    match res {
        Ok(key) => key.clone(),
        Err(r) => select_boundary::<KS, PS>(&r),
    }
}

pub fn level_merge<KS, PS>(
    level: usize,
    src_tree: &BPlusTree<KS, PS>,
    dest_tree: &dyn LevelTree,
    prune: bool,
) -> usize
where
    KS: Slice<EntryKey> + Debug + 'static,
    PS: Slice<NodeCellRef> + 'static,
{
    debug!("Merging LSM tree level {}", level);
    let root = src_tree.get_root();
    let key_boundary = select_boundary::<KS, PS>(&root);
    merge_with_boundary(level, src_tree, dest_tree, &key_boundary, prune)
}

pub fn merge_with_boundary<KS, PS>(
    level: usize,
    src_tree: &BPlusTree<KS, PS>,
    dest_tree: &dyn LevelTree,
    key_boundary: &EntryKey,
    prune: bool,
) -> usize
where
    KS: Slice<EntryKey> + Debug + 'static,
    PS: Slice<NodeCellRef> + 'static,
{
    debug!(
        "Level merge level {} with boundary {:?}",
        level, key_boundary
    );
    if cfg!(debug_assertions) {
        debug!("Pre-merge verification at level {}", level);
        verification::tree_has_no_empty_node(&src_tree);
        verification::is_tree_in_order(&src_tree, level);
        debug!("Pre-merge verification for source at level {} completed", level);
    }
    let num_keys = {
        debug!("Start merge prune level {}", level);
        let (num_keys, new_root) = merge_prune(
            0,
            &src_tree.get_root(),
            src_tree,
            dest_tree,
            &key_boundary,
            prune,
            level,
        ); 
        debug!("Merged {} keys, pruning: {}", num_keys, prune);
        if let Some(new_root_ref) = new_root {
            debug!("Level merge update source root {:?}", &new_root_ref);
            *src_tree.root.write() = new_root_ref;
        }
        num_keys
    };
    if prune {
        debug!("Post-merge verification at level {}", level);
        debug_assert!(verification::tree_has_no_empty_node(&src_tree));
        debug_assert!(verification::is_tree_in_order(&src_tree, level));
        debug!("Post-merge verification for pruned source at level {} completed", level);
    }
    debug!("MERGE LEVEL {} COMPLETED", level);
    return num_keys;
}
