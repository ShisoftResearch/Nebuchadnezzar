use index::btree::insert::check_root_modification;
use index::btree::internal::InNode;
use index::btree::node::read_node;
use index::btree::node::read_unchecked;
use index::btree::node::write_node;
use index::btree::node::write_non_empty;
use index::btree::node::write_targeted;
use index::btree::node::Node;
use index::btree::node::NodeData;
use index::btree::node::NodeReadHandler;
use index::btree::search::mut_search;
use index::btree::search::MutSearchResult;
use index::btree::BPlusTree;
use index::btree::NodeCellRef;
use index::EntryKey;
use index::Slice;
use itertools::Itertools;
use std::fmt::Debug;

enum MergeSearch {
    External,
    Internal(NodeCellRef),
    RightNode(NodeCellRef),
}

fn merge_into_internal<KS, PS>(
    node: &NodeCellRef,
    lower_level_new_pages: Vec<(EntryKey, NodeCellRef)>,
    new_pages: &mut Vec<(EntryKey, NodeCellRef)>,
) where
    KS: Slice<EntryKey> + Debug + 'static,
    PS: Slice<NodeCellRef> + 'static,
{
    let mut node_guard = write_node::<KS, PS>(node);
    for (pivot, node) in lower_level_new_pages {
        let mut target_guard = write_non_empty(write_targeted(node_guard, &pivot));
        {
            debug_assert!(!target_guard.is_none());
            let innode = target_guard.innode_mut();
            let pos = innode.search(&pivot);
            if innode.len == KS::slice_len() - 1 {
                // full node, going to split
                let (node_ref, key) = innode.split_insert(pivot, node, pos);
                new_pages.push((key, node_ref));
            } else {
                innode.insert_in_place(pivot, node, pos);
            }
        }
        node_guard = target_guard;
    }
}

pub fn merge_into_tree_node<KS, PS>(
    tree: &BPlusTree<KS, PS>,
    node: &NodeCellRef,
    parent: &NodeCellRef,
    keys: Vec<EntryKey>,
    level: usize,
) -> Vec<(EntryKey, NodeCellRef)>
where
    KS: Slice<EntryKey> + Debug + 'static,
    PS: Slice<NodeCellRef> + 'static,
{
    let search = mut_search::<KS, PS>(node, &keys[0]);
    let mut new_pages = match search {
        MutSearchResult::External => {
            // merge keys into internal pages
            // this is a oneshot action.
            // after the merge, it will return all new inserted new pages to upper level
            debug!("Merge into internal with keys {:?}", &keys);
            let keys_len = keys.len();
            let mut merging_pos = 0;
            let mut current_guard = write_node::<KS, PS>(&node);
            let mut new_pages = vec![];
            // merge by pages
            while merging_pos < keys_len {
                let start_key = &keys[merging_pos];
                debug!("Start merging with page at {:?}", start_key);
                let mut target_page_guard = write_targeted(current_guard, start_key);
                let mut right_guard =
                    write_node::<KS, PS>(target_page_guard.right_ref_mut_no_empty().unwrap());
                let key_upper_bound = if right_guard.is_none() {
                    None
                } else {
                    Some(right_guard.first_key().clone())
                };
                {
                    let mut ext_node = target_page_guard.extnode_mut();
                    ext_node.remove_contains(&mut *tree.deleted.write());
                    let selection = keys[merging_pos..keys_len]
                        .iter()
                        .filter(|k| match &key_upper_bound {
                            &Some(ref upper) => k < &upper,
                            &None => true,
                        })
                        .take(KS::slice_len() - ext_node.len)
                        .collect_vec();
                    ext_node.merge_sort(selection.as_slice());
                    merging_pos += selection.len();
                }
                if merging_pos >= keys_len {
                    break;
                }
                let next_insert_key = &keys[merging_pos];
                debug_assert!(
                    next_insert_key > &smallvec!(0),
                    "empty key at {}, keys {:?}",
                    merging_pos,
                    keys
                );
                if key_upper_bound.is_none() || next_insert_key < key_upper_bound.as_ref().unwrap()
                {
                    // trigger a split and put the split node into a cache
                    let insert_pos = target_page_guard.search(&keys[merging_pos]);
                    let target_node_ref = target_page_guard.node_ref().clone();
                    let (new_node, pivot) = target_page_guard.extnode_mut().split_insert(
                        next_insert_key.clone(),
                        insert_pos,
                        &target_node_ref,
                        &mut right_guard,
                        tree,
                    );
                    merging_pos += 1;
                    current_guard = write_node(&new_node);
                    new_pages.push((pivot, new_node));
                } else {
                    debug_assert!(!right_guard.is_none());
                    current_guard = right_guard;
                }
            }
            new_pages
        }
        MutSearchResult::Internal(sub_node) => {
            let lower_level_new_pages =
                merge_into_tree_node(tree, &sub_node, node, keys, level + 1);
            let mut new_pages = vec![];
            if lower_level_new_pages.len() > 0 {
                merge_into_internal::<KS, PS>(node, lower_level_new_pages, &mut new_pages);
            }
            new_pages
        }
    };
    if level == 0 && new_pages.len() > 0 {
        // it is impossible to have a node been changed during merge for merges are performed in serial
        debug_assert_eq!(
            read_unchecked::<KS, PS>(&tree.get_root()).first_key(),
            read_unchecked::<KS, PS>(&node).first_key()
        )
    }
    return new_pages;
}
