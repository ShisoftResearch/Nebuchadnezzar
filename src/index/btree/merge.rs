use index::EntryKey;
use index::btree::BPlusTree;
use index::btree::NodeCellRef;
use index::btree::node::read_node;
use index::btree::node::NodeReadHandler;
use std::fmt::Debug;
use index::Slice;
use index::btree::search::mut_search;
use index::btree::search::MutSearchResult;
use index::btree::node::write_key_page;
use index::btree::node::write_node;
use index::btree::node::write_non_empty;
use itertools::Itertools;

enum MergeSearch {
    External,
    Internal(NodeCellRef),
    RightNode(NodeCellRef)
}

pub fn merge<KS, PS>(
    tree: &BPlusTree<KS, PS>,
    keys: Vec<EntryKey>
)
    where KS: Slice<EntryKey> + Debug + 'static,
          PS: Slice<NodeCellRef> + 'static
{

}

pub fn merge_into_tree_node<KS, PS>(
    tree: &BPlusTree<KS, PS>,
    node: &NodeCellRef,
    parent: &NodeCellRef,
    keys: Vec<EntryKey>
)
    where KS: Slice<EntryKey> + Debug + 'static,
          PS: Slice<NodeCellRef> + 'static
{
    let search = mut_search::<KS, PS>(node, &keys[0]);
    match search {
        MutSearchResult::RightNode(node) => {
            merge_into_tree_node(tree, &node, parent, keys)
        }
        MutSearchResult::External => {
            unimplemented!()
        }
        MutSearchResult::Internal(sub_node) => {
            // merge keys into internal pages
            // this is a oneshot action.
            // after the merge, it will return all new inserted new pages to upper level
            let mut merging_pos = 0;
            let mut current_page = sub_node;
            let mut new_pages: Vec<NodeCellRef> = vec![];
            // merge by pages
            while merging_pos < keys.len() {
                let mut current_guard = write_node::<KS, PS>(&current_page);
                let start_key = &keys[merging_pos];
                let mut target_page_guard = write_key_page(current_guard, start_key);
                let key_upper_bound = target_page_guard
                    .right_ref()
                    .map(|r| write_non_empty(write_node::<KS, PS>(r)).first_key().clone());
                let selection = keys[merging_pos..].iter()
                    .filter(|k| match &key_upper_bound {
                        &Some(ref upper) => k < &upper,
                        &None => true
                    })
                    .take(KS::slice_len() - target_page_guard.len())
                    .collect_vec();
                target_page_guard.extnode_mut().merge_sort(selection.as_slice());
            }
            unimplemented!()
        }
    }
}