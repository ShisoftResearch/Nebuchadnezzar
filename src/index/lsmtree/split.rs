use super::placement::sm::client::SMClient;
use super::placement::sm::CmdError;
use super::service::{AsyncServiceClient, DEFAULT_SERVICE_ID};
use bifrost::rpc::RPCError;
use bifrost::rpc::DEFAULT_CLIENT_POOL;
use client::AsyncClient;
use dovahkiin::types::custom_types::id::Id;
use futures::prelude::*;
use index::btree::max_entry_key;
use index::lsmtree::tree::LSMTree;
use index::Cursor;
use index::EntryKey;
use index::Ordering::{Backward, Forward};
use itertools::Itertools;
use ram::types::RandValue;
use rayon::prelude::*;
use server::{rpc_client_by_id, NebServer};
use smallvec::SmallVec;
use std::sync::atomic::Ordering::Relaxed;
use std::sync::Arc;

pub struct SplitStatus {
    pub pivot: EntryKey,
    pub target: Id,
}

pub fn mid_key(tree: &LSMTree) -> EntryKey {
    // TODO: more accurate mid key take account of all tree levels
    // Current implementation only take the mid key from the tree with the most number of keys
    tree.trees
        .iter()
        .map(|tree| (tree.mid_key(), tree.count()))
        .filter_map(|(mid, count)| mid.map(|mid| (mid, count)))
        .max_by_key(|(mid, count)| *count)
        .map(|(mid, _)| mid)
        .unwrap()
}

pub fn placement_client(
    id: &Id,
    neb: &Arc<NebServer>,
) -> impl Future<Item = Arc<AsyncServiceClient>, Error = RPCError> {
    rpc_client_by_id(id, neb).map(move |c| AsyncServiceClient::new(DEFAULT_SERVICE_ID, &c))
}

pub fn check_and_split(tree: &LSMTree, sm: &Arc<SMClient>, neb: &Arc<NebServer>) -> Option<usize> {
    if tree.epoch() > 0 && tree.is_full() && tree.split.lock().is_none() {
        debug!("LSM Tree {:?} is full, will split", tree.id);
        // need to initiate a split
        let tree_key_range = tree.range.lock();
        let mut mid_key = mid_key(tree);
        let mut new_placement_id = Id::rand();
        debug!(
            "Split at {:?}, split tree id {:?}",
            mid_key, new_placement_id
        );
        // First check with the placement driver
        match sm.prepare_split(&tree.id).wait() {
            Ok(Err(CmdError::AnotherSplitInProgress(split))) => {
                mid_key = SmallVec::from(split.pivot);
                new_placement_id = split.dest;
                debug!(
                    "Placement driver reported an ongoing id {:?}",
                    new_placement_id
                );
            }
            Ok(Ok(())) => {}
            Ok(o) => panic!("Unknown state {:?}", o),
            Err(e) => panic!("Error on state machine {:?}", e),
        }
        // Then save this metadata to current tree 'split' field
        let mut split = tree.split.lock();
        *split = Some(SplitStatus {
            pivot: mid_key.clone(),
            target: new_placement_id,
        });
        // Create the tree in split host
        let client = placement_client(&new_placement_id, neb).wait().unwrap();
        let mid_vec = mid_key.iter().cloned().collect_vec();
        let new_tree_created = client
            .new_tree(
                mid_vec.clone(),
                tree_key_range.1.iter().cloned().collect(),
                new_placement_id,
            )
            .wait()
            .unwrap();
        debug!(
            "Create new split tree with id {:?}, succeed {:?}",
            new_placement_id, new_tree_created
        );
        // Inform the placement driver that this tree is going to split so it can direct all write
        // and read request to the new tree
        let src_epoch = tree.bump_epoch();
        sm.start_split(&tree.id, &new_placement_id, &mid_vec, &src_epoch)
            .wait()
            .unwrap();
        debug!("Bumped source tree {:?} epoch to {}", tree.id, src_epoch);
    }
    let mut tree_split = tree.split.lock();
    let mut total_removed = 0;
    // check if current tree is in the middle of split, so it can (re)start from the process
    if let Some(tree_split) = &*tree_split {
        // Get a cursor from the last key, backwards
        // Backwards are better for rolling batch migration for migrated keys in a batch can be
        // removed from the source tree right after they have been transferred to split tree
        let mut cursor = tree.seek(&max_entry_key(), Backward);
        let batch_size = tree.last_level_size();
        let target_id = tree_split.target;
        let target_client = placement_client(&target_id, neb).wait().unwrap();
        debug!(
            "Start to split {:?} to {:?} pivot {:?}, batch size {}",
            tree.id, tree_split.target, tree_split.pivot, batch_size
        );
        loop {
            let mut batch: Vec<Vec<_>> = Vec::with_capacity(batch_size);
            loop {
                if batch.len() >= batch_size {
                    debug!("Collected one batch");
                    break;
                }
                if !cursor.current().is_some() {
                    debug!("Cursor depleted");
                    break;
                }
                let key = cursor.current().unwrap().clone();
                if &key < &tree_split.pivot {
                    // break batch loop when current key out of mid key bound
                    debug!("Cursor out of pivot, break");
                    break;
                }
                batch.push(key.into_iter().collect());
                if !cursor.next() {
                    debug!("Cursor depleted on iteration");
                    break;
                }
            }
            if batch.is_empty() {
                // break the main transfer loop when this batch is empty
                debug!("Empty batch, assumed complete split");
                break;
            }
            batch.reverse();
            let left_most_batch_key = batch.first().unwrap().clone();
            let batch_len = batch.len();
            debug!(
                "Collected batch with size {:?}, first key {:?}",
                batch_len, left_most_batch_key
            );
            // submit this batch to new tree
            target_client
                .merge(target_id, batch, 0)
                .wait()
                .unwrap()
                .unwrap();
            // remove this batch in current tree
            let start_key = &SmallVec::from(left_most_batch_key);
            let dbg_all_to_right = if cfg!(debug_assertions) {
                tree.count_to_right(start_key)
            } else {
                0
            };
            let removed = tree.remove_to_right(start_key);
            total_removed += removed;
            debug_assert_eq!(
                removed,
                dbg_all_to_right,
                "removed count and to right count unmatched, differ {}",
                removed - dbg_all_to_right
            );
            debug_assert_eq!(
                batch_len,
                dbg_all_to_right,
                "to right count and batch count unmatched, differ {}",
                dbg_all_to_right - batch_len
            );
            debug_assert_eq!(
                removed,
                batch_len,
                "batch count and removed count unmatched, differ {}",
                removed - batch_len
            );
        }
        debug!("Split completed, remove tomestones to right");
        // split completed
        tree.remove_following_tombstones(&tree_split.pivot);
        debug!("Tomestones removed, finalizing target");
        // Bump source epoch and inform the placement driver this tree have completed split
        debug!("Updating placement driver");
        let src_epoch = tree.bump_epoch();
        sm.complete_split(&tree.id, &target_id, &src_epoch)
            .wait()
            .unwrap();
        // Set new tree epoch from 0 to 1
        let prev_epoch = target_client
            .set_epoch(target_id, 1)
            .wait()
            .unwrap()
            .unwrap();
        debug_assert_eq!(prev_epoch, 0);
    } else {
        return None;
    }
    *tree_split = None;
    debug!("LSM-Tree split completed for {:?}", tree.id);
    Some(total_removed)
}
