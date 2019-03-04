use super::placement::sm::client::SMClient;
use super::placement::sm::CmdError;
use super::service::{AsyncServiceClient, DEFAULT_SERVICE_ID};
use bifrost::rpc::RPCError;
use bifrost::rpc::DEFAULT_CLIENT_POOL;
use client::AsyncClient;
use dovahkiin::types::custom_types::id::Id;
use futures::prelude::*;
use index::lsmtree::tree::LSMTree;
use index::Cursor;
use index::EntryKey;
use index::Ordering::Forward;
use itertools::Itertools;
use ram::types::RandValue;
use rayon::prelude::*;
use smallvec::SmallVec;
use std::sync::Arc;

pub struct SplitStatus {
    start: EntryKey,
    target: Id,
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
    client: &Arc<AsyncClient>,
) -> impl Future<Item = Arc<AsyncServiceClient>, Error = RPCError> {
    let server_id = client.locate_server_id(id).unwrap();
    let client = client.clone();
    DEFAULT_CLIENT_POOL
        .get_by_id_async(server_id, move |sid| client.conshash().to_server_name(sid))
        .map_err(|e| RPCError::IOError(e))
        .map(move |c| AsyncServiceClient::new(DEFAULT_SERVICE_ID, &c))
}

pub fn check_and_split(tree: &LSMTree, sm: &Arc<SMClient>, client: &Arc<AsyncClient>) -> bool {
    if tree.is_full() && tree.split.lock().is_none() {
        // need to initiate a split
        let tree_key_range = tree.range.lock();
        let mut mid_key = mid_key(tree);
        let mut new_placement_id = Id::rand();
        // First check with the placement driver
        match sm.prepare_split(&tree.id, &new_placement_id).wait() {
            Ok(Err(CmdError::AnotherSplitInProgress(split))) => {
                mid_key = SmallVec::from(split.mid);
                new_placement_id = split.dest;
            }
            Ok(Ok(())) => {}
            _ => panic!("Error on split"),
        }
        // Then save this metadata to current tree 'split' field
        let mut split = tree.split.lock();
        *split = Some(SplitStatus {
            start: mid_key.clone(),
            target: new_placement_id,
        });
        // Create the tree in split host
        let client = placement_client(&new_placement_id, client).wait().unwrap();
        client.new_tree(
            mid_key.iter().cloned().collect(),
            tree_key_range.1.iter().cloned().collect(),
            new_placement_id,
        );
        unimplemented!();
        // Inform the placement driver that this tree is going to split so it can direct all write
        // and read request to the new tree
        unimplemented!();
    }
    let mut tree_split = tree.split.lock();
    // check if current tree is in the middle of split, so it can (re)start from the process
    if let Some(tree_split) = &*tree_split {
        // Get a cursor from mid key, forwarding keys
        let mut cursor = tree.seek(tree_split.start.clone(), Forward);
        let batch_size = tree.last_level_size();
        while cursor.current().is_some() {
            let mut batch = Vec::with_capacity(batch_size);
            while batch.len() < batch_size && cursor.current().is_some() {
                batch.push(cursor.current().unwrap().clone());
                cursor.next();
            }
            // submit this batch to new tree
            unimplemented!();
            // remove this batch in current tree
            unimplemented!();
        }
        // split completed
        tree.remove_following_tombstones(&tree_split.start);
        // Set new tree epoch from 0 to 1
        unimplemented!();
        // Inform the placement driver this tree have completed split
        unimplemented!();
    } else {
        return false;
    }
    *tree_split = None;
    true
}
