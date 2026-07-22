use super::cursor::RTCursor;
use super::node::read_node;
use super::node::NodeData;
use super::node::NodeReadHandler;
use super::*;
use std::fmt::Debug;

pub fn search_node<KS, PS>(
    node_ref: &NodeCellRef,
    key: &EntryKey,
    ordering: Ordering,
    deletion: &Arc<DeletionSet>,
    filter_deleted: bool,
) -> RTCursor<KS, PS>
where
    KS: Slice<EntryKey> + Debug + 'static,
    PS: Slice<NodeCellRef> + 'static,
{
    let mut node;
    let mut node_ref = node_ref;
    let backoff = crossbeam::utils::Backoff::new();
    loop {
        // The closure must stay free of side effects: read_node re-runs it when
        // the node version changes under a concurrent writer.
        let r = read_node(node_ref, |node_handler: &NodeReadHandler<KS, PS>| {
            let node = &**node_handler;
            if let Some(right_node) = node.key_at_right_node(key) {
                trace!("Search found a node at the right side");
                return Err(right_node.clone());
            }
            let pos = match node.search_unwindable(key) {
                Ok(pos) => pos,
                Err(_) => {
                    warn!("Search cursor failed, expecting retry");
                    return Err(node_ref.clone());
                }
            };
            match node {
                &NodeData::External(ref n) => {
                    trace!(
                        "search in external for {:?}, len {}, ordering {:?}, content: {:?}",
                        key,
                        n.len,
                        ordering,
                        &n.keys.as_slice_immute()[..n.len]
                    );
                    // Capture only the key at the found position; the rest of
                    // the page is snapshotted lazily on the first advance.
                    let all = &n.keys.as_slice_immute()[..n.len];
                    let found = match ordering {
                        Ordering::Forward => {
                            if pos < n.len {
                                Some(pos)
                            } else {
                                None
                            }
                        }
                        Ordering::Backward => {
                            // Position at the largest key <= the seek key; when
                            // no such key exists in this page, fall through to
                            // the previous page.
                            if pos < n.len && &all[pos] == key {
                                Some(pos)
                            } else if pos > 0 {
                                Some(pos - 1)
                            } else {
                                None
                            }
                        }
                    };
                    Ok(match found {
                        Some(idx) => RTCursor::from_lazy(
                            all[idx].clone(),
                            node_ref.clone(),
                            ordering,
                            deletion.clone(),
                            filter_deleted,
                        ),
                        None => {
                            // Off-page position: hand the follow ref to the
                            // cursor and let initialize() move on.
                            let follow = match ordering {
                                Ordering::Forward => n.next.clone(),
                                Ordering::Backward => n.prev.clone(),
                            };
                            RTCursor::from_snapshot(
                                Vec::new(),
                                usize::MAX,
                                node_ref.clone(),
                                follow,
                                ordering,
                                deletion.clone(),
                                filter_deleted,
                            )
                        }
                    })
                }
                &NodeData::Internal(ref n) => {
                    trace!(
                        "search in internal node for {:?}, len {}, pos {}",
                        key,
                        n.len,
                        pos
                    );
                    let next_node_ref = &n.ptrs.as_slice_immute()[pos];
                    debug_assert!(pos <= n.len);
                    Err(next_node_ref.clone())
                }
                &NodeData::Empty(ref n) => Err(n.right.clone()),
                &NodeData::None => Ok(RTCursor::empty(
                    ordering,
                    deletion.clone(),
                    filter_deleted,
                )),
            }
        });
        match r {
            Ok(mut cursor) => {
                cursor.initialize();
                return cursor;
            }
            Err(e) => {
                node = e;
                node_ref = &node;
                backoff.spin();
            }
        }
    }
}

pub enum MutSearchResult {
    External,
    Internal(NodeCellRef),
}

pub fn mut_search<KS, PS>(node_ref: &NodeCellRef, key: &EntryKey) -> MutSearchResult
where
    KS: Slice<EntryKey> + Debug + 'static,
    PS: Slice<NodeCellRef> + 'static,
{
    let mut other_ref;
    let mut node_ref = node_ref;
    let backoff = crossbeam::utils::Backoff::new();
    loop {
        match read_node(node_ref, |node: &NodeReadHandler<KS, PS>| match &**node {
            &NodeData::Internal(ref n) => {
                let pos = match n.search_unwindable(key) {
                    Ok(pos) => pos,
                    Err(_) => {
                        warn!("Search paniced in mut_search, expecting retry");
                        return Err(node_ref.clone());
                    }
                };
                let sub_node = n.ptrs.as_slice_immute()[pos].clone();
                Ok(MutSearchResult::Internal(sub_node))
            }
            &NodeData::External(_) => Ok(MutSearchResult::External),
            &NodeData::Empty(ref n) => Err(n.right.clone()),
            &NodeData::None => unreachable!(),
        }) {
            Ok(res) => return res,
            Err(e) => {
                other_ref = e;
                node_ref = &other_ref;
                backoff.spin();
            }
        }
    }
}

pub fn mut_first<KS, PS>(node_ref: &NodeCellRef) -> MutSearchResult
where
    KS: Slice<EntryKey> + Debug + 'static,
    PS: Slice<NodeCellRef> + 'static,
{
    let res = read_node(node_ref, |node: &NodeReadHandler<KS, PS>| match &**node {
        &NodeData::Internal(ref n) => {
            let sub_node = n.ptrs.as_slice_immute()[0].clone();
            Ok(MutSearchResult::Internal(sub_node))
        }
        &NodeData::External(_) => Ok(MutSearchResult::External),
        &NodeData::Empty(ref n) => Err(n.right.clone()),
        &NodeData::None => unreachable!(),
    });
    res.unwrap_or_else(|e| mut_first::<KS, PS>(&e))
}
