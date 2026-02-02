use super::cursor::RTCursor;
use super::node::read_node;
use super::node::NodeData;
use super::node::NodeReadHandler;
use super::*;
use std::fmt::Debug;
use std::marker::PhantomData;

pub fn search_node<KS, PS>(
    node_ref: &NodeCellRef,
    key: &EntryKey,
    ordering: Ordering,
) -> RTCursor<KS, PS>
where
    KS: Slice<EntryKey> + Debug + 'static,
    PS: Slice<NodeCellRef> + 'static,
{
    let mut node;
    let mut node_ref = node_ref;
    let backoff = crossbeam::utils::Backoff::new();
    loop {
        let r = read_node(node_ref, |node_handler: &NodeReadHandler<KS, PS>| {
            let node = &**node_handler;
            let gen_empty_cursor = || RTCursor {
                index: 0,
                ordering,
                page: None,
                marker: PhantomData,
                current: None,
            };
            if let Some(right_node) = node.key_at_right_node(key) {
                trace!("Search found a node at the right side");
                return Err(right_node.clone());
            }
            let mut pos = match node.search_unwindable(key) {
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
                    if ordering == Ordering::Backward {
                        trace!("found cursor pos {} for backwards, will be corrected", pos);
                        if pos > 0 && (pos >= n.len || &n.keys.as_slice_immute()[pos] != key) {
                            pos -= 1;
                        }
                        trace!("cursor pos have been corrected to {}", pos);
                    }
                    Ok(RTCursor::new(pos, node_ref, ordering))
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
                &NodeData::None => Ok(gen_empty_cursor()),
            }
        });
        match r {
            Ok(res) => return res,
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
