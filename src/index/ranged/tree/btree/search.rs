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
    // A node that never resolves -- write-locked by a task cancelled
    // mid-write, say -- used to spin here silently forever, pinning the
    // thread. Say so, periodically, so the hang has a name.
    let mut spins: u64 = 0;
    loop {
        // The closure must stay free of side effects: read_node re-runs it when
        // the node version changes under a concurrent writer.
        let r = read_node(node_ref, |node_handler: &NodeReadHandler<KS, PS>| {
            let node = &**node_handler;
            // None first: the off-page continuation below hands the loop a
            // default follow ref at the end of a chain, and read_node then
            // presents the shared all-None node -- which key_at_right_node's
            // is_empty() answers with unreachable!(). The seek is simply
            // over.
            if node.is_none() {
                return Ok(RTCursor::empty(ordering, deletion.clone(), filter_deleted));
            }
            // A Backward descent passes through empty pages LEFTWARD. Left
            // alone, key_at_right_node bounces an empty page right
            // unconditionally, and a Backward off-page continuation then
            // ping-pongs empty -> right, non-empty -> prev forever (named by
            // the retry counter within seconds: tombstone compaction leaves
            // len==0 pages all over a deleted window). The cursor's page walk
            // was always direction-aware here -- read_page follows prev for
            // Backward -- and the descent continuation must be too. An empty
            // node WITHOUT a left link falls through: that is the bypass
            // shape (left None, right = the node it stands for), where
            // following right is correct for both directions.
            if ordering == Ordering::Backward && node.is_empty() {
                if let Some(left) = node.left_ref() {
                    let Some(follow) = left.try_clone_speculative() else {
                        return Err(node_ref.clone());
                    };
                    return Err(follow);
                }
            }
            if let Some(right_node) = node.key_at_right_node(key) {
                trace!("Search found a node at the right side");
                // Pointer read from unlatched data: clone speculatively and
                // retry on this node if its target is already condemned.
                return Err(right_node
                    .try_clone_speculative()
                    .unwrap_or_else(|| node_ref.clone()));
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
                        "search in external for {:?}, len {}, ordering {:?}",
                        key,
                        n.len,
                        ordering
                    );
                    // Capture only the key at the found position; the rest of
                    // the page is snapshotted lazily on the first advance.
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
                            if pos < n.len && n.keys.cmp_at(pos, key) == std::cmp::Ordering::Equal {
                                Some(pos)
                            } else if pos > 0 {
                                Some(pos - 1)
                            } else {
                                None
                            }
                        }
                    };
                    match found {
                        Some(idx) => Ok(RTCursor::from_lazy(
                            n.keys.key_at(idx),
                            filter_deleted
                                && !deletion.is_empty()
                                && deletion.contains(&n.keys.key_at(idx)),
                            node_ref.clone(),
                            ordering,
                            deletion.clone(),
                            filter_deleted,
                        )),
                        None => {
                            // Off-page position: CONTINUE THE DESCENT at the
                            // sibling instead of materializing a cursor that
                            // would yield the sibling's raw first key at a
                            // later read. Only a validated search on the node
                            // that answers can guarantee the result is not
                            // ordered before the seek key: the old
                            // empty-snapshot cursor read the sibling AFTER
                            // this closure, so a front-insert landing there
                            // in between handed back a key BELOW the seek key
                            // -- the "fresh root descent regressed" give-up
                            // storms (2026-08-31). The same window also let
                            // key_at_right_node's unvalidated sibling peek
                            // (torn mid-memmove first key) strand a descent
                            // one page short; descending through the sibling
                            // makes both harmless. Modeled in
                            // docs/tla/BLinkSeek.tla: SeekGE is violated by
                            // the old semantics and exhaustive under these.
                            let follow_src = match ordering {
                                Ordering::Forward => &n.next,
                                Ordering::Backward => &n.prev,
                            };
                            let Some(follow) = follow_src.try_clone_speculative() else {
                                return Err(node_ref.clone());
                            };
                            Err(follow)
                        }
                    }
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
                    Err(next_node_ref
                        .try_clone_speculative()
                        .unwrap_or_else(|| node_ref.clone()))
                }
                &NodeData::Empty(ref n) => Err(n
                    .right
                    .try_clone_speculative()
                    .unwrap_or_else(|| node_ref.clone())),
                &NodeData::None => Ok(RTCursor::empty(ordering, deletion.clone(), filter_deleted)),
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
                spins += 1;
                if spins.is_power_of_two() && spins >= node::STUCK_LATCH_WARN_SPINS {
                    warn!(
                        "search_node has retried {} times for key {:?}: a node is not resolving",
                        spins, key
                    );
                }
                if spins >= node::STUCK_LATCH_YIELD_SPINS {
                    backoff.snooze();
                } else {
                    backoff.spin();
                }
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
    // Same discipline as search_node: a condemned pointer that never heals
    // must not become a silent forever-spin.
    let mut spins: u64 = 0;
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
                match n.ptrs.as_slice_immute()[pos].try_clone_speculative() {
                    Some(sub_node) => Ok(MutSearchResult::Internal(sub_node)),
                    None => Err(node_ref.clone()),
                }
            }
            &NodeData::External(_) => Ok(MutSearchResult::External),
            &NodeData::Empty(ref n) => Err(n
                .right
                .try_clone_speculative()
                .unwrap_or_else(|| node_ref.clone())),
            &NodeData::None => unreachable!(),
        }) {
            Ok(res) => return res,
            Err(e) => {
                other_ref = e;
                node_ref = &other_ref;
                spins += 1;
                if spins.is_power_of_two() && spins >= node::STUCK_LATCH_WARN_SPINS {
                    warn!(
                        "mut_search has retried {} times for key {:?}: a node is not resolving",
                        spins, key
                    );
                }
                if spins >= node::STUCK_LATCH_YIELD_SPINS {
                    backoff.snooze();
                } else {
                    backoff.spin();
                }
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
        &NodeData::Internal(ref n) => match n.ptrs.as_slice_immute()[0].try_clone_speculative() {
            Some(sub_node) => Ok(MutSearchResult::Internal(sub_node)),
            None => Err(node_ref.clone()),
        },
        &NodeData::External(_) => Ok(MutSearchResult::External),
        &NodeData::Empty(ref n) => Err(n
            .right
            .try_clone_speculative()
            .unwrap_or_else(|| node_ref.clone())),
        &NodeData::None => unreachable!(),
    });
    res.unwrap_or_else(|e| mut_first::<KS, PS>(&e))
}
