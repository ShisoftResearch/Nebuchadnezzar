// Tree split by COPY: build a new tree holding copies of the live keys at or
// past `pivot`, leaving the source untouched until the caller commits with
// `split::retain`. Source and copy share no pages, so every page has exactly
// one owner and an abort is "drain and drop the copy". See
// docs/ranged-index-robustness-plan.md (proposal 1) and docs/tla/CopySplit.tla.
// The caller must hold the source frozen (migration marker) for the whole
// copy-to-retain window.
use super::leaf_keys::LeafKeys;
use super::node::NodeData;
use super::reconstruct::TreeConstructor;
use super::*;
use std::fmt::Debug;

pub struct SplitOff {
    pub new_root: NodeCellRef,
    pub new_height: usize,
    pub moved_len: usize,
    pub new_head_id: Id,
}

/// Copy-based split: build a NEW tree holding copies of every LIVE key at or
/// past `pivot`, touching the source not at all (docs/
/// ranged-index-robustness-plan.md, proposal 1). The source and the copy
/// share NO pages -- ownership always has one answer -- so aborting a split
/// is "drain and drop the copy", and committing is a placement flip plus
/// `retain` on the source; the seam severing, bounded recovery, marker
/// chain-surgery and reabsorb of the shared-leaf design all become
/// unnecessary.
///
/// Two deliberate choices, both about the write-back flusher's tombstone
/// pairing (`remove_contains` consumes a tombstone against whichever copy it
/// meets first):
/// - The walk is FILTERED: tombstoned keys are never copied, so no key ever
///   has two copies with a tombstone in play. Their single copies stay in
///   the source until `retain` removes them; the tombstones linger unpaired
///   (bounded by uncompacted deletes in the moved range) and are inert.
/// - Live keys DO exist twice between copy and retain, but with no
///   tombstone there is nothing to mispair, and the caller holds the tree
///   frozen so no delete can land inside the window.
///
/// Costs ~4ms per 500K keys moved (measured, release) against 0.35ms for
/// the spine cut -- both under a tree that is frozen anyway.
pub fn copy_off<KS, PS>(tree: &BPlusTree<KS, PS>, pivot: &EntryKey) -> Option<SplitOff>
where
    KS: Slice<EntryKey> + Debug + 'static,
    PS: Slice<NodeCellRef> + 'static,
{
    let cap = KS::slice_len();
    let anchor = tree.head_id();
    let mut all: Vec<EntryKey> = Vec::new();
    {
        let mut cursor = tree.seek(pivot, Ordering::Forward);
        while let Some(key) = cursor.next() {
            all.push(key);
        }
    }
    if all.is_empty() {
        return None;
    }
    let moved_len = all.len();
    let mut constructor = TreeConstructor::<KS, PS>::new();
    let mut head_id: Option<Id> = None;
    let mut prev_ref: Option<NodeCellRef> = None;
    let chunks: Vec<&[EntryKey]> = all.chunks(cap).collect();
    for (i, chunk) in chunks.iter().enumerate() {
        let right_bound = match chunks.get(i + 1) {
            Some(next) => next[0].clone(),
            None => max_entry_key(),
        };
        let new_id = BPlusTree::<KS, PS>::new_page_id_near(&anchor);
        let mut leaf = ExtNode::<KS, PS>::new(new_id, right_bound);
        leaf.keys = LeafKeys::from_keys(chunk, cap);
        leaf.len = chunk.len();
        leaf.prev = prev_ref.clone().unwrap_or_default();
        let leaf_ref = NodeCellRef::new(Node::with_external(leaf));
        if let Some(prev) = &prev_ref {
            let mut prev_guard = write_node::<KS, PS>(prev);
            prev_guard.extnode_mut_no_persist().next = leaf_ref.clone();
        }
        if head_id.is_none() {
            head_id = Some(new_id);
        }
        constructor.push_extnode(&leaf_ref, chunk[0].clone());
        // Fresh pages must reach disk before the placement flip; the
        // caller's barrier orders on this queueing.
        external::make_changed(&leaf_ref, tree);
        prev_ref = Some(leaf_ref);
    }
    Some(SplitOff {
        new_root: constructor.root(),
        new_height: constructor.levels(),
        moved_len,
        new_head_id: head_id.expect("non-empty copy implies a head"),
    })
}

/// Drain every key out of `tree`, emptying each leaf UNDER ITS OWN LATCH as
/// its keys are collected. For rolling back a failed split: the moved pages
/// were dirty-queued on the write-back hub (often the very backlog that
/// failed the seam barrier), and the queue holds owned refs -- so after the
/// target is dropped, the flusher still processes those orphaned pages, and
/// `remove_contains` there pairs a key's STALE copy with the live deletion
/// set: a delete landing after the rollback gets its tombstone consumed
/// against the orphan, resurrecting the reabsorbed copy (the 3h soak's
/// persistent resurrections, five runs). Draining keeps a single copy of
/// every key at every instant -- in the old page, or in the returned Vec,
/// or reinserted -- never two, so the orphan flush finds nothing to
/// mispair. The caller holds the tree frozen (migration marker).
pub fn drain_all_keys<KS, PS>(tree: &BPlusTree<KS, PS>) -> Vec<EntryKey>
where
    KS: Slice<EntryKey> + Debug + 'static,
    PS: Slice<NodeCellRef> + 'static,
{
    // Descend to the leftmost leaf.
    let mut cur = tree.get_root();
    loop {
        let next = match &*read_unchecked::<KS, PS>(&cur) {
            &NodeData::Internal(ref n) => Some(n.ptrs.as_slice_immute()[0].clone()),
            &NodeData::Empty(ref n) => Some(n.right.clone()),
            _ => None,
        };
        match next {
            Some(n) => cur = n,
            None => break,
        }
    }
    // Walk the chain, emptying each leaf as it is read. Detach as well:
    // emptying protects against tombstone mispairing, detaching also stops
    // the flusher from persisting a cell nothing will ever reference.
    let mut keys = Vec::new();
    while !cur.is_default() {
        cur.deref::<KS, PS>().detach();
        let next = {
            let mut guard = write_node::<KS, PS>(&cur);
            match &mut *guard {
                &mut NodeData::External(ref mut n) => {
                    keys.extend(n.keys.to_vec(0..n.len));
                    n.len = 0;
                    n.next.clone()
                }
                &mut NodeData::Empty(ref e) => e.right.clone(),
                _ => break,
            }
        };
        cur = next;
    }
    keys
}
