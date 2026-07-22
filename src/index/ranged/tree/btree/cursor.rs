use super::leaf_keys::PackedKeys;
use super::*;

// Runtime cursor for iteration.
//
// The cursor never holds locks. Each page it visits is snapshotted (keys copied
// out) inside a single validated `read_node` closure, then iterated locally.
// This matters for correctness: `read_node` re-runs its closure when the page
// version changes mid-read, so the closure must be free of side effects on the
// cursor. The previous implementation mutated `self.index` inside the closure
// and skipped or repeated keys whenever a writer forced a retry.
pub struct RTCursor<KS, PS>
where
    KS: Slice<EntryKey> + Debug + 'static,
    PS: Slice<NodeCellRef> + 'static,
{
    pub index: usize,
    pub ordering: Ordering,
    // Page the current snapshot was taken from; kept so callers can mark the
    // page changed for write-back. None once the cursor is exhausted.
    pub page: Option<NodeCellRef>,
    pub marker: PhantomData<(KS, PS)>,
    pub current: Option<EntryKey>,
    pub deletion: Arc<DeletionSet>,
    pub filter_deleted: bool,
    // Snapshot of the current page's keys, in key order.
    keys: SnapKeys,
    // Next page to visit in iteration direction (next for Forward, prev for
    // Backward). Default ref means the iteration ends after this snapshot.
    follow: NodeCellRef,
    // When set, only `current` was captured at seek time; the rest of its
    // page is snapshotted on the first advance (point seeks never pay for a
    // tail copy). Position is re-derived from `current` by binary search, so
    // the deferred read stays correct if the page split in between.
    lazy: bool,
    // Tombstone verdict for the lazy `current`, taken inside the same
    // validated read that captured it (checking later races with tombstone
    // reclamation).
    current_deleted: bool,
}

// Page snapshot storage. Packed keeps the page's compressed form (one
// suffix memcpy at snapshot time, per-key reconstruction only at yield);
// Full materializes every key and is used when tombstone filtering must be
// applied inside the validated read (see DeletionReclaim.tla — the filter
// and the snapshot must be atomic with respect to reclamation).
pub(super) enum SnapKeys {
    Full(Vec<EntryKey>),
    Packed(PackedKeys),
}

impl SnapKeys {
    #[inline]
    fn len(&self) -> usize {
        match self {
            SnapKeys::Full(v) => v.len(),
            SnapKeys::Packed(p) => p.len(),
        }
    }

    #[inline]
    fn key(&self, index: usize) -> EntryKey {
        match self {
            SnapKeys::Full(v) => v[index].clone(),
            SnapKeys::Packed(p) => p.key(index),
        }
    }

    fn empty() -> Self {
        SnapKeys::Full(Vec::new())
    }
}

// Result of reading one page while walking the sibling chain.
enum PageSnap {
    // Keys of an external page plus the ref to follow afterwards.
    Page(SnapKeys, NodeCellRef),
    // Node holds no data (empty page or tombstone); continue with this ref.
    Skip(NodeCellRef),
    // A speculative pointer clone failed (target condemned); re-read.
    Retry,
    // End of the chain.
    End,
}

impl<KS, PS> RTCursor<KS, PS>
where
    KS: Slice<EntryKey> + Debug + 'static,
    PS: Slice<NodeCellRef> + 'static,
{
    pub fn empty(ordering: Ordering, deletion: Arc<DeletionSet>, filter_deleted: bool) -> Self {
        RTCursor {
            index: 0,
            ordering,
            page: None,
            marker: PhantomData,
            current: None,
            deletion,
            filter_deleted,
            keys: SnapKeys::empty(),
            follow: NodeCellRef::default(),
            lazy: false,
            current_deleted: false,
        }
    }

    // Build a cursor from a page snapshot taken by the caller inside a
    // validated read. `index == usize::MAX` means there is no valid position
    // in this snapshot and the cursor must move to the following page first;
    // `initialize` settles that.
    pub(super) fn from_snapshot(
        keys: Vec<EntryKey>,
        index: usize,
        page: NodeCellRef,
        follow: NodeCellRef,
        ordering: Ordering,
        deletion: Arc<DeletionSet>,
        filter_deleted: bool,
    ) -> Self {
        RTCursor {
            index,
            ordering,
            page: Some(page),
            marker: PhantomData,
            current: None,
            deletion,
            filter_deleted,
            keys: SnapKeys::Full(keys),
            follow,
            lazy: false,
            current_deleted: false,
        }
    }

    // Build a cursor that has captured only the key at the seek position;
    // the rest of the page is read on first advance.
    pub(super) fn from_lazy(
        current: EntryKey,
        current_deleted: bool,
        page: NodeCellRef,
        ordering: Ordering,
        deletion: Arc<DeletionSet>,
        filter_deleted: bool,
    ) -> Self {
        RTCursor {
            index: usize::MAX,
            ordering,
            page: Some(page),
            marker: PhantomData,
            current: Some(current),
            deletion,
            filter_deleted,
            keys: SnapKeys::empty(),
            follow: NodeCellRef::default(),
            lazy: true,
            current_deleted,
        }
    }

    // Settle the initial position: establish `current`, moving to the next
    // page or past deleted keys as needed. Called once, outside any read
    // closure.
    pub(super) fn initialize(&mut self) {
        if self.lazy {
            // Snapshot-time verdict from the seek closure; iteration relies
            // on snapshot-time filtering throughout.
            if self.current_deleted {
                self.advance();
            }
            return;
        }
        if self.index < self.keys.len() {
            self.current = Some(self.keys.key(self.index));
        } else if self.load_following_page() {
            self.current = Some(self.keys.key(self.index));
        } else {
            self.current = None;
            self.page = None;
        }
    }

    // Read one page of the sibling chain without side effects on the cursor.
    // Tombstoned keys are filtered inside the validated closure: tombstone
    // reclamation drops a tombstone under the page's write latch, so a
    // validated snapshot sees either key+tombstone (filtered here) or
    // neither — checking at yield time instead would race the reclaim.
    fn read_page(
        page_ref: &NodeCellRef,
        ordering: Ordering,
        deletion: &DeletionSet,
        filter_deleted: bool,
    ) -> PageSnap {
        if page_ref.is_default() {
            return PageSnap::End;
        }
        read_node(page_ref, |node: &NodeReadHandler<KS, PS>| match &**node {
            &NodeData::External(ref n) => {
                let follow_src = match ordering {
                    Ordering::Forward => &n.next,
                    Ordering::Backward => &n.prev,
                };
                let Some(follow) = follow_src.try_clone_speculative() else {
                    return PageSnap::Retry;
                };
                // One emptiness check per page instead of one hash lookup
                // per key when nothing is tombstoned (the common case).
                let filtering = filter_deleted && deletion.len() > 0;
                let keys = if filtering {
                    SnapKeys::Full(
                        n.keys
                            .to_vec(0..n.len)
                            .into_iter()
                            .filter(|k| !deletion.contains(k))
                            .collect(),
                    )
                } else {
                    SnapKeys::Packed(n.keys.packed_snapshot(0..n.len))
                };
                if keys.len() == 0 {
                    PageSnap::Skip(follow)
                } else {
                    PageSnap::Page(keys, follow)
                }
            }
            &NodeData::Empty(ref n) => {
                let next = match ordering {
                    Ordering::Forward => n.right.try_clone_speculative(),
                    Ordering::Backward => match n.left.as_ref() {
                        Some(l) => l.try_clone_speculative(),
                        None => Some(NodeCellRef::default()),
                    },
                };
                match next {
                    Some(next) => PageSnap::Skip(next),
                    None => PageSnap::Retry,
                }
            }
            &NodeData::None => PageSnap::End,
            &NodeData::Internal(_) => unreachable!("cursor reached an internal node"),
        })
    }

    // Move the snapshot to the next page in iteration direction. Returns false
    // when the chain ends; on success `index` points at the first candidate of
    // the new snapshot.
    fn load_following_page(&mut self) -> bool {
        let mut follow = mem::take(&mut self.follow);
        loop {
            match Self::read_page(&follow, self.ordering, &self.deletion, self.filter_deleted) {
                PageSnap::Retry => continue,
                PageSnap::End => return false,
                PageSnap::Skip(next) => {
                    if next.is_default() {
                        return false;
                    }
                    follow = next;
                }
                PageSnap::Page(keys, next_follow) => {
                    debug_assert!(keys.len() > 0);
                    self.index = match self.ordering {
                        Ordering::Forward => 0,
                        Ordering::Backward => keys.len() - 1,
                    };
                    self.keys = keys;
                    self.page = Some(follow);
                    self.follow = next_follow;
                    return true;
                }
            }
        }
    }

    // Snapshot the part of the current page that lies beyond `current` in
    // iteration direction. Runs once per lazy cursor, on first advance. The
    // position is re-derived from the current key, which keeps the snapshot
    // correct even if the page changed since the seek.
    fn materialize_tail(&mut self) {
        self.lazy = false;
        let cur = match &self.current {
            Some(k) => k.clone(),
            None => return,
        };
        let page_ref = match &self.page {
            Some(r) => r.clone(),
            None => return,
        };
        let snap = loop {
            let attempt = read_node(&page_ref, |node: &NodeReadHandler<KS, PS>| match &**node {
            &NodeData::External(ref n) => {
                let filtering = self.filter_deleted && self.deletion.len() > 0;
                let snap = |range: std::ops::Range<usize>| -> SnapKeys {
                    if filtering {
                        SnapKeys::Full(
                            n.keys
                                .to_vec(range)
                                .into_iter()
                                .filter(|k| !self.deletion.contains(k))
                                .collect(),
                        )
                    } else {
                        SnapKeys::Packed(n.keys.packed_snapshot(range))
                    }
                };
                match self.ordering {
                    Ordering::Forward => {
                        // First key strictly greater than the current one.
                        let lb = n.keys.search(n.len, &cur);
                        let pos = if lb < n.len
                            && n.keys.cmp_at(lb, &cur) == std::cmp::Ordering::Equal
                        {
                            lb + 1
                        } else {
                            lb
                        };
                        match n.next.try_clone_speculative() {
                            Some(follow) => PageSnap::Page(snap(pos..n.len), follow),
                            None => PageSnap::Retry,
                        }
                    }
                    Ordering::Backward => {
                        // Keys strictly smaller than the current one.
                        let pos = n.keys.search(n.len, &cur);
                        match n.prev.try_clone_speculative() {
                            Some(follow) => PageSnap::Page(snap(0..pos), follow),
                            None => PageSnap::Retry,
                        }
                    }
                }
            }
            &NodeData::Empty(ref e) => {
                let next = match self.ordering {
                    Ordering::Forward => e.right.try_clone_speculative(),
                    Ordering::Backward => match e.left.as_ref() {
                        Some(l) => l.try_clone_speculative(),
                        None => Some(NodeCellRef::default()),
                    },
                };
                match next {
                    Some(next) => PageSnap::Skip(next),
                    None => PageSnap::Retry,
                }
            }
            &NodeData::None => PageSnap::End,
            &NodeData::Internal(_) => unreachable!("cursor reached an internal node"),
            });
            match attempt {
                PageSnap::Retry => continue,
                other => break other,
            }
        };
        match snap {
            PageSnap::Page(keys, follow) => {
                // Sentinel positions: "before the first" for Forward (wraps to
                // 0 on the next step), "after the last" for Backward.
                self.index = match self.ordering {
                    Ordering::Forward => usize::MAX,
                    Ordering::Backward => keys.len(),
                };
                self.keys = keys;
                self.follow = follow;
            }
            PageSnap::Skip(next) => {
                self.index = match self.ordering {
                    Ordering::Forward => usize::MAX,
                    Ordering::Backward => 0,
                };
                self.keys = SnapKeys::empty();
                self.follow = next;
            }
            PageSnap::End => {
                self.index = 0;
                self.keys = SnapKeys::empty();
                self.follow = NodeCellRef::default();
            }
            // The retry loop above never breaks with Retry.
            PageSnap::Retry => unreachable!(),
        }
    }

    // Advance `current` to the next non-deleted key, or None at the end.
    fn advance(&mut self) {
        if self.lazy {
            self.materialize_tail();
        }
        loop {
            let stepped = match self.ordering {
                Ordering::Forward => {
                    // usize::MAX marks "before the first key" and wraps to 0.
                    let next = self.index.wrapping_add(1);
                    if next < self.keys.len() {
                        self.index = next;
                        true
                    } else {
                        false
                    }
                }
                Ordering::Backward => {
                    if self.index > 0 && self.index <= self.keys.len() {
                        self.index -= 1;
                        true
                    } else {
                        false
                    }
                }
            };
            if !stepped && !self.load_following_page() {
                self.current = None;
                self.page = None;
                return;
            }
            // Snapshots are filtered against the deletion set when they are
            // taken; no per-yield lookup is needed.
            self.current = Some(self.keys.key(self.index));
            return;
        }
    }
}

impl<KS, PS> Cursor for RTCursor<KS, PS>
where
    KS: Slice<EntryKey> + Debug + 'static,
    PS: Slice<NodeCellRef> + 'static,
{
    // Returns the current key and advances to the following one.
    fn next(&mut self) -> Option<EntryKey> {
        if self.lazy && self.current.is_some() {
            // Materialize while `current` is still set; it anchors the
            // deferred page read.
            self.materialize_tail();
        }
        let out = self.current.take();
        if out.is_some() {
            self.advance();
        }
        out
    }

    fn current(&self) -> Option<&EntryKey> {
        self.current.as_ref()
    }
}
