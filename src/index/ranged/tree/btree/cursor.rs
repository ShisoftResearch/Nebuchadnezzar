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
    keys: Vec<EntryKey>,
    // Next page to visit in iteration direction (next for Forward, prev for
    // Backward). Default ref means the iteration ends after this snapshot.
    follow: NodeCellRef,
    // When set, only `current` was captured at seek time; the rest of its
    // page is snapshotted on the first advance (point seeks never pay for a
    // tail copy). Position is re-derived from `current` by binary search, so
    // the deferred read stays correct if the page split in between.
    lazy: bool,
}

// Result of reading one page while walking the sibling chain.
enum PageSnap {
    // Keys of an external page plus the ref to follow afterwards.
    Page(Vec<EntryKey>, NodeCellRef),
    // Node holds no data (empty page or tombstone); continue with this ref.
    Skip(NodeCellRef),
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
            keys: Vec::new(),
            follow: NodeCellRef::default(),
            lazy: false,
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
            keys,
            follow,
            lazy: false,
        }
    }

    // Build a cursor that has captured only the key at the seek position;
    // the rest of the page is read on first advance.
    pub(super) fn from_lazy(
        current: EntryKey,
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
            keys: Vec::new(),
            follow: NodeCellRef::default(),
            lazy: true,
        }
    }

    // Settle the initial position: establish `current`, moving to the next
    // page or past deleted keys as needed. Called once, outside any read
    // closure.
    pub(super) fn initialize(&mut self) {
        if self.lazy {
            if self.current_is_deleted() {
                self.advance();
            }
            return;
        }
        if self.index < self.keys.len() {
            self.current = Some(self.keys[self.index].clone());
        } else if self.load_following_page() {
            self.current = Some(self.keys[self.index].clone());
        } else {
            self.current = None;
            self.page = None;
            return;
        }
        if self.current_is_deleted() {
            self.advance();
        }
    }

    fn current_is_deleted(&self) -> bool {
        self.filter_deleted
            && self
                .current
                .as_ref()
                .map(|key| self.deletion.contains(key))
                .unwrap_or(false)
    }

    // Read one page of the sibling chain without side effects on the cursor.
    fn read_page(page_ref: &NodeCellRef, ordering: Ordering) -> PageSnap {
        if page_ref.is_default() {
            return PageSnap::End;
        }
        read_node(page_ref, |node: &NodeReadHandler<KS, PS>| match &**node {
            &NodeData::External(ref n) => {
                let follow = match ordering {
                    Ordering::Forward => n.next.clone(),
                    Ordering::Backward => n.prev.clone(),
                };
                if n.len == 0 {
                    PageSnap::Skip(follow)
                } else {
                    PageSnap::Page(n.keys.as_slice_immute()[..n.len].to_vec(), follow)
                }
            }
            &NodeData::Empty(ref n) => PageSnap::Skip(match ordering {
                Ordering::Forward => n.right.clone(),
                Ordering::Backward => n.left.clone().unwrap_or_default(),
            }),
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
            match Self::read_page(&follow, self.ordering) {
                PageSnap::End => return false,
                PageSnap::Skip(next) => {
                    if next.is_default() {
                        return false;
                    }
                    follow = next;
                }
                PageSnap::Page(keys, next_follow) => {
                    debug_assert!(!keys.is_empty());
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
        let snap = read_node(&page_ref, |node: &NodeReadHandler<KS, PS>| match &**node {
            &NodeData::External(ref n) => {
                let keys = &n.keys.as_slice_immute()[..n.len];
                match self.ordering {
                    Ordering::Forward => {
                        // First key strictly greater than the current one.
                        let pos = match keys.binary_search(&cur) {
                            Ok(i) => i + 1,
                            Err(i) => i,
                        };
                        PageSnap::Page(keys[pos..].to_vec(), n.next.clone())
                    }
                    Ordering::Backward => {
                        // Keys strictly smaller than the current one.
                        let pos = keys.binary_search(&cur).unwrap_or_else(|i| i);
                        PageSnap::Page(keys[..pos].to_vec(), n.prev.clone())
                    }
                }
            }
            &NodeData::Empty(ref e) => PageSnap::Skip(match self.ordering {
                Ordering::Forward => e.right.clone(),
                Ordering::Backward => e.left.clone().unwrap_or_default(),
            }),
            &NodeData::None => PageSnap::End,
            &NodeData::Internal(_) => unreachable!("cursor reached an internal node"),
        });
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
                self.keys = Vec::new();
                self.follow = next;
            }
            PageSnap::End => {
                self.index = 0;
                self.keys = Vec::new();
                self.follow = NodeCellRef::default();
            }
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
            let candidate = &self.keys[self.index];
            if self.filter_deleted && self.deletion.contains(candidate) {
                continue;
            }
            self.current = Some(candidate.clone());
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
