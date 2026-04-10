use super::*;

// This is the runtime cursor on iteration
// It hold a copy of the containing page next page lock guard
// These lock guards are preventing the node and their neighbourhoods been changed externally
// Ordering are specified that can also change lock pattern
pub struct RTCursor<KS, PS>
where
    KS: Slice<EntryKey> + Debug + 'static,
    PS: Slice<NodeCellRef> + 'static,
{
    pub index: usize,
    pub ordering: Ordering,
    pub page: Option<NodeCellRef>,
    pub marker: PhantomData<(KS, PS)>,
    pub current: Option<EntryKey>,
    pub deletion: Arc<DeletionSet>,
    pub filter_deleted: bool,
}

impl<KS, PS> RTCursor<KS, PS>
where
    KS: Slice<EntryKey> + Debug + 'static,
    PS: Slice<NodeCellRef> + 'static,
{
    pub fn new(
        pos: usize,
        page: &NodeCellRef,
        ordering: Ordering,
        deletion: Arc<DeletionSet>,
        filter_deleted: bool,
    ) -> Self {
        let mut cursor = RTCursor {
            index: pos,
            ordering,
            page: Some(page.clone()),
            marker: PhantomData,
            current: None,
            deletion,
            filter_deleted,
        };
        match ordering {
            Ordering::Forward
                if pos >= read_node(page, |node: &NodeReadHandler<KS, PS>| node.len()) =>
            {
                let _ = cursor.next_raw_candidate();
                cursor.skip_deleted_current();
            }
            _ => {
                cursor.current = Self::read_current(page, pos);
                cursor.skip_deleted_current();
            }
        }
        trace!(
            "Created cursor with pos {}, current {:?}, ordering: {:?}",
            cursor.index, cursor.current, cursor.ordering
        );
        cursor
    }

    fn read_current(node: &NodeCellRef, pos: usize) -> Option<EntryKey> {
        read_node(node, |node: &NodeReadHandler<KS, PS>| {
            // node can be empty only if the node have been changed in the middle
            // if so the node should be reloaded from the outside by `read_node` function
            if node.is_empty_node() {
                None
            } else {
                Some(node.extnode().keys.as_slice_immute()[pos].clone())
            }
        })
    }

    fn current_is_deleted(&self) -> bool {
        self.filter_deleted
            && self
                .current
                .as_ref()
                .map(|key| self.deletion.contains(key))
                .unwrap_or(false)
    }

    fn skip_deleted_current(&mut self) {
        while self.current_is_deleted() {
            let _ = self.next_raw_candidate();
        }
    }

    fn next_raw_candidate(&mut self) -> Option<EntryKey> {
        loop {
            let search_result = if self.page.is_some() {
                let current_page = self.page.clone().unwrap();
                read_node(&current_page, |page: &NodeReadHandler<KS, PS>| {
                    let mut other_current;
                    // let ext_page = page.extnode();
                    // debug!("Next id with index: {}, length: {}", self.index + 1, ext_page.len);
                    match self.ordering {
                        Ordering::Forward => {
                            if page.is_empty() || self.index + 1 >= page.len() {
                                let next_node_ref = page.right_ref().unwrap();
                                if next_node_ref.is_default() {
                                    // No next page, return current item (last item) then stop
                                    let mut other_current = None;
                                    mem::swap(&mut self.current, &mut other_current);
                                    self.page = None;
                                    return Some(other_current);
                                }
                                return read_node(
                                    next_node_ref,
                                    |next_node: &NodeReadHandler<KS, PS>| {
                                        let mut other_current;
                                        if next_node.is_none() {
                                            // Return the current item (last item in the tree) before stopping
                                            // After this, current will be None
                                            other_current = None;
                                            mem::swap(&mut self.current, &mut other_current);
                                            self.page = None;
                                            return Some(other_current);
                                        } else if next_node.is_empty() {
                                            return None;
                                        } else if next_node.is_ext() {
                                            self.index = 0;
                                            self.page = Some(next_node_ref.clone());
                                            other_current =
                                                Self::read_current(next_node_ref, self.index);
                                            mem::swap(&mut self.current, &mut other_current);
                                            return Some(other_current);
                                        } else {
                                            unreachable!()
                                        }
                                    },
                                );
                            } else {
                                self.index += 1;
                                // debug!("Advancing cursor to index {}", self.index);
                            }
                        }
                        Ordering::Backward => {
                            if page.is_empty() || self.index == 0 {
                                let prev_node_ref = page.left_ref().unwrap();
                                if prev_node_ref.is_default() {
                                    // No prev page, return current item (first item in reverse) then stop
                                    let mut other_current = None;
                                    mem::swap(&mut self.current, &mut other_current);
                                    self.page = None;
                                    return Some(other_current);
                                }
                                return read_node(
                                    prev_node_ref,
                                    |prev_node: &NodeReadHandler<KS, PS>| {
                                        let mut other_current;
                                        if prev_node.is_none() {
                                            // Return the current item (last item in backward traversal) before stopping
                                            // After this, current will be None
                                            other_current = None;
                                            mem::swap(&mut self.current, &mut other_current);
                                            self.page = None;
                                            return Some(other_current);
                                        } else if prev_node.is_empty() {
                                            return None;
                                        } else if prev_node.is_ext() {
                                            self.index = prev_node.len() - 1;
                                            self.page = Some(prev_node_ref.clone());
                                            other_current =
                                                Self::read_current(prev_node_ref, self.index);
                                            mem::swap(&mut self.current, &mut other_current);
                                            return Some(other_current);
                                        } else {
                                            unreachable!()
                                        }
                                    },
                                );
                            } else {
                                self.index -= 1;
                                // debug!("Advancing cursor to index {}", self.index);
                            }
                        }
                    }
                    other_current = Self::read_current(&current_page, self.index);
                    mem::swap(&mut self.current, &mut other_current);
                    Some(other_current)
                })
            } else {
                Some(None)
            };

            if let Some(res) = search_result {
                return res;
            }
        }
    }

    fn next_candidate(&mut self) -> Option<EntryKey> {
        let res = self.next_raw_candidate();
        self.skip_deleted_current();
        res
    }
}

impl<KS, PS> Cursor for RTCursor<KS, PS>
where
    KS: Slice<EntryKey> + Debug + 'static,
    PS: Slice<NodeCellRef> + 'static,
{
    // TODO: Copy current after next
    fn next(&mut self) -> Option<EntryKey> {
        if let Some(swapped_old_candidate) = self.next_candidate() {
            return Some(swapped_old_candidate);
        } else {
            return None;
        }
    }

    // TODO: Use copied key reference
    fn current(&self) -> Option<&EntryKey> {
        self.current.as_ref()
    }
}
