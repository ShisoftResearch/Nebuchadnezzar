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
    pub deleted: DeletionSet,
    pub marker: PhantomData<(KS, PS)>,
    pub current: Option<EntryKey>,
}

impl<KS, PS> RTCursor<KS, PS>
where
    KS: Slice<EntryKey> + Debug + 'static,
    PS: Slice<NodeCellRef> + 'static,
{
    pub fn new(pos: usize, page: &NodeCellRef, ordering: Ordering, deleted: &DeletionSet) -> Self {
        let mut cursor = RTCursor {
            index: pos,
            ordering,
            page: Some(page.clone()),
            deleted: deleted.clone(),
            marker: PhantomData,
            current: None,
        };
        match ordering {
            Ordering::Forward
                if pos >= read_node(page, |node: &NodeReadHandler<KS, PS>| node.len()) =>
            {
                cursor.next();
            }
            _ => {
                cursor.current = Some(Self::read_current(page, pos));
            }
        }
        debug!(
            "Created cursor with pos {}, current {:?}, ordering: {:?}",
            cursor.index, cursor.current, cursor.ordering
        );
        cursor
    }
    fn boxed(self) -> Box<IndexCursor> {
        box self
    }

    fn read_current(node: &NodeCellRef, pos: usize) -> EntryKey {
        read_node(node, |node: &NodeReadHandler<KS, PS>| {
            node.extnode().keys.as_slice_immute()[pos].clone()
        })
    }

    fn next_candidate(&mut self) -> bool {
        if self.page.is_some() {
            let current_page = self.page.clone().unwrap();
            read_node(&current_page, |page: &NodeReadHandler<KS, PS>| {
                let ext_page = page.extnode();
                // debug!("Next id with index: {}, length: {}", self.index + 1, ext_page.len);
                match self.ordering {
                    Ordering::Forward => {
                        if page.is_empty() || self.index + 1 >= page.len() {
                            let next_node_ref = &ext_page.next;
                            return read_node(
                                next_node_ref,
                                |next_node: &NodeReadHandler<KS, PS>| {
                                    if next_node.is_none() {
                                        self.page = None;
                                        self.current = None;
                                        return false;
                                    } else if next_node.is_empty() {
                                        return self.next();
                                    } else if next_node.is_ext() {
                                        self.index = 0;
                                        self.page = Some(next_node_ref.clone());
                                        self.current =
                                            Some(Self::read_current(next_node_ref, self.index));
                                        return true;
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
                            let prev_node_ref = &ext_page.prev;
                            return read_node(
                                prev_node_ref,
                                |prev_node: &NodeReadHandler<KS, PS>| {
                                    if prev_node.is_none() {
                                        self.page = None;
                                        self.current = None;
                                        return false;
                                    } else if prev_node.is_empty() {
                                        return self.next();
                                    } else if prev_node.is_ext() {
                                        self.index = prev_node.len() - 1;
                                        self.page = Some(prev_node_ref.clone());
                                        self.current =
                                            Some(Self::read_current(prev_node_ref, self.index));
                                        return true;
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
                self.current = Some(page.extnode().keys.as_slice_immute()[self.index].clone());
                true
            })
        } else {
            false
        }
    }
}

impl<KS, PS> IndexCursor for RTCursor<KS, PS>
where
    KS: Slice<EntryKey> + Debug + 'static,
    PS: Slice<NodeCellRef> + 'static,
{
    // TODO: Copy current after next
    fn next(&mut self) -> bool {
        loop {
            let has_candidate = self.next_candidate();
            if has_candidate {
                // search in deleted set and skip if exists
                if !self.deleted.read().contains(self.current().unwrap()) {
                    return true;
                }
            } else {
                return false;
            }
        }
    }

    // TODO: Use copied key reference
    fn current(&self) -> Option<&EntryKey> {
        self.current.as_ref()
    }
}
