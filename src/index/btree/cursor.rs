use super::*;

// This is the runtime cursor on iteration
// It hold a copy of the containing page next page lock guard
// These lock guards are preventing the node and their neighbourhoods been changed externally
// Ordering are specified that can also change lock pattern
pub struct RTCursor<KS, PS>
    where KS: Slice<EntryKey> + Debug + 'static,
          PS: Slice<NodeCellRef> + 'static
{
    pub index: usize,
    pub ordering: Ordering,
    pub page: Option<NodeCellRef>,
    pub marker: PhantomData<(KS, PS)>
}

impl <KS, PS>RTCursor<KS, PS>
    where KS: Slice<EntryKey> + Debug + 'static,
          PS: Slice<NodeCellRef> + 'static
{
    pub fn new(pos: usize, page: &NodeCellRef, ordering: Ordering) -> Self {
        let mut cursor = RTCursor {
            index: pos,
            ordering,
            page: Some(page.clone()),
            marker: PhantomData
        };
        match ordering {
            Ordering::Forward => {
                let len = read_node(page, |node: &NodeReadHandler<KS, PS>| node.len());
                if pos >= len {
                    cursor.next();
                }
            }
            Ordering::Backward => {}
        }
        cursor
    }
    fn boxed(self) -> Box<IndexCursor> {
        box self
    }
}

impl <KS, PS>IndexCursor for RTCursor<KS, PS>
    where KS: Slice<EntryKey> + Debug + 'static,
          PS: Slice<NodeCellRef> + 'static
{
    fn next(&mut self) -> bool {
        if self.page.is_some() {
            let current_page = self.page.clone().unwrap();
            read_node(&current_page, |page: &NodeReadHandler<KS, PS>| {
                let ext_page = page.extnode();
                // debug!("Next id with index: {}, length: {}", self.index + 1, ext_page.len);
                match self.ordering {
                    Ordering::Forward => {
                        if page.is_empty() {
                            let next_node = ext_page.next.deref::<KS, PS>().read_unchecked();
                            self.index = 0;
                            self.page = Some(ext_page.next.clone());
                            if next_node.is_none() {
                                self.page = None;
                                return false;
                            }
                            else if next_node.is_empty() {
                                return self.next();
                            } else if next_node.is_ext() {
                                return true;
                            } else {
                                unreachable!()
                            }
                        }
                        if self.index + 1 >= page.len() {
                            // next page
                            return read_node(&ext_page.next, |next_page: &NodeReadHandler<KS, PS>| {
                                if !next_page.is_none() {
                                    // debug!("Shifting page forward, next page len: {}", next_page.len());
                                    self.index = 0;
                                    self.page = Some(ext_page.next.clone());
                                    return true;
                                } else {
                                    // debug!("iterated to end");
                                    self.page = None;
                                    return false;
                                }
                            });
                        } else {
                            self.index += 1;
                            // debug!("Advancing cursor to index {}", self.index);
                        }
                    }
                    Ordering::Backward => {
                        if page.is_empty() {
                            let prev_node = ext_page.prev.deref::<KS, PS>().read_unchecked();
                            self.index = 0;
                            self.page = Some(ext_page.next.clone());
                            if prev_node.is_none() {
                                self.page = None;
                                return false;
                            } else if prev_node.is_empty() {
                                return self.next();
                            } else if prev_node.is_ext() {
                                return true;
                            } else {
                                unreachable!()
                            }
                        }
                        if self.index == 0 {
                            // next page
                            return read_node(&ext_page.prev, |prev_page: &NodeReadHandler<KS, PS>| {
                                if !prev_page.is_none() {
                                    debug!("Shifting page backward");
                                    self.page = Some(ext_page.prev.clone());
                                    self.index = prev_page.len() - 1;
                                    return true;
                                } else {
                                    debug!("iterated to end");
                                    self.page = None;
                                    return false;
                                }
                            });
                        } else {
                            self.index -= 1;
                            // debug!("Advancing cursor to index {}", self.index);
                        }
                    }
                }
                true
            })
        } else {
            false
        }
    }

    fn current(&self) -> Option<&EntryKey> {
        if let &Some(ref page_ref) = &self.page {
            read_node(page_ref, |handler: &NodeReadHandler<KS, PS>| {
                let page = unsafe { &*handler.ptr };
                if self.index >= page.len() {
                    panic!("cursor position overflowed {}/{}", self.index, page.len())
                }
                Some(&page.extnode().keys.as_slice_immute()[self.index])
            })
        } else {
            return None;
        }
    }
}
