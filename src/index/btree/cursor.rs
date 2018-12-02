use super::*;

// This is the runtime cursor on iteration
// It hold a copy of the containing page next page lock guard
// These lock guards are preventing the node and their neighbourhoods been changed externally
// Ordering are specified that can also change lock pattern
pub struct RTCursor {
    pub index: usize,
    pub ordering: Ordering,
    pub page: Option<NodeCellRef>,
}

impl RTCursor {
    pub fn new(pos: usize, page: &NodeCellRef, ordering: Ordering) -> RTCursor {
        let mut cursor = RTCursor {
            index: pos,
            ordering,
            page: Some(page.clone()),
        };
        match ordering {
            Ordering::Forward => {
                let len = page.read(|node| node.len());
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

impl IndexCursor for RTCursor {
    fn next(&mut self) -> bool {
        if self.page.is_some() {
            let current_page = self.page.clone().unwrap();
            current_page.read(|page| {
                let ext_page = page.extnode();
                // debug!("Next id with index: {}, length: {}", self.index + 1, ext_page.len);
                match self.ordering {
                    Ordering::Forward => {
                        if self.index + 1 >= page.len() {
                            // next page
                            return ext_page.next.read(|next_page| {
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
                        if self.index == 0 {
                            // next page
                            return ext_page.prev.read(|prev_page| {
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
            page_ref.read(|handler| {
                let page = unsafe { &*handler.ptr };
                if self.index >= page.len() {
                    panic!("cursor position overflowed {}/{}", self.index, page.len())
                }
                Some(&page.extnode().keys[self.index])
            })
        } else {
            return None;
        }
    }
}