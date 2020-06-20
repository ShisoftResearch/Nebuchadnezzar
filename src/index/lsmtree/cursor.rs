use crate::index::trees::Cursor;
use crate::index::trees::EntryKey;
use crate::index::trees::Ordering;

pub struct LSMTreeCursor {
    level_cursors: Vec<Box<Cursor>>,
    ordering: Ordering,
}

impl LSMTreeCursor {
    pub fn new(cursors: Vec<Box<Cursor>>, ordering: Ordering) -> Self {
        LSMTreeCursor {
            level_cursors: cursors,
            ordering,
        }
    }
}

impl Cursor for LSMTreeCursor {
    fn next(&mut self) -> bool {
        if let Some(prev_key) = self.current().map(|k| k.to_owned()) {
            {
                let cmp = |a: &&mut Box<Cursor>, b: &&mut Box<Cursor>| {
                    a.current().unwrap().cmp(b.current().unwrap())
                };
                let pre = self
                    .level_cursors
                    .iter_mut()
                    .filter(|c| c.current().is_some());
                let pre = match self.ordering {
                    Ordering::Forward => pre.min_by(cmp),
                    Ordering::Backward => pre.max_by(cmp),
                };
                pre.map(|c| c.next());
            }
            let dedupe_current = if let Some(current) = self.current() {
                if current == &prev_key {
                    None
                } else {
                    Some(true)
                }
            } else {
                Some(false)
            };
            if let Some(has_next) = dedupe_current {
                has_next
            } else {
                self.next()
            }
        } else {
            false
        }
    }

    fn current(&self) -> Option<&EntryKey> {
        let pre = self.level_cursors.iter().filter_map(|c| c.current());
        match self.ordering {
            Ordering::Forward => pre.min(),
            Ordering::Backward => pre.max(),
        }
    }
}
