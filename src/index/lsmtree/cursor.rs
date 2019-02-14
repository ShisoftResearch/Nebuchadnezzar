use index::Cursor;
use index::EntryKey;

pub struct LSMTreeCursor {
    level_cursors: Vec<Box<Cursor>>,
}

impl LSMTreeCursor {
    pub fn new(cursors: Vec<Box<Cursor>>) -> Self {
        LSMTreeCursor {
            level_cursors: cursors,
        }
    }
}

impl Cursor for LSMTreeCursor {
    fn next(&mut self) -> bool {
        if let Some(prev_key) = self.current().map(|k| k.to_owned()) {
            self.level_cursors
                .iter_mut()
                .filter(|c| c.current().is_some())
                .min_by(|a, b| a.current().unwrap().cmp(b.current().unwrap()))
                .map(|c| c.next());
            let dedupe_current = if let Some(current) = self.current() {
                if current <= &prev_key {
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
        self.level_cursors.iter().filter_map(|c| c.current()).min()
    }
}
