use super::*;
use std::cell::RefCell;

thread_local! {
    static AFTER_EXTERNAL_BORROW: RefCell<Option<Box<dyn FnOnce()>>> = const { RefCell::new(None) };
    static AFTER_MEDIAN_COPY: RefCell<Option<Box<dyn FnOnce()>>> = const { RefCell::new(None) };
    static BEFORE_REF_INCREMENT: RefCell<Option<Box<dyn FnOnce()>>> = const { RefCell::new(None) };
}

pub(super) fn after_median_copy() {
    let action = AFTER_MEDIAN_COPY.with(|hook| hook.borrow_mut().take());
    if let Some(action) = action {
        action();
    }
}

pub(super) fn before_ref_increment() {
    let action = BEFORE_REF_INCREMENT.with(|hook| hook.borrow_mut().take());
    if let Some(action) = action {
        action();
    }
}

type Keys = [EntryKey; 5];
type Ptrs = [NodeCellRef; 6];
fn key(n: u64) -> EntryKey {
    EntryKey::from_id(&Id::from_parts(1, n))
}
fn leaf() -> BPlusTree<Keys, Ptrs> {
    BPlusTree::new(&Arc::new(DeletionSet::with_capacity(16)))
}

#[test]
fn rev1_median_insert_after_empty_copy() {
    let tree = Arc::new(leaf());
    let writer = tree.clone();
    AFTER_MEDIAN_COPY.with(|hook| {
        *hook.borrow_mut() = Some(Box::new(move || {
            std::thread::spawn(move || assert!(writer.insert(&key(10))))
                .join()
                .unwrap();
        }))
    });
    assert_eq!(tree.mid_key(), Some(key(10)));
}

#[test]
fn rev1_median_child_condemned_before_clone() {
    let make_leaf = |n| {
        let mut payload = ExtNode::<Keys, Ptrs>::new(Id::rand(), max_entry_key());
        payload.keys.insert_at(key(n), 0, &mut payload.len);
        NodeCellRef::new(Node::with_external(payload))
    };
    let child = make_leaf(10);
    let replacement = make_leaf(20);
    let mut internal = InNode::<Keys, Ptrs>::new(0, max_entry_key());
    Slice::as_slice(&mut internal.ptrs)[0] = child;
    let root = NodeCellRef::new(Node::with_internal(internal));
    let writer = root.clone();
    AFTER_MEDIAN_COPY.with(|hook| {
        *hook.borrow_mut() = Some(Box::new(move || {
            BEFORE_REF_INCREMENT.with(|hook| {
                *hook.borrow_mut() = Some(Box::new(move || {
                    std::thread::spawn(move || {
                        let mut guard = write_node::<Keys, Ptrs>(&writer);
                        Slice::as_slice(&mut guard.innode_mut().ptrs)[0] = replacement;
                    })
                    .join()
                    .unwrap();
                }))
            });
        }))
    });
    assert_eq!(
        level::select_merge_boundary::<Keys, Ptrs>(&root),
        Some(key(20))
    );
}

pub(super) struct DropProbe(pub Arc<std::sync::atomic::AtomicBool>);
impl Drop for DropProbe {
    fn drop(&mut self) {
        self.0.store(true, Release);
    }
}

pub(super) fn after_external_borrow() {
    let action = AFTER_EXTERNAL_BORROW.with(|hook| hook.borrow_mut().take());
    if let Some(action) = action {
        action();
    }
}

fn payload_lifetime(cursor_step: bool, clear: bool) {
    let tree = Arc::new(leaf());
    assert!(tree.insert(&key(10)));
    assert!(tree.insert(&key(20)));
    let root = tree.get_root();
    let dropped = Arc::new(std::sync::atomic::AtomicBool::new(false));
    write_node::<Keys, Ptrs>(&root)
        .extnode_mut_no_persist()
        .drop_probe = Some(DropProbe(dropped.clone()));
    let mut cursor = if cursor_step {
        Some(tree.seek(&key(10), Ordering::Forward))
    } else {
        None
    };
    let writer_root = root.clone();
    let writer_tree = tree.clone();
    let retained = dropped.clone();
    AFTER_EXTERNAL_BORROW.with(|hook| {
        *hook.borrow_mut() = Some(Box::new(move || {
            std::thread::spawn(move || {
                if clear {
                    writer_tree.clear();
                } else {
                    write_node::<Keys, Ptrs>(&writer_root).make_empty_node(false);
                }
            })
            .join()
            .unwrap();
            assert!(
                !retained.load(Acquire),
                "payload dropped while an optimistic reader holds its reference"
            );
        }))
    });
    if let Some(cursor) = &mut cursor {
        assert_eq!(cursor.next(), Some(key(10)));
        assert_eq!(cursor.current(), None);
    } else {
        assert_eq!(tree.seek(&key(10), Ordering::Forward).current(), None);
    }
    for _ in 0..1000 {
        let epoch = crossbeam_epoch::pin();
        epoch.flush();
        drop(epoch);
        if dropped.load(Acquire) {
            break;
        }
        std::thread::yield_now();
    }
    assert!(
        dropped.load(Acquire),
        "payload was not reclaimed after the reader left its epoch"
    );
}

#[test]
fn rev1_payload_seek_prune() {
    payload_lifetime(false, false);
}
#[test]
fn rev1_payload_seek_clear() {
    payload_lifetime(false, true);
}
#[test]
fn rev1_payload_cursor_prune() {
    payload_lifetime(true, false);
}
#[test]
fn rev1_payload_cursor_clear() {
    payload_lifetime(true, true);
}
