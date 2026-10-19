use super::*;
use std::cell::RefCell;

thread_local! {
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

