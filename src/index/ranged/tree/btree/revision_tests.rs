use super::*;
use std::cell::RefCell;

thread_local! {
    static AFTER_EXTERNAL_BORROW: RefCell<Option<Box<dyn FnOnce()>>> = const { RefCell::new(None) };
    static AFTER_INTERNAL_BORROW: RefCell<Option<Box<dyn FnOnce()>>> = const { RefCell::new(None) };
    static LATCH_WAIT: RefCell<Option<Box<dyn FnOnce()>>> = const { RefCell::new(None) };
    static RETIREMENT_STATS: RefCell<Option<Arc<RetirementStats>>> = const { RefCell::new(None) };
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

#[test]
fn rev1_sustained_writer_releases_reader() {
    let tree = leaf();
    assert!(tree.insert(&key(10)));
    let root = tree.get_root();
    let writer_root = root.clone();
    let (request, requests) = std::sync::mpsc::channel();
    let (started, starts) = std::sync::mpsc::channel();
    let (ack, acknowledgements) = std::sync::mpsc::channel();
    let writer = std::thread::spawn(move || {
        let mut writes = 0;
        while requests.recv().is_ok() {
            started.send(()).unwrap();
            {
                let mut guard = write_node::<Keys, Ptrs>(&writer_root);
                writes += 1;
                let node = guard.extnode_mut_no_persist();
                node.keys.set_from(&[key(10 + writes)], &mut node.len);
            }
            if ack.send(()).is_err() {
                break;
            }
        }
        writes
    });
    let mut attempts = 0;
    let result = std::panic::catch_unwind(std::panic::AssertUnwindSafe(|| {
        read_node(&root, |node: &NodeReadHandler<Keys, Ptrs>| {
            attempts += 1;
            assert!(
                attempts <= 4096,
                "reader exceeded the control budget under sustained writes"
            );
            let captured = node.extnode().keys.key_at(0);
            let latched = is_node_locked::<Keys, Ptrs>(&root);
            request.send(()).unwrap();
            starts.recv().unwrap();
            if !latched {
                acknowledgements.recv().unwrap();
            }
            captured
        })
    }));
    if result.is_ok() {
        acknowledgements.recv().unwrap();
    }
    drop(request);
    let writes = writer.join().unwrap();
    assert!(
        result.is_ok(),
        "reader did not return during sustained version changes"
    );
    assert_eq!(result.unwrap(), key(10 + attempts - 1));
    assert_eq!(writes, attempts);
    assert_eq!(attempts, 9);
}

pub(super) fn after_internal_borrow<KS, PS>(slot: &Box<InNode<KS, PS>>)
where
    KS: Slice<EntryKey> + Debug + 'static,
    PS: Slice<NodeCellRef> + 'static,
{
    let action = AFTER_INTERNAL_BORROW.with(|hook| hook.borrow_mut().take());
    if let Some(action) = action {
        // Read the box pointer bits. Do not dereference a possibly replaced payload.
        let address = || unsafe { std::ptr::read(slot as *const _ as *const usize) };
        let before = address();
        action();
        assert_eq!(
            address(),
            before,
            "internal borrow still references the live Box slot"
        );
    }
}

pub(super) fn latch_wait() {
    let action = LATCH_WAIT.with(|hook| hook.borrow_mut().take());
    if let Some(action) = action {
        action();
    }
}

fn internal_replacement(mutation_search: bool) {
    let leaf = Arc::new(leaf());
    assert!(leaf.insert(&key(55)));
    let mut internal = InNode::<Keys, Ptrs>::new(1, max_entry_key());
    internal.keys = InternalKeys::from_keys(&[key(30)]);
    Slice::as_slice(&mut internal.ptrs)[1] = leaf.get_root();
    internal.right = leaf.get_root();
    let root = NodeCellRef::new(Node::with_internal(internal));
    let writer_root = root.clone();
    AFTER_INTERNAL_BORROW.with(|hook| {
        *hook.borrow_mut() = Some(Box::new(move || {
            std::thread::spawn(move || {
                write_node::<Keys, Ptrs>(&writer_root).make_empty_node(false)
            })
            .join()
            .unwrap();
        }))
    });
    if mutation_search {
        assert!(matches!(
            search::mut_search::<Keys, Ptrs>(&root, &key(55)),
            search::MutSearchResult::External
        ));
    } else {
        let deletion = Arc::new(DeletionSet::with_capacity(0));
        let cursor =
            search::search_node::<Keys, Ptrs>(&root, &key(55), Ordering::Forward, &deletion, true);
        assert_eq!(cursor.current(), Some(&key(55)));
    }
}

#[test]
fn rev2_internal_replacement_during_seek() {
    internal_replacement(false);
}
#[test]
fn rev2_internal_replacement_during_mutation_search() {
    internal_replacement(true);
}

fn deleted_gap() -> BPlusTree<[EntryKey; 128], [NodeCellRef; 129]> {
    let deletion = Arc::new(DeletionSet::with_capacity(0));
    let tree = BPlusTree::new(&deletion);
    let count = 131456;
    for n in 1..=count {
        assert!(tree.insert(&key(n)));
    }
    for n in 2..count {
        assert!(deletion.insert(key(n)));
    }
    tree
}

#[test]
fn rev2_reviewer_tombstone_heavy_scan_without_writers() {
    let tree = deleted_gap();
    let mut cursor = tree.seek(&key(1), Ordering::Forward);
    assert_eq!(cursor.next(), Some(key(1)));
    assert_eq!(cursor.next(), Some(key(131456)));
    assert_eq!(cursor.next(), None);
}

#[test]
fn rev2_reviewer_cursor_advance_keeps_its_anchor_after_retry() {
    let tree = deleted_gap();
    let mut cursor = tree.seek(&key(1), Ordering::Forward);
    let page = cursor.page.as_ref().unwrap().clone();
    AFTER_EXTERNAL_BORROW.with(|hook| {
        *hook.borrow_mut() = Some(Box::new(move || {
            std::thread::spawn(move || {
                drop(write_node::<[EntryKey; 128], [NodeCellRef; 129]>(&page))
            })
            .join()
            .unwrap();
        }))
    });
    assert_eq!(cursor.next(), Some(key(1)));
    assert_eq!(cursor.next_id(), Some(key(131456).id()));
    assert_eq!(cursor.next_id(), None);
}

#[test]
fn rev2_reviewer_held_neighbor_latch_preserves_serialized_continuation() {
    use crate::ram::types::Value;
    type LargeKeys = [EntryKey; 128];
    type LargePtrs = [NodeCellRef; 129];
    let tree = Arc::new(BPlusTree::<LargeKeys, LargePtrs>::new(&Arc::new(
        DeletionSet::with_capacity(0),
    )));
    for n in 1..=129 {
        assert!(tree.insert(&key(n)));
    }
    let root = tree.get_root();
    let first = read_node(&root, |n: &NodeReadHandler<LargeKeys, LargePtrs>| {
        n.ptrs()[0].try_clone_speculative().unwrap()
    });
    let right = read_node(&first, |n: &NodeReadHandler<LargeKeys, LargePtrs>| {
        n.right_ref().unwrap().try_clone_speculative().unwrap()
    });
    let expected = read_node(&right, |n: &NodeReadHandler<LargeKeys, LargePtrs>| {
        n.ext_id()
    });
    let right_latch = write_node::<LargeKeys, LargePtrs>(&right);
    let (waiting, wait) = std::sync::mpsc::channel();
    let (result, received) = std::sync::mpsc::channel();
    let worker_tree = tree.clone();
    let worker = std::thread::spawn(move || {
        LATCH_WAIT
            .with(|hook| *hook.borrow_mut() = Some(Box::new(move || waiting.send(()).unwrap())));
        let first_latch = write_node::<LargeKeys, LargePtrs>(&first);
        let cell = first_latch.extnode().to_cell(&worker_tree.deletion);
        result
            .send(cell.data[*NEXT_PAGE_KEY_HASH].id().copied().unwrap())
            .unwrap();
    });
    wait.recv().unwrap();
    assert!(matches!(
        received.try_recv(),
        Err(std::sync::mpsc::TryRecvError::Empty)
    ));
    drop(right_latch);
    assert_eq!(received.recv().unwrap(), expected);
    worker.join().unwrap();
}

#[derive(Default)]
struct RetirementStats {
    pending: AtomicUsize,
    peak: AtomicUsize,
}

pub(super) struct RetirementToken(Arc<RetirementStats>);
impl Drop for RetirementToken {
    fn drop(&mut self) {
        self.0.pending.fetch_sub(1, AcqRel);
    }
}

pub(super) fn track_retirement() -> Option<RetirementToken> {
    RETIREMENT_STATS.with(|slot| {
        slot.borrow().as_ref().map(|stats| {
            let count = stats.pending.fetch_add(1, AcqRel) + 1;
            stats.peak.fetch_max(count, AcqRel);
            RetirementToken(stats.clone())
        })
    })
}

#[test]
fn rev2_stalled_pin_bounds_retirement_for_a_fixed_node_set() {
    let trees: Vec<_> = (0..64)
        .map(|_| {
            let t = leaf();
            assert!(t.insert(&key(10)));
            t
        })
        .collect();
    let (ready, started) = std::sync::mpsc::channel();
    let (release, resume) = std::sync::mpsc::channel();
    let stalled = std::thread::spawn(move || {
        let epoch = crossbeam_epoch::pin();
        ready.send(()).unwrap();
        resume.recv().unwrap();
        drop(epoch);
    });
    started.recv().unwrap();
    let stats = Arc::new(RetirementStats::default());
    RETIREMENT_STATS.with(|slot| *slot.borrow_mut() = Some(stats.clone()));
    for tree in &trees {
        let root = tree.get_root();
        write_node::<Keys, Ptrs>(&root).make_empty_node(false);
        for _ in 0..8 {
            clear::clear_by_node::<Keys, Ptrs>(&root);
            write_node::<Keys, Ptrs>(&root).make_empty_node(false);
        }
    }
    assert_eq!(stats.pending.load(Acquire), 64);
    assert_eq!(stats.peak.load(Acquire), 64);
    RETIREMENT_STATS.with(|slot| *slot.borrow_mut() = None);
    release.send(()).unwrap();
    stalled.join().unwrap();
    for _ in 0..1000 {
        let epoch = crossbeam_epoch::pin();
        epoch.flush();
        drop(epoch);
        if stats.pending.load(Acquire) == 0 {
            break;
        }
        std::thread::yield_now();
    }
    assert_eq!(stats.pending.load(Acquire), 0);
}
