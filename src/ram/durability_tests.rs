use super::durability::*;
use super::segs::{PauseRewindSync, Segment, SEGMENT_SIZE};
use crate::ram::chunk::Chunks;
use crate::ram::schema::LocalSchemasCache;
use crate::server::ServerMeta;
use std::sync::atomic::Ordering::Relaxed;
use std::sync::Arc;

fn store(dir: &tempfile::TempDir) -> Arc<Chunks> {
    Chunks::new(
        1,
        4 * SEGMENT_SIZE,
        Arc::new(ServerMeta {
            schemas: LocalSchemasCache::new_local(""),
        }),
        None,
        Some(dir.path().join("backup").to_string_lossy().into_owned()),
        Some(dir.path().join("wal").to_string_lossy().into_owned()),
        None,
    )
}

fn segment(chunks: &Chunks) -> lightning::aarc::Arc<Segment> {
    let chunk = &chunks.list[0];
    let seg = chunk.allocator.alloc_seg(&chunk.file_manager).unwrap();
    let id = seg.id;
    chunk.put_segment(seg);
    chunk.segs.get(&(id as usize)).unwrap()
}

fn append(seg: &Segment, i: usize, transactional: bool) -> Receipt {
    let addr = seg.addr + i * 8;
    unsafe {
        *(addr as *mut u64) = 0;
    }
    let (result, mut receipts) = capture(|| seg.write_wal(addr, 8, transactional));
    result.unwrap();
    receipts.pop().unwrap()
}

#[tokio::test]
async fn durable_ack_covers_true_frame_offsets_including_transactions() {
    let dir = tempfile::tempdir().unwrap();
    let chunks = store(&dir);
    let seg = segment(&chunks);
    let _pause = PauseRewindSync::new(&seg);
    let first = append(&seg, 0, true);
    let second = append(&seg, 1, false);
    let frame = crate::ram::wal_format::RECORD_HEADER_SIZE as u64 + 8;
    assert_eq!(
        second.offset,
        crate::ram::wal_format::FILE_HEADER_SIZE as u64 + 2 * frame
    );
    assert!(second.file.watermark() < first.offset);
    wait(vec![second.clone()]).await.unwrap();
    assert!(second.file.watermark() >= second.offset);
}

#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn durable_ack_concurrent_writers_share_one_segment_sync() {
    let dir = tempfile::tempdir().unwrap();
    let chunks = store(&dir);
    let seg = segment(&chunks);
    let _pause = PauseRewindSync::new(&seg);
    let ready = Arc::new(tokio::sync::Barrier::new(32));
    let before = GROUP_SYNCS.load(Relaxed);
    let mut writers = Vec::new();
    for i in 0..32 {
        let (seg, ready) = (seg.clone(), ready.clone());
        writers.push(tokio::spawn(async move {
            let receipt = append(&seg, i, false);
            ready.wait().await;
            wait(vec![receipt.clone()]).await.unwrap();
            assert!(receipt.file.watermark() >= receipt.offset);
        }));
    }
    for writer in writers {
        writer.await.unwrap();
    }
    let syncs = GROUP_SYNCS.load(Relaxed) - before;
    assert!(
        syncs > 0 && syncs < 32,
        "32 acknowledgements used {syncs} syncs"
    );
}

#[tokio::test]
async fn durable_ack_sync_failure_fails_batch_and_later_waits() {
    let dir = tempfile::tempdir().unwrap();
    let chunks = store(&dir);
    let seg = segment(&chunks);
    let _pause = PauseRewindSync::new(&seg);
    let receipts: Vec<_> = (0..32).map(|i| append(&seg, i, false)).collect();
    seg.durable_file().unwrap().inject_failure();
    let results = futures::future::join_all(receipts.into_iter().map(|r| wait(vec![r]))).await;
    for result in results {
        assert!(matches!(
            result,
            Err(crate::ram::cell::WriteError::Durability(
                DurabilityError::Sync { .. }
            ))
        ));
    }
    assert!(matches!(
        wait(vec![append_receipt(&seg)]).await,
        Err(crate::ram::cell::WriteError::Durability(_))
    ));
    assert!(seg.write_wal(seg.addr + 32 * 8, 8, false).is_err());
    assert_eq!(
        seg.durable_file().unwrap().watermark(),
        crate::ram::wal_format::FILE_HEADER_SIZE as u64
    );
    assert_eq!(chunks.durability_failures().len(), 1);
}

#[tokio::test]
async fn durable_ack_rejects_missing_wal() {
    let file = DurableFile::new(0, 0, 1);
    assert!(matches!(
        wait(vec![Receipt {
            file,
            offset: 0,
            epoch: 0,
            descriptor: None
        }])
        .await,
        Err(crate::ram::cell::WriteError::Durability(
            DurabilityError::WalMissing { .. }
        ))
    ));
}

#[tokio::test]
async fn durable_ack_append_during_sync_requires_next_sync() {
    let dir = tempfile::tempdir().unwrap();
    let chunks = store(&dir);
    let seg = segment(&chunks);
    let _pause = PauseRewindSync::new(&seg);
    let first = append(&seg, 0, false);
    let gate = Arc::new(std::sync::Barrier::new(2));
    first.file.set_gate(gate.clone());
    let mut pending = Box::pin(wait(vec![first.clone()]));
    assert!(futures::poll!(&mut pending).is_pending());
    // Wait until the worker captured the first end before appending again.
    // The gate is consumed only after the snapshot.
    while first.file.test_gate_pending() {
        tokio::task::yield_now().await;
    }
    let second = append(&seg, 1, false);
    gate.wait();
    pending.await.unwrap();
    assert_eq!(first.file.watermark(), first.offset);
    wait(vec![second.clone()]).await.unwrap();
    assert!(second.file.watermark() >= second.offset);
}

#[tokio::test]
async fn durable_ack_receipt_keeps_descriptor_only_until_completion() {
    let dir = tempfile::tempdir().unwrap();
    let chunks = store(&dir);
    let seg = segment(&chunks);
    let receipt = append(&seg, 0, false);
    let descriptor = Arc::downgrade(receipt.descriptor.as_ref().unwrap());
    // Model the descriptor close at archive while a receipt is outstanding.
    seg.file_state.lock().wal.take();
    assert!(descriptor.upgrade().is_some());
    wait(vec![receipt.clone()]).await.unwrap();
    assert!(receipt.file.watermark() >= receipt.offset);
    drop(receipt);
    tokio::time::timeout(std::time::Duration::from_secs(1), async {
        while descriptor.upgrade().is_some() {
            tokio::task::yield_now().await;
        }
    })
    .await
    .expect("completed receipts must not retain an archived WAL descriptor");
}

fn append_receipt(seg: &Segment) -> Receipt {
    let state = seg.file_state.lock();
    Receipt {
        file: state.durability.clone().unwrap(),
        offset: state.durability.as_ref().unwrap().end(),
        epoch: state.durability.as_ref().unwrap().snapshot().1,
        descriptor: state.wal.clone(),
    }
}

#[tokio::test]
async fn durable_ack_batch_captures_and_waits_once_per_touched_segment() {
    let dir = tempfile::tempdir().unwrap();
    let chunks = store(&dir);
    let segments = [segment(&chunks), segment(&chunks)];
    let pauses: Vec<_> = segments.iter().map(|s| PauseRewindSync::new(s)).collect();
    let before = WAIT_CALLS.load(Relaxed);
    let capture_before = BATCH_RECEIPTS_TAKEN.load(Relaxed);
    let results = batch(async {
        let mut results = Vec::new();
        for i in 0..1024 {
            let seg = &segments[i % 2];
            let (result, receipts) = capture(|| seg.write_wal(seg.addr, 8, false));
            assert!(receipts.is_empty());
            results.push(result.map_err(crate::ram::cell::WriteError::from_wal_io));
        }
        results
    })
    .await;
    assert!(results.iter().all(Result::is_ok));
    assert_eq!(WAIT_CALLS.load(Relaxed) - before, 2);
    assert_eq!(BATCH_RECEIPTS_TAKEN.load(Relaxed) - capture_before, 2);
    for seg in segments {
        let file = seg.durable_file().unwrap();
        assert_eq!(file.watermark(), file.end());
    }
    drop(pauses);
}

#[tokio::test]
async fn durable_ack_pending_prefixes_share_one_sync() {
    let dir = tempfile::tempdir().unwrap();
    let chunks = store(&dir);
    let seg = segment(&chunks);
    let _pause = PauseRewindSync::new(&seg);
    let receipts: Vec<_> = (0..8).map(|i| append(&seg, i, false)).collect();
    let file = receipts[0].file.clone();
    let gate = Arc::new(std::sync::Barrier::new(2));
    file.set_capture_gate(gate.clone());
    let before = GROUP_SYNCS.load(Relaxed);
    let mut waits: Vec<_> = receipts
        .into_iter()
        .map(|r| Box::pin(wait(vec![r])))
        .collect();
    for pending in &mut waits {
        assert!(futures::poll!(pending).is_pending());
    }
    assert_eq!(file.pending_waiters(), 8);
    gate.wait();
    for result in futures::future::join_all(waits).await {
        result.unwrap();
    }
    assert_eq!(GROUP_SYNCS.load(Relaxed) - before, 1);
}

#[test]
fn durable_ack_default_append_has_no_receipt_state() {
    let dir = tempfile::tempdir().unwrap();
    let chunks = store(&dir);
    let seg = segment(&chunks);
    seg.write_wal(seg.addr, 8, false).unwrap();
    assert!(seg.durable_file().is_none());
    assert!(seg.durability_failure().is_none());
    seg.force_wal_sync().unwrap();
    assert!(seg.durable_file().is_none());
    let (result, receipts) = capture(|| seg.write_wal(seg.addr + 8, 8, false));
    result.unwrap();
    let before = receipts[0].file.end();
    seg.write_wal(seg.addr + 16, 8, false).unwrap();
    assert_eq!(
        receipts[0].file.end(),
        before,
        "default appends must not update receipt state"
    );
}

#[tokio::test]
async fn durable_ack_rewind_refuses_stale_receipts_after_offsets_are_reused() {
    let dir = tempfile::tempdir().unwrap();
    let chunks = store(&dir);
    let seg = segment(&chunks);
    let _pause = PauseRewindSync::new(&seg);
    let first = append(&seg, 0, true);
    let gate = Arc::new(std::sync::Barrier::new(2));
    first.file.set_gate(gate.clone());
    let mut pending = Box::pin(wait(vec![first.clone()]));
    assert!(futures::poll!(&mut pending).is_pending());
    tokio::time::timeout(std::time::Duration::from_secs(1), async {
        while first.file.test_gate_pending() {
            tokio::task::yield_now().await;
        }
    })
    .await
    .unwrap();
    seg.truncate_wal_to(crate::ram::wal_format::FILE_HEADER_SIZE as u64)
        .unwrap();
    let second = append(&seg, 0, true);
    assert_eq!(first.offset, second.offset);
    gate.wait();
    let result = tokio::time::timeout(std::time::Duration::from_secs(1), pending)
        .await
        .unwrap();
    assert!(
        result.is_err(),
        "a rewound receipt must not certify replacement bytes"
    );
    wait(vec![second.clone()]).await.unwrap();
    assert!(
        wait(vec![first]).await.is_err(),
        "a stale receipt must fail even when its offset is covered again"
    );
    assert!(second.file.watermark() >= second.offset);
    assert!(
        seg.durability_failure().is_none(),
        "a rewind is not a sync failure"
    );
}

#[tokio::test]
async fn durable_ack_rewind_keeps_receipts_before_the_retained_length() {
    let dir = tempfile::tempdir().unwrap();
    let chunks = store(&dir);
    let seg = segment(&chunks);
    let _pause = PauseRewindSync::new(&seg);
    let kept = append(&seg, 0, true);
    let waiting = append(&seg, 1, true);
    let retained = seg.wal_append_position();
    let retracted = append(&seg, 2, true);
    let gate = Arc::new(std::sync::Barrier::new(2));
    kept.file.set_gate(gate.clone());
    let mut pending_waiting = Box::pin(wait(vec![waiting.clone()]));
    let mut pending_retracted = Box::pin(wait(vec![retracted.clone()]));
    assert!(futures::poll!(&mut pending_waiting).is_pending());
    assert!(futures::poll!(&mut pending_retracted).is_pending());
    tokio::time::timeout(std::time::Duration::from_secs(1), async {
        while kept.file.test_gate_pending() {
            tokio::task::yield_now().await;
        }
    })
    .await
    .unwrap();
    // The protection of an old version takes a token of the COMMIT receipt.
    let protection = crate::ram::cleaner::protection::Protection::default();
    let txn = crate::server::transactions::test_hlc(1, 1);
    protection.pin_until_covered(&txn, Arc::new(vec![kept.token()]));
    seg.truncate_wal_to(retained).unwrap();
    gate.wait();
    let timeout = std::time::Duration::from_secs(1);
    tokio::time::timeout(timeout, pending_waiting)
        .await
        .unwrap()
        .expect("a receipt before the retained length must stay valid");
    assert!(
        tokio::time::timeout(timeout, pending_retracted)
            .await
            .unwrap()
            .is_err(),
        "a receipt after the retained length must fail"
    );
    wait(vec![kept.clone()])
        .await
        .expect("a later wait for a retained receipt must succeed");
    assert!(kept.file.covers_receipt(kept.offset, kept.epoch));
    assert!(
        !protection.transaction_pending(),
        "a synced, retained COMMIT receipt must release the pin of the old version"
    );
}
