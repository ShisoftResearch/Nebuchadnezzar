//! Quiescent-state based reclamation for segment memory.
//!
//! Reclaiming a segment is destructive three times over: its pages are
//! dropped (`MADV_DONTNEED`, so they read back as zeros), its backup and WAL
//! are deleted, and its address returns to the allocator's free list to be
//! handed to the next segment. A reader still inside therefore does not merely
//! read stale data -- it reads zeros, and then, once the address is recycled,
//! another segment's live bytes at the same address.
//!
//! Unpublishing a segment from `Chunk::segs` stops new readers finding it, but
//! says nothing about the readers already inside. QSBR closes that gap without
//! putting an atomic on the read path: a thread announces that it holds nothing
//! (a *quiescent state*), and anything unpublished before every thread has
//! passed through such a state is safe to reclaim.
//!
//! The read path costs a thread-local counter, plus one relaxed load and one
//! release store to the thread's own cache line when the outermost section is
//! entered and left. Nothing shared is contended.
//!
//! Note what this protects that a reference count cannot: the cell index holds
//! raw addresses into segment memory, so there is a window between resolving an
//! address and taking a reference on its segment. A per-segment count says
//! nothing about a thread in that window; "no thread is inside any segment
//! critical section" does.

use std::cell::Cell;
use std::sync::atomic::{AtomicUsize, Ordering};
use std::sync::OnceLock;

use lightning::thread_local::ThreadLocal;

/// Per-thread quiescence state.
struct ThreadState {
    /// `0` means quiescent: this thread is not inside any segment critical
    /// section and cannot hold a pointer into segment memory. Otherwise it is
    /// the epoch at which the thread entered its outermost section.
    entry_epoch: AtomicUsize,
    /// Nesting depth. Only ever touched by its own thread.
    depth: Cell<usize>,
}

pub struct SegmentQsbr {
    /// Bumped once per retirement, so a retirement can be ordered against the
    /// sections that were already running when it happened.
    epoch: AtomicUsize,
    threads: ThreadLocal<ThreadState>,
}

impl SegmentQsbr {
    fn new() -> Self {
        Self {
            // Starts at 1: 0 is reserved to mean "quiescent".
            epoch: AtomicUsize::new(1),
            threads: ThreadLocal::new(),
        }
    }

    #[inline(always)]
    fn state(&self) -> &mut ThreadState {
        self.threads.get_or(|| ThreadState {
            entry_epoch: AtomicUsize::new(0),
            depth: Cell::new(0),
        })
    }

    /// Enter a segment critical section.
    #[inline(always)]
    pub fn enter(&self) {
        let state = self.state();
        let depth = state.depth.get();
        if depth == 0 {
            // Publishing a stale epoch here is safe: it can only make a
            // retirement wait longer than strictly necessary, never let one
            // through early.
            state
                .entry_epoch
                .store(self.epoch.load(Ordering::Acquire), Ordering::Release);
        }
        state.depth.set(depth + 1);
    }

    /// Leave a segment critical section, announcing quiescence at depth zero.
    #[inline(always)]
    pub fn exit(&self) {
        let state = self.state();
        let depth = state.depth.get();
        if depth <= 1 {
            state.depth.set(0);
            state.entry_epoch.store(0, Ordering::Release);
        } else {
            state.depth.set(depth - 1);
        }
    }

    /// Stamp a segment that has just been unpublished.
    ///
    /// Must be called *after* the segment is removed from `Chunk::segs`, so
    /// that any thread whose entry epoch is at least this stamp provably could
    /// not have found it.
    /// Run `f` with this thread's section suspended, then restore it.
    ///
    /// **A thread cannot reclaim past its own section.** `is_quiesced` asks
    /// whether every thread has left the sections it was in when a segment
    /// was retired; a thread that is *inside* one is, by that definition, a
    /// reader the retirement must wait for -- including when the thing it
    /// is doing inside the section is the collection itself. Measured on a
    /// 13.6M-edge import: six writers, each holding a `CellGuard` (which
    /// carries a section) while running the emergency collection from the
    /// allocator, reported `blocking_threads=6 references=0` on 61 retired
    /// segments, 171 full passes freed nothing, and the import fell from
    /// 423k to 11k edges/s.
    ///
    /// Suspending is sound only when the caller holds no bare pointer into a
    /// segment that the collection could free. The allocator's emergency
    /// path meets that: it runs after the guard has pinned its own segment
    /// by reference (`CellGuard::segment`), and a pinned segment is never
    /// freed (`no_references` gates it). The section's job -- covering the
    /// lookup-to-pin window -- is already done by then.
    ///
    /// Nesting is restored exactly, so a suspended outer section resumes as
    /// the outer section.
    pub fn suspended<R>(&self, f: impl FnOnce() -> R) -> R {
        let _out = self.suspend();
        f()
    }

    /// Suspend this thread's section until the returned guard drops.
    ///
    /// The guard form exists for a function like the writer's allocation
    /// loop, which waits in several places (a head-queue spin, the GC lock,
    /// the collection itself) and returns from the middle of them; one guard
    /// at entry covers every wait, where a closure would have to wrap each.
    pub fn suspend(&self) -> SuspendedSection<'_> {
        let state = self.state();
        let depth = state.depth.get();
        if depth != 0 {
            state.entry_epoch.store(0, Ordering::Release);
            state.depth.set(0);
        }
        SuspendedSection { qsbr: self, depth }
    }

    pub fn retire_stamp(&self) -> usize {
        self.epoch.fetch_add(1, Ordering::AcqRel) + 1
    }

    /// Whether every thread has left the sections that were running when
    /// `stamp` was taken.
    pub fn is_quiesced(&self, stamp: usize) -> bool {
        self.threads.iter_threads().all(|state| {
            let entry = state.entry_epoch.load(Ordering::Acquire);
            entry == 0 || entry >= stamp
        })
    }

    /// Threads currently inside a section that began before `stamp`, for
    /// reporting a retirement that will not drain.
    pub fn blocking_threads(&self, stamp: usize) -> usize {
        self.threads
            .iter_threads()
            .filter(|state| {
                let entry = state.entry_epoch.load(Ordering::Acquire);
                entry != 0 && entry < stamp
            })
            .count()
    }
}

pub fn segment_qsbr() -> &'static SegmentQsbr {
    static QSBR: OnceLock<SegmentQsbr> = OnceLock::new();
    QSBR.get_or_init(SegmentQsbr::new)
}

/// RAII form, for call sites that are not already paired with the segment
/// reference counter.
pub struct QsbrSection;

impl QsbrSection {
    #[inline(always)]
    pub fn new() -> Self {
        segment_qsbr().enter();
        Self
    }
}

/// A suspended section; dropping it resumes the section at its depth.
pub struct SuspendedSection<'a> {
    qsbr: &'a SegmentQsbr,
    depth: usize,
}

impl Drop for SuspendedSection<'_> {
    fn drop(&mut self) {
        if self.depth == 0 {
            return;
        }
        let state = self.qsbr.state();
        // Re-enter at the CURRENT epoch, not the one we left at: anything
        // retired while we were out was retired against a quiescent thread
        // and owes us nothing, and claiming the older epoch would make us
        // block it for no reason.
        state
            .entry_epoch
            .store(self.qsbr.epoch.load(Ordering::Acquire), Ordering::Release);
        state.depth.set(self.depth);
    }
}

impl Drop for QsbrSection {
    #[inline(always)]
    fn drop(&mut self) {
        segment_qsbr().exit();
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn a_retirement_waits_for_a_section_that_began_before_it() {
        let qsbr = SegmentQsbr::new();

        qsbr.enter();
        let stamp = qsbr.retire_stamp();
        assert!(
            !qsbr.is_quiesced(stamp),
            "a thread inside a section that began before the retirement must hold it back"
        );
        assert_eq!(qsbr.blocking_threads(stamp), 1);

        qsbr.exit();
        assert!(
            qsbr.is_quiesced(stamp),
            "leaving the section announces quiescence"
        );
        assert_eq!(qsbr.blocking_threads(stamp), 0);
    }

    #[test]
    fn a_section_entered_after_the_retirement_does_not_hold_it_back() {
        let qsbr = SegmentQsbr::new();

        let stamp = qsbr.retire_stamp();
        qsbr.enter();
        assert!(
            qsbr.is_quiesced(stamp),
            "a section entered after the unpublish cannot have found the segment"
        );
        qsbr.exit();
    }

    #[test]
    fn a_suspended_section_is_quiescent_and_comes_back_at_its_depth() {
        let qsbr = SegmentQsbr::new();
        qsbr.enter();
        qsbr.enter(); // depth 2
        let stamp = qsbr.retire_stamp();
        assert!(!qsbr.is_quiesced(stamp), "inside a section we block a retirement");
        assert_eq!(qsbr.blocking_threads(stamp), 1);
        let seen = qsbr.suspended(|| qsbr.is_quiesced(stamp));
        assert!(seen, "while suspended the thread must not block its own collection");
        // back at depth 2: two exits are needed before we are quiescent
        let later = qsbr.retire_stamp();
        assert!(!qsbr.is_quiesced(later));
        qsbr.exit();
        assert!(!qsbr.is_quiesced(later), "one exit from depth 2 is not the outermost");
        qsbr.exit();
        assert!(qsbr.is_quiesced(later));
    }

    #[test]
    fn a_suspend_guard_resumes_on_every_exit_path() {
        let qsbr = SegmentQsbr::new();
        qsbr.enter();
        let stamp = qsbr.retire_stamp();
        let early = || -> bool {
            let _g = qsbr.suspend();
            if qsbr.is_quiesced(stamp) {
                return true; // early return: the guard must still resume us
            }
            false
        };
        assert!(early());
        let later = qsbr.retire_stamp();
        assert!(!qsbr.is_quiesced(later), "resumed at depth 1 after the early return");
        qsbr.exit();
        assert!(qsbr.is_quiesced(later));
    }

    #[test]
    fn nesting_only_announces_at_the_outermost_exit() {
        let qsbr = SegmentQsbr::new();

        qsbr.enter();
        qsbr.enter();
        let stamp = qsbr.retire_stamp();
        qsbr.exit();
        assert!(
            !qsbr.is_quiesced(stamp),
            "the inner exit must not announce quiescence while the outer section runs"
        );
        qsbr.exit();
        assert!(qsbr.is_quiesced(stamp));
    }
}
