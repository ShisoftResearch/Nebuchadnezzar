//! Exact unit quotas. The uncontended acquire and release paths use only atomics.
//! A FIFO queue parks excess callers. Its mutex is used only on the slow path.

use futures::task::AtomicWaker;
use parking_lot::Mutex;
use std::collections::BTreeMap;
use std::future::Future;
use std::pin::Pin;
use std::sync::atomic::{AtomicBool, AtomicUsize, Ordering};
use std::sync::Arc;
use std::task::{Context, Poll};

const QUEUED: usize = 1 << (usize::BITS - 1);

pub(crate) struct Budget {
    // The high bit excludes fast operations while the queue owns permit handoff.
    state: AtomicUsize,
    limit: usize,
    queue: Mutex<Queue>,
}

#[derive(Default)]
struct Queue {
    next: u64,
    waiters: BTreeMap<u64, Arc<Waiter>>,
}

struct Waiter {
    granted: AtomicBool,
    waker: AtomicWaker,
}

impl Budget {
    pub(crate) fn new(limit: usize) -> Arc<Self> {
        assert!(limit > 0 && limit < QUEUED);
        Arc::new(Self {
            state: AtomicUsize::new(0),
            limit,
            queue: Mutex::new(Queue::default()),
        })
    }

    pub(crate) fn try_acquire(self: &Arc<Self>) -> Option<Permit> {
        let mut state = self.state.load(Ordering::Acquire);
        while state < self.limit {
            match self.state.compare_exchange_weak(
                state,
                state + 1,
                Ordering::AcqRel,
                Ordering::Acquire,
            ) {
                Ok(_) => return Some(Permit(Arc::clone(self))),
                Err(actual) => state = actual,
            }
        }
        None
    }

    pub(crate) fn acquire(self: &Arc<Self>) -> Acquire {
        Acquire {
            budget: Arc::clone(self),
            waiter: None,
        }
    }

    pub(crate) fn acquire_blocking(self: &Arc<Self>) -> Permit {
        if let Some(permit) = self.try_acquire() {
            return permit;
        }
        struct ThreadWake(std::thread::Thread);
        impl futures::task::ArcWake for ThreadWake {
            fn wake_by_ref(this: &Arc<Self>) {
                this.0.unpark();
            }
        }
        let waker = futures::task::waker(Arc::new(ThreadWake(std::thread::current())));
        let mut cx = Context::from_waker(&waker);
        let mut acquire = std::pin::pin!(self.acquire());
        loop {
            match acquire.as_mut().poll(&mut cx) {
                Poll::Ready(permit) => return permit,
                Poll::Pending => std::thread::park(),
            }
        }
    }

    // Call only with the queue locked and QUEUED set. Fast CAS operations cannot
    // then change the count. Direct handoff reserves permits before any wake.
    fn dispatch(&self, queue: &mut Queue, mut used: usize) -> Vec<Arc<Waiter>> {
        let mut wake = Vec::new();
        while used < self.limit {
            let Some((_, waiter)) = queue.waiters.pop_first() else {
                break;
            };
            used += 1;
            waiter.granted.store(true, Ordering::Release);
            wake.push(waiter);
        }
        self.state.store(
            used | if queue.waiters.is_empty() { 0 } else { QUEUED },
            Ordering::Release,
        );
        wake
    }

    fn release(&self) {
        let mut state = self.state.load(Ordering::Acquire);
        loop {
            if state & QUEUED != 0 {
                let mut queue = self.queue.lock();
                let used = self.state.fetch_or(QUEUED, Ordering::AcqRel) & !QUEUED;
                let wake = self.dispatch(&mut queue, used - 1);
                drop(queue);
                Self::wake(wake);
                return;
            }
            match self.state.compare_exchange_weak(
                state,
                state - 1,
                Ordering::AcqRel,
                Ordering::Acquire,
            ) {
                Ok(_) => return,
                Err(actual) => state = actual,
            }
        }
    }

    fn wake(waiters: Vec<Arc<Waiter>>) {
        // A user waker must never run under the queue mutex.
        for waiter in waiters {
            waiter.waker.wake();
        }
    }
}

pub(crate) struct Permit(Arc<Budget>);

impl Drop for Permit {
    fn drop(&mut self) {
        self.0.release();
    }
}

pub(crate) struct Acquire {
    budget: Arc<Budget>,
    waiter: Option<(u64, Arc<Waiter>)>,
}

impl Future for Acquire {
    type Output = Permit;

    fn poll(mut self: Pin<&mut Self>, cx: &mut Context<'_>) -> Poll<Permit> {
        if self.waiter.is_none() {
            if let Some(permit) = self.budget.try_acquire() {
                return Poll::Ready(permit);
            }
            let waiter = Arc::new(Waiter {
                granted: AtomicBool::new(false),
                waker: AtomicWaker::new(),
            });
            waiter.waker.register(cx.waker());
            let (id, wake) = {
                let mut queue = self.budget.queue.lock();
                let used = self.budget.state.fetch_or(QUEUED, Ordering::AcqRel) & !QUEUED;
                let id = queue.next;
                queue.next += 1;
                queue.waiters.insert(id, Arc::clone(&waiter));
                (id, self.budget.dispatch(&mut queue, used))
            };
            self.waiter = Some((id, waiter));
            Budget::wake(wake);
        }
        let (_, waiter) = self.waiter.as_ref().unwrap();
        waiter.waker.register(cx.waker());
        if waiter.granted.load(Ordering::Acquire) {
            self.waiter = None;
            Poll::Ready(Permit(Arc::clone(&self.budget)))
        } else {
            Poll::Pending
        }
    }
}

impl Drop for Acquire {
    fn drop(&mut self) {
        if let Some((id, _waiter)) = self.waiter.take() {
            let mut queue = self.budget.queue.lock();
            let mut used = self.budget.state.fetch_or(QUEUED, Ordering::AcqRel) & !QUEUED;
            if queue.waiters.remove(&id).is_none() {
                // Handoff won the race with cancellation. Return its reservation.
                used -= 1;
            }
            let wake = self.budget.dispatch(&mut queue, used);
            drop(queue);
            Budget::wake(wake);
        }
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use futures::task::{waker, ArcWake};

    #[test]
    fn available_budget_never_takes_the_waiter_mutex() {
        let budget = Budget::new(1);
        let _queue = budget.queue.lock();
        let permit = budget.try_acquire().unwrap();
        drop(permit);
        drop(budget.try_acquire().unwrap());
    }

    struct CountWake(AtomicUsize);
    impl ArcWake for CountWake {
        fn wake_by_ref(this: &Arc<Self>) {
            this.0.fetch_add(1, Ordering::SeqCst);
        }
    }

    #[test]
    fn parked_waiter_wakes_in_fifo_order_without_polling() {
        let budget = Budget::new(1);
        let held = budget.try_acquire().unwrap();
        let count = Arc::new(CountWake(AtomicUsize::new(0)));
        let waker = waker(Arc::clone(&count));
        let mut cx = Context::from_waker(&waker);
        let mut first = Box::pin(budget.acquire());
        let mut second = Box::pin(budget.acquire());
        assert!(first.as_mut().poll(&mut cx).is_pending());
        assert!(second.as_mut().poll(&mut cx).is_pending());
        assert_eq!(count.0.load(Ordering::SeqCst), 0);
        drop(held);
        assert_eq!(count.0.load(Ordering::SeqCst), 1);
        assert!(
            budget.try_acquire().is_none(),
            "new callers must not steal a handoff"
        );
        assert!(second.as_mut().poll(&mut cx).is_pending());
        let Poll::Ready(first) = first.as_mut().poll(&mut cx) else {
            panic!("first waiter was not granted")
        };
        drop(first);
        assert_eq!(count.0.load(Ordering::SeqCst), 2);
        let Poll::Ready(second) = second.as_mut().poll(&mut cx) else {
            panic!("second waiter was not granted")
        };
        drop(second);
        assert!(budget.try_acquire().is_some());
    }

    #[test]
    fn cancellation_before_and_after_handoff_returns_exactly_one_unit() {
        for cancel_after_release in [false, true] {
            let budget = Budget::new(1);
            let held = budget.try_acquire().unwrap();
            let mut waiter = Box::pin(budget.acquire());
            let mut cx = Context::from_waker(futures::task::noop_waker_ref());
            assert!(waiter.as_mut().poll(&mut cx).is_pending());
            if cancel_after_release {
                drop(held);
                drop(waiter);
            } else {
                drop(waiter);
                drop(held);
            }
            let _all = budget.try_acquire().expect("cancellation leaked a unit");
            assert!(
                budget.try_acquire().is_none(),
                "cancellation released twice"
            );
        }
    }

    #[test]
    fn blocking_wait_parks_inside_an_existing_executor() {
        let budget = Budget::new(1);
        let held = budget.try_acquire().unwrap();
        std::thread::scope(|scope| {
            let releasing = scope.spawn(|| {
                // The queue records the actual park boundary; no timing guess.
                while budget.state.load(Ordering::Acquire) & QUEUED == 0 {
                    std::thread::yield_now();
                }
                drop(held);
            });
            futures::executor::block_on(async {
                let _permit = budget.acquire_blocking();
            });
            releasing.join().unwrap();
        });
        assert!(budget.try_acquire().is_some());
    }

    #[test]
    fn panic_returns_the_budget() {
        let budget = Budget::new(1);
        let result = std::panic::catch_unwind(std::panic::AssertUnwindSafe(|| {
            let _permit = budget.try_acquire().unwrap();
            panic!("quota holder panicked");
        }));
        assert!(result.is_err());
        let _permit = budget.try_acquire().unwrap();
        assert!(budget.try_acquire().is_none());
    }

    #[test]
    fn concurrent_acquire_release_and_cancel_never_exceed_the_limit() {
        let budget = Budget::new(4);
        let live = AtomicUsize::new(0);
        std::thread::scope(|scope| {
            for _ in 0..16 {
                let budget = &budget;
                let live = &live;
                scope.spawn(move || {
                    for attempt in 0..500 {
                        if attempt % 3 == 0 {
                            let mut pending = Box::pin(budget.acquire());
                            let mut cx = Context::from_waker(futures::task::noop_waker_ref());
                            if let Poll::Ready(permit) = pending.as_mut().poll(&mut cx) {
                                drop(permit);
                            }
                        }
                        let _permit = budget.acquire_blocking();
                        assert!(live.fetch_add(1, Ordering::SeqCst) < 4);
                        std::thread::yield_now();
                        live.fetch_sub(1, Ordering::SeqCst);
                    }
                });
            }
        });
        assert_eq!(live.load(Ordering::SeqCst), 0);
        let all: Vec<_> = (0..4).map(|_| budget.try_acquire().unwrap()).collect();
        assert!(budget.try_acquire().is_none());
        drop(all);
    }
}
