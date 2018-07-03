use std::time::SystemTime;
use std::sync::atomic::{AtomicU32, Ordering};
use std::sync::Arc;
use std::thread::{spawn, sleep};
use std::time::Duration;

// A not so accurate but fast wall clock for cell timestamp
lazy_static! {
    static ref WALL_CLOCK: Arc<AtomicU32> = {
        let atomic = Arc::new(AtomicU32::new(actual_now()));
        let atomic_clone = atomic.clone();
        spawn(move || {
            atomic_clone.store(actual_now(), Ordering::Relaxed);
            sleep(Duration::from_secs(1));
        });
        return atomic;
    };
}

fn actual_now() -> u32 {
    SystemTime::now().duration_since(SystemTime::UNIX_EPOCH).unwrap().as_secs() as u32
}

pub fn now() -> u32 {
    WALL_CLOCK.load(Ordering::Relaxed)
}