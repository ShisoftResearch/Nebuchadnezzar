//! Phase distributions for the abort probe. Enable only for profile runs.
use parking_lot::Mutex;
use std::time::Instant;

#[derive(Clone, Copy, Debug)]
#[repr(usize)]
pub enum Phase {
    Rollback,
    Rewind,
    Drain,
    Padding,
    QueueSync,
    WalWait,
    WalHold,
    ForceSync,
}

pub const PHASES: [Phase; 8] = [
    Phase::Rollback,
    Phase::Rewind,
    Phase::Drain,
    Phase::Padding,
    Phase::QueueSync,
    Phase::WalWait,
    Phase::WalHold,
    Phase::ForceSync,
];

static SAMPLES: [Mutex<Vec<u64>>; 8] = [const { Mutex::new(Vec::new()) }; 8];

pub struct Guard(Phase, Instant);

pub fn guard(phase: Phase) -> Guard {
    Guard(phase, Instant::now())
}

pub fn record(phase: Phase, nanos: u64) {
    SAMPLES[phase as usize].lock().push(nanos);
}

impl Drop for Guard {
    fn drop(&mut self) {
        record(self.0, self.1.elapsed().as_nanos() as u64);
    }
}

/// Call at a quiescent probe boundary. The samples include background syncs.
pub fn take() -> Vec<(Phase, Vec<u64>)> {
    PHASES
        .into_iter()
        .map(|phase| (phase, std::mem::take(&mut *SAMPLES[phase as usize].lock())))
        .collect()
}

#[cfg(test)]
mod tests {
    use super::*;
    #[test]
    fn guard_records_a_sample() {
        let before = SAMPLES[Phase::Rollback as usize].lock().len();
        drop(guard(Phase::Rollback));
        assert!(SAMPLES[Phase::Rollback as usize].lock().len() > before);
    }
}
