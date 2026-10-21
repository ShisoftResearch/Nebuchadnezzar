//! Phase distributions for the abort probe. Enable only for profile runs.
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

static SAMPLES: [crossbeam::queue::SegQueue<u64>; 8] =
    [const { crossbeam::queue::SegQueue::new() }; 8];

pub struct Guard(Phase, Instant);

pub fn guard(phase: Phase) -> Guard {
    Guard(phase, Instant::now())
}

pub fn record(phase: Phase, nanos: u64) {
    SAMPLES[phase as usize].push(nanos);
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
        .map(|phase| {
            (
                phase,
                (0..SAMPLES[phase as usize].len())
                    .filter_map(|_| SAMPLES[phase as usize].pop())
                    .collect(),
            )
        })
        .collect()
}

#[cfg(test)]
mod tests {
    use super::*;
    #[test]
    fn guard_records_a_sample() {
        let before = SAMPLES[Phase::Rollback as usize].len();
        drop(guard(Phase::Rollback));
        assert!(SAMPLES[Phase::Rollback as usize].len() > before);
    }
}

#[cfg(test)]
mod concurrent_tests {
    use super::*;

    #[test]
    fn concurrent_profile_producers_retain_every_sample() {
        while SAMPLES[Phase::Padding as usize].pop().is_some() {}
        std::thread::scope(|scope| {
            for producer in 0..8 {
                scope.spawn(move || {
                    for sample in 0..1000 {
                        record(Phase::Padding, producer * 1000 + sample);
                    }
                });
            }
        });
        let mut samples = take()
            .into_iter()
            .find(|(phase, _)| *phase as usize == Phase::Padding as usize)
            .unwrap()
            .1;
        samples.sort_unstable();
        assert_eq!(samples, (0..8000).collect::<Vec<_>>());
    }
}
