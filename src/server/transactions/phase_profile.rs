use std::{
    array,
    sync::atomic::{AtomicU64, AtomicUsize, Ordering},
    time::Instant,
};

#[repr(usize)]
#[derive(Clone, Copy, Debug, Eq, PartialEq)]
pub enum Phase {
    ReadSiteRpc,
    AffectedObjectGrouping,
    PrepareParticipantLookup,
    PrepareBarrier,
    CommitBarrier,
    AbortParticipantLookup,
    AbortCleanup,
    EndParticipantLookup,
    EndCleanup,
    ParticipantPrepare,
    ParticipantCommit,
    ParticipantAbort,
    ParticipantEnd,
}

pub const PHASE_COUNT: usize = 13;

pub const PHASES: [Phase; PHASE_COUNT] = [
    Phase::ReadSiteRpc,
    Phase::AffectedObjectGrouping,
    Phase::PrepareParticipantLookup,
    Phase::PrepareBarrier,
    Phase::CommitBarrier,
    Phase::AbortParticipantLookup,
    Phase::AbortCleanup,
    Phase::EndParticipantLookup,
    Phase::EndCleanup,
    Phase::ParticipantPrepare,
    Phase::ParticipantCommit,
    Phase::ParticipantAbort,
    Phase::ParticipantEnd,
];

#[derive(Clone, Copy, Debug, Default, Eq, PartialEq)]
pub struct PhaseMeasurement {
    pub total_ns: u64,
    pub invocation_count: u64,
}

#[derive(Clone, Debug, Eq, PartialEq)]
pub struct Snapshot {
    pub phases: [PhaseMeasurement; PHASE_COUNT],
    pub active_guards: usize,
}

#[derive(Clone, Copy, Debug, Eq, PartialEq)]
pub struct ActiveGuards(pub usize);

struct Registry {
    totals_ns: [AtomicU64; PHASE_COUNT],
    counts: [AtomicU64; PHASE_COUNT],
    active_guards: AtomicUsize,
}

pub struct Guard {
    registry: &'static Registry,
    phase: Phase,
    started_at: Instant,
}

static REGISTRY: Registry = Registry::new();

impl Phase {
    pub fn as_str(self) -> &'static str {
        match self {
            Phase::ReadSiteRpc => "read_site_rpc",
            Phase::AffectedObjectGrouping => "affected_object_grouping",
            Phase::PrepareParticipantLookup => "prepare_participant_lookup",
            Phase::PrepareBarrier => "prepare_barrier",
            Phase::CommitBarrier => "commit_barrier",
            Phase::AbortParticipantLookup => "abort_participant_lookup",
            Phase::AbortCleanup => "abort_cleanup",
            Phase::EndParticipantLookup => "end_participant_lookup",
            Phase::EndCleanup => "end_cleanup",
            Phase::ParticipantPrepare => "participant_prepare",
            Phase::ParticipantCommit => "participant_commit",
            Phase::ParticipantAbort => "participant_abort",
            Phase::ParticipantEnd => "participant_end",
        }
    }
}

impl Registry {
    const fn new() -> Self {
        Self {
            totals_ns: [
                AtomicU64::new(0),
                AtomicU64::new(0),
                AtomicU64::new(0),
                AtomicU64::new(0),
                AtomicU64::new(0),
                AtomicU64::new(0),
                AtomicU64::new(0),
                AtomicU64::new(0),
                AtomicU64::new(0),
                AtomicU64::new(0),
                AtomicU64::new(0),
                AtomicU64::new(0),
                AtomicU64::new(0),
            ],
            counts: [
                AtomicU64::new(0),
                AtomicU64::new(0),
                AtomicU64::new(0),
                AtomicU64::new(0),
                AtomicU64::new(0),
                AtomicU64::new(0),
                AtomicU64::new(0),
                AtomicU64::new(0),
                AtomicU64::new(0),
                AtomicU64::new(0),
                AtomicU64::new(0),
                AtomicU64::new(0),
                AtomicU64::new(0),
            ],
            active_guards: AtomicUsize::new(0),
        }
    }

    fn guard(&'static self, phase: Phase) -> Guard {
        self.active_guards.fetch_add(1, Ordering::AcqRel);
        Guard {
            registry: self,
            phase,
            started_at: Instant::now(),
        }
    }

    fn record(&self, phase: Phase, elapsed_ns: u64) {
        let index = phase as usize;
        self.totals_ns[index].fetch_add(elapsed_ns, Ordering::Relaxed);
        self.counts[index].fetch_add(1, Ordering::Relaxed);
    }

    fn reset(&self) -> Result<(), ActiveGuards> {
        let active_guards = self.active_guards.load(Ordering::Acquire);
        if active_guards != 0 {
            return Err(ActiveGuards(active_guards));
        }

        for total in &self.totals_ns {
            total.store(0, Ordering::Relaxed);
        }

        for count in &self.counts {
            count.store(0, Ordering::Relaxed);
        }

        Ok(())
    }

    fn snapshot(&self) -> Snapshot {
        Snapshot {
            phases: array::from_fn(|index| PhaseMeasurement {
                total_ns: self.totals_ns[index].load(Ordering::Relaxed),
                invocation_count: self.counts[index].load(Ordering::Relaxed),
            }),
            active_guards: self.active_guards.load(Ordering::Acquire),
        }
    }
}

impl Drop for Guard {
    fn drop(&mut self) {
        let elapsed_ns = self.started_at.elapsed().as_nanos().min(u64::MAX as u128) as u64;
        self.registry.record(self.phase, elapsed_ns);
        self.registry.active_guards.fetch_sub(1, Ordering::AcqRel);
    }
}

pub fn guard(phase: Phase) -> Guard {
    REGISTRY.guard(phase)
}

pub fn reset() -> Result<(), ActiveGuards> {
    REGISTRY.reset()
}

pub fn snapshot() -> Snapshot {
    REGISTRY.snapshot()
}

#[cfg(test)]
mod tests {
    use super::*;
    use std::{thread, time::Duration};

    fn local_registry() -> &'static Registry {
        Box::leak(Box::new(Registry::new()))
    }

    #[test]
    fn phase_names_match_expected_snake_case_order() {
        let expected = [
            "read_site_rpc",
            "affected_object_grouping",
            "prepare_participant_lookup",
            "prepare_barrier",
            "commit_barrier",
            "abort_participant_lookup",
            "abort_cleanup",
            "end_participant_lookup",
            "end_cleanup",
            "participant_prepare",
            "participant_commit",
            "participant_abort",
            "participant_end",
        ];

        let actual = PHASES.map(Phase::as_str);

        assert_eq!(actual, expected);
    }

    #[test]
    fn reset_clears_recorded_measurements() {
        let registry = local_registry();

        registry.record(Phase::ReadSiteRpc, 11);
        let before = registry.snapshot();

        assert_eq!(
            before.phases[Phase::ReadSiteRpc as usize],
            PhaseMeasurement {
                total_ns: 11,
                invocation_count: 1,
            }
        );

        assert_eq!(registry.reset(), Ok(()));

        let after = registry.snapshot();

        assert_eq!(
            after.phases[Phase::ReadSiteRpc as usize],
            PhaseMeasurement::default()
        );
        assert_eq!(after.active_guards, 0);
    }

    #[test]
    fn records_multiple_manual_entries_and_raii_timing() {
        let registry = local_registry();

        registry.record(Phase::PrepareBarrier, 10);
        registry.record(Phase::PrepareBarrier, 20);

        {
            let _guard = registry.guard(Phase::ParticipantCommit);
            thread::sleep(Duration::from_millis(2));
        }

        let snapshot = registry.snapshot();

        assert_eq!(
            snapshot.phases[Phase::PrepareBarrier as usize],
            PhaseMeasurement {
                total_ns: 30,
                invocation_count: 2,
            }
        );

        let participant_commit = snapshot.phases[Phase::ParticipantCommit as usize];
        assert!(participant_commit.total_ns > 0);
        assert_eq!(participant_commit.invocation_count, 1);
        assert_eq!(snapshot.active_guards, 0);
    }

    #[test]
    fn reset_rejects_active_guards_until_they_drop() {
        let registry = local_registry();
        let guard = registry.guard(Phase::AbortCleanup);

        assert_eq!(registry.reset(), Err(ActiveGuards(1)));

        drop(guard);

        assert_eq!(registry.reset(), Ok(()));
        assert_eq!(registry.snapshot().active_guards, 0);
    }
}
