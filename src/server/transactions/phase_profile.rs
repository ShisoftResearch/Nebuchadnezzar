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
    CommitStorageValidate,
    CommitOldCellRead,
    CommitInstall,
    EndPromotion,
}

pub const PHASE_COUNT: usize = 17;

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
    Phase::CommitStorageValidate,
    Phase::CommitOldCellRead,
    Phase::CommitInstall,
    Phase::EndPromotion,
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

#[must_use = "keep the phase profiling guard alive for the scope you want to measure"]
pub struct Guard {
    registry: &'static Registry,
    phase: Phase,
    started_at: Instant,
}

static REGISTRY: Registry = Registry::new();

impl Phase {
    pub const fn as_str(self) -> &'static str {
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
            Phase::CommitStorageValidate => "commit_storage_validate",
            Phase::CommitOldCellRead => "commit_old_cell_read",
            Phase::CommitInstall => "commit_install",
            Phase::EndPromotion => "end_promotion",
        }
    }
}

impl Registry {
    const fn new() -> Self {
        Self {
            totals_ns: [const { AtomicU64::new(0) }; PHASE_COUNT],
            counts: [const { AtomicU64::new(0) }; PHASE_COUNT],
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

#[must_use = "bind the returned guard to keep timing active until the measured scope ends"]
pub fn guard(phase: Phase) -> Guard {
    REGISTRY.guard(phase)
}

/// Resets accumulated phase measurements for a new benchmark interval.
///
/// Intended for quiescent, serialized benchmark boundaries only. This is not a
/// synchronization barrier or seqlock; callers must ensure no concurrent guard
/// creation or drop is racing with the reset, and must not treat it as making
/// other threads observe a globally ordered cutoff.
///
/// The reset rejects guards that are already active when it checks
/// `active_guards`, but it cannot prevent a new guard from starting
/// concurrently after that check. Callers must provide that external
/// quiescence.
pub fn reset() -> Result<(), ActiveGuards> {
    REGISTRY.reset()
}

/// Returns the current accumulated measurements and active guard count.
///
/// Intended for quiescent, serialized benchmark boundaries only. This is not a
/// synchronization barrier or seqlock; callers must ensure no concurrent guard
/// creation or drop is racing with the snapshot if they want to treat the
/// returned values as final for an interval.
pub fn snapshot() -> Snapshot {
    REGISTRY.snapshot()
}

#[cfg(test)]
mod tests {
    use super::*;
    use std::{thread, time::Duration};

    const PARTICIPANT_END_NAME_IS_CONST: &str = Phase::ParticipantEnd.as_str();

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
            "commit_storage_validate",
            "commit_old_cell_read",
            "commit_install",
            "end_promotion",
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

    #[test]
    fn concurrent_guard_drops_record_every_invocation() {
        const THREADS: usize = 4;
        const GUARDS_PER_THREAD: usize = 128;

        let registry = local_registry();

        thread::scope(|scope| {
            for _ in 0..THREADS {
                scope.spawn(|| {
                    for _ in 0..GUARDS_PER_THREAD {
                        let _guard = registry.guard(Phase::ParticipantAbort);
                    }
                });
            }
        });

        let snapshot = registry.snapshot();

        assert_eq!(PARTICIPANT_END_NAME_IS_CONST, "participant_end");
        assert_eq!(
            snapshot.phases[Phase::ParticipantAbort as usize].invocation_count,
            (THREADS * GUARDS_PER_THREAD) as u64
        );
        assert_eq!(snapshot.active_guards, 0);
    }
}
