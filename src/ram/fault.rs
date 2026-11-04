//! Armed faults for the corruption soak.
//!
//! I/O faults: the soak harness arms a site with a count. The next `count`
//! calls to `fire` at that site return `EIO` in place of the real I/O.
//!
//! Crash points: the harness arms a point with a number of passes to skip.
//! The pass after those kills the process with SIGKILL, so a kill lands at a
//! known step of the protocol (soak class F2).
//!
//! Without the `fault-injection` feature, `fire` and `crash_point` are empty
//! functions and every site compiles to the plain code.

use std::io;

#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub enum Site {
    /// The `write_all` of one framed WAL record.
    WalWrite,
    /// Every WAL `fsync`: inline, explicit and from the group-sync pool.
    WalSync,
    /// The `fsync` of an archived segment file, before its rename.
    Archive,
}

impl Site {
    pub fn parse(name: &str) -> Option<Self> {
        match name {
            "wal_write" => Some(Site::WalWrite),
            "wal_sync" => Some(Site::WalSync),
            "archive" => Some(Site::Archive),
            _ => None,
        }
    }
}

/// A step of the storage protocol where the harness can kill the process.
#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub enum Point {
    /// A transaction's entries are written, before the prepare sync.
    PrepareEntries,
    /// The prepare sync returned.
    PrepareSynced,
    /// One entry of a transaction is written, the others not yet.
    ApplyMid,
    /// The decision's COMMIT records are written, before their sync.
    CommitPlaced,
    /// An abort restored one cell, the others not yet.
    RollbackMid,
    /// A cleaner combine archived every destination and published none.
    CombineStaged,
    /// A segment archive is synced at its temporary path, before the rename.
    ArchiveSynced,
}

impl Point {
    pub const ALL: [Point; 7] = [
        Point::PrepareEntries,
        Point::PrepareSynced,
        Point::ApplyMid,
        Point::CommitPlaced,
        Point::RollbackMid,
        Point::CombineStaged,
        Point::ArchiveSynced,
    ];

    pub fn name(self) -> &'static str {
        match self {
            Point::PrepareEntries => "prepare_entries",
            Point::PrepareSynced => "prepare_synced",
            Point::ApplyMid => "apply_mid",
            Point::CommitPlaced => "commit_placed",
            Point::RollbackMid => "rollback_mid",
            Point::CombineStaged => "combine_staged",
            Point::ArchiveSynced => "archive_synced",
        }
    }

    pub fn parse(name: &str) -> Option<Self> {
        Self::ALL.into_iter().find(|p| p.name() == name)
    }
}

#[cfg(feature = "fault-injection")]
mod armed {
    use super::Site;
    use std::io;
    use std::sync::atomic::{AtomicU64, Ordering::Relaxed};

    static ARMED: [AtomicU64; 3] = [AtomicU64::new(0), AtomicU64::new(0), AtomicU64::new(0)];
    static FIRED: [AtomicU64; 3] = [AtomicU64::new(0), AtomicU64::new(0), AtomicU64::new(0)];

    pub fn arm(site: Site, count: u64) {
        ARMED[site as usize].store(count, Relaxed);
    }

    pub fn fired(site: Site) -> u64 {
        FIRED[site as usize].load(Relaxed)
    }

    const UNARMED: u64 = u64::MAX;
    static CRASH_AFTER: [AtomicU64; 7] = [const { AtomicU64::new(UNARMED) }; 7];

    /// Kill the process at the pass of `point` that follows `skip` passes.
    pub fn arm_crash(point: super::Point, skip: u64) {
        CRASH_AFTER[point as usize].store(skip, Relaxed);
    }

    pub fn crash_point(point: super::Point) {
        let slot = &CRASH_AFTER[point as usize];
        let mut left = slot.load(Relaxed);
        while left != UNARMED {
            let next = if left == 0 { UNARMED } else { left - 1 };
            match slot.compare_exchange_weak(left, next, Relaxed, Relaxed) {
                Ok(_) if left == 0 => unsafe {
                    libc::kill(libc::getpid(), libc::SIGKILL);
                },
                Ok(_) => return,
                Err(current) => left = current,
            }
        }
    }

    pub fn fire(site: Site) -> io::Result<()> {
        let slot = &ARMED[site as usize];
        let mut left = slot.load(Relaxed);
        while left > 0 {
            match slot.compare_exchange_weak(left, left - 1, Relaxed, Relaxed) {
                Ok(_) => {
                    FIRED[site as usize].fetch_add(1, Relaxed);
                    return Err(io::Error::from_raw_os_error(libc::EIO));
                }
                Err(current) => left = current,
            }
        }
        Ok(())
    }
}

#[cfg(feature = "fault-injection")]
pub use armed::{arm, arm_crash, crash_point, fire, fired};

#[cfg(not(feature = "fault-injection"))]
#[inline(always)]
pub fn fire(_site: Site) -> io::Result<()> {
    Ok(())
}

#[cfg(not(feature = "fault-injection"))]
#[inline(always)]
pub fn crash_point(_point: Point) {}

#[cfg(all(test, feature = "fault-injection"))]
mod tests {
    use super::*;

    #[test]
    fn an_armed_site_fails_its_count_then_passes() {
        arm(Site::Archive, 2);
        let before = fired(Site::Archive);
        assert_eq!(
            fire(Site::Archive).unwrap_err().raw_os_error(),
            Some(libc::EIO)
        );
        assert!(fire(Site::Archive).is_err());
        assert!(fire(Site::Archive).is_ok());
        assert_eq!(fired(Site::Archive) - before, 2);
    }
}
