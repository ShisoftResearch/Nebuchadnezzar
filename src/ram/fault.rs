//! Armed I/O faults for the corruption soak.
//!
//! The soak harness arms a site with a count. The next `count` calls to
//! `fire` at that site return `EIO` in place of the real I/O. Without the
//! `fault-injection` feature, `fire` is an empty function and every site
//! compiles to the plain I/O call.

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
pub use armed::{arm, fire, fired};

#[cfg(not(feature = "fault-injection"))]
#[inline(always)]
pub fn fire(_site: Site) -> io::Result<()> {
    Ok(())
}

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
