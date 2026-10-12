//! Kernel observation of boot-local process identities.
//!
//! This module does not assign addresses, skip wrappers, or authorize actions.
//! Persisted bindings must separately record and validate their boot identity.

mod procfs;

use crate::{Ancestry, Identifiable, Process, Unidentified};
use procfs::{Collecting, Procfs, ReadingProcesses};
use std::os::unix::net::UnixStream;

/// Captures the calling CLI itself, before it observes its ancestry.
pub trait Capturable: Sized {
    fn current() -> Result<Self, Unidentified>;
}

impl Capturable for Process {
    fn current() -> Result<Self, Unidentified> {
        let source = Procfs;
        Ok(source.read(i64::from(std::process::id()))?.process)
    }
}

impl Identifiable for Process {
    fn identify(&self) -> Result<Ancestry, Unidentified> {
        Procfs.collect(self)
    }
}

/// Checks a message's ancestry against the kernel-observed socket peer.
///
/// This is an observation, not a lifetime guarantee or an authorization.
/// Consumers must not cache it across later requests or across boots.
pub trait Witnessing {
    fn witness(&self, claimed: &Ancestry) -> Result<Ancestry, Unidentified>;
}

impl Witnessing for UnixStream {
    fn witness(&self, claimed: &Ancestry) -> Result<Ancestry, Unidentified> {
        let credentials =
            rustix::net::sockopt::socket_peercred(self).map_err(|_| Unidentified::PeerMismatch)?;
        let pid = i64::from(credentials.pid.as_raw_nonzero().get());
        Procfs.witness(pid, claimed)
    }
}

#[cfg(test)]
mod tests;
