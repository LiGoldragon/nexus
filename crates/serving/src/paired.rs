//! The paired translator, pinned by its process identity.

use std::os::unix::net::UnixStream;

use nexus_exchange::lane::PeerCalling;
use shared_translator::unix::LivePeer;
use shared_translator::{Caller, PeerIdentity};

/// The one process a Nexus is paired with, held through a pidfd so a reused
/// process id is not mistaken for it.
pub struct PairedPeer {
    live: LivePeer,
}

pub trait PeerMatching: Sized {
    /// Pins the process `identity` names, which must be alive.
    fn capture(identity: PeerIdentity) -> std::io::Result<Self>;

    /// Whether the caller is the pinned process, still alive.
    fn matches(&self, caller: &Caller) -> bool;

    /// Refuses a connection whose peer, by SO_PEERCRED, is not the pinned
    /// process.
    fn require_peer(&self, stream: &UnixStream) -> std::io::Result<()>;
}

impl PeerMatching for PairedPeer {
    fn capture(identity: PeerIdentity) -> std::io::Result<Self> {
        LivePeer::capture(identity).map(|live| Self { live })
    }

    fn matches(&self, caller: &Caller) -> bool {
        self.live.matches_caller(caller)
    }

    fn require_peer(&self, stream: &UnixStream) -> std::io::Result<()> {
        let actual = stream
            .peer_caller()
            .map_err(|error| std::io::Error::other(error.to_string()))?;
        if self.matches(&actual) {
            Ok(())
        } else {
            Err(std::io::Error::new(
                std::io::ErrorKind::PermissionDenied,
                "peer is not the paired translator",
            ))
        }
    }
}
