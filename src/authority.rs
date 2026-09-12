//! The access one Nexus socket grants, and the peers it answers.
//!
//! Every Nexus opens at least two sockets: the ordinary socket, for any
//! authenticated peer, and the meta socket — the Nexus's root, through which
//! configuration and privileged operations alone pass. The meta socket is
//! therefore not merely a second filename. It is bound for the owning user
//! alone, and a connection on it is answered only when the kernel says the
//! peer is that user.
//!
//! Both halves of that rule are here because both are universal: a Nexus that
//! bound its privileged socket the way it binds its ordinary one would have no
//! privileged surface, and getting it wrong is a defect rather than a design
//! choice. What a refused peer is *told* is not universal — that is a value of
//! the contract the socket bears — so it is not here.

/// The access one socket grants.
#[derive(Clone, Copy, Debug, Eq, PartialEq)]
pub enum SocketAuthority {
    /// Readable and writable by the owning user and group.
    Ordinary,
    /// Readable and writable by the owning user alone, and answered only for
    /// that user.
    Privileged,
}

/// An authority states the file mode that expresses it, and which peers it
/// admits once the mode has let them connect.
pub trait Permissive {
    fn mode(&self) -> u32;
    fn admits(&self, peer_user: u32, owner: u32) -> bool;
}

impl Permissive for SocketAuthority {
    fn mode(&self) -> u32 {
        match self {
            Self::Ordinary => 0o660,
            Self::Privileged => 0o600,
        }
    }

    fn admits(&self, peer_user: u32, owner: u32) -> bool {
        match self {
            Self::Ordinary => true,
            Self::Privileged => peer_user == owner,
        }
    }
}
