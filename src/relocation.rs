//! The owner's declaration that a store was moved rather than copied.
//!
//! [`crate::Situation`] records where a Nexus was bound, and a store opened
//! anywhere else is refused, because the socket paths it carries belong to
//! whichever Nexus is still running where the original lives. That refusal is
//! right and, on its own, it is a dead end: a store the owner deliberately
//! carried somewhere else is refused for the same reason a copy is, and the
//! record that would have to change lives inside the store, reachable only
//! through a Nexus that will not start.
//!
//! The way out is not a looser guard. When a store is found somewhere other
//! than where it records itself, there are exactly two worlds: the store was
//! duplicated, and the original is still a claimant to those sockets; or the
//! store was carried, and is the same single claimant at a new address. The
//! store holds nothing that tells them apart, because what tells them apart
//! is a fact about the world outside it — whether a second claimant exists —
//! and a fact about intent, which only the owner has.
//!
//! So a relocation is a declaration, and this is its type: one store, named
//! origin, named destination. It is honoured by exactly the move it names.
//! A copy taken afterwards sits at neither end of it; a declaration lifted
//! into some other store names a move that store never made; and a
//! declaration that admitted any destination would be a standing licence to
//! run a store from anywhere, which is the guard given up rather than
//! recovered from.
//!
//! Where the declaration lives, and who may write it, are the component's to
//! decide — but the shape of the question is universal, because every Nexus
//! binds sockets it must not steal and every Nexus's store can be carried.
//! What is universal, and is here, is that the declaration must be inside the
//! store. A declaration beside the store is copied along with it by every
//! means that produces a dangerous copy — `cp -r`, `rsync`, `tar`, a snapshot
//! of the directory — and so would admit the copies it exists to refuse.
//! Inside the store it is copied too, and is harmless there, because it names
//! a destination the copy is not at.

/// One store's declared move: the address it was bound at, and the address it
/// has been carried to.
#[derive(rkyv::Archive, rkyv::Serialize, rkyv::Deserialize, Clone, Debug, PartialEq, Eq)]
pub struct Relocation {
    pub origin: String,
    pub destination: String,
}

/// A relocation is declared by naming both ends of the move.
pub trait Relocating: Sized {
    fn declared(origin: String, destination: String) -> Self;
}

impl Relocating for Relocation {
    fn declared(origin: String, destination: String) -> Self {
        Self {
            origin,
            destination,
        }
    }
}

/// A declared relocation names its two addresses, and says whether a
/// particular open is the move it declared.
pub trait Relocated {
    fn origin(&self) -> &str;
    fn destination(&self) -> &str;

    /// Whether this declaration honours the open now being refused: the store
    /// records having been bound at `recorded_store_path` and has been opened
    /// at `opened_store_path`.
    fn admits(&self, recorded_store_path: &str, opened_store_path: &str) -> bool;
}

impl Relocated for Relocation {
    fn origin(&self) -> &str {
        &self.origin
    }

    fn destination(&self) -> &str {
        &self.destination
    }

    /// Both ends must match, and they must differ from each other.
    ///
    /// Both ends, because a declaration that checked only the destination
    /// would be honoured by any store carried to that path, and one that
    /// checked only the origin would be honoured by every copy of the store
    /// it was written into. Differing from each other, because the only open
    /// a same-address declaration could ever be consulted for is one that was
    /// never refused: a store opened where it records itself is not carried,
    /// so such a declaration can only be a forgery aimed at some other open,
    /// and refusing it costs a legitimate owner nothing.
    fn admits(&self, recorded_store_path: &str, opened_store_path: &str) -> bool {
        self.origin != self.destination
            && self.origin == recorded_store_path
            && self.destination == opened_store_path
    }
}
