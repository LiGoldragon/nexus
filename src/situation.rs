//! Where a Nexus actually ran, and what it makes of finding itself elsewhere.
//!
//! A Nexus's persisted configuration is desired state: where it intends to
//! listen, and the only thing a meta Configure changes. This is the other
//! half — observed state, written by the Nexus itself once both sockets are
//! bound, and never read back as configuration. It records the store file
//! that was actually opened, the socket paths that were actually bound, and
//! the process, boot and host that bound them.
//!
//! The reason it exists is that the store is portable and its configuration
//! is not. A copy of a populated store, opened somewhere else, would bind the
//! socket paths it carries — which belong to the Nexus still running where
//! the original lives. The record is what lets the copy recognise itself.
//!
//! What it records the store *as* is the whole difficulty. A path is an
//! address; a file is an identity. Record only the address and a move is
//! indistinguishable from a copy, because both change the address — so a
//! store its owner deliberately carried is refused for the same reason a
//! duplicate is, and the record that would have to change lives inside the
//! store, reachable only through a Nexus that will not start. Record the file
//! as the filesystem names it, and the three cases come apart: the same file
//! at the address it records, the same file at a new address, and a different
//! file bearing its record. Only the last is a copy, and only the last needs
//! anybody to say anything. See [`Bearing`].
//!
//! It is universal: every Nexus binds sockets it must not steal, and every
//! Nexus's store can be copied.

use std::path::Path;

/// A store file as the filesystem names it, together with the address it was
/// reached by.
///
/// The device and inode are the identity; the path is where it answered. Both
/// are recorded because both are needed: a reader wants the address, and the
/// guard wants the identity.
#[derive(rkyv::Archive, rkyv::Serialize, rkyv::Deserialize, Clone, Debug, PartialEq, Eq)]
pub struct StoreIdentity {
    pub path: String,
    pub device: u64,
    pub inode: u64,
}

/// A path is asked what file is at it, and the answer says whether two paths
/// lead to one file.
pub trait Identifying: Sized {
    fn of(store_path: &Path) -> Self;
    fn path(&self) -> &str;
    fn device(&self) -> u64;
    fn inode(&self) -> u64;

    /// Whether the filesystem named a file at this address.
    fn is_named(&self) -> bool;

    /// Whether these two addresses lead to one file.
    fn is_same_file(&self, other: &Self) -> bool;
}

impl Identifying for StoreIdentity {
    /// A path with no file at it keeps its address and gets no identity,
    /// rather than refusing: the callers that matter ask about a store the
    /// engine has already opened, and the one that does not — an operator's
    /// tool asking about an address — wants to be told there is nothing there
    /// in its own words.
    fn of(store_path: &Path) -> Self {
        let named = std::fs::metadata(store_path).ok().map(|stat| {
            use std::os::unix::fs::MetadataExt;
            (stat.dev(), stat.ino())
        });
        let (device, inode) = named.unwrap_or((0, 0));
        Self {
            path: store_path.display().to_string(),
            device,
            inode,
        }
    }

    fn path(&self) -> &str {
        &self.path
    }

    fn device(&self) -> u64 {
        self.device
    }

    fn inode(&self) -> u64 {
        self.inode
    }

    fn is_named(&self) -> bool {
        self.inode != 0
    }

    /// Two unnamed identities are never the same file: an absence is not
    /// something two addresses can share, and treating it as one would let
    /// every unreadable identity match every other.
    fn is_same_file(&self, other: &Self) -> bool {
        self.is_named()
            && other.is_named()
            && self.device == other.device
            && self.inode == other.inode
    }
}

/// What a store's own record makes of the store now being opened.
#[derive(Clone, Copy, Debug, Eq, PartialEq)]
pub enum Bearing {
    /// The same address the record names. Nothing has happened, whatever file
    /// is there now.
    Settled,
    /// The same file, at an address the record does not name. It was moved.
    Moved,
    /// A different file, at an address the record does not name. It is a copy
    /// — or a move that did not preserve the file, which no record can tell
    /// from a copy.
    Carried,
}

/// A recorded situation answers where its Nexus was, and what to make of
/// where its store is now.
pub trait Situated {
    fn store(&self) -> &StoreIdentity;
    fn bound_socket_vector(&self) -> &[String];
    fn process_id(&self) -> i64;
    fn boot_identity(&self) -> &str;
    fn host_identity(&self) -> &str;

    fn bearing(&self, opened: &StoreIdentity) -> Bearing;
}

/// The record a Nexus writes once its sockets are bound.
pub trait Situating: Sized {
    fn situated(store: StoreIdentity, bound_socket_vector: Vec<String>) -> Self;
}

/// Where a Nexus process was actually situated: written at bind, never read
/// as configuration.
#[derive(rkyv::Archive, rkyv::Serialize, rkyv::Deserialize, Clone, Debug, PartialEq, Eq)]
pub struct Situation {
    pub store: StoreIdentity,
    pub bound_socket_vector: Vec<String>,
    pub process_id: i64,
    pub boot_identity: String,
    pub host_identity: String,
}

impl Situating for Situation {
    /// The process, boot and host are read from the running system rather
    /// than passed in: they are facts about where this call is happening, and
    /// a caller that could supply them could supply the wrong ones. The store
    /// identity is passed in, because the caller is the one holding the store
    /// it actually opened and re-deriving it here would ask the filesystem a
    /// second time about a file that may have changed in between.
    fn situated(store: StoreIdentity, bound_socket_vector: Vec<String>) -> Self {
        Self {
            store,
            bound_socket_vector,
            process_id: i64::from(std::process::id()),
            boot_identity: Self::read_kernel_identity("/proc/sys/kernel/random/boot_id"),
            host_identity: Self::read_kernel_identity("/proc/sys/kernel/hostname"),
        }
    }
}

/// The two system identities a situation carries, read where Linux publishes
/// them.
///
/// An identity that cannot be read is recorded as the empty string rather
/// than guessed at or refused: neither is compared by [`Situated::bearing`],
/// so an unreadable one costs a reader some context and costs the guard
/// nothing. A kernel without these files would otherwise make every Nexus
/// unstartable for the sake of a field nothing decides on.
trait ReadsKernelIdentity {
    fn read_kernel_identity(path: &str) -> String;
}

impl ReadsKernelIdentity for Situation {
    fn read_kernel_identity(path: &str) -> String {
        std::fs::read_to_string(path)
            .map(|identity| identity.trim().to_owned())
            .unwrap_or_default()
    }
}

impl Situated for Situation {
    fn store(&self) -> &StoreIdentity {
        &self.store
    }

    fn bound_socket_vector(&self) -> &[String] {
        &self.bound_socket_vector
    }

    fn process_id(&self) -> i64 {
        self.process_id
    }

    fn boot_identity(&self) -> &str {
        &self.boot_identity
    }

    fn host_identity(&self) -> &str {
        &self.host_identity
    }

    /// The address decides first, and the file decides second.
    ///
    /// The address first, because the danger this record guards against is
    /// two stores answering at one set of socket paths, and a store at the
    /// address its own record names is the one those paths belong to — even
    /// if the file there is a different one, as it is after a restore in
    /// place. There is no second claimant for such a store to be mistaken
    /// for.
    ///
    /// Then the file, because at any other address the question is whether
    /// this is the one store under a new name or a second copy of it. One
    /// file cannot be two claimants, so the same file at a new address is a
    /// move, and the owner is free to make it without telling anyone. A
    /// different file is a copy as far as anything here can tell — including
    /// when it is a move that crossed a filesystem, which arrives as new
    /// bytes in a new file exactly as a duplication does.
    fn bearing(&self, opened: &StoreIdentity) -> Bearing {
        if self.store.path() == opened.path() {
            Bearing::Settled
        } else if self.store.is_same_file(opened) {
            Bearing::Moved
        } else {
            Bearing::Carried
        }
    }
}
