//! Where a Nexus actually ran.
//!
//! A Nexus's persisted configuration is desired state: where it intends to
//! listen, and the only thing a meta Configure changes. This is the other
//! half — observed state, written by the Nexus itself once both sockets are
//! bound, and never read back as configuration. It records the absolute path
//! of the store file that was actually opened, the socket paths that were
//! actually bound, and the process and boot that bound them.
//!
//! The reason it exists is that the store is portable and its configuration
//! is not. A copy of a populated store, opened somewhere else, would bind the
//! socket paths it carries — which belong to the Nexus still running where
//! the original lives. The record is what lets the copy recognise itself.
//!
//! It is universal: every Nexus binds sockets it must not steal, and every
//! Nexus's store can be copied.

/// Where a Nexus process was actually situated: written at bind, never read
/// as configuration.
#[derive(rkyv::Archive, rkyv::Serialize, rkyv::Deserialize, Clone, Debug, PartialEq, Eq)]
pub struct Situation {
    pub store_path: String,
    pub bound_socket_vector: Vec<String>,
    pub process_id: i64,
    pub boot_identity: String,
    pub host_identity: String,
}

/// The record a Nexus writes once both its sockets are bound.
pub trait Situating: Sized {
    fn situated(store_path: String, bound_socket_vector: Vec<String>) -> Self;
}

impl Situating for Situation {
    /// The process, boot and host are read from the running system rather
    /// than passed in: they are facts about where this call is happening, and
    /// a caller that could supply them could supply the wrong ones.
    fn situated(store_path: String, bound_socket_vector: Vec<String>) -> Self {
        Self {
            store_path,
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
/// than guessed at or refused: neither is compared by [`Situated::is_carried`],
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

/// A recorded situation answers where its Nexus was, and whether the store
/// now being opened is the one it was written into.
pub trait Situated {
    fn store_path(&self) -> &str;
    fn bound_socket_vector(&self) -> &[String];
    fn process_id(&self) -> i64;
    fn boot_identity(&self) -> &str;
    fn host_identity(&self) -> &str;

    /// A store opened anywhere other than where its own record says it lives
    /// is a copy that has been carried, and the socket paths it holds belong
    /// to the Nexus still running where the original is.
    fn is_carried(&self, opened_store_path: &str) -> bool;
}

impl Situated for Situation {
    fn store_path(&self) -> &str {
        &self.store_path
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

    fn is_carried(&self, opened_store_path: &str) -> bool {
        self.store_path != opened_store_path
    }
}
