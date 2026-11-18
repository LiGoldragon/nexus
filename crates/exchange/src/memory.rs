//! Where a Nexus keeps its Memory in a place: `<name>.memory` in the place's
//! state directory.
//!
//! The path is hand-written, as the socket paths are: it is generic over the
//! Nexus it names, and a declared Ethos type archives no path.

use std::path::PathBuf;

use crate::generated::{MemoryNaming, Named};
use shared_translator::placement::Place;

/// The place a Nexus keeps its Memory in.
pub trait MemoryPlacing {
    /// The Memory file of the Nexus `Component`, named after it, in the
    /// place's state directory.
    fn memory_path<Component: Named>(&self) -> PathBuf;
}

impl MemoryPlacing for Place {
    fn memory_path<Component: Named>(&self) -> PathBuf {
        shared_translator::placement::state_path(self).join(Component::name().memory_file().0)
    }
}
