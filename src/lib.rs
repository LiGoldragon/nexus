//! Universal ontology and lifecycle types for Nexus components.
//!
//! Component repositories own their domain configuration and effects. This
//! crate owns the standard state and transitions shared by every Nexus.

#![forbid(unsafe_code)]

mod authority;
mod configuration;
mod relocation;
mod situation;

pub use authority::{Permissive, SocketAuthority};
pub use configuration::{Configurable, ConfigurationState, ConfigurationTransitionError};
pub use relocation::{Relocated, Relocating, Relocation};
pub use situation::{Bearing, Identifying, Situated, Situating, Situation, StoreIdentity};
