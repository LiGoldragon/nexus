//! Universal ontology and lifecycle types for Nexus components.
//!
//! Component repositories own their domain configuration and effects. This
//! crate owns the standard state and transitions shared by every Nexus.

#![forbid(unsafe_code)]

mod authority;
mod configuration;
mod situation;

pub use authority::{Permissive, SocketAuthority};
pub use configuration::{Configurable, ConfigurationState, ConfigurationTransitionError};
pub use situation::{Situated, Situating, Situation};
