//! Universal ontology and lifecycle types for Nexus components.
//!
//! Component repositories own their domain configuration and effects. This
//! crate owns the standard state and transitions shared by every Nexus.

#![forbid(unsafe_code)]

mod configuration;

pub use configuration::{Configurable, ConfigurationState, ConfigurationTransitionError};
