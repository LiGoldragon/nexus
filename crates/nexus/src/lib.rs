//! Universal ontology and lifecycle types for Nexus components.
//!
//! Component repositories own their domain configuration and effects. This
//! crate owns the standard state and transitions shared by every Nexus.
//!
//! The kinds declared in `ethos/library.ethos` are generated into
//! `src/generated/library.rs` by ethos-zero; the build script refuses a
//! committed generation that differs from what the source generates.

#![forbid(unsafe_code)]

mod authority;
mod configuration;
#[rustfmt::skip]
#[path = "generated/library.rs"]
mod generated;
mod publisher;
mod relocation;
mod situation;

pub use authority::{Permissive, SocketAuthority};
pub use configuration::{Configurable, ConfigurationState, ConfigurationTransitionError};
pub use generated::{Capacity, CapacityRefusal, Publishing, Reached, Subscribable, Unobserved};
pub use publisher::Publisher;
pub use relocation::{Relocated, Relocating, Relocation};
pub use situation::{Bearing, Identifying, Situated, Situating, Situation, StoreIdentity};
mod sending;
pub use sending::{
    EmissionSender, Sending, SignalReceiving, SignalSubmitting, TransmissionError, WireValue,
};
