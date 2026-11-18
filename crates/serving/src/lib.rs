//! The serving every Nexus daemon shares.
//!
//! A Nexus binds its meta socket first and its public socket once it is
//! configured, both named after it in its place's runtime directory. Its meta
//! socket answers only its paired translator, one blocking exchange at a
//! time; its public socket admits the paired translator and the peers the
//! Nexus has bound, and serves each connection on the runtime, every exchange
//! on a task of its own, a subscription's frames until its stream ends. The
//! Nexus sends each external effect to its translator on the emission lane,
//! and a failure of that lane is the outcome's own variant, apart from what
//! the world answered. A Nexus starts from the one [`NexusBootstrap`] frame
//! its pair launcher writes: its place, its admission, its paired
//! translator's identity and the identifier of this run.
//!
//! What a Nexus writes is its own bodies: [`MetaAnswering`] answers a meta
//! query, [`PublicServing`] admits a public query and answers it once or with
//! a stream, and [`PeerAdmitting`] names who besides the translator may
//! connect. The kinds declared in `ethos/serving.ethos` are generated into
//! `src/generated/serving.rs` by ethos-zero; the flake's check refuses a
//! committed generation that differs from what the source generates.

#![forbid(unsafe_code)]

mod asynchronous;
mod bootstrap;
mod closing;
mod emission;
#[rustfmt::skip]
#[path = "generated/serving.rs"]
mod generated;
mod listener;
mod listeners;
mod meta;
mod paired;
mod public;

pub use asynchronous::{AsyncFrameReading, AsyncValueWriting};
pub use bootstrap::BootstrapReading;
pub use closing::{Closable, SubscriptionClosing};
pub use emission::{EmissionLane, Emitting};
pub use generated::{Closing, Continuation, InstanceId, MetaAnswering, NexusBootstrap};
pub use listener::{OwnedListener, SocketBinding};
pub use listeners::{AcceptedPublic, Listeners, Listening, PeerAdmitting};
pub use meta::MetaServing;
pub use nexus_exchange::{LaneFailing, LaneFailure, LaneFault, SocketPaths};
pub use paired::{PairedPeer, PeerMatching};
pub use public::{ConnectionServing, PublicConnection, PublicServing, Reply, TransportBounded};
