//! The exchange layer both sides of a Nexus socket speak.
//!
//! Every Nexus serves an ordinary and a meta socket on `signal`'s exchange
//! layer, and its translator speaks to it and to its clients in lane frames.
//! What both sides share is here: the socket names a Nexus's name gives and
//! their paths in a place, the bootstrap admission its pair launcher hands
//! both processes, the blocking frame helpers of the exchange layer, and the
//! lane framing.
//!
//! The kinds declared in `ethos/exchange.ethos` are generated into
//! `src/generated/exchange.rs` by ethos-zero; the flake's check refuses a
//! committed generation that differs from what the source generates.

#![forbid(unsafe_code)]

mod admission;
mod exchange;
#[rustfmt::skip]
#[path = "generated/exchange.rs"]
mod generated;
pub mod lane;
mod naming;
mod paths;

pub use admission::ValidAdmission;
pub use exchange::{
    EndingFrameReading, ExchangeError, IdleFrameReading, RetryingAccept, ValueRestoring,
    ValueWriting,
};
pub use generated::{
    Admission, ComponentName, ComponentTitle, FrameCapacityBytes, FrameDeadlineMilliseconds, Kind,
    Lane, MetaNexusSocket, MetaTranslatorSocket, Named, PublicNexusSocket, PublicTranslatorSocket,
    RetryDelayMilliseconds, SocketNames, SocketNaming,
};
pub use paths::SocketPaths;
pub use shared_translator::unix::DeadlineStream;
