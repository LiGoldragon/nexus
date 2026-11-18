//! The exchange layer both sides of a Nexus socket speak.
//!
//! Every Nexus serves an ordinary and a meta socket on `signal`'s exchange
//! layer, and its translator speaks to it and to its clients in lane frames.
//! What both sides share is here: the socket names a Nexus's name gives and
//! their paths in a place, the Memory file its name gives in its place's
//! state directory, the bootstrap admission its pair launcher hands
//! both processes, the accept every listener makes within the admission's
//! connection capacity, the blocking frame helpers of the exchange layer, the
//! lane framing, and the failure of an emission's lane, which every emission
//! outcome carries as one variant of its own ([`LaneFailing`]), apart from
//! what the world answered.
//!
//! The kinds declared in `ethos/exchange.ethos` are generated into
//! `src/generated/exchange.rs` by ethos-zero; the flake's check refuses a
//! committed generation that differs from what the source generates.

#![forbid(unsafe_code)]

mod accepting;
mod admission;
mod exchange;
#[rustfmt::skip]
#[path = "generated/exchange.rs"]
mod generated;
pub mod lane;
mod memory;
mod naming;
mod paths;

pub use accepting::{
    Admitted, CapacityAccepting, ConnectionGate, ConnectionGating, ConnectionSlot,
};
pub use admission::ValidAdmission;
pub use exchange::{
    EndingFrameReading, ExchangeError, IdleFrameReading, ValueRestoring, ValueWriting,
};
pub use generated::{
    Admission, ComponentName, ComponentTitle, ConnectionCapacity, FrameCapacityBytes,
    FrameDeadlineMilliseconds, Kind, Lane, LaneFailing, LaneFailure, LaneFault, MemoryFile,
    MemoryNaming, MetaNexusSocket, MetaTranslatorSocket, Named, PublicNexusSocket,
    PublicTranslatorSocket, SocketNames, SocketNaming,
};
pub use memory::MemoryPlacing;
pub use paths::SocketPaths;
pub use shared_translator::unix::DeadlineStream;
