//! The shared ordinary and meta CLI. Component input and output types belong
//! to the translator; this library resolves only its name and place.
#![forbid(unsafe_code)]

#[cfg(feature = "datom")]
mod calling;
mod configuration;
mod exchange;
#[rustfmt::skip]
#[path = "generated/library.rs"]
mod generated;

#[cfg(feature = "datom")]
pub use calling::Calling;
pub use exchange::{ExchangeFailure, Exchanging, TextExchange};
pub use generated::{Served, TextReply, TextRequest};

pub use configuration::{
    CliConfiguration, CliEnvironment, ConfigurationFile, ConfigurationReading, EnvironmentReading,
    PlaceName, SocketResolving,
};

/// The name a component contributes to its CLI, without the `-nexus` suffix.
pub trait Named {
    fn name() -> &'static str;
}

/// The translator text socket selected by one CLI executable.
#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub enum TextSocket {
    Ordinary,
    Meta,
}

/// A CLI refusal before its request was sent. Paths name the configuration file.
#[derive(Clone, Debug, PartialEq, Eq, datom_codec::Datomizable)]
pub enum Unsent {
    Usage,
    Missing(String),
    Unreadable { path: String, reason: String },
    Invalid { path: String, reason: String },
}

/// The process status, independent of any component's typed reply.
#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub enum Ending {
    Answered,
    Failed,
    Unsent,
}

impl From<Ending> for std::process::ExitCode {
    fn from(ending: Ending) -> Self {
        match ending {
            Ending::Answered => Self::SUCCESS,
            Ending::Failed => Self::FAILURE,
            Ending::Unsent => Self::from(2),
        }
    }
}
