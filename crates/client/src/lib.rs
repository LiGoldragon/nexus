//! The component-independent CLI and its shared, generated text contract.
#![forbid(unsafe_code)]

#[cfg(feature = "datom")]
mod calling;
#[cfg(feature = "datom")]
mod configuration;
mod exchange;
#[rustfmt::skip]
#[path = "generated/library.rs"]
mod generated;

#[cfg(feature = "datom")]
pub use calling::CallingWith;
#[cfg(feature = "datom")]
pub use configuration::{
    CliEnvironment, ConfigurationFile, ConfigurationReading, EnvironmentReading, SocketResolving,
};
pub use exchange::{ExchangeFailure, Exchanging, TextExchange};
pub use generated::{
    Calling, CliConfiguration, Diagnostic, Ending, Invalid_Data, Named, Path, PlaceName, Reason,
    Served, TextReply, TextRequest, TextSocket, Unreadable_Data, Unsent,
};
pub use nexus::Unobserved;

impl Copy for TextSocket {}

impl From<Ending> for std::process::ExitCode {
    fn from(ending: Ending) -> Self {
        match ending {
            Ending::Answered => Self::SUCCESS,
            Ending::Failed => Self::FAILURE,
            Ending::Unsent(_) => Self::from(2),
        }
    }
}
