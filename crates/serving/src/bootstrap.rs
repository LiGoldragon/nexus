//! The frame a Nexus is started with.

use std::io::Read;

use nexus_exchange::{ExchangeError, ValueRestoring};
use signal::{FrameCapacity, FrameReading};

use crate::generated::NexusBootstrap;

pub trait BootstrapReading: Sized {
    /// Reads the one bootstrap frame the pair launcher writes, within
    /// Signal's largest frame.
    fn read_bootstrap(input: &mut impl Read) -> Result<Self, ExchangeError>;
}

impl BootstrapReading for NexusBootstrap {
    fn read_bootstrap(input: &mut impl Read) -> Result<Self, ExchangeError> {
        input
            .read_frame(FrameCapacity::from(signal::MAXIMUM_SIGNAL_BYTES))?
            .restored()
    }
}
