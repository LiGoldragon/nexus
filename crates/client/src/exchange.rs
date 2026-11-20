//! One request and its replies over ordinary Signal framing. The connection
//! belongs to that exchange and is dropped when the translator serves it.
use crate::{Served, TextReply, TextRequest};
use signal::{FrameCapacity, FrameReading, FrameWriting, Restorable, Signal, Signalizable};
use std::{io::Write, os::unix::net::UnixStream};

/// A connected translator, consumed by its one exchange.
pub struct TextExchange {
    pub stream: UnixStream,
    pub capacity: FrameCapacity,
}

#[derive(Debug)]
pub enum ExchangeFailure {
    Transport(signal::FrameError),
    Archive(String),
    Output(std::io::Error),
}

/// Send unchanged text or a description request, and print each received datom
/// immediately. Only a Served value finishes successfully at the wire level.
pub trait Exchanging {
    fn exchange(
        self,
        request: TextRequest,
        output: &mut impl Write,
    ) -> Result<Served, ExchangeFailure>;
}

impl Exchanging for TextExchange {
    fn exchange(
        mut self,
        request: TextRequest,
        output: &mut impl Write,
    ) -> Result<Served, ExchangeFailure> {
        let request = request
            .signalize()
            .map_err(|error| ExchangeFailure::Archive(format!("{error:?}")))?;
        self.stream
            .write_frame(&request, self.capacity)
            .map_err(ExchangeFailure::Transport)?;
        loop {
            let body = self
                .stream
                .read_frame(self.capacity)
                .map_err(ExchangeFailure::Transport)?;
            let reply = Signal::<TextReply>::from(Vec::from(body))
                .restore()
                .map_err(|error| ExchangeFailure::Archive(format!("{error:?}")))?;
            match reply {
                TextReply::Datom(text) => {
                    output
                        .write_all(text.as_bytes())
                        .and_then(|()| output.write_all(b"\n"))
                        .and_then(|()| output.flush())
                        .map_err(ExchangeFailure::Output)?;
                }
                TextReply::Served(served) => return Ok(served),
            }
        }
    }
}
