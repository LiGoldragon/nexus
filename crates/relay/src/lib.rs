//! One exchange through a configured socket. Both frame bodies remain opaque.
//! Generic Rust I/O capabilities are hand-written; the configured path is
//! authored in Ethos and its projection is generated in the metadata step.

use std::io::{self, Read, Write};
use std::os::unix::net::UnixStream;

use signal::{FRAME_PREFIX_BYTES, FramePrefix, LengthDeclaring as _};

#[rustfmt::skip]
#[path = "generated/library.rs"]
mod generated;
pub use generated::RelaySocketPath;

/// A readable source can copy precisely its next frame, preserving its prefix
/// and leaving every byte of a following frame unread. Streaming the body
/// introduces neither a frame-size policy nor a body-sized allocation.
pub trait OpaqueFrameCopying: Read {
    fn copy_frame_to<Output: Write>(&mut self, output: &mut Output) -> io::Result<()>
    where
        Self: Sized,
    {
        let mut prefix = [0_u8; FRAME_PREFIX_BYTES];
        self.read_exact(&mut prefix)?;
        let length = FramePrefix::from(prefix).declared_length();
        let length = u64::try_from(length).map_err(io::Error::other)?;
        output.write_all(&prefix)?;
        let copied = io::copy(&mut self.take(length), output)?;
        if copied != length {
            return Err(io::ErrorKind::UnexpectedEof.into());
        }
        output.flush()
    }
}

impl<Input: Read> OpaqueFrameCopying for Input {}

/// One invocation opens one local connection, forwards one request and one
/// reply, then drops the connection. It neither greets nor interprets a peer.
pub trait Relaying {
    fn relay<Input: Read, Output: Write>(
        &self,
        input: &mut Input,
        output: &mut Output,
    ) -> io::Result<()>;
}

impl Relaying for RelaySocketPath {
    fn relay<Input: Read, Output: Write>(
        &self,
        input: &mut Input,
        output: &mut Output,
    ) -> io::Result<()> {
        let mut socket = UnixStream::connect(&self.0)?;
        input.copy_frame_to(&mut socket)?;
        socket.copy_frame_to(output)
    }
}
