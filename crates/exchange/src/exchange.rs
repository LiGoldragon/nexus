//! The blocking half of `signal`'s exchange layer, as both sides of a Nexus
//! socket read and write it.
//!
//! The querying side greets a connection once with the digest of its call
//! contract's authored source, then opens any number of exchanges on it; the
//! Nexus answers each exchange on its identifier until it sends
//! `Delivery::End`, or the querying side abandons the exchange or closes the
//! connection. Framing is `signal`'s four-byte big-endian length prefix; this
//! module adds the two ways a blocking Unix stream waits for a frame, how a
//! frame body is restored and how a value is written whole within a deadline.

use std::io::Read;
use std::os::unix::net::UnixStream;
use std::time::Duration;

use signal::{
    Capacious, ExchangeFault, FRAME_PREFIX_BYTES, FrameBody, FrameCapacity, FrameError,
    FrameWriting, HandshakeReceipt, Restorable, Signal, Signalizable,
};

use crate::DeadlineStream;

/// What goes wrong for one side of an exchange connection.
#[derive(Debug)]
pub enum ExchangeError {
    Frame(FrameError),
    Archive(String),
    /// The Nexus refused the greeting: the two sides were built from
    /// different contract sources.
    GreetingRefused(HandshakeReceipt),
    /// The exchange layer refused what this side asked of it.
    Fault(ExchangeFault),
    /// The other side sent a frame this side did not ask for.
    Unexpected(&'static str),
}

impl std::fmt::Display for ExchangeError {
    fn fmt(&self, formatter: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        write!(formatter, "Nexus exchange error: {self:?}")
    }
}

impl std::error::Error for ExchangeError {}

impl From<FrameError> for ExchangeError {
    fn from(error: FrameError) -> Self {
        Self::Frame(error)
    }
}

impl From<std::io::Error> for ExchangeError {
    fn from(error: std::io::Error) -> Self {
        Self::Frame(FrameError::Io(error))
    }
}

impl From<ExchangeFault> for ExchangeError {
    fn from(fault: ExchangeFault) -> Self {
        Self::Fault(fault)
    }
}

/// A byte source that bounds its own waits, read one whole Signal frame at a
/// time.
pub trait EndingFrameReading {
    /// Reads one whole Signal frame. `None` is the peer closing between
    /// frames.
    fn read_frame_or_end(
        &mut self,
        capacity: FrameCapacity,
    ) -> Result<Option<FrameBody>, FrameError>;
}

impl<R: Read> EndingFrameReading for R {
    fn read_frame_or_end(
        &mut self,
        capacity: FrameCapacity,
    ) -> Result<Option<FrameBody>, FrameError> {
        let mut first = 0_u8;
        loop {
            match self.read(std::slice::from_mut(&mut first)) {
                Ok(0) => return Ok(None),
                Ok(_) => return rest_of_frame(first, self, capacity).map(Some),
                Err(error) if error.kind() == std::io::ErrorKind::Interrupted => {}
                Err(error) => return Err(FrameError::Io(error)),
            }
        }
    }
}

/// An exchange connection, quiet for as long as its exchanges are.
pub trait IdleFrameReading {
    /// Reads one whole Signal frame. The wait for its first byte is
    /// unbounded; once the frame has begun, the rest of it has one absolute
    /// progress deadline, so a partial or drip-fed frame cannot hold the
    /// connection. `None` is the peer closing between frames.
    fn read_frame_after_idle(
        &self,
        progress: Duration,
        capacity: FrameCapacity,
    ) -> Result<Option<FrameBody>, FrameError>;
}

impl IdleFrameReading for UnixStream {
    fn read_frame_after_idle(
        &self,
        progress: Duration,
        capacity: FrameCapacity,
    ) -> Result<Option<FrameBody>, FrameError> {
        let mut idle = self.try_clone()?;
        idle.set_read_timeout(None)?;
        let mut first = 0_u8;
        loop {
            match idle.read(std::slice::from_mut(&mut first)) {
                Ok(0) => return Ok(None),
                Ok(_) => break,
                Err(error) if error.kind() == std::io::ErrorKind::Interrupted => {}
                Err(error) => return Err(FrameError::Io(error)),
            }
        }
        let mut progressing = DeadlineStream::new(idle, progress)?;
        rest_of_frame(first, &mut progressing, capacity).map(Some)
    }
}

fn rest_of_frame(
    first: u8,
    input: &mut impl Read,
    capacity: FrameCapacity,
) -> Result<FrameBody, FrameError> {
    let mut prefix = [0_u8; FRAME_PREFIX_BYTES];
    prefix[0] = first;
    input.read_exact(&mut prefix[1..])?;
    let length = capacity.admit(u32::from_be_bytes(prefix) as usize)?;
    let mut body = vec![0_u8; length];
    input.read_exact(&mut body)?;
    Ok(FrameBody::from(body))
}

/// A frame body, restored as one value of the exchange layer.
pub trait ValueRestoring {
    fn restored<T>(self) -> Result<T, ExchangeError>
    where
        T: rkyv::Archive,
        Signal<T>: Restorable<T>;
}

impl ValueRestoring for FrameBody {
    fn restored<T>(self) -> Result<T, ExchangeError>
    where
        T: rkyv::Archive,
        Signal<T>: Restorable<T>,
    {
        Signal::<T>::from(Vec::from(self))
            .restore()
            .map_err(|error| ExchangeError::Archive(format!("{error:?}")))
    }
}

/// A blocking connection one value of the exchange layer is written to.
pub trait ValueWriting {
    /// Writes the value as one frame, the whole frame within `timeout`.
    fn write_value<T: Signalizable>(
        &self,
        value: &T,
        timeout: Duration,
        capacity: FrameCapacity,
    ) -> Result<(), ExchangeError>;
}

impl ValueWriting for UnixStream {
    fn write_value<T: Signalizable>(
        &self,
        value: &T,
        timeout: Duration,
        capacity: FrameCapacity,
    ) -> Result<(), ExchangeError> {
        let signal = value
            .signalize()
            .map_err(|error| ExchangeError::Archive(format!("{error:?}")))?;
        let mut output = DeadlineStream::new(self.try_clone()?, timeout)?;
        output.write_frame(&signal, capacity)?;
        Ok(())
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use std::io::Write;
    use std::time::Instant;

    #[test]
    fn a_frame_may_be_quiet_beyond_its_progress_bound() {
        let (mut writer, reader) = UnixStream::pair().expect("socket pair");
        let reading = std::thread::spawn(move || {
            reader.read_frame_after_idle(Duration::from_millis(30), FrameCapacity::from(32))
        });
        std::thread::sleep(Duration::from_millis(75));
        writer.write_all(&3_u32.to_be_bytes()).expect("prefix");
        writer.write_all(b"new").expect("body");
        assert_eq!(
            reading.join().expect("reader thread").expect("frame"),
            Some(FrameBody::from(b"new".to_vec()))
        );
    }

    #[test]
    fn a_partial_frame_has_one_absolute_progress_deadline() {
        let (mut writer, reader) = UnixStream::pair().expect("socket pair");
        writer.write_all(&[0]).expect("first prefix byte");
        let started = Instant::now();
        let failure = reader
            .read_frame_after_idle(Duration::from_millis(30), FrameCapacity::from(32))
            .expect_err("partial frame must time out");
        assert!(started.elapsed() < Duration::from_millis(250));
        assert!(matches!(failure, FrameError::Io(_)));
    }

    #[test]
    fn the_peer_closing_between_frames_ends_the_connection() {
        let (writer, reader) = UnixStream::pair().expect("socket pair");
        drop(writer);
        assert_eq!(
            reader
                .read_frame_after_idle(Duration::from_millis(30), FrameCapacity::from(32))
                .expect("clean shutdown"),
            None
        );
    }

    #[test]
    fn an_oversized_frame_is_refused_before_its_body() {
        let mut bytes = std::io::Cursor::new(33_u32.to_be_bytes().to_vec());
        assert!(matches!(
            bytes.read_frame_or_end(FrameCapacity::from(32)),
            Err(FrameError::BodyTooLarge {
                found: 33,
                capacity: 32
            })
        ));
    }
}
