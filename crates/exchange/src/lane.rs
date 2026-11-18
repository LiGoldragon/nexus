//! The translator's lane frames: a two-byte lane and kind header ahead of the
//! payload, inside one length-prefixed frame.
//!
//! A client speaks text to the translator on the public and meta text lanes,
//! and reads each answer back until the one frame that ends them: `Ended`
//! when the exchange completed, `Failed`, carrying its text, when it did not.
//! The Nexus sends the translator each external effect on the emission lane,
//! a Signal value, and reads its outcome back on it.

use std::io::{Read, Write};
use std::os::unix::net::UnixStream;
use std::time::Duration;

pub use crate::generated::{Kind, Lane};

/// The bytes of a frame's length prefix and its lane and kind header.
pub const HEADER_BYTES: usize = shared_translator::unix::HEADER_BYTES;

/// One lane frame.
#[derive(Clone, Debug, PartialEq, Eq)]
pub struct Frame {
    pub lane: Lane,
    pub kind: Kind,
    pub payload: Vec<u8>,
}

/// A frame on a text lane carries UTF-8 text.
pub trait Textual {
    fn text(&self) -> Result<&str, TransportError>;
}

impl Textual for Frame {
    fn text(&self) -> Result<&str, TransportError> {
        match self.lane {
            Lane::PublicText | Lane::MetaText => {
                std::str::from_utf8(&self.payload).map_err(|_| TransportError::InvalidUtf8)
            }
            Lane::EmissionSignal => Err(TransportError::WrongLane),
        }
    }
}

#[derive(Debug)]
pub enum TransportError {
    Io(std::io::Error),
    InvalidLimit,
    InvalidLength,
    TooLarge,
    UnknownLane(u8),
    UnknownKind(u8),
    InvalidUtf8,
    WrongLane,
    PeerCredential(String),
}

impl std::fmt::Display for TransportError {
    fn fmt(&self, formatter: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        write!(formatter, "Nexus transport error: {self:?}")
    }
}

impl std::error::Error for TransportError {}

impl From<std::io::Error> for TransportError {
    fn from(error: std::io::Error) -> Self {
        Self::Io(error)
    }
}

impl From<shared_translator::unix::Error> for TransportError {
    fn from(error: shared_translator::unix::Error) -> Self {
        match error {
            shared_translator::unix::Error::Io(error) => Self::Io(error),
            shared_translator::unix::Error::InvalidLimit => Self::InvalidLimit,
            shared_translator::unix::Error::InvalidLength => Self::InvalidLength,
            shared_translator::unix::Error::TooLarge => Self::TooLarge,
            shared_translator::unix::Error::PeerCredential(message) => {
                Self::PeerCredential(message)
            }
        }
    }
}

/// A write that failed partway, with the bytes it had submitted.
#[derive(Debug)]
pub struct IncompleteSubmission {
    pub written: usize,
    pub error: TransportError,
}

/// A byte source read one lane frame at a time.
pub trait LaneReading {
    /// Reads one lane frame, refusing one larger than `admission_limit` before
    /// its body, an unknown lane or kind, and text that is not UTF-8.
    fn read_lane_frame(&mut self, admission_limit: usize) -> Result<Frame, TransportError>;
}

impl<R: Read> LaneReading for R {
    fn read_lane_frame(&mut self, admission_limit: usize) -> Result<Frame, TransportError> {
        let raw = shared_translator::unix::read_frame(self, admission_limit)?;
        checked(raw)
    }
}

/// A stream whose answer may be long in coming.
pub trait IdleLaneReading {
    /// Waits without an idle deadline for the first response byte, then
    /// bounds all remaining frame progress by one absolute deadline. `None`
    /// is the peer closing before the frame began.
    fn read_lane_frame_after_idle(
        self,
        progress_timeout: Duration,
        admission_limit: usize,
    ) -> Result<Option<Frame>, TransportError>;
}

impl IdleLaneReading for UnixStream {
    fn read_lane_frame_after_idle(
        self,
        progress_timeout: Duration,
        admission_limit: usize,
    ) -> Result<Option<Frame>, TransportError> {
        let Some(raw) = shared_translator::unix::read_frame_after_idle(
            self,
            progress_timeout,
            admission_limit,
        )?
        else {
            return Ok(None);
        };
        checked(raw).map(Some)
    }
}

/// A byte sink written one lane frame at a time.
pub trait LaneWriting {
    fn write_lane_frame(
        &mut self,
        frame: &Frame,
        admission_limit: usize,
    ) -> Result<(), TransportError>;

    /// Writes one frame while witnessing whether every byte was submitted.
    /// Any failure returned here proves the request was incomplete; loss
    /// after `Ok` is the separate possibly-executed case.
    fn write_lane_frame_witness(
        &mut self,
        frame: &Frame,
        admission_limit: usize,
    ) -> Result<(), IncompleteSubmission>;
}

impl<W: Write> LaneWriting for W {
    fn write_lane_frame(
        &mut self,
        frame: &Frame,
        admission_limit: usize,
    ) -> Result<(), TransportError> {
        if matches!(frame.lane, Lane::PublicText | Lane::MetaText) {
            frame.text()?;
        }
        shared_translator::unix::write_frame(self, &raw(frame), admission_limit).map_err(Into::into)
    }

    fn write_lane_frame_witness(
        &mut self,
        frame: &Frame,
        admission_limit: usize,
    ) -> Result<(), IncompleteSubmission> {
        if matches!(frame.lane, Lane::PublicText | Lane::MetaText) {
            frame
                .text()
                .map_err(|error| IncompleteSubmission { written: 0, error })?;
        }
        shared_translator::unix::write_frame_witness(self, &raw(frame), admission_limit).map_err(
            |failure| IncompleteSubmission {
                written: failure.written,
                error: failure.error.into(),
            },
        )
    }
}

/// A connected Unix stream names the process at its other end.
pub trait PeerCalling {
    /// The peer's process and user, from the kernel's SO_PEERCRED.
    fn peer_caller(&self) -> Result<shared_translator::Caller, TransportError>;
}

impl PeerCalling for UnixStream {
    fn peer_caller(&self) -> Result<shared_translator::Caller, TransportError> {
        let credentials = shared_translator::unix::peer_credentials(self)?;
        Ok(shared_translator::Caller {
            process_id: credentials.process_id,
            user_id: credentials.user_id,
        })
    }
}

fn checked(raw: shared_translator::unix::Frame) -> Result<Frame, TransportError> {
    let frame = Frame {
        lane: decode_lane(raw.lane)?,
        kind: decode_kind(raw.kind)?,
        payload: raw.payload,
    };
    if matches!(frame.lane, Lane::PublicText | Lane::MetaText) {
        frame.text()?;
    }
    Ok(frame)
}

fn raw(frame: &Frame) -> shared_translator::unix::Frame {
    shared_translator::unix::Frame {
        lane: encode_lane(&frame.lane),
        kind: encode_kind(&frame.kind),
        payload: frame.payload.clone(),
    }
}

fn decode_lane(value: u8) -> Result<Lane, TransportError> {
    match value {
        1 => Ok(Lane::PublicText),
        2 => Ok(Lane::MetaText),
        3 => Ok(Lane::EmissionSignal),
        other => Err(TransportError::UnknownLane(other)),
    }
}

fn encode_lane(lane: &Lane) -> u8 {
    match lane {
        Lane::PublicText => 1,
        Lane::MetaText => 2,
        Lane::EmissionSignal => 3,
    }
}

fn decode_kind(value: u8) -> Result<Kind, TransportError> {
    match value {
        0 => Ok(Kind::Request),
        1 => Ok(Kind::Answered),
        2 => Ok(Kind::Failed),
        3 => Ok(Kind::Ended),
        other => Err(TransportError::UnknownKind(other)),
    }
}

fn encode_kind(kind: &Kind) -> u8 {
    match kind {
        Kind::Request => 0,
        Kind::Answered => 1,
        Kind::Failed => 2,
        Kind::Ended => 3,
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use std::io::Cursor;

    #[test]
    fn total_length_includes_lane_and_kind() {
        let frame = Frame {
            lane: Lane::PublicText,
            kind: Kind::Request,
            payload: b"List.main".to_vec(),
        };
        let mut bytes = Vec::new();
        bytes.write_lane_frame(&frame, 128).expect("write");
        assert_eq!(u32::from_be_bytes(bytes[..4].try_into().unwrap()), 11);
        assert_eq!(Cursor::new(bytes).read_lane_frame(128).unwrap(), frame);
    }

    #[test]
    fn an_answer_ends_with_a_frame_of_its_own_kind() {
        let ended = Frame {
            lane: Lane::MetaText,
            kind: Kind::Ended,
            payload: Vec::new(),
        };
        let mut bytes = Vec::new();
        bytes.write_lane_frame(&ended, 128).expect("write");
        assert_eq!(bytes[5], 3);
        assert_eq!(Cursor::new(bytes).read_lane_frame(128).unwrap(), ended);
    }

    #[test]
    fn rejects_oversize_before_reading_body() {
        let mut bytes = Cursor::new((129_u32).to_be_bytes().to_vec());
        assert!(matches!(
            bytes.read_lane_frame(128),
            Err(TransportError::TooLarge)
        ));
    }

    #[test]
    fn text_lane_requires_utf8_and_unknown_lane_is_not_guessed() {
        let invalid_text = Frame {
            lane: Lane::PublicText,
            kind: Kind::Request,
            payload: vec![0xff],
        };
        assert!(matches!(
            Vec::<u8>::new().write_lane_frame(&invalid_text, 128),
            Err(TransportError::InvalidUtf8)
        ));
        let mut unknown = vec![0, 0, 0, 2, 9, 0];
        assert!(matches!(
            Cursor::new(&mut unknown).read_lane_frame(128),
            Err(TransportError::UnknownLane(9))
        ));
    }

    #[test]
    fn submission_witness_distinguishes_partial_from_complete() {
        struct StopsAfter {
            limit: usize,
            bytes: Vec<u8>,
        }
        impl Write for StopsAfter {
            fn write(&mut self, bytes: &[u8]) -> std::io::Result<usize> {
                if self.bytes.len() == self.limit {
                    return Err(std::io::Error::new(
                        std::io::ErrorKind::BrokenPipe,
                        "fixture stop",
                    ));
                }
                let count = bytes.len().min(self.limit - self.bytes.len());
                self.bytes.extend_from_slice(&bytes[..count]);
                Ok(count)
            }
            fn flush(&mut self) -> std::io::Result<()> {
                Ok(())
            }
        }
        let frame = Frame {
            lane: Lane::EmissionSignal,
            kind: Kind::Request,
            payload: vec![1, 2, 3],
        };
        let mut partial = StopsAfter {
            limit: 5,
            bytes: Vec::new(),
        };
        let failure = partial
            .write_lane_frame_witness(&frame, 64)
            .expect_err("partial fixture");
        assert_eq!(failure.written, 5);
        let mut complete = StopsAfter {
            limit: 9,
            bytes: Vec::new(),
        };
        complete
            .write_lane_frame_witness(&frame, 64)
            .expect("complete submission");
    }

    #[test]
    fn idle_before_first_response_byte_has_no_lifetime_timeout() {
        let (reader, mut writer) = UnixStream::pair().expect("pair");
        let expected = Frame {
            lane: Lane::EmissionSignal,
            kind: Kind::Answered,
            payload: vec![1, 2, 3],
        };
        let sent = expected.clone();
        let peer = std::thread::spawn(move || {
            std::thread::sleep(Duration::from_millis(60));
            writer
                .write_lane_frame(&sent, 64)
                .expect("late complete response");
        });
        let began = std::time::Instant::now();
        assert_eq!(
            reader
                .read_lane_frame_after_idle(Duration::from_millis(10), 64)
                .expect("idle response"),
            Some(expected)
        );
        assert!(began.elapsed() >= Duration::from_millis(50));
        peer.join().expect("peer");
    }

    #[test]
    fn progress_after_first_response_byte_has_one_absolute_deadline() {
        let (reader, mut writer) = UnixStream::pair().expect("pair");
        let peer = std::thread::spawn(move || {
            writer.write_all(&[0]).expect("first prefix byte");
            std::thread::sleep(Duration::from_millis(80));
        });
        let began = std::time::Instant::now();
        assert!(matches!(
            reader.read_lane_frame_after_idle(Duration::from_millis(15), 64),
            Err(TransportError::Io(_))
        ));
        assert!(began.elapsed() < Duration::from_millis(70));
        peer.join().expect("peer");
    }

    #[test]
    fn a_nexus_names_its_four_sockets_after_itself() {
        use crate::{ComponentName, SocketNaming};
        let names = ComponentName("capsule".into()).socket_names();
        assert_eq!(names.public_nexus_socket.0, "capsule.sock");
        assert_eq!(names.meta_nexus_socket.0, "capsule-meta.sock");
        assert_eq!(names.public_translator_socket.0, "capsule-translator.sock");
        assert_eq!(
            names.meta_translator_socket.0,
            "capsule-meta-translator.sock"
        );
    }
}
