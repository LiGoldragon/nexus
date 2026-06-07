//! Daemon-side errors. Parse-time errors carry through from
//! [`nota_next::NotaDecodeError`]; wire-frame decode errors from
//! [`signal::FrameDecodeError`]; i/o from [`std::io::Error`].
//! Daemon-specific failure modes (frames too large for the
//! length prefix, criome rejecting the handshake, an unsupported
//! verb in M0) get their own typed variants.

use thiserror::Error;

#[derive(Debug, Error)]
pub enum Error {
    #[error("i/o: {0}")]
    Io(#[from] std::io::Error),

    #[error("nota-next: {0}")]
    Codec(#[from] nota_next::NotaDecodeError),

    #[error("frame decode: {0}")]
    Frame(#[from] signal::FrameDecodeError),

    #[error("frame {length} bytes exceeds the 4-byte length-prefix maximum (u32::MAX)")]
    FrameTooLarge { length: usize },

    #[error("criome rejected the handshake: {reason:?}")]
    HandshakeRejected {
        reason: signal::HandshakeRejectionReason,
    },

    #[error(
        "criome returned an unexpected reply shape after handshake — got `{got}`, expected `HandshakeAccepted` / `HandshakeRejected`"
    )]
    HandshakePostReplyShape { got: &'static str },

    #[error("nexus verb `{verb}` is not in the current compatibility parser scope")]
    VerbNotInM0Scope { verb: &'static str },

    /// An actor message failed (timeout, sender dropped, actor stopped).
    /// Carries a free-form detail string so the caller can log; the
    /// connection actor's shuttle maps these to text-rendered
    /// `(Diagnostic …)` replies.
    #[error("actor call: {0}")]
    ActorCall(String),

    /// Actor startup failed during daemon startup.
    #[error("actor spawn: {0}")]
    ActorSpawn(String),
}

pub type Result<T> = std::result::Result<T, Error>;
