//! The emission lane: a Nexus sends one external effect to its translator as
//! a Signal value and reads the outcome back.

use std::os::unix::net::UnixStream;
use std::path::PathBuf;
use std::time::Duration;

use nexus_exchange::DeadlineStream;
use nexus_exchange::lane::{Frame, IdleLaneReading, Kind, Lane, LaneWriting};
use signal::{ByteViewable, Restorable, Signal, Signalizable};

use crate::generated::EmissionFailure;

/// The translator's public socket, which the Nexus's emissions go to.
pub struct EmissionLane {
    translator_socket: PathBuf,
}

impl From<PathBuf> for EmissionLane {
    fn from(translator_socket: PathBuf) -> Self {
        Self { translator_socket }
    }
}

pub trait Emitting {
    /// Sends one emission and waits for its outcome: the request is written
    /// whole within `timeout`, the answer may be long in coming and is then
    /// read within `timeout`, each frame within `capacity`. A failure before
    /// the request was wholly submitted is `Unsubmitted`, and the effect did
    /// not happen; one after is `Unanswered`, and it may have.
    fn emit<Emission, Outcome>(
        &self,
        emission: &Emission,
        timeout: Duration,
        capacity: usize,
    ) -> Result<Outcome, EmissionFailure>
    where
        Emission: Signalizable,
        Outcome: rkyv::Archive,
        Signal<Outcome>: Restorable<Outcome>;

    fn translator_socket(&self) -> &std::path::Path;
}

impl Emitting for EmissionLane {
    fn emit<Emission, Outcome>(
        &self,
        emission: &Emission,
        timeout: Duration,
        capacity: usize,
    ) -> Result<Outcome, EmissionFailure>
    where
        Emission: Signalizable,
        Outcome: rkyv::Archive,
        Signal<Outcome>: Restorable<Outcome>,
    {
        let unsubmitted = |message: String| EmissionFailure::Unsubmitted(message);
        let stream = UnixStream::connect(&self.translator_socket)
            .map_err(|error| unsubmitted(error.to_string()))?;
        let response_stream = stream
            .try_clone()
            .map_err(|error| unsubmitted(error.to_string()))?;
        let mut output =
            DeadlineStream::new(stream, timeout).map_err(|error| unsubmitted(error.to_string()))?;
        let request = emission
            .signalize()
            .map_err(|error| unsubmitted(format!("{error:?}")))?;
        output
            .write_lane_frame_witness(
                &Frame {
                    lane: Lane::EmissionSignal,
                    kind: Kind::Request,
                    payload: request.bytes().to_vec(),
                },
                capacity,
            )
            .map_err(|failure| {
                unsubmitted(format!(
                    "incomplete request submission after {} bytes: {:?}",
                    failure.written, failure.error
                ))
            })?;
        drop(output);
        let answered = (|| -> Result<Outcome, Box<dyn std::error::Error>> {
            let body = response_stream
                .read_lane_frame_after_idle(timeout, capacity)?
                .ok_or("translator closed without an emission answer")?;
            if body.lane != Lane::EmissionSignal || body.kind != Kind::Answered {
                return Err("translator returned a non-answer emission frame".into());
            }
            Ok(Signal::<Outcome>::from(body.payload).restore()?)
        })();
        answered.map_err(|error| EmissionFailure::Unanswered(error.to_string()))
    }

    fn translator_socket(&self) -> &std::path::Path {
        &self.translator_socket
    }
}
