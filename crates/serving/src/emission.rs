//! The emission lane: a Nexus sends one external effect to its translator as
//! a Signal value and reads the outcome back.

use std::os::unix::net::UnixStream;
use std::path::PathBuf;
use std::time::Duration;

use nexus_exchange::lane::{Frame, IdleLaneReading, Kind, Lane, LaneWriting};
use nexus_exchange::{DeadlineStream, LaneFailing, LaneFailure, LaneFault};
use signal::{ByteViewable, Restorable, Signal, Signalizable};

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
    /// Sends one emission and returns its outcome: the request is written
    /// whole within `timeout`, the answer may be long in coming and is then
    /// read within `timeout`, each frame within `capacity`. A failed lane is
    /// the outcome's own variant, [`LaneFailing::lane_failed`]: `Unsubmitted`
    /// before the request was wholly written, and the effect did not happen;
    /// `Declined` when the translator answered with a failure, before acting;
    /// `Unanswered` when no answer was read, and it may have happened.
    fn emit<Emission, Outcome>(
        &self,
        emission: &Emission,
        timeout: Duration,
        capacity: usize,
    ) -> Outcome
    where
        Emission: Signalizable,
        Outcome: LaneFailing + rkyv::Archive,
        Signal<Outcome>: Restorable<Outcome>;

    fn translator_socket(&self) -> &std::path::Path;
}

impl Emitting for EmissionLane {
    fn emit<Emission, Outcome>(
        &self,
        emission: &Emission,
        timeout: Duration,
        capacity: usize,
    ) -> Outcome
    where
        Emission: Signalizable,
        Outcome: LaneFailing + rkyv::Archive,
        Signal<Outcome>: Restorable<Outcome>,
    {
        match self.submit(emission, timeout, capacity) {
            Ok(response_stream) => response_stream.answer(timeout, capacity),
            Err(fault) => Outcome::lane_failed(LaneFailure::Unsubmitted(fault)),
        }
    }

    fn translator_socket(&self) -> &std::path::Path {
        &self.translator_socket
    }
}

impl EmissionLane {
    /// Connects to the translator and writes the request whole, returning
    /// the connection its answer is read from.
    fn submit<Emission: Signalizable>(
        &self,
        emission: &Emission,
        timeout: Duration,
        capacity: usize,
    ) -> Result<UnixStream, LaneFault> {
        let stream = UnixStream::connect(&self.translator_socket)
            .map_err(|error| LaneFault(error.to_string()))?;
        let response_stream = stream
            .try_clone()
            .map_err(|error| LaneFault(error.to_string()))?;
        let mut output =
            DeadlineStream::new(stream, timeout).map_err(|error| LaneFault(error.to_string()))?;
        let request = emission
            .signalize()
            .map_err(|error| LaneFault(format!("{error:?}")))?;
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
                LaneFault(format!(
                    "incomplete request submission after {} bytes: {:?}",
                    failure.written, failure.error
                ))
            })?;
        Ok(response_stream)
    }
}

/// The connection a wholly submitted emission is answered on.
trait EmissionAnswerReading {
    /// The outcome the translator answered with, or the lane's failure.
    fn answer<Outcome>(self, timeout: Duration, capacity: usize) -> Outcome
    where
        Outcome: LaneFailing + rkyv::Archive,
        Signal<Outcome>: Restorable<Outcome>;
}

impl EmissionAnswerReading for UnixStream {
    fn answer<Outcome>(self, timeout: Duration, capacity: usize) -> Outcome
    where
        Outcome: LaneFailing + rkyv::Archive,
        Signal<Outcome>: Restorable<Outcome>,
    {
        let unanswered =
            |message: String| Outcome::lane_failed(LaneFailure::Unanswered(LaneFault(message)));
        let body = match self.read_lane_frame_after_idle(timeout, capacity) {
            Ok(Some(body)) => body,
            Ok(None) => return unanswered("translator closed without an emission answer".into()),
            Err(error) => return unanswered(format!("{error:?}")),
        };
        match (body.lane, body.kind) {
            (Lane::EmissionSignal, Kind::Answered) => Signal::<Outcome>::from(body.payload)
                .restore()
                .unwrap_or_else(|error| unanswered(format!("{error:?}"))),
            (Lane::EmissionSignal, Kind::Failed) => Outcome::lane_failed(LaneFailure::Declined(
                LaneFault(String::from_utf8_lossy(&body.payload).into_owned()),
            )),
            _ => unanswered("translator returned a non-answer emission frame".into()),
        }
    }
}
