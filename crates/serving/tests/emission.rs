//! A failed emission lane reaches the Nexus as the lane failure its outcome
//! carries, apart from every outcome the translator answers with.

use std::os::unix::net::UnixListener;
use std::path::PathBuf;
use std::time::Duration;

use nexus_exchange::lane::{Frame, Kind, Lane, LaneReading, LaneWriting};
use nexus_serving::{EmissionLane, Emitting, LaneFailing, LaneFailure, LaneFault};
use signal::{ByteViewable, Signalizable};

const LIMIT: usize = 4096;

#[derive(rkyv::Archive, rkyv::Serialize, rkyv::Deserialize, Clone, Debug, PartialEq)]
enum Emission {
    Go(u32),
}

/// An emission outcome: what the translator observed, or its lane's failure.
#[derive(rkyv::Archive, rkyv::Serialize, rkyv::Deserialize, Clone, Debug, PartialEq)]
enum Outcome {
    Went(u32),
    LaneFailed(LaneFailure),
}

impl LaneFailing for Outcome {
    fn lane_failed(lane_failure: LaneFailure) -> Self {
        Self::LaneFailed(lane_failure)
    }
}

fn fixture(name: &str) -> PathBuf {
    let directory = std::env::temp_dir().join(format!(
        "nexus-serving-emission-{name}-{}",
        std::process::id()
    ));
    let _ = std::fs::remove_dir_all(&directory);
    std::fs::create_dir_all(&directory).expect("fixture directory");
    directory
}

/// A translator that reads one emission and answers it with `answer`, or
/// closes without an answer.
fn translator(socket: &std::path::Path, answer: Option<Frame>) -> std::thread::JoinHandle<()> {
    let listener = UnixListener::bind(socket).expect("translator socket");
    std::thread::spawn(move || {
        let (mut stream, _) = listener.accept().expect("Nexus connection");
        let request = stream.read_lane_frame(LIMIT).expect("emission request");
        assert_eq!(
            (request.lane, request.kind),
            (Lane::EmissionSignal, Kind::Request)
        );
        if let Some(answer) = answer {
            stream
                .write_lane_frame(&answer, LIMIT)
                .expect("emission answer");
        }
    })
}

/// The outcome the Nexus reads for `Go(3)` from a translator answering
/// `answer`.
fn emitted(name: &str, answer: Option<Frame>) -> Outcome {
    let directory = fixture(name);
    let socket = directory.join("probe-translator.sock");
    let answering = translator(&socket, answer);
    let outcome = EmissionLane::from(socket).emit::<_, Outcome>(
        &Emission::Go(3),
        Duration::from_secs(1),
        LIMIT,
    );
    answering.join().expect("translator");
    std::fs::remove_dir_all(directory).expect("fixture cleanup");
    outcome
}

fn emission_frame(kind: Kind, payload: Vec<u8>) -> Frame {
    Frame {
        lane: Lane::EmissionSignal,
        kind,
        payload,
    }
}

#[test]
fn an_answered_emission_is_the_translators_outcome() {
    let answer = Outcome::Went(3).signalize().expect("outcome");
    assert_eq!(
        emitted(
            "answered",
            Some(emission_frame(Kind::Answered, answer.bytes().to_vec()))
        ),
        Outcome::Went(3)
    );
}

#[test]
fn an_unreached_translator_is_an_unsubmitted_lane() {
    let directory = fixture("unreached");
    let outcome = EmissionLane::from(directory.join("absent.sock")).emit::<_, Outcome>(
        &Emission::Go(3),
        Duration::from_secs(1),
        LIMIT,
    );
    std::fs::remove_dir_all(directory).expect("fixture cleanup");
    assert!(
        matches!(outcome, Outcome::LaneFailed(LaneFailure::Unsubmitted(_))),
        "{outcome:?}"
    );
}

#[test]
fn a_failure_the_translator_answers_is_a_declined_lane() {
    assert_eq!(
        emitted(
            "declined",
            Some(emission_frame(
                Kind::Failed,
                b"translator is awaiting settings".to_vec()
            ))
        ),
        Outcome::LaneFailed(LaneFailure::Declined(LaneFault(
            "translator is awaiting settings".into()
        )))
    );
}

#[test]
fn a_lost_or_unreadable_answer_is_an_unanswered_lane() {
    for (name, answer) in [
        ("closed", None),
        (
            "unreadable",
            Some(emission_frame(Kind::Answered, vec![1, 2, 3])),
        ),
        (
            "misdirected",
            Some(Frame {
                lane: Lane::PublicText,
                kind: Kind::Answered,
                payload: b"Went.3".to_vec(),
            }),
        ),
    ] {
        let outcome = emitted(name, answer);
        assert!(
            matches!(outcome, Outcome::LaneFailed(LaneFailure::Unanswered(_))),
            "{name}: {outcome:?}"
        );
    }
}
