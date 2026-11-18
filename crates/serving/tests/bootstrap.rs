//! A Nexus's bootstrap is the one frame its pair launcher writes, read back
//! whole, in the bytes the Nexus read before the frame was shared.

use nexus_exchange::{
    Admission, ConnectionCapacity, FrameCapacityBytes, FrameDeadlineMilliseconds,
};
use nexus_serving::{BootstrapReading, InstanceId, NexusBootstrap};
use shared_translator::placement::{Place, PlaceName, Runtime, State};
use shared_translator::{PeerIdentity, ProcessId, UserId};
use signal::{ByteViewable, FrameCapacity, FrameWriting, Signalizable};

fn bootstrap() -> NexusBootstrap {
    NexusBootstrap {
        place: Place {
            place_name: PlaceName("one".into()),
            runtime: Runtime("/run/user/1000/nexus/one".into()),
            state: State("/home/one/.local/state/nexus/one".into()),
        },
        admission: Admission {
            frame_deadline_milliseconds: FrameDeadlineMilliseconds(70_000),
            frame_capacity_bytes: FrameCapacityBytes(8_388_608),
            connection_capacity: ConnectionCapacity(64),
        },
        peer_identity: PeerIdentity {
            user_id: UserId(1000),
            process_id: ProcessId(4242),
        },
        instance_id: InstanceId("0123456789abcdef0123456789abcdef".into()),
    }
}

#[test]
fn a_bootstrap_frame_is_read_back_whole() {
    let mut frame = Vec::new();
    frame
        .write_frame(
            &bootstrap().signalize().expect("bootstrap"),
            FrameCapacity::from(signal::MAXIMUM_SIGNAL_BYTES),
        )
        .expect("bootstrap frame");
    assert_eq!(
        NexusBootstrap::read_bootstrap(&mut frame.as_slice()).expect("bootstrap read"),
        bootstrap()
    );
    assert!(NexusBootstrap::read_bootstrap(&mut &frame[..frame.len() - 1]).is_err());
}

/// The bootstrap as a Nexus declared it for itself before it was shared: a
/// place, an admission, a peer identity and an instance identifier.
#[derive(rkyv::Archive, rkyv::Serialize, rkyv::Deserialize)]
struct OwnBootstrap {
    place: Place,
    admission: Admission,
    peer_identity: PeerIdentity,
    instance_id: OwnInstanceId,
}

#[derive(rkyv::Archive, rkyv::Serialize, rkyv::Deserialize)]
struct OwnInstanceId(String);

#[test]
fn the_shared_bootstrap_keeps_the_bytes_of_a_nexuss_own() {
    let shared = bootstrap();
    let own = OwnBootstrap {
        place: shared.place.clone(),
        admission: shared.admission.clone(),
        peer_identity: shared.peer_identity.clone(),
        instance_id: OwnInstanceId(shared.instance_id.0.clone()),
    };
    assert_eq!(
        own.signalize().expect("own").bytes(),
        shared.signalize().expect("shared").bytes()
    );
}
