//! The sender's observable boundary, tested without any text codec.
use nexus::{EmissionSender, Sending, SignalReceiving, SignalSubmitting, Unobserved};
use signal::FrameCapacity;
use tokio::{io::AsyncReadExt, net::UnixListener};

#[tokio::test]
async fn no_connection_is_unsubmitted() {
    let scratch = tempfile::tempdir().unwrap();
    let sender = EmissionSender {
        socket_path: scratch.path().join("absent.sock"),
        frame_capacity: FrameCapacity::default(),
    };
    assert_eq!(
        sender.send::<u64, u64>(41).await,
        Err(Unobserved::Unsubmitted)
    );
}

#[tokio::test]
async fn submission_without_an_outcome_is_unanswered() {
    let scratch = tempfile::tempdir().unwrap();
    let path = scratch.path().join("emission.sock");
    let listener = UnixListener::bind(&path).unwrap();
    let peer = tokio::spawn(async move {
        let (mut stream, _) = listener.accept().await.unwrap();
        assert_eq!(
            stream
                .receive::<u64>(FrameCapacity::default())
                .await
                .unwrap(),
            41
        );
    });
    let sender = EmissionSender {
        socket_path: path,
        frame_capacity: FrameCapacity::default(),
    };
    assert_eq!(
        sender.send::<u64, u64>(41).await,
        Err(Unobserved::Unanswered)
    );
    peer.await.unwrap();
}

#[tokio::test]
async fn one_submission_gets_one_typed_outcome_and_closes() {
    let scratch = tempfile::tempdir().unwrap();
    let path = scratch.path().join("emission.sock");
    let listener = UnixListener::bind(&path).unwrap();
    let peer = tokio::spawn(async move {
        let (mut stream, _) = listener.accept().await.unwrap();
        let request = stream
            .receive::<u64>(FrameCapacity::default())
            .await
            .unwrap();
        stream
            .submit(request + 1, FrameCapacity::default())
            .await
            .unwrap();
        let mut byte = [0];
        assert_eq!(stream.read(&mut byte).await.unwrap(), 0);
    });
    let sender = EmissionSender {
        socket_path: path,
        frame_capacity: FrameCapacity::default(),
    };
    assert_eq!(sender.send::<u64, u64>(41).await, Ok(42));
    peer.await.unwrap();
}
