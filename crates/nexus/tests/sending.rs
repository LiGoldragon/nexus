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

// The production sender's connection capability makes partial writes deterministic.
// The same SendingWith body is used by Sending over a real Unix socket above.
struct WriteFailureConnection {
    remaining: usize,
    written: std::sync::Arc<std::sync::atomic::AtomicUsize>,
}
impl tokio::io::AsyncRead for WriteFailureConnection {
    fn poll_read(
        self: std::pin::Pin<&mut Self>,
        _: &mut std::task::Context<'_>,
        _: &mut tokio::io::ReadBuf<'_>,
    ) -> std::task::Poll<std::io::Result<()>> {
        panic!("a failed submission must not read an outcome")
    }
}
impl tokio::io::AsyncWrite for WriteFailureConnection {
    fn poll_write(
        mut self: std::pin::Pin<&mut Self>,
        _: &mut std::task::Context<'_>,
        bytes: &[u8],
    ) -> std::task::Poll<std::io::Result<usize>> {
        if self.remaining == 0 {
            return std::task::Poll::Ready(Err(std::io::ErrorKind::BrokenPipe.into()));
        }
        let count = self.remaining.min(bytes.len());
        self.remaining -= count;
        self.written
            .fetch_add(count, std::sync::atomic::Ordering::SeqCst);
        std::task::Poll::Ready(Ok(count))
    }
    fn poll_flush(
        self: std::pin::Pin<&mut Self>,
        _: &mut std::task::Context<'_>,
    ) -> std::task::Poll<std::io::Result<()>> {
        std::task::Poll::Ready(Ok(()))
    }
    fn poll_shutdown(
        self: std::pin::Pin<&mut Self>,
        _: &mut std::task::Context<'_>,
    ) -> std::task::Poll<std::io::Result<()>> {
        std::task::Poll::Ready(Ok(()))
    }
}
struct WriteFailureConnector {
    connection: std::sync::Mutex<Option<WriteFailureConnection>>,
    calls: std::sync::atomic::AtomicUsize,
}
impl nexus::EmissionConnecting for WriteFailureConnector {
    type Connection = WriteFailureConnection;
    async fn connect(&self) -> std::io::Result<Self::Connection> {
        self.calls.fetch_add(1, std::sync::atomic::Ordering::SeqCst);
        Ok(self.connection.lock().unwrap().take().unwrap())
    }
}

#[tokio::test]
async fn local_preparation_refusal_does_not_connect() {
    use nexus::SendingWith;
    let written = std::sync::Arc::new(std::sync::atomic::AtomicUsize::new(0));
    let connector = WriteFailureConnector {
        connection: std::sync::Mutex::new(Some(WriteFailureConnection {
            remaining: 0,
            written,
        })),
        calls: std::sync::atomic::AtomicUsize::new(0),
    };
    let sender = EmissionSender {
        socket_path: "unused".into(),
        frame_capacity: FrameCapacity::from(1),
    };
    assert_eq!(
        sender.send_with::<u64, u64, _>(41, &connector).await,
        Err(Unobserved::Unsubmitted)
    );
    assert_eq!(connector.calls.load(std::sync::atomic::Ordering::SeqCst), 0);
}

#[tokio::test]
async fn post_connect_zero_and_partial_writes_are_both_unanswered() {
    use nexus::SendingWith;
    for accepted in [0, 3] {
        let written = std::sync::Arc::new(std::sync::atomic::AtomicUsize::new(0));
        let connector = WriteFailureConnector {
            connection: std::sync::Mutex::new(Some(WriteFailureConnection {
                remaining: accepted,
                written: written.clone(),
            })),
            calls: std::sync::atomic::AtomicUsize::new(0),
        };
        let sender = EmissionSender {
            socket_path: "unused".into(),
            frame_capacity: FrameCapacity::default(),
        };
        assert_eq!(
            sender.send_with::<u64, u64, _>(41, &connector).await,
            Err(Unobserved::Unanswered)
        );
        assert_eq!(connector.calls.load(std::sync::atomic::Ordering::SeqCst), 1);
        assert_eq!(written.load(std::sync::atomic::Ordering::SeqCst), accepted);
    }
}

#[tokio::test]
async fn malformed_outcome_after_a_received_emission_is_unanswered() {
    use signal::{Framable, Signal};
    use tokio::io::AsyncWriteExt;
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
        // A shared Signal frame whose archived bool is invalid, not a new framing codec.
        let invalid = Signal::<bool>::from(vec![2])
            .framed(FrameCapacity::default())
            .unwrap();
        stream.write_all(&invalid).await.unwrap();
    });
    let sender = EmissionSender {
        socket_path: path,
        frame_capacity: FrameCapacity::default(),
    };
    assert_eq!(
        sender.send::<u64, bool>(41).await,
        Err(Unobserved::Unanswered)
    );
    peer.await.unwrap();
}
