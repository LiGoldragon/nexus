#![cfg(feature = "datom")]

use datom_codec::{Actualizing, Budget, Potential};
use nexus_client::{
    CallingWith, CliEnvironment, Ending, Exchanging, Named, Served, TextExchange, TextReply,
    TextRequest, TextSocket,
};
use signal::{FrameCapacity, FrameReading, FrameWriting, Restorable, Signal, Signalizable};
use std::{
    ffi::OsString,
    io::{Read, Write},
    os::unix::net::{UnixListener, UnixStream},
    sync::mpsc,
};

struct Example;
impl Named for Example {
    fn name() -> String {
        "example".into()
    }
}

struct ObservedOutput {
    bytes: Vec<u8>,
    first_flush: Option<mpsc::Sender<()>>,
}
impl Write for ObservedOutput {
    fn write(&mut self, bytes: &[u8]) -> std::io::Result<usize> {
        self.bytes.extend_from_slice(bytes);
        Ok(bytes.len())
    }
    fn flush(&mut self) -> std::io::Result<()> {
        if let Some(notify) = self.first_flush.take() {
            notify.send(()).unwrap();
        }
        Ok(())
    }
}

#[test]
fn multiline_text_is_unchanged_and_replies_flush_before_the_exchange_ends() {
    let (stream, mut server) = UnixStream::pair().unwrap();
    let request = "Send.{\n «multiline\ncontent»\n}";
    let (flushed, observed) = mpsc::channel();
    let peer = std::thread::spawn(move || {
        let body = server.read_frame(FrameCapacity::default()).unwrap();
        let received = Signal::<TextRequest>::from(Vec::from(body))
            .restore()
            .unwrap();
        assert_eq!(received, TextRequest::Datom(request.into()));
        server
            .write_frame(
                &TextReply::Datom("{ snapshot\n «a\nb» }".into())
                    .signalize()
                    .unwrap(),
                FrameCapacity::default(),
            )
            .unwrap();
        observed.recv().unwrap();
        server
            .write_frame(
                &TextReply::Datom("Change.next".into()).signalize().unwrap(),
                FrameCapacity::default(),
            )
            .unwrap();
        server
            .write_frame(
                &TextReply::Served(Served::Answered).signalize().unwrap(),
                FrameCapacity::default(),
            )
            .unwrap();
        let mut byte = [0];
        assert_eq!(
            server.read(&mut byte).unwrap(),
            0,
            "one exchange owns and closes its connection"
        );
    });
    let mut output = ObservedOutput {
        bytes: Vec::new(),
        first_flush: Some(flushed),
    };
    let served = TextExchange {
        stream,
        capacity: FrameCapacity::default(),
    }
    .exchange(TextRequest::Datom(request.into()), &mut output)
    .unwrap();
    assert_eq!(served, Served::Answered);
    assert_eq!(
        output.bytes,
        "{ snapshot\n «a\nb» }\nChange.next\n".as_bytes()
    );
    peer.join().unwrap();
}

#[test]
fn eof_before_served_is_failure_even_after_a_reply() {
    for send_reply in [false, true] {
        let (stream, mut server) = UnixStream::pair().unwrap();
        let peer = std::thread::spawn(move || {
            server.read_frame(FrameCapacity::default()).unwrap();
            if send_reply {
                server
                    .write_frame(
                        &TextReply::Datom("Refused.Typed".into())
                            .signalize()
                            .unwrap(),
                        FrameCapacity::default(),
                    )
                    .unwrap();
            }
        });
        let mut output = Vec::new();
        assert!(
            TextExchange {
                stream,
                capacity: FrameCapacity::default()
            }
            .exchange(TextRequest::Describe, &mut output)
            .is_err()
        );
        assert_eq!(
            output,
            if send_reply {
                b"Refused.Typed\n".to_vec()
            } else {
                Vec::new()
            }
        );
        peer.join().unwrap();
    }
}

#[test]
fn malformed_archive_and_partial_frame_are_failures() {
    for bytes in [vec![0, 0], vec![0, 0, 0, 1, 0xff]] {
        let (stream, mut server) = UnixStream::pair().unwrap();
        let peer = std::thread::spawn(move || {
            server.read_frame(FrameCapacity::default()).unwrap();
            server.write_all(&bytes).unwrap();
        });
        assert!(
            TextExchange {
                stream,
                capacity: FrameCapacity::default()
            }
            .exchange(TextRequest::Describe, &mut Vec::new())
            .is_err()
        );
        peer.join().unwrap();
    }
}

#[test]
fn caller_output_failure_closes_the_subscription_connection() {
    let (stream, mut server) = UnixStream::pair().unwrap();
    let peer = std::thread::spawn(move || {
        server.read_frame(FrameCapacity::default()).unwrap();
        server
            .write_frame(
                &TextReply::Datom("Snapshot.initial".into())
                    .signalize()
                    .unwrap(),
                FrameCapacity::default(),
            )
            .unwrap();
        let mut byte = [0];
        assert_eq!(server.read(&mut byte).unwrap(), 0);
    });
    struct Closed;
    impl Write for Closed {
        fn write(&mut self, _: &[u8]) -> std::io::Result<usize> {
            Err(std::io::ErrorKind::BrokenPipe.into())
        }
        fn flush(&mut self) -> std::io::Result<()> {
            Ok(())
        }
    }
    assert!(
        TextExchange {
            stream,
            capacity: FrameCapacity::default()
        }
        .exchange(TextRequest::Describe, &mut Closed)
        .is_err()
    );
    peer.join().unwrap();
}

#[test]
fn cli_describe_and_each_served_outcome_have_the_declared_status() {
    let unreadable = datom_codec::Error {
        layer: datom_codec::ErrorLayer::Composition,
        path: vec![],
        kind: datom_codec::ErrorKind::Value {
            expected: "an input with { positions }".into(),
            value: "malformed [ «text with spaces» ]".into(),
        },
    };
    for (socket, served, expected) in [
        (TextSocket::Ordinary, Served::Answered, Ending::Answered),
        (TextSocket::Meta, Served::NexusDown, Ending::Failed),
        (
            TextSocket::Ordinary,
            Served::Unreadable(unreadable),
            Ending::Failed,
        ),
    ] {
        let scratch = tempfile::tempdir().unwrap();
        let runtime = scratch.path().join("runtime");
        let configuration = scratch.path().join("configuration");
        std::fs::create_dir_all(runtime.join("nexus/main")).unwrap();
        std::fs::create_dir_all(configuration.join("nexus")).unwrap();
        std::fs::write(configuration.join("nexus/example-nexus.datom"), "{ main }").unwrap();
        let file = match socket {
            TextSocket::Ordinary => "example-translator.sock",
            TextSocket::Meta => "example-meta-translator.sock",
        };
        let listener = UnixListener::bind(runtime.join("nexus/main").join(file)).unwrap();
        let diagnostic = nexus_client::Diagnostic::Served(served.clone());
        let peer = std::thread::spawn(move || {
            let (mut stream, _) = listener.accept().unwrap();
            let body = stream.read_frame(FrameCapacity::default()).unwrap();
            assert_eq!(
                Signal::<TextRequest>::from(Vec::from(body))
                    .restore()
                    .unwrap(),
                TextRequest::Describe
            );
            stream
                .write_frame(
                    &TextReply::Datom("InputAndOutputEthos".into())
                        .signalize()
                        .unwrap(),
                    FrameCapacity::default(),
                )
                .unwrap();
            stream
                .write_frame(
                    &TextReply::Served(served).signalize().unwrap(),
                    FrameCapacity::default(),
                )
                .unwrap();
        });
        let environment = CliEnvironment {
            configuration_home: Some(configuration.into_os_string()),
            runtime_directory: Some(runtime.into_os_string()),
            home: None,
        };
        let (mut output, mut diagnostics) = (Vec::new(), Vec::new());
        assert_eq!(
            Example::call_with(socket, [], &environment, &mut output, &mut diagnostics),
            expected.clone()
        );
        assert_eq!(output, b"InputAndOutputEthos\n");
        if expected == Ending::Answered {
            assert!(diagnostics.is_empty());
        } else {
            let mut budget = Budget {
                remaining: 10_000,
                reader: protos::ReaderBudget::try_from(10_000).unwrap(),
                depth: 0,
                maximum_depth: 64,
            };
            let printed = String::from_utf8(diagnostics).unwrap();
            let restored = Potential::<nexus_client::Diagnostic>::from(printed)
                .actualize(&mut budget)
                .unwrap();
            assert_eq!(restored, diagnostic);
        }
        peer.join().unwrap();
    }
}

#[test]
fn usage_and_configuration_failure_send_nothing() {
    let mut diagnostics = Vec::new();
    assert_eq!(
        Example::call_with(
            TextSocket::Ordinary,
            [OsString::from("first"), OsString::from("second")],
            &CliEnvironment::default(),
            &mut Vec::new(),
            &mut diagnostics
        ),
        Ending::Unsent(nexus_client::Unsent::Usage)
    );
    assert_eq!(diagnostics, b"Usage\n");
    let scratch = tempfile::tempdir().unwrap();
    let environment = CliEnvironment {
        configuration_home: Some(scratch.path().into()),
        ..CliEnvironment::default()
    };
    diagnostics.clear();
    assert!(matches!(
        Example::call_with(
            TextSocket::Meta,
            [OsString::from("Opaque.unparsed")],
            &environment,
            &mut Vec::new(),
            &mut diagnostics
        ),
        Ending::Unsent(nexus_client::Unsent::Missing(_))
    ));
    let refusal = String::from_utf8(diagnostics).unwrap();
    assert!(
        refusal.starts_with("Missing.") && refusal.contains("example-nexus.datom"),
        "{refusal}"
    );
}

#[test]
fn cli_single_datom_is_opaque_and_typed_refusal_needs_a_served_ending() {
    for complete in [false, true] {
        let scratch = tempfile::tempdir().unwrap();
        let configuration = scratch.path().join("configuration");
        let runtime = scratch.path().join("runtime");
        std::fs::create_dir_all(configuration.join("nexus")).unwrap();
        std::fs::create_dir_all(runtime.join("nexus/main")).unwrap();
        std::fs::write(configuration.join("nexus/example-nexus.datom"), "{ main }").unwrap();
        let listener =
            UnixListener::bind(runtime.join("nexus/main/example-translator.sock")).unwrap();
        let text = "{ a datom\n «kept exactly» }";
        let peer = std::thread::spawn(move || {
            let (mut stream, _) = listener.accept().unwrap();
            let body = stream.read_frame(FrameCapacity::default()).unwrap();
            assert_eq!(
                Signal::<TextRequest>::from(Vec::from(body))
                    .restore()
                    .unwrap(),
                TextRequest::Datom(text.into())
            );
            stream
                .write_frame(
                    &TextReply::Datom("Refused.Policy".into())
                        .signalize()
                        .unwrap(),
                    FrameCapacity::default(),
                )
                .unwrap();
            if complete {
                stream
                    .write_frame(
                        &TextReply::Served(Served::Answered).signalize().unwrap(),
                        FrameCapacity::default(),
                    )
                    .unwrap();
            }
        });
        let environment = CliEnvironment {
            configuration_home: Some(configuration.into_os_string()),
            runtime_directory: Some(runtime.into_os_string()),
            home: None,
        };
        let (mut output, mut diagnostics) = (Vec::new(), Vec::new());
        assert_eq!(
            Example::call_with(
                TextSocket::Ordinary,
                [OsString::from(text)],
                &environment,
                &mut output,
                &mut diagnostics
            ),
            if complete {
                Ending::Answered
            } else {
                Ending::Failed
            }
        );
        assert_eq!(output, b"Refused.Policy\n");
        assert_eq!(diagnostics.is_empty(), complete);
        if !complete {
            assert_eq!(
                String::from_utf8(diagnostics).unwrap(),
                "Unobserved.Unanswered\n"
            );
        }
        peer.join().unwrap();
    }
}

#[test]
fn connection_and_output_failures_print_declared_diagnostics() {
    for connected in [false, true] {
        let scratch = tempfile::tempdir().unwrap();
        let configuration = scratch.path().join("configuration");
        let runtime = scratch.path().join("runtime");
        std::fs::create_dir_all(configuration.join("nexus")).unwrap();
        std::fs::create_dir_all(runtime.join("nexus/main")).unwrap();
        std::fs::write(configuration.join("nexus/example-nexus.datom"), "{ main }").unwrap();
        let socket = runtime.join("nexus/main/example-translator.sock");
        let peer = if connected {
            let listener = UnixListener::bind(socket).unwrap();
            Some(std::thread::spawn(move || {
                let (mut stream, _) = listener.accept().unwrap();
                stream.read_frame(FrameCapacity::default()).unwrap();
                stream
                    .write_frame(
                        &TextReply::Datom("Reply.value".into()).signalize().unwrap(),
                        FrameCapacity::default(),
                    )
                    .unwrap();
            }))
        } else {
            None
        };
        struct BrokenOutput;
        impl Write for BrokenOutput {
            fn write(&mut self, _: &[u8]) -> std::io::Result<usize> {
                Err(std::io::Error::other(
                    "failure with spaces { braces } [ «delimiters» ]",
                ))
            }
            fn flush(&mut self) -> std::io::Result<()> {
                Ok(())
            }
        }
        let environment = CliEnvironment {
            configuration_home: Some(configuration.into_os_string()),
            runtime_directory: Some(runtime.into_os_string()),
            home: None,
        };
        let mut diagnostics = Vec::new();
        assert_eq!(
            Example::call_with(
                TextSocket::Ordinary,
                [OsString::from("Call.value")],
                &environment,
                &mut BrokenOutput,
                &mut diagnostics
            ),
            Ending::Failed
        );
        let mut budget = Budget {
            remaining: 100,
            reader: protos::ReaderBudget::try_from(100).unwrap(),
            depth: 0,
            maximum_depth: 10,
        };
        let diagnostic =
            Potential::<nexus_client::Diagnostic>::from(String::from_utf8(diagnostics).unwrap())
                .actualize(&mut budget)
                .unwrap();
        assert_eq!(
            diagnostic,
            nexus_client::Diagnostic::Unobserved(if connected {
                nexus_client::Unobserved::Unanswered
            } else {
                nexus_client::Unobserved::Unsubmitted
            })
        );
        if let Some(peer) = peer {
            peer.join().unwrap();
        }
    }
}
