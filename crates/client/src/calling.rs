//! Process entry points, shared by the ordinary and meta component executables.
use crate::{
    CliEnvironment, Ending, EnvironmentReading, Exchanging, Named, Served, SocketResolving,
    TextExchange, TextRequest, TextSocket, Unsent,
};
use datom_codec::Datomizable;
use protos::{Compactable, Protosizable};
use std::{ffi::OsString, io::Write, os::unix::net::UnixStream};

/// The whole CLI. A component contributes only Named and chooses its socket.
pub trait Calling: Named {
    fn call(socket: TextSocket) -> Ending;
    /// The same invocation with explicit arguments, roots and output sinks;
    /// arguments exclude the executable name. No process global is changed.
    fn call_with(
        socket: TextSocket,
        arguments: impl IntoIterator<Item = OsString>,
        environment: &CliEnvironment,
        output: &mut impl Write,
        diagnostics: &mut impl Write,
    ) -> Ending;
}

impl<N: Named> Calling for N {
    fn call(socket: TextSocket) -> Ending {
        Self::call_with(
            socket,
            std::env::args_os().skip(1),
            &CliEnvironment::of_process(),
            &mut std::io::stdout().lock(),
            &mut std::io::stderr().lock(),
        )
    }

    fn call_with(
        socket: TextSocket,
        arguments: impl IntoIterator<Item = OsString>,
        environment: &CliEnvironment,
        output: &mut impl Write,
        diagnostics: &mut impl Write,
    ) -> Ending {
        let mut arguments = arguments.into_iter();
        let request = match (arguments.next(), arguments.next()) {
            (None, None) => TextRequest::Describe,
            (Some(text), None) => match text.into_string() {
                Ok(text) => TextRequest::Datom(text),
                Err(_) => {
                    let _ = writeln!(
                        diagnostics,
                        "{}",
                        Unsent::Usage.datomize(vec![]).protosize().compact()
                    );
                    return Ending::Unsent;
                }
            },
            _ => {
                let _ = writeln!(
                    diagnostics,
                    "{}",
                    Unsent::Usage.datomize(vec![]).protosize().compact()
                );
                return Ending::Unsent;
            }
        };
        let socket = match environment.translator_socket::<N>(socket) {
            Ok(socket) => socket,
            Err(refusal) => {
                let _ = writeln!(
                    diagnostics,
                    "{}",
                    refusal.datomize(vec![]).protosize().compact()
                );
                return Ending::Unsent;
            }
        };
        let stream = match UnixStream::connect(socket) {
            Ok(stream) => stream,
            Err(error) => {
                let _ = writeln!(diagnostics, "Failed.{}", error);
                return Ending::Failed;
            }
        };
        match (TextExchange {
            stream,
            capacity: signal::FrameCapacity::default(),
        })
        .exchange(request, output)
        {
            Ok(Served::Answered) => Ending::Answered,
            Ok(served) => {
                let _ = writeln!(
                    diagnostics,
                    "Served.{}",
                    served.datomize(vec![]).protosize().compact()
                );
                Ending::Failed
            }
            Err(error) => {
                let _ = writeln!(diagnostics, "Failed.{error:?}");
                Ending::Failed
            }
        }
    }
}
