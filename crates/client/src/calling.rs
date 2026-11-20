//! Process adapters and the single Datom diagnostic translation boundary.
use crate::{
    Calling, CliEnvironment, Diagnostic, Ending, EnvironmentReading, Exchanging, Named, Served,
    SocketResolving, TextExchange, TextRequest, TextSocket, Unobserved, Unsent,
};
use datom_codec::Datomizable;
use protos::{Compactable, Protosizable};
use std::{ffi::OsString, io::Write, os::unix::net::UnixStream};

/// The OS-native invocation capability used for embedding and isolated tests.
pub trait CallingWith: Named {
    fn call_with(
        socket: TextSocket,
        arguments: impl IntoIterator<Item = OsString>,
        environment: &CliEnvironment,
        output: &mut impl Write,
        diagnostics: &mut impl Write,
    ) -> Ending;
}

/// Print a declared diagnostic or pre-send refusal through its Datom codec.
trait Reporting {
    fn report(&self, diagnostics: &mut impl Write) -> std::io::Result<()>;
}
impl<T: Datomizable> Reporting for T {
    fn report(&self, diagnostics: &mut impl Write) -> std::io::Result<()> {
        writeln!(
            diagnostics,
            "{}",
            self.datomize(vec![]).protosize().compact()
        )
    }
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
}

impl<N: Named> CallingWith for N {
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
                    let refusal = Unsent::Usage;
                    let _ = refusal.report(diagnostics);
                    return Ending::Unsent(refusal);
                }
            },
            _ => {
                let refusal = Unsent::Usage;
                let _ = refusal.report(diagnostics);
                return Ending::Unsent(refusal);
            }
        };
        let socket = match environment.translator_socket::<N>(socket) {
            Ok(socket) => socket,
            Err(refusal) => {
                let _ = refusal.report(diagnostics);
                return Ending::Unsent(refusal);
            }
        };
        let stream = match UnixStream::connect(socket) {
            Ok(stream) => stream,
            Err(_) => {
                let _ = Diagnostic::Unobserved(Unobserved::Unsubmitted).report(diagnostics);
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
                let _ = Diagnostic::Served(served).report(diagnostics);
                Ending::Failed
            }
            Err(_) => {
                let _ = Diagnostic::Unobserved(Unobserved::Unanswered).report(diagnostics);
                Ending::Failed
            }
        }
    }
}
