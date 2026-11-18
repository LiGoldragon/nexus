//! The meta socket's exchange loop.

use std::io::{Read, Write};

use nexus_exchange::{EndingFrameReading, ValueRestoring};
use signal::{
    Answer, Contracted, Delivery, Dispatch, Ending, ExchangeFault, ExchangeLedger,
    ExchangeTracking, Exchanged, FrameCapacity, FrameWriting, Greeted, HandshakeReceipt,
    Restorable, Signal, Signalizable,
};

use crate::generated::MetaAnswering;

/// A connection the meta socket serves, blocking, within the deadline the
/// stream itself carries.
pub trait MetaServing {
    /// Serves one meta connection: the greeting, then each exchange in turn,
    /// each answered with its one Answer, until the peer closes the
    /// connection. The caller supplies the frame capacity; no runtime default
    /// is hidden here.
    fn serve_meta_connection<Answering>(
        &mut self,
        capacity: FrameCapacity,
        answering: &Answering,
    ) -> Result<(), Box<dyn std::error::Error>>
    where
        Answering: MetaAnswering,
        Answering::Query: Contracted,
        Dispatch<Answering::Query>: rkyv::Archive,
        Signal<Dispatch<Answering::Query>>: Restorable<Dispatch<Answering::Query>>,
        Delivery<Answering::Response>: Signalizable;
}

impl<S: Read + Write> MetaServing for S {
    fn serve_meta_connection<Answering>(
        &mut self,
        capacity: FrameCapacity,
        answering: &Answering,
    ) -> Result<(), Box<dyn std::error::Error>>
    where
        Answering: MetaAnswering,
        Answering::Query: Contracted,
        Dispatch<Answering::Query>: rkyv::Archive,
        Signal<Dispatch<Answering::Query>>: Restorable<Dispatch<Answering::Query>>,
        Delivery<Answering::Response>: Signalizable,
    {
        let mut ledger = ExchangeLedger::default();
        let deliver = |stream: &mut S,
                       delivery: Delivery<Answering::Response>|
         -> Result<(), Box<dyn std::error::Error>> {
            stream.write_frame(&delivery.signalize()?, capacity)?;
            Ok(())
        };
        while let Some(body) = self.read_frame_or_end(capacity)? {
            let Ok(dispatch) = body.restored::<Dispatch<Answering::Query>>() else {
                return deliver(
                    self,
                    Delivery::End(Ending::connection_faulted(ExchangeFault::UnreadableQuery)),
                );
            };
            match dispatch {
                Dispatch::Greet(greeting) => {
                    if let Err(fault) = ledger.greet() {
                        return deliver(self, Delivery::End(Ending::connection_faulted(fault)));
                    }
                    let receipt = <Answering::Query as Contracted>::receipt(&greeting);
                    let settled = matches!(receipt, HandshakeReceipt::Greeted(_));
                    deliver(self, Delivery::Greeted(receipt))?;
                    if !settled {
                        return Ok(());
                    }
                }
                Dispatch::Open(opening) => {
                    if let Err(fault) = ledger.require_greeting() {
                        return deliver(self, Delivery::End(Ending::connection_faulted(fault)));
                    }
                    if let Err(fault) = ledger.admit(opening.exchange) {
                        let ending = Ending::faulted(opening.exchange, fault);
                        let connection_wide = ending.is_connection_wide();
                        deliver(self, Delivery::End(ending))?;
                        if connection_wide {
                            return Ok(());
                        }
                        continue;
                    }
                    let response = answering.answer(opening.query);
                    ledger
                        .release(opening.exchange)
                        .map_err(|fault| format!("meta exchange release failed: {fault:?}"))?;
                    deliver(
                        self,
                        Delivery::Answer(Answer {
                            exchange: opening.exchange,
                            response,
                        }),
                    )?;
                }
                Dispatch::Abandon(exchange) => {
                    // Every exchange is answered before the next frame is
                    // read, so none is open for the peer to abandon.
                    let fault = ledger
                        .release(exchange)
                        .err()
                        .unwrap_or(ExchangeFault::UnknownExchange);
                    deliver(self, Delivery::End(Ending::faulted(exchange, fault)))?;
                }
            }
        }
        Ok(())
    }
}
