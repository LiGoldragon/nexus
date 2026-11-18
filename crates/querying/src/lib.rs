//! The querying end of one connection to a Nexus socket.
//!
//! A translator, a test or another Nexus greets the socket with the digest of
//! its call contract, opens any number of exchanges on the greeted
//! connection, and reads the deliveries the Nexus answers them with. Every
//! exchange ends with `Delivery::End`, a one-answer exchange after its answer
//! as a subscription after its last frame, so [`ExchangeFollowing`] reads any
//! exchange the same way: each answer, until its end. The connection is
//! blocking; how long it waits is its [`Patience`].

#![forbid(unsafe_code)]

use std::marker::PhantomData;
use std::os::unix::net::UnixStream;
use std::time::{Duration, Instant};

use nexus_exchange::{
    DeadlineStream, EndingFrameReading, ExchangeError, IdleFrameReading, ValueRestoring,
    ValueWriting,
};
use signal::{
    Conclusion, Contracted, Delivery, Dispatch, ExchangeId, ExchangeLedger, ExchangeMinting,
    Exchanged, FrameCapacity, FrameError, Greeted, HandshakeReceipt, Opening, Restorable, Signal,
    Signalizable,
};

/// How long the querying side waits on the Nexus.
#[derive(Clone, Copy, Debug)]
pub enum Patience {
    /// Each frame written within the bound; each frame read waits without a
    /// bound for its first byte and then has the bound for the rest.
    Progress(Duration),
    /// Every frame written and read within one absolute deadline.
    Until(Instant),
}

/// The time a patience leaves for the next frame.
pub trait Remaining {
    fn remaining(&self) -> Result<Duration, ExchangeError>;
}

impl Remaining for Patience {
    fn remaining(&self) -> Result<Duration, ExchangeError> {
        match self {
            Self::Progress(bound) => Ok(*bound),
            Self::Until(deadline) => deadline
                .checked_duration_since(Instant::now())
                .filter(|remaining| !remaining.is_zero())
                .ok_or_else(|| {
                    ExchangeError::Frame(FrameError::Io(std::io::Error::new(
                        std::io::ErrorKind::TimedOut,
                        "exchange deadline elapsed",
                    )))
                }),
        }
    }
}

/// The querying end of one connection to a Nexus socket, greeted with the
/// contract of its query `Q`, reading deliveries of its response `R`.
pub struct QueryingConnection<Q, R> {
    stream: UnixStream,
    ledger: ExchangeLedger,
    patience: Patience,
    capacity: FrameCapacity,
    contract: PhantomData<fn(Q) -> R>,
}

/// What the querying end of a connection does.
pub trait Querying: Sized {
    type Query;
    type Response;

    /// Greets a connected stream and waits for the Nexus to settle the
    /// contract.
    fn greeted(
        stream: UnixStream,
        patience: Patience,
        capacity: FrameCapacity,
    ) -> Result<Self, ExchangeError>;

    /// Opens an exchange carrying one query.
    fn open(&mut self, query: Self::Query) -> Result<ExchangeId, ExchangeError>;

    /// The next frame the Nexus delivers, or `None` when it closed the
    /// connection between frames.
    fn delivery(&mut self) -> Result<Option<Delivery<Self::Response>>, ExchangeError>;
}

impl<Q, R> Querying for QueryingConnection<Q, R>
where
    Q: Contracted,
    Dispatch<Q>: Signalizable,
    R: rkyv::Archive,
    Delivery<R>: rkyv::Archive,
    Signal<Delivery<R>>: Restorable<Delivery<R>>,
{
    type Query = Q;
    type Response = R;

    fn greeted(
        stream: UnixStream,
        patience: Patience,
        capacity: FrameCapacity,
    ) -> Result<Self, ExchangeError> {
        let mut connection = Self {
            stream,
            ledger: ExchangeLedger::default(),
            patience,
            capacity,
            contract: PhantomData,
        };
        connection.dispatch(&Dispatch::Greet(Q::greeting()))?;
        match connection.delivery()? {
            Some(Delivery::Greeted(HandshakeReceipt::Greeted(_))) => {
                connection.ledger.greet()?;
                Ok(connection)
            }
            Some(Delivery::Greeted(refused)) => Err(ExchangeError::GreetingRefused(refused)),
            Some(Delivery::End(ending)) => match ending.conclusion {
                signal::Conclusion::Faulted(fault) => Err(ExchangeError::Fault(fault)),
                signal::Conclusion::Completed => {
                    Err(ExchangeError::Unexpected("the Nexus ended the connection"))
                }
            },
            Some(Delivery::Answer(_)) => Err(ExchangeError::Unexpected(
                "the Nexus answered before settling the contract",
            )),
            None => Err(ExchangeError::Unexpected(
                "the Nexus closed before settling the contract",
            )),
        }
    }

    fn open(&mut self, query: Q) -> Result<ExchangeId, ExchangeError> {
        let exchange = self.ledger.open()?;
        self.dispatch(&Dispatch::Open(Opening { exchange, query }))?;
        Ok(exchange)
    }

    fn delivery(&mut self) -> Result<Option<Delivery<R>>, ExchangeError> {
        let body = match self.patience {
            Patience::Progress(bound) => self.stream.read_frame_after_idle(bound, self.capacity)?,
            Patience::Until(_) => {
                let mut input =
                    DeadlineStream::new(self.stream.try_clone()?, self.patience.remaining()?)?;
                input.read_frame_or_end(self.capacity)?
            }
        };
        body.map(|body| body.restored()).transpose()
    }
}

/// One exchange of a querying connection, read to its end.
pub trait ExchangeFollowing: Querying {
    /// Hands each answer of `exchange` to `answered`, in order, until the
    /// Nexus ends the exchange: an end Completed is `Ok`. A fault, the
    /// connection closed before the end, or a delivery for another exchange
    /// is the error, as is the first error `answered` returns.
    fn follow<Failure>(
        &mut self,
        exchange: ExchangeId,
        answered: impl FnMut(Self::Response) -> Result<(), Failure>,
    ) -> Result<(), Failure>
    where
        Failure: From<ExchangeError>;
}

impl<Connection: Querying> ExchangeFollowing for Connection {
    fn follow<Failure>(
        &mut self,
        exchange: ExchangeId,
        mut answered: impl FnMut(Self::Response) -> Result<(), Failure>,
    ) -> Result<(), Failure>
    where
        Failure: From<ExchangeError>,
    {
        loop {
            match self.delivery()? {
                Some(Delivery::Answer(answer)) if answer.exchange == exchange => {
                    answered(answer.response)?;
                }
                Some(Delivery::End(ending))
                    if ending.exchange == exchange || ending.is_connection_wide() =>
                {
                    return match ending.conclusion {
                        Conclusion::Completed => Ok(()),
                        Conclusion::Faulted(fault) => Err(ExchangeError::Fault(fault).into()),
                    };
                }
                Some(_) => {
                    return Err(ExchangeError::Unexpected(
                        "the Nexus delivered on another exchange",
                    )
                    .into());
                }
                None => {
                    return Err(ExchangeError::Unexpected(
                        "the Nexus closed before ending the exchange",
                    )
                    .into());
                }
            }
        }
    }
}

impl<Q, R> QueryingConnection<Q, R>
where
    Dispatch<Q>: Signalizable,
{
    fn dispatch(&self, dispatch: &Dispatch<Q>) -> Result<(), ExchangeError> {
        self.stream
            .write_value(dispatch, self.patience.remaining()?, self.capacity)
    }
}
