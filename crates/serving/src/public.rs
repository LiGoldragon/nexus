//! The public socket's connections.

use std::future::Future;
use std::os::unix::net::UnixStream;
use std::sync::{Arc, Mutex};
use std::time::Duration;

use nexus_exchange::lane::PeerCalling;
use nexus_exchange::{ExchangeError, Named, ValueRestoring};
use shared_translator::Caller;
use signal::{
    Contracted, Delivery, Dispatch, Ending, ExchangeFault, ExchangeId, ExchangeLedger,
    ExchangeTracking, Exchanged, FrameCapacity, FrameError, Greeted, HandshakeReceipt, Opening,
    Restorable, Signal, Signalizable,
};

use crate::asynchronous::{AsyncFrameReading, AsyncValueWriting};
use crate::closing::{Closable, SubscriptionClosing};
use crate::generated::{Closing, Continuation};

/// The bounds a configured Nexus serves its public socket within.
pub trait TransportBounded {
    /// The deadline every frame is written within, and the rest of a begun
    /// frame read within.
    fn transport_timeout(&self) -> Duration;

    fn frame_capacity(&self) -> FrameCapacity;
}

/// How a public exchange is answered: once, or with every frame of a stream
/// until it finishes.
pub enum Reply<Output, State, Change> {
    Once(Output),
    Subscribed(signal::Stream<State, Change>),
}

/// The bodies a Nexus writes for its public socket.
pub trait PublicServing: Clone + Send + Sync + 'static {
    type Component: Named;
    /// The query of every exchange, the call its contract declares.
    type Query: Contracted + Send + 'static;
    /// The response every answer on an exchange carries.
    type Response: Send + 'static;
    /// What answering one query yields.
    type Output: Send + 'static;
    type State: Send + 'static;
    type Change: Send + 'static;
    /// The configuration the Nexus is serving under, read anew for each
    /// frame.
    type Configuration: TransportBounded + Send + Sync + 'static;
    /// What a connection was admitted as when it was accepted.
    type Authority: Send + Sync + 'static;
    /// Who an exchange is answered for.
    type Grant: Clone + Send + Sync + 'static;
    /// The query an admitted exchange answers.
    type Input: Send + 'static;

    /// The configuration as it stands, or `None` once it is gone.
    fn configuration(&self) -> Option<Arc<Self::Configuration>>;

    /// Admits one exchange of a connection admitted under `authority` from
    /// `caller`, the peer's own credentials. `None` ends the connection
    /// unanswered.
    fn admitted(
        &self,
        authority: &Self::Authority,
        caller: &Caller,
        query: Self::Query,
    ) -> Option<(Self::Grant, Self::Input)>;

    /// Answers one admitted exchange. It runs on the runtime's blocking pool,
    /// so a slow attempt holds no other exchange back.
    fn answer(
        &self,
        grant: Self::Grant,
        input: Self::Input,
        configuration: Arc<Self::Configuration>,
    ) -> Reply<Self::Output, Self::State, Self::Change>;

    /// Registers a subscription opened for `grant`, which the Nexus may close
    /// later; `false` refuses it and ends the connection.
    fn carried(&self, grant: &Self::Grant, closing: &Arc<SubscriptionClosing>) -> bool;

    /// The response an answer carries.
    fn answered(output: Self::Output) -> Self::Response;

    /// The answer that carries one frame of a subscription's stream.
    fn subscribed(frame: signal::Frame<Self::State, Self::Change>) -> Self::Output;
}

/// One accepted public connection: one greeting, then any number of
/// exchanges.
///
/// The connection is served on the runtime. The authority is the
/// connection's, taken from its peer credentials at accept. Each exchange is
/// answered by a task of its own, its attempt on the blocking pool; a
/// subscription goes on answering on its exchange from its stream's task,
/// woken by the next frame or by its end. Every frame is written whole, one
/// at a time, within the configured transport timeout.
pub struct PublicConnection<Nexus: PublicServing> {
    writing: tokio::sync::Mutex<tokio::net::unix::OwnedWriteHalf>,
    ledger: Mutex<ExchangeLedger>,
    /// The subscriptions open on this connection, or `None` once the
    /// connection has ended.
    subscriptions: Mutex<Option<Vec<CarriedSubscription>>>,
    /// Set once the connection ends, which wakes its reading.
    ended: tokio::sync::watch::Sender<bool>,
    authority: Nexus::Authority,
    caller: Caller,
}

/// A subscription open on an exchange of a connection.
struct CarriedSubscription {
    exchange: ExchangeId,
    closing: Arc<SubscriptionClosing>,
}

pub trait ConnectionServing {
    type Nexus: PublicServing;

    /// Serves an accepted connection until the peer closes it or breaks the
    /// protocol. The greeting must arrive within the transport timeout;
    /// after it the connection may be quiet for as long as the peer likes.
    fn serve(
        stream: UnixStream,
        authority: <Self::Nexus as PublicServing>::Authority,
        nexus: Self::Nexus,
        configuration: Arc<<Self::Nexus as PublicServing>::Configuration>,
    ) -> impl Future<Output = ()> + Send + 'static;
}

impl<Nexus> ConnectionServing for PublicConnection<Nexus>
where
    Nexus: PublicServing,
    Dispatch<Nexus::Query>: rkyv::Archive,
    Signal<Dispatch<Nexus::Query>>: Restorable<Dispatch<Nexus::Query>>,
    Delivery<Nexus::Response>: Signalizable,
{
    type Nexus = Nexus;

    async fn serve(
        stream: UnixStream,
        authority: Nexus::Authority,
        nexus: Nexus,
        configuration: Arc<Nexus::Configuration>,
    ) {
        let title = Nexus::Component::title().0;
        let caller = match stream.peer_caller() {
            Ok(caller) => caller,
            Err(error) => {
                eprintln!("{title} public peer lookup failed: {error}");
                return;
            }
        };
        let stream = match stream
            .set_nonblocking(true)
            .and_then(|()| tokio::net::UnixStream::from_std(stream))
        {
            Ok(stream) => stream,
            Err(error) => {
                eprintln!("{title} public connection failed: {error}");
                return;
            }
        };
        let (mut reading, writing) = stream.into_split();
        let connection = Arc::new(Self {
            writing: tokio::sync::Mutex::new(writing),
            ledger: Mutex::new(ExchangeLedger::default()),
            subscriptions: Mutex::new(Some(Vec::new())),
            ended: tokio::sync::watch::Sender::new(false),
            authority,
            caller,
        });
        if let Continuation::Going = connection.greeted(&mut reading, &configuration).await {
            connection.listen(&mut reading, &nexus).await;
        }
        connection.end().await;
    }
}

impl<Nexus> PublicConnection<Nexus>
where
    Nexus: PublicServing,
    Dispatch<Nexus::Query>: rkyv::Archive,
    Signal<Dispatch<Nexus::Query>>: Restorable<Dispatch<Nexus::Query>>,
    Delivery<Nexus::Response>: Signalizable,
{
    async fn greeted(
        &self,
        reading: &mut tokio::net::unix::OwnedReadHalf,
        configuration: &Nexus::Configuration,
    ) -> Continuation {
        let read = reading
            .read_frame_within_async(
                configuration.transport_timeout(),
                configuration.frame_capacity(),
            )
            .await;
        let Ok(Some(body)) = read else {
            return Continuation::Ended;
        };
        match body.restored::<Dispatch<Nexus::Query>>() {
            Ok(Dispatch::Greet(greeting)) => {
                let receipt = <Nexus::Query as Contracted>::receipt(&greeting);
                let settled = matches!(receipt, HandshakeReceipt::Greeted(_));
                if self
                    .deliver(Delivery::Greeted(receipt), configuration)
                    .await
                    .is_err()
                    || !settled
                {
                    return Continuation::Ended;
                }
                let _ = self
                    .ledger
                    .lock()
                    .unwrap_or_else(|p| p.into_inner())
                    .greet();
                Continuation::Going
            }
            Ok(_) => {
                self.faulted(
                    Ending::connection_faulted(ExchangeFault::GreetingExpected),
                    configuration,
                )
                .await
            }
            Err(_) => {
                self.faulted(
                    Ending::connection_faulted(ExchangeFault::UnreadableQuery),
                    configuration,
                )
                .await
            }
        }
    }

    /// Reads the greeted connection's frames until it ends.
    async fn listen(
        self: &Arc<Self>,
        reading: &mut tokio::net::unix::OwnedReadHalf,
        nexus: &Nexus,
    ) {
        let mut ended = self.ended.subscribe();
        loop {
            let Some(configuration) = nexus.configuration() else {
                return;
            };
            let read = tokio::select! {
                biased;
                _ = ended.changed() => return,
                read = reading.read_frame_after_idle_async(
                    configuration.transport_timeout(),
                    configuration.frame_capacity(),
                ) => read,
            };
            let body = match read {
                Ok(Some(body)) => body,
                Ok(None) | Err(FrameError::Io(_)) => return,
                Err(FrameError::BodyTooLarge { .. }) => {
                    self.faulted(
                        Ending::connection_faulted(ExchangeFault::UnreadableQuery),
                        &configuration,
                    )
                    .await;
                    return;
                }
            };
            let continuation = match body.restored::<Dispatch<Nexus::Query>>() {
                Err(_) => {
                    self.faulted(
                        Ending::connection_faulted(ExchangeFault::UnreadableQuery),
                        &configuration,
                    )
                    .await
                }
                Ok(Dispatch::Greet(_)) => {
                    self.faulted(
                        Ending::connection_faulted(ExchangeFault::GreetingRepeated),
                        &configuration,
                    )
                    .await
                }
                Ok(Dispatch::Open(opening)) => self.opened(nexus, opening, configuration).await,
                Ok(Dispatch::Abandon(exchange)) => self.abandoned(exchange, &configuration).await,
            };
            if let Continuation::Ended = continuation {
                return;
            }
        }
    }

    async fn opened(
        self: &Arc<Self>,
        nexus: &Nexus,
        opening: Opening<Nexus::Query>,
        configuration: Arc<Nexus::Configuration>,
    ) -> Continuation {
        let exchange = opening.exchange;
        let admitted = self
            .ledger
            .lock()
            .unwrap_or_else(|p| p.into_inner())
            .admit(exchange);
        if let Err(fault) = admitted {
            let ending = Ending::faulted(exchange, fault);
            if ending.is_connection_wide() {
                return self.faulted(ending, &configuration).await;
            }
            return match self.deliver(Delivery::End(ending), &configuration).await {
                Ok(()) => Continuation::Going,
                Err(_) => Continuation::Ended,
            };
        }
        // The authority is the connection's: the Nexus admits every exchange
        // under it again, and an exchange it does not admit ends the
        // connection unanswered.
        let Some((grant, input)) = nexus.admitted(&self.authority, &self.caller, opening.query)
        else {
            return Continuation::Ended;
        };
        tokio::spawn(Arc::clone(self).exchanged(
            nexus.clone(),
            exchange,
            grant,
            input,
            configuration,
        ));
        Continuation::Going
    }

    /// Answers one exchange: its attempt runs on the blocking pool, and its
    /// answer is given once or as the stream's frames.
    async fn exchanged(
        self: Arc<Self>,
        nexus: Nexus,
        exchange: ExchangeId,
        grant: Nexus::Grant,
        input: Nexus::Input,
        configuration: Arc<Nexus::Configuration>,
    ) {
        let answering = nexus.clone();
        let attempted = Arc::clone(&configuration);
        let attempting = grant.clone();
        let answer =
            tokio::task::spawn_blocking(move || answering.answer(attempting, input, attempted))
                .await;
        match answer {
            Ok(Reply::Once(output)) => self.answer(exchange, output, &configuration).await,
            Ok(Reply::Subscribed(stream)) => self.subscribed(&nexus, exchange, grant, stream).await,
            Err(error) => {
                eprintln!(
                    "{} public attempt failed: {error}",
                    Nexus::Component::title().0
                );
                self.end().await;
            }
        }
    }

    /// Carries a subscription's stream on its exchange, once the Nexus has
    /// registered it.
    async fn subscribed(
        self: &Arc<Self>,
        nexus: &Nexus,
        exchange: ExchangeId,
        grant: Nexus::Grant,
        stream: signal::Stream<Nexus::State, Nexus::Change>,
    ) {
        let closing = Arc::new(SubscriptionClosing::default());
        if !nexus.carried(&grant, &closing) {
            self.end().await;
            return;
        }
        self.carry(exchange, &closing);
        self.follow(nexus, exchange, stream, closing).await;
    }

    /// Sends each frame of the stream as one answer on its exchange, until
    /// the stream finishes or the subscription closes.
    async fn follow(
        &self,
        nexus: &Nexus,
        exchange: ExchangeId,
        mut stream: signal::Stream<Nexus::State, Nexus::Change>,
        closing: Arc<SubscriptionClosing>,
    ) {
        use futures_util::StreamExt;
        let concluded = loop {
            let frame = tokio::select! {
                biased;
                closed = closing.closed() => break closed,
                frame = stream.next() => frame,
            };
            let Some(frame) = frame else {
                break Closing::Finished;
            };
            let Some(configuration) = nexus.configuration() else {
                break Closing::Withdrawn;
            };
            let output = Nexus::subscribed(frame);
            if self.stream(exchange, output, &configuration).await.is_err() {
                break Closing::Withdrawn;
            }
        };
        drop(stream);
        self.conclude(exchange, concluded, nexus).await;
    }

    async fn abandoned(
        &self,
        exchange: ExchangeId,
        configuration: &Nexus::Configuration,
    ) -> Continuation {
        let released = self
            .ledger
            .lock()
            .unwrap_or_else(|p| p.into_inner())
            .release(exchange);
        match released {
            Ok(_) => {
                if let Some(closing) = self.withdrawn(exchange) {
                    closing.close(Closing::Abandoned);
                }
                Continuation::Going
            }
            Err(fault) => {
                let ending = Ending::faulted(exchange, fault);
                if ending.is_connection_wide() {
                    return self.faulted(ending, configuration).await;
                }
                match self.deliver(Delivery::End(ending), configuration).await {
                    Ok(()) => Continuation::Going,
                    Err(_) => Continuation::Ended,
                }
            }
        }
    }

    /// Answers a one-answer exchange, which ends with it. An exchange the
    /// peer abandoned meanwhile is not answered.
    async fn answer(
        &self,
        exchange: ExchangeId,
        output: Nexus::Output,
        configuration: &Nexus::Configuration,
    ) {
        let released = self
            .ledger
            .lock()
            .unwrap_or_else(|p| p.into_inner())
            .release(exchange);
        if released.is_ok()
            && self
                .deliver(Self::answered(exchange, output), configuration)
                .await
                .is_err()
        {
            self.end().await;
        }
    }

    /// Sends one more answer on a subscription's exchange.
    async fn stream(
        &self,
        exchange: ExchangeId,
        output: Nexus::Output,
        configuration: &Nexus::Configuration,
    ) -> Result<(), ExchangeError> {
        let bears = self
            .ledger
            .lock()
            .unwrap_or_else(|p| p.into_inner())
            .bears(exchange);
        if !bears {
            return Ok(());
        }
        self.deliver(Self::answered(exchange, output), configuration)
            .await
    }

    /// Records a subscription on its exchange. A connection that has already
    /// ended abandons it at once.
    fn carry(&self, exchange: ExchangeId, closing: &Arc<SubscriptionClosing>) {
        match self
            .subscriptions
            .lock()
            .unwrap_or_else(|p| p.into_inner())
            .as_mut()
        {
            Some(subscriptions) => subscriptions.push(CarriedSubscription {
                exchange,
                closing: Arc::clone(closing),
            }),
            None => closing.close(Closing::Abandoned),
        }
    }

    fn withdrawn(&self, exchange: ExchangeId) -> Option<Arc<SubscriptionClosing>> {
        let mut subscriptions = self.subscriptions.lock().unwrap_or_else(|p| p.into_inner());
        let subscriptions = subscriptions.as_mut()?;
        let at = subscriptions
            .iter()
            .position(|carried| carried.exchange == exchange)?;
        Some(subscriptions.remove(at).closing)
    }

    /// Concludes a subscription as its closing says: a stream that finished
    /// ends its exchange Completed, and a subscriber whose authority is gone
    /// has its connection closed.
    async fn conclude(&self, exchange: ExchangeId, closing: Closing, nexus: &Nexus) {
        self.withdrawn(exchange);
        let released = self
            .ledger
            .lock()
            .unwrap_or_else(|p| p.into_inner())
            .release(exchange);
        match closing {
            Closing::Abandoned => {}
            Closing::Finished => {
                let Some(configuration) = nexus.configuration() else {
                    return self.end().await;
                };
                let ending = Delivery::End(Ending::completed(exchange));
                if released.is_ok() && self.deliver(ending, &configuration).await.is_err() {
                    self.end().await;
                }
            }
            Closing::Withdrawn => self.end().await,
        }
    }

    fn answered(exchange: ExchangeId, output: Nexus::Output) -> Delivery<Nexus::Response> {
        Delivery::Answer(signal::Answer {
            exchange,
            response: Nexus::answered(output),
        })
    }

    /// Ends the connection with a fault it names.
    async fn faulted(&self, ending: Ending, configuration: &Nexus::Configuration) -> Continuation {
        let _ = self.deliver(Delivery::End(ending), configuration).await;
        Continuation::Ended
    }

    async fn deliver(
        &self,
        delivery: Delivery<Nexus::Response>,
        configuration: &Nexus::Configuration,
    ) -> Result<(), ExchangeError> {
        let mut writing = self.writing.lock().await;
        writing
            .write_value_async(
                &delivery,
                configuration.transport_timeout(),
                configuration.frame_capacity(),
            )
            .await
    }

    /// Ends the connection: every subscription on it is abandoned, its
    /// reading stops and the peer reads the end of the stream.
    async fn end(&self) {
        use tokio::io::AsyncWriteExt;
        let carried = self
            .subscriptions
            .lock()
            .unwrap_or_else(|p| p.into_inner())
            .take();
        for carried in carried.into_iter().flatten() {
            carried.closing.close(Closing::Abandoned);
        }
        self.ended.send_replace(true);
        let _ = self.writing.lock().await.shutdown().await;
    }
}
