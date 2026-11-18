//! The meta socket's connections, served on the runtime as the public
//! socket's are.
//!
//! A meta connection is a public connection whose only peer is the paired
//! translator and whose bounds are the bootstrap admission's: every exchange
//! on a task of its own, answered once or with every frame of a stream, and
//! ended with `Delivery::End`. The bodies and the serving are hand-written:
//! [`MetaAnswering::answer`] yields [`Reply`], generic over the Nexus's own
//! types, which Ethos cannot name.

use std::future::Future;
use std::marker::PhantomData;
use std::os::unix::net::UnixStream;
use std::sync::Arc;
use std::time::Duration;

use nexus_exchange::{Named, ValidAdmission};
use shared_translator::Caller;
use signal::{Contracted, Delivery, Dispatch, FrameCapacity, Restorable, Signal, Signalizable};

use crate::closing::SubscriptionClosing;
use crate::public::{ConnectionServing, PublicConnection, PublicServing, Reply, TransportBounded};

/// The bodies a Nexus writes for its meta socket.
pub trait MetaAnswering: Send + Sync + 'static {
    type Component: Named;
    /// The query of every meta exchange, the call its contract declares.
    type Query: Contracted + Send + 'static;
    /// The response every answer on a meta exchange carries.
    type Response: Send + 'static;
    /// The state a meta subscription opens with: the configuration, as a
    /// Nexus answers a Subscribe of it.
    type State: Send + 'static;
    /// One change to that state.
    type Change: Send + 'static;

    /// Answers one meta query, once or with a stream that carries each
    /// change to the configuration until it finishes. It runs on the
    /// runtime's blocking pool, so a slow Configure holds no other exchange
    /// back.
    fn answer(&self, query: Self::Query) -> Reply<Self::Response, Self::State, Self::Change>;

    /// The response that carries one frame of a meta subscription's stream.
    fn subscribed(frame: signal::Frame<Self::State, Self::Change>) -> Self::Response;
}

/// One accepted meta connection, from the paired translator.
pub struct MetaConnection<Answering> {
    answering: PhantomData<fn() -> Answering>,
}

pub trait MetaServing {
    type Answering: MetaAnswering;

    /// Serves an accepted meta connection until the translator closes it or
    /// breaks the protocol, each frame within the admission's deadline and
    /// capacity; after the greeting the connection may be quiet for as long
    /// as the translator likes, and a subscription open on it goes on
    /// answering until its stream finishes.
    fn serve(
        stream: UnixStream,
        answering: Arc<Self::Answering>,
        admission: ValidAdmission,
    ) -> impl Future<Output = ()> + Send + 'static;
}

impl<Answering> MetaServing for MetaConnection<Answering>
where
    Answering: MetaAnswering,
    Dispatch<Answering::Query>: rkyv::Archive,
    Signal<Dispatch<Answering::Query>>: Restorable<Dispatch<Answering::Query>>,
    Delivery<Answering::Response>: Signalizable,
{
    type Answering = Answering;

    fn serve(
        stream: UnixStream,
        answering: Arc<Answering>,
        admission: ValidAdmission,
    ) -> impl Future<Output = ()> + Send + 'static {
        let bounds = Arc::new(MetaBounds { admission });
        PublicConnection::<MetaSide<Answering>>::serve(
            stream,
            (),
            MetaSide {
                answering,
                bounds: Arc::clone(&bounds),
            },
            bounds,
        )
    }
}

/// The bounds a meta connection is served within: the bootstrap admission's,
/// which hold before the Nexus is configured.
struct MetaBounds {
    admission: ValidAdmission,
}

impl TransportBounded for MetaBounds {
    fn transport_timeout(&self) -> Duration {
        self.admission.deadline
    }

    fn frame_capacity(&self) -> FrameCapacity {
        FrameCapacity::from(self.admission.frame_capacity)
    }
}

/// A Nexus's meta bodies as the connection serving reads them: the paired
/// translator is its only peer, admitted for every exchange, and every
/// subscription is carried until its stream finishes or the translator
/// abandons it.
struct MetaSide<Answering> {
    answering: Arc<Answering>,
    bounds: Arc<MetaBounds>,
}

impl<Answering> Clone for MetaSide<Answering> {
    fn clone(&self) -> Self {
        Self {
            answering: Arc::clone(&self.answering),
            bounds: Arc::clone(&self.bounds),
        }
    }
}

impl<Answering: MetaAnswering> PublicServing for MetaSide<Answering> {
    type Component = Answering::Component;
    type Query = Answering::Query;
    type Response = Answering::Response;
    type Output = Answering::Response;
    type State = Answering::State;
    type Change = Answering::Change;
    type Configuration = MetaBounds;
    type Authority = ();
    type Grant = ();
    type Input = Answering::Query;

    fn configuration(&self) -> Option<Arc<MetaBounds>> {
        Some(Arc::clone(&self.bounds))
    }

    fn admitted(
        &self,
        _authority: &(),
        _caller: &Caller,
        query: Self::Query,
    ) -> Option<((), Self::Query)> {
        Some(((), query))
    }

    fn answer(
        &self,
        _grant: (),
        input: Self::Query,
        _configuration: Arc<MetaBounds>,
    ) -> Reply<Self::Response, Self::State, Self::Change> {
        self.answering.answer(input)
    }

    fn carried(&self, _grant: &(), _closing: &Arc<SubscriptionClosing>) -> bool {
        true
    }

    fn answered(output: Self::Response) -> Self::Response {
        output
    }

    fn subscribed(frame: signal::Frame<Self::State, Self::Change>) -> Self::Response {
        Answering::subscribed(frame)
    }
}
