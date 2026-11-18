//! A Nexus that writes only its bodies is served whole: its meta socket and
//! its public socket each answer once or with every frame of a stream, and
//! every exchange then ends; a change the Nexus makes on one meta exchange
//! reaches a subscription open on another.

use std::sync::{Arc, Mutex};
use std::time::Duration;

use futures_channel::mpsc::{UnboundedSender, unbounded};
use nexus_exchange::{ComponentName, ComponentTitle, ExchangeError, Named, ValidAdmission};
use nexus_querying::{ExchangeFollowing, Patience, Querying, QueryingConnection};
use nexus_serving::{
    ConnectionServing, MetaAnswering, MetaConnection, MetaServing, PublicConnection, PublicServing,
    Reply, SubscriptionClosing, TransportBounded,
};
use signal::{Contracted, Delivery, FrameCapacity};

#[derive(rkyv::Archive, rkyv::Serialize, rkyv::Deserialize, Clone, Debug, PartialEq)]
enum Query {
    Count(u32),
    Follow,
    Set(u32),
}

#[derive(rkyv::Archive, rkyv::Serialize, rkyv::Deserialize, Clone, Debug, PartialEq)]
enum Response {
    Counted(u32),
    Followed(signal::Frame<u32, u32>),
}

impl Contracted for Query {
    const CONTRACT_SOURCE: &'static str = "Signal [] [ Count.Integer Follow Set.Integer ] [] []";
}

struct Counter;

impl Named for Counter {
    fn name() -> ComponentName {
        ComponentName("counter".into())
    }

    fn title() -> ComponentTitle {
        ComponentTitle("Counter Nexus".into())
    }
}

struct Bounds;

impl TransportBounded for Bounds {
    fn transport_timeout(&self) -> Duration {
        Duration::from_millis(500)
    }

    fn frame_capacity(&self) -> FrameCapacity {
        FrameCapacity::from(4096)
    }
}

#[derive(Clone)]
struct CounterNexus;

/// A Nexus's meta side: its one setting, and the subscriptions following it.
#[derive(Default)]
struct SettingNexus {
    setting: Mutex<u32>,
    following: Mutex<Vec<UnboundedSender<signal::Frame<u32, u32>>>>,
}

type Frames = std::pin::Pin<Box<dyn futures_util::Stream<Item = signal::Frame<u32, u32>> + Send>>;

/// A count is answered once; a follow opens on the setting and carries each
/// change a set makes, on whichever exchange it is made.
impl MetaAnswering for SettingNexus {
    type Component = Counter;
    type Query = Query;
    type Response = Response;
    type State = u32;
    type Change = u32;

    fn answer(&self, input: Query) -> Reply<Response, u32, u32> {
        match input {
            Query::Count(count) => Reply::Once(Response::Counted(count * 2)),
            Query::Follow => {
                let setting = self.setting.lock().expect("setting");
                let (sender, frames) = unbounded();
                sender
                    .unbounded_send(signal::Frame::Opened(*setting))
                    .expect("opened");
                self.following.lock().expect("following").push(sender);
                Reply::Subscribed(signal::Stream::from(Box::pin(frames) as Frames))
            }
            Query::Set(value) => {
                let mut setting = self.setting.lock().expect("setting");
                *setting = value;
                self.following
                    .lock()
                    .expect("following")
                    .retain(|sender| sender.unbounded_send(signal::Frame::Changed(value)).is_ok());
                Reply::Once(Response::Counted(value))
            }
        }
    }

    fn subscribed(frame: signal::Frame<u32, u32>) -> Response {
        Response::Followed(frame)
    }
}

impl SettingNexus {
    /// Ends every subscription: each stream finishes, and its exchange ends.
    fn finish(&self) {
        self.following.lock().expect("following").clear();
    }
}

fn admission() -> ValidAdmission {
    ValidAdmission {
        deadline: Duration::from_millis(500),
        frame_capacity: 4096,
        retry_delay: Duration::from_millis(5),
    }
}

impl PublicServing for CounterNexus {
    type Component = Counter;
    type Query = Query;
    type Response = Response;
    type Output = Response;
    type State = u32;
    type Change = u32;
    type Configuration = Bounds;
    type Authority = ();
    type Grant = ();
    type Input = Query;

    fn configuration(&self) -> Option<Arc<Bounds>> {
        Some(Arc::new(Bounds))
    }

    fn admitted(
        &self,
        _authority: &(),
        _caller: &shared_translator::Caller,
        query: Query,
    ) -> Option<((), Query)> {
        Some(((), query))
    }

    fn answer(
        &self,
        _grant: (),
        input: Query,
        _configuration: Arc<Bounds>,
    ) -> Reply<Response, u32, u32> {
        match input {
            Query::Count(count) | Query::Set(count) => Reply::Once(Response::Counted(count + 1)),
            Query::Follow => {
                let frames = futures_util::stream::iter::<[signal::Frame<u32, u32>; 2]>([
                    signal::Frame::Opened(0),
                    signal::Frame::Changed(1),
                ]);
                Reply::Subscribed(signal::Stream::from(Box::pin(frames)
                    as std::pin::Pin<
                        Box<dyn futures_util::Stream<Item = signal::Frame<u32, u32>> + Send>,
                    >))
            }
        }
    }

    fn carried(&self, _grant: &(), _closing: &Arc<SubscriptionClosing>) -> bool {
        true
    }

    fn answered(output: Response) -> Response {
        output
    }

    fn subscribed(frame: signal::Frame<u32, u32>) -> Response {
        Response::Followed(frame)
    }
}

fn greeted(stream: std::os::unix::net::UnixStream) -> QueryingConnection<Query, Response> {
    QueryingConnection::<Query, Response>::greeted(
        stream,
        Patience::Progress(Duration::from_millis(500)),
        FrameCapacity::from(4096),
    )
    .expect("greeted")
}

/// The next delivery ends `exchange` Completed.
fn ended(querying: &mut QueryingConnection<Query, Response>, exchange: signal::ExchangeId) {
    match querying.delivery().expect("end") {
        Some(Delivery::End(ending)) => assert_eq!(ending, signal::Ending::completed(exchange)),
        other => panic!("the exchange ends after its answer, got {other:?}"),
    }
}

fn answer(delivery: Option<Delivery<Response>>) -> Response {
    match delivery {
        Some(Delivery::Answer(answer)) => answer.response,
        other => panic!("an answer was delivered, got {other:?}"),
    }
}

fn runtime() -> tokio::runtime::Runtime {
    tokio::runtime::Builder::new_multi_thread()
        .enable_all()
        .build()
        .expect("runtime")
}

#[test]
fn the_meta_socket_answers_a_query_once_then_ends_it() {
    let runtime = runtime();
    let (server, client) = std::os::unix::net::UnixStream::pair().expect("pair");
    runtime.spawn(MetaConnection::<SettingNexus>::serve(
        server,
        Arc::new(SettingNexus::default()),
        admission(),
    ));
    let mut querying = greeted(client);
    let counting = querying.open(Query::Count(4)).expect("open");
    assert_eq!(
        answer(querying.delivery().expect("delivery")),
        Response::Counted(8)
    );
    ended(&mut querying, counting);
    let again = querying.open(Query::Count(1)).expect("open again");
    let mut answers = Vec::new();
    querying
        .follow(again, |response| {
            answers.push(response);
            Ok::<_, ExchangeError>(())
        })
        .expect("followed to its end");
    assert_eq!(answers, [Response::Counted(2)]);
}

#[test]
fn a_meta_subscription_carries_each_change_made_on_another_connection_until_it_ends() {
    let runtime = runtime();
    let nexus = Arc::new(SettingNexus::default());
    let connected = || {
        let (server, client) = std::os::unix::net::UnixStream::pair().expect("pair");
        runtime.spawn(MetaConnection::<SettingNexus>::serve(
            server,
            Arc::clone(&nexus),
            admission(),
        ));
        greeted(client)
    };
    let mut following = connected();
    let subscription = following.open(Query::Follow).expect("follow");
    assert_eq!(
        answer(following.delivery().expect("opened")),
        Response::Followed(signal::Frame::Opened(0))
    );
    let mut setting = connected();
    let set = setting.open(Query::Set(7)).expect("set");
    assert_eq!(
        answer(setting.delivery().expect("set answered")),
        Response::Counted(7)
    );
    ended(&mut setting, set);
    assert_eq!(
        answer(following.delivery().expect("changed")),
        Response::Followed(signal::Frame::Changed(7))
    );
    nexus.finish();
    ended(&mut following, subscription);
}

#[test]
fn the_public_socket_answers_once_or_with_every_frame_then_ends_the_exchange() {
    let runtime = tokio::runtime::Builder::new_multi_thread()
        .enable_all()
        .build()
        .expect("runtime");
    let (server, client) = std::os::unix::net::UnixStream::pair().expect("pair");
    runtime.spawn(PublicConnection::<CounterNexus>::serve(
        server,
        (),
        CounterNexus,
        Arc::new(Bounds),
    ));
    let mut querying = greeted(client);
    let counting = querying.open(Query::Count(2)).expect("count");
    assert_eq!(
        answer(querying.delivery().expect("delivery")),
        Response::Counted(3)
    );
    ended(&mut querying, counting);
    let following = querying.open(Query::Follow).expect("follow");
    assert_eq!(
        answer(querying.delivery().expect("opened")),
        Response::Followed(signal::Frame::Opened(0))
    );
    assert_eq!(
        answer(querying.delivery().expect("changed")),
        Response::Followed(signal::Frame::Changed(1))
    );
    match querying.delivery().expect("end") {
        Some(Delivery::End(ending)) => assert_eq!(ending, signal::Ending::completed(following)),
        other => panic!("the finished stream ends its exchange, got {other:?}"),
    }
}
