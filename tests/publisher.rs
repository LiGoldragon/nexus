//! The subscription publisher: a stream opens on the state as it stands and
//! follows each change after it, a subscriber that falls behind is handed the
//! state anew and then only what follows it, and no subscriber holds the
//! publisher up.

use futures_util::StreamExt;
use nexus::{Capacity, CapacityRefusal, Following, Publisher, Publishing, Reached, Subscribable};
use signal::Frame;

/// A state that remembers every change folded into it, so a change received
/// twice or missed shows in the comparison.
#[derive(Clone, Debug, PartialEq, Eq)]
struct Ledger(Vec<u32>);

impl Following for Ledger {
    type Change = u32;

    fn after(state: Self, change: u32) -> Self {
        let mut entries = state.0;
        entries.push(change);
        Ledger(entries)
    }
}

/// A subscriber's own copy of the state, built only from the frames it read.
fn fold(frames: &[Frame<Ledger, u32>]) -> Ledger {
    frames
        .iter()
        .fold(Ledger(Vec::new()), |state, frame| match frame {
            Frame::Opened(standing) | Frame::Lagged(standing) => standing.clone(),
            Frame::Changed(change) => Ledger::after(state, *change),
        })
}

#[tokio::test]
async fn a_stream_opens_on_the_initial_state() {
    let publisher = Publisher::<Ledger, u32>::create(Ledger(vec![7]), Capacity(8))
        .expect("a capacity of at least one");
    let mut stream = publisher.open();
    assert_eq!(stream.next().await, Some(Frame::Opened(Ledger(vec![7]))));
}

#[tokio::test]
async fn changes_after_open_arrive_in_order() {
    let publisher = Publisher::<Ledger, u32>::create(Ledger(Vec::new()), Capacity(8))
        .expect("a capacity of at least one");
    let mut stream = publisher.open();
    for change in [1, 2, 3] {
        publisher.publish(change);
    }
    assert_eq!(stream.next().await, Some(Frame::Opened(Ledger(Vec::new()))));
    for change in [1, 2, 3] {
        assert_eq!(stream.next().await, Some(Frame::Changed(change)));
    }
    assert_eq!(publisher.state(), Ledger(vec![1, 2, 3]));
}

#[tokio::test]
async fn a_stream_opened_later_opens_on_the_folded_state() {
    let publisher = Publisher::<Ledger, u32>::create(Ledger(Vec::new()), Capacity(8))
        .expect("a capacity of at least one");
    publisher.publish(1);
    publisher.publish(2);
    let mut stream = publisher.open();
    publisher.publish(3);
    assert_eq!(stream.next().await, Some(Frame::Opened(Ledger(vec![1, 2]))));
    assert_eq!(stream.next().await, Some(Frame::Changed(3)));
}

#[test]
fn publish_reaches_each_open_stream() {
    let publisher = Publisher::<Ledger, u32>::create(Ledger(Vec::new()), Capacity(8))
        .expect("a capacity of at least one");
    assert_eq!(publisher.publish(1), Reached(0));
    let first = publisher.open();
    let second = publisher.open();
    assert_eq!(publisher.publish(2), Reached(2));
    drop(first);
    assert_eq!(publisher.publish(3), Reached(1));
    drop(second);
    assert_eq!(publisher.publish(4), Reached(0));
}

#[tokio::test]
async fn a_lagging_subscriber_is_handed_the_state_then_only_what_follows_it() {
    let publisher = Publisher::<Ledger, u32>::create(Ledger(Vec::new()), Capacity(2))
        .expect("a capacity of at least one");
    let mut stream = publisher.open();
    for change in 1..=5 {
        publisher.publish(change);
    }
    let mut frames = Vec::new();
    for _ in 0..2 {
        frames.push(stream.next().await.expect("a frame"));
    }
    assert_eq!(frames[0], Frame::Opened(Ledger(Vec::new())));
    assert_eq!(frames[1], Frame::Lagged(Ledger(vec![1, 2, 3, 4, 5])));
    publisher.publish(6);
    publisher.publish(7);
    for _ in 0..2 {
        frames.push(stream.next().await.expect("a frame"));
    }
    assert_eq!(frames[2..], [Frame::Changed(6), Frame::Changed(7)]);
    assert_eq!(fold(&frames), publisher.state());
    drop(publisher);
    assert_eq!(stream.next().await, None);
}

#[tokio::test]
async fn dropping_the_publisher_ends_each_stream() {
    let publisher = Publisher::<Ledger, u32>::create(Ledger(Vec::new()), Capacity(8))
        .expect("a capacity of at least one");
    let mut first = publisher.open();
    let mut second = publisher.open();
    drop(publisher);
    for stream in [&mut first, &mut second] {
        assert_eq!(stream.next().await, Some(Frame::Opened(Ledger(Vec::new()))));
        assert_eq!(stream.next().await, None);
    }
}

#[tokio::test]
async fn a_slow_subscriber_holds_up_neither_the_publisher_nor_another_subscriber() {
    let publisher = Publisher::<Ledger, u32>::create(Ledger(Vec::new()), Capacity(2))
        .expect("a capacity of at least one");
    let mut slow = publisher.open();
    let mut prompt = publisher.open();
    assert_eq!(prompt.next().await, Some(Frame::Opened(Ledger(Vec::new()))));
    for change in 1..=100 {
        assert_eq!(publisher.publish(change), Reached(2));
        assert_eq!(prompt.next().await, Some(Frame::Changed(change)));
    }
    let standing = publisher.state();
    assert_eq!(standing.0.len(), 100);
    assert_eq!(slow.next().await, Some(Frame::Opened(Ledger(Vec::new()))));
    assert_eq!(slow.next().await, Some(Frame::Lagged(standing)));
    drop(publisher);
    assert_eq!(slow.next().await, None);
    assert_eq!(prompt.next().await, None);
}

#[test]
fn a_capacity_below_one_is_refused() {
    assert!(matches!(
        Publisher::<Ledger, u32>::create(Ledger(Vec::new()), Capacity(0)),
        Err(CapacityRefusal::Zero)
    ));
    assert!(matches!(
        Publisher::<Ledger, u32>::create(Ledger(Vec::new()), Capacity(-1)),
        Err(CapacityRefusal::Negative)
    ));
}
