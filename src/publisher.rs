//! The subscription publisher: one current state, and the streams that follow
//! it.
//!
//! A Nexus that answers a subscription holds its state in a [`Publisher`] and
//! reads it from there; it keeps no second copy. Each change it makes is
//! published: folded into the state with [`Following::after`], given the next
//! position, and handed to every open stream. A stream opens on the state as
//! it stands, then carries each change published after it, in order.
//!
//! No subscriber slows the Nexus or another subscriber, and none is dropped
//! for being slow. Publishing waits for no one: the changes ride a broadcast
//! channel of the configured [`Capacity`], and a subscriber that falls more
//! than that far behind is handed the state anew as `Lagged`, then the changes
//! that follow it. A position, the Publisher's own counter and never a wire
//! value, is what makes the replacement exact: after a state at position `p`,
//! the stream drops every change at or before `p`, so the subscriber receives
//! each change it has not yet seen once. The changes still waiting for a
//! lagging subscriber are all folded into the state it is handed, so they are
//! drained with it, and the subscriber has the whole capacity again for what
//! follows.
//!
//! The Publisher's fields are its own and it is generic over its state and
//! change, so it is written here by hand; every trait it bears, and the
//! [`Capacity`] and [`Reached`] it speaks in, are declared in
//! `ethos/library.ethos`.

use std::pin::Pin;
use std::sync::{Arc, Mutex, MutexGuard};

use futures_util::stream::{self, StreamExt};
use signal::Frame;
use signal::generated::Ending;
use tokio::sync::broadcast::{self, error::RecvError};

use crate::generated::{Capacity, Following, Publishing, Reached, Subscribable};

/// The one current published state of a subscription, and the channel its
/// changes are handed to the open streams on.
pub struct Publisher<State, Change> {
    /// The position of the last change folded in, and the state after it.
    /// Held together so that a reader takes both at once, and empty only
    /// while a change is being folded in.
    published: Arc<Mutex<Option<(u64, State)>>>,
    /// Each change with its position, to every open stream.
    changes: broadcast::Sender<(u64, Change)>,
}

impl<State, Change> Publisher<State, Change>
where
    State: Following<Change = Change> + Clone + Send + Sync + 'static,
    Change: Clone + Send + Sync + 'static,
{
    /// A publisher of `state`, whose streams may fall `capacity` changes
    /// behind before they are handed the state anew.
    ///
    /// # Panics
    ///
    /// When `capacity` is below one: a stream that can hold no change can
    /// follow none.
    pub fn new(state: State, capacity: Capacity) -> Self {
        let capacity = usize::try_from(capacity.0).expect("a publisher's capacity is at least one");
        let (changes, _) = broadcast::channel(capacity);
        Self {
            published: Arc::new(Mutex::new(Some((0, state)))),
            changes,
        }
    }
}

/// The published state and its position, locked against publishing until the
/// guard is dropped.
fn hold<State>(published: &Mutex<Option<(u64, State)>>) -> MutexGuard<'_, Option<(u64, State)>> {
    published
        .lock()
        .expect("a publisher's state is poisoned only by a fold that panicked")
}

/// A copy of the state the guard holds, with its position.
fn standing<State: Clone>(published: &Option<(u64, State)>) -> (u64, State) {
    published
        .clone()
        .expect("a publisher's state is taken only while a change is folded in")
}

/// The published state as it stands, with its position.
fn current<State: Clone>(published: &Mutex<Option<(u64, State)>>) -> (u64, State) {
    standing(&hold(published))
}

impl<State, Change> Subscribable for Publisher<State, Change>
where
    State: Following<Change = Change> + Clone + Send + Sync + 'static,
    Change: Clone + Send + Sync + 'static,
{
    type State = State;
    type Change = Change;

    fn state(&self) -> State {
        current(&self.published).1
    }
}

impl<State, Change> Publishing for Publisher<State, Change>
where
    State: Following<Change = Change> + Clone + Send + Sync + 'static,
    Change: Clone + Send + Sync + 'static,
{
    type State = State;
    type Change = Change;

    /// Folds the change into the state, then hands it to every open stream,
    /// all under the one lock a stream opens under, so a stream is handed
    /// exactly the changes after the state it opened on.
    fn publish(&self, change: Change) -> Reached {
        let mut published = hold(&self.published);
        let (position, state) = published
            .take()
            .expect("a publisher's state is taken only while a change is folded in");
        let position = position + 1;
        *published = Some((position, State::after(state, change.clone())));
        let reached = self.changes.send((position, change)).unwrap_or(0);
        Reached(i64::try_from(reached).unwrap_or(i64::MAX))
    }

    fn open(&self) -> signal::Stream<State, Change> {
        // Subscribed under the lock publish folds under, so the receiver
        // holds every change after the state read with it.
        let (receiver, (position, state)) = {
            let published = hold(&self.published);
            (self.changes.subscribe(), standing(&published))
        };
        let following = stream::unfold(
            Some((receiver, position, Arc::clone(&self.published))),
            |cursor| async move {
                let (mut receiver, seen, published) = cursor?;
                loop {
                    match receiver.recv().await {
                        Ok((position, change)) if position > seen => {
                            return Some((
                                Frame::Changed(change),
                                Some((receiver, position, published)),
                            ));
                        }
                        Ok(_) => {}
                        Err(RecvError::Lagged(_)) => {
                            // Every change the channel still holds is folded
                            // into the state read under the same lock; drained
                            // here, they leave the subscriber the whole capacity
                            // for the changes that follow.
                            let (position, state) = {
                                let locked = hold(&published);
                                while receiver.try_recv().is_ok() {}
                                standing(&locked)
                            };
                            return Some((
                                Frame::Lagged(state),
                                Some((receiver, position, published)),
                            ));
                        }
                        Err(RecvError::Closed) => {
                            return Some((Frame::Ended(Ending::Closed), None));
                        }
                    }
                }
            },
        );
        let frames: Pin<Box<dyn stream::Stream<Item = Frame<State, Change>> + Send>> =
            Box::pin(stream::iter([Frame::Opened(state)]).chain(following));
        signal::Stream::from(frames)
    }
}
