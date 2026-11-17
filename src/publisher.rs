//! The subscription publisher: one current state, and the streams that follow
//! it.
//!
//! A Nexus that answers a subscription holds its state in a [`Publisher`] and
//! reads it from there; it keeps no second copy. Each change it makes is
//! published: folded into the state with [`Following::after`], the fold
//! [`signal`] declares for every subscription state, given the next
//! position, and kept among the last changes for every open stream. A stream
//! opens on the state as it stands, then carries each change published after
//! it, in order.
//!
//! No subscriber slows the Nexus or another subscriber, and none is dropped
//! for being slow. Publishing waits for no one: the Publisher keeps the last
//! [`Capacity`] changes, and a subscriber that falls more than that far behind
//! is handed the state anew as `Lagged`, then the changes that follow it. A
//! position, the Publisher's own counter and never a wire value, is what makes
//! the replacement exact: each stream holds the position of the last change
//! its subscriber has, and after a state at position `p` it is handed only the
//! changes after `p`, so the subscriber receives each change it has not yet
//! seen once. A stream wakes when a change is announced and reads what follows
//! its position under the lock the change was kept under. When the Publisher
//! is dropped each stream finishes after the changes it still holds; a
//! subscription is one exchange, and its end is the exchange's.
//!
//! The capacity is the Publisher's to change: [`Publishing::resize`] sets it,
//! and every open stream follows it at once, since every stream reads the one
//! set of kept changes. A stream whose backlog is within the new capacity
//! carries it over exactly; one further behind is handed `Lagged` with the
//! state as it stands and goes on, the position keeping what follows exact.
//!
//! The Publisher's fields are its own and it is generic over its state and
//! change, so it is written here by hand; every trait it bears, and the
//! [`Capacity`] and [`Reached`] it speaks in, are declared in
//! `ethos/library.ethos`. The bound `State: Following` asserts that the state
//! a [`signal::Stream`] carries bears signal's [`Following`].

use std::collections::VecDeque;
use std::pin::Pin;
use std::sync::{Arc, Mutex, MutexGuard};

use futures_util::stream::{self, StreamExt};
use signal::{Following, Frame};
use tokio::sync::watch;

use crate::generated::{Capacity, CapacityRefusal, Publishing, Reached, Subscribable};

/// The one current published state of a subscription, the changes kept for
/// the open streams, and the announcement that wakes them.
pub struct Publisher<State, Change> {
    published: Arc<Mutex<Published<State, Change>>>,
    /// Sent once for each change kept; every open stream holds a receiver, so
    /// their count is the streams a change reaches. Dropped with the
    /// Publisher, which finishes every stream.
    announcement: watch::Sender<()>,
}

/// What a reader takes at once, under one lock: the position of the last
/// change folded in, the state after it, and the changes kept before it.
struct Published<State, Change> {
    position: u64,
    /// Empty only while a change is being folded in.
    state: Option<State>,
    /// The last changes, each with its position, oldest first and ending at
    /// `position`; never more than `capacity` of them.
    kept: VecDeque<(u64, Change)>,
    capacity: usize,
}

/// The published state, locked against publishing until the guard is dropped.
fn hold<State, Change>(
    published: &Mutex<Published<State, Change>>,
) -> MutexGuard<'_, Published<State, Change>> {
    published
        .lock()
        .expect("a publisher's state is poisoned only by a fold that panicked")
}

/// The number of changes a capacity keeps; below one is refused, since a
/// stream that can hold no change can follow none.
fn kept_count(capacity: Capacity) -> Result<usize, CapacityRefusal> {
    match usize::try_from(capacity.0) {
        Ok(0) => Err(CapacityRefusal::Zero),
        Ok(count) => Ok(count),
        Err(_) => Err(CapacityRefusal::Negative),
    }
}

impl<State: Clone, Change: Clone> Published<State, Change> {
    /// A copy of the state as it stands.
    fn standing(&self) -> State {
        self.state
            .clone()
            .expect("a publisher's state is taken only while a change is folded in")
    }

    /// The frame that follows `seen` for a stream, with the position the
    /// stream has after it: the next change when it is still kept, the state
    /// as `Lagged` when it is not, nothing when the stream is up to date.
    fn following(&self, seen: u64) -> Option<(u64, Frame<State, Change>)> {
        if self.position == seen {
            return None;
        }
        match self.kept.front() {
            Some((oldest, _)) if *oldest <= seen + 1 => {
                let index = usize::try_from(seen + 1 - oldest)
                    .expect("a kept change's index is below the capacity");
                let (position, change) = &self.kept[index];
                Some((*position, Frame::Changed(change.clone())))
            }
            _ => Some((self.position, Frame::Lagged(self.standing()))),
        }
    }

    /// Keeps no more than `capacity` changes from now on, letting the oldest
    /// go when there are more.
    fn keep(&mut self, capacity: usize) {
        self.capacity = capacity;
        while self.kept.len() > capacity {
            self.kept.pop_front();
        }
        self.kept.shrink_to(capacity);
    }
}

/// Where a stream stands: the position of the last change its subscriber
/// has, the state it reads from, and the announcement it wakes on.
struct Cursor<State, Change> {
    seen: u64,
    published: Arc<Mutex<Published<State, Change>>>,
    announcement: watch::Receiver<()>,
}

impl<State: Clone, Change: Clone> Cursor<State, Change> {
    /// The next frame and the cursor after it, or `None` when the Publisher
    /// is gone and nothing it kept is left for this stream.
    async fn advance(mut self) -> Option<(Frame<State, Change>, Self)> {
        loop {
            let following = hold(&self.published).following(self.seen);
            if let Some((seen, frame)) = following {
                self.seen = seen;
                return Some((frame, self));
            }
            if self.announcement.changed().await.is_err() {
                // The Publisher is gone: after what it kept the stream
                // finishes, and the serving side ends the exchange, Completed.
                let following = hold(&self.published).following(self.seen);
                return following.map(|(seen, frame)| {
                    self.seen = seen;
                    (frame, self)
                });
            }
        }
    }
}

impl<State, Change> Publisher<State, Change> {
    /// The streams open now.
    fn reached(&self) -> Reached {
        Reached(i64::try_from(self.announcement.receiver_count()).unwrap_or(i64::MAX))
    }
}

impl<State, Change> Subscribable for Publisher<State, Change>
where
    State: Following<Change = Change> + Clone + Send + Sync + 'static,
    Change: Clone + Send + Sync + 'static,
{
    type State = State;
    type Change = Change;

    fn state(&self) -> State {
        hold(&self.published).standing()
    }
}

impl<State, Change> Publishing for Publisher<State, Change>
where
    State: Following<Change = Change> + Clone + Send + Sync + 'static,
    Change: Clone + Send + Sync + 'static,
{
    type State = State;
    type Change = Change;

    /// A publisher of `state`, whose streams may fall `capacity` changes
    /// behind before they are handed the state anew. A capacity below one is
    /// refused.
    fn create(state: State, capacity: Capacity) -> Result<Self, CapacityRefusal> {
        let capacity = kept_count(capacity)?;
        let (announcement, _) = watch::channel(());
        Ok(Self {
            published: Arc::new(Mutex::new(Published {
                position: 0,
                state: Some(state),
                kept: VecDeque::new(),
                capacity,
            })),
            announcement,
        })
    }

    /// Folds the change into the state and keeps it, under the one lock a
    /// stream opens and reads under, so a stream is handed exactly the
    /// changes after the state it opened on; then announces it.
    fn publish(&self, change: Change) -> Reached {
        {
            let mut published = hold(&self.published);
            let state = published
                .state
                .take()
                .expect("a publisher's state is taken only while a change is folded in");
            published.position += 1;
            let position = published.position;
            published.state = Some(State::after(state, change.clone()));
            if published.kept.len() == published.capacity {
                published.kept.pop_front();
            }
            published.kept.push_back((position, change));
        }
        self.announcement.send_replace(());
        self.reached()
    }

    /// Sets the capacity; every open stream follows it, and the count of them
    /// is what it reached. A stream more than the new capacity behind is
    /// handed `Lagged` when it next reads. A capacity below one is refused,
    /// and the capacity stays as it was.
    fn resize(&mut self, capacity: Capacity) -> Result<Reached, CapacityRefusal> {
        let capacity = kept_count(capacity)?;
        hold(&self.published).keep(capacity);
        Ok(self.reached())
    }

    fn open(&self) -> signal::Stream<State, Change> {
        // The receiver is taken and the state read under the lock publish
        // keeps under, so the stream follows from exactly that state.
        let (cursor, state) = {
            let published = hold(&self.published);
            let cursor = Cursor {
                seen: published.position,
                published: Arc::clone(&self.published),
                announcement: self.announcement.subscribe(),
            };
            (cursor, published.standing())
        };
        let following = stream::unfold(cursor, Cursor::advance);
        let frames: Pin<Box<dyn stream::Stream<Item = Frame<State, Change>> + Send>> =
            Box::pin(stream::iter([Frame::Opened(state)]).chain(following));
        signal::Stream::from(frames)
    }
}
