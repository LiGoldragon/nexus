//! A listener's accept, bounded by how many connections it holds open.
//!
//! The accept answers an event: it accepts while fewer connections are open
//! than the capacity, and a connection ending wakes it. A client that comes
//! while the listener is full waits in the listen backlog. Nothing here keeps
//! a clock. The gate holds a mutex and a condition variable, and the accepted
//! connection a stream, which Ethos cannot name, so this is hand-written.

use std::os::unix::net::{UnixListener, UnixStream};
use std::sync::{Arc, Condvar, Mutex};

use crate::generated::{LaneFailure, LaneFault};

/// How many connections one listener holds open at once, and how many are
/// open now. Clones share one count.
#[derive(Clone)]
pub struct ConnectionGate {
    openings: Arc<Openings>,
}

struct Openings {
    capacity: usize,
    open: Mutex<usize>,
    ended: Condvar,
}

/// One connection's place under its listener's capacity. Dropping it gives
/// the place back and wakes an accept waiting for one, so it is held for as
/// long as the connection is served.
#[must_use = "a connection holds its place under the capacity until its slot is dropped"]
pub struct ConnectionSlot {
    openings: Arc<Openings>,
}

/// A connection accepted, and the place it holds under its listener's
/// capacity.
pub struct Admitted {
    pub stream: UnixStream,
    pub slot: ConnectionSlot,
}

pub trait ConnectionGating {
    /// A gate of `capacity` connections, none open.
    fn with_capacity(capacity: usize) -> Self;

    /// Waits until fewer connections are open than the capacity, woken by
    /// one ending, and takes a place.
    fn admit(&self) -> ConnectionSlot;

    /// How many connections hold a place now.
    fn open(&self) -> usize;
}

impl ConnectionGating for ConnectionGate {
    fn with_capacity(capacity: usize) -> Self {
        Self {
            openings: Arc::new(Openings {
                capacity,
                open: Mutex::new(0),
                ended: Condvar::new(),
            }),
        }
    }

    fn admit(&self) -> ConnectionSlot {
        let mut open = self
            .openings
            .open
            .lock()
            .unwrap_or_else(|poisoned| poisoned.into_inner());
        while *open >= self.openings.capacity {
            open = self
                .openings
                .ended
                .wait(open)
                .unwrap_or_else(|poisoned| poisoned.into_inner());
        }
        *open += 1;
        ConnectionSlot {
            openings: Arc::clone(&self.openings),
        }
    }

    fn open(&self) -> usize {
        *self
            .openings
            .open
            .lock()
            .unwrap_or_else(|poisoned| poisoned.into_inner())
    }
}

impl Drop for ConnectionSlot {
    fn drop(&mut self) {
        let mut open = self
            .openings
            .open
            .lock()
            .unwrap_or_else(|poisoned| poisoned.into_inner());
        *open -= 1;
        self.openings.ended.notify_one();
    }
}

/// A listener whose accept is bounded by a connection capacity.
pub trait CapacityAccepting {
    /// Takes a place under the gate's capacity, waiting for a connection to
    /// end while it is full, then accepts the next connection. An accept that
    /// fails is the listener's own lane failure, `Unaccepted`, and ends the
    /// accept, so the listener's owner ends with it and the failure is seen;
    /// an accept a signal interrupted is accepted again.
    fn accept_within(&self, gate: &ConnectionGate) -> Result<Admitted, LaneFailure>;
}

impl CapacityAccepting for UnixListener {
    fn accept_within(&self, gate: &ConnectionGate) -> Result<Admitted, LaneFailure> {
        let slot = gate.admit();
        loop {
            match self.accept() {
                Ok((stream, _)) => return Ok(Admitted { stream, slot }),
                Err(error) if error.kind() == std::io::ErrorKind::Interrupted => {}
                Err(error) => return Err(LaneFailure::Unaccepted(LaneFault(error.to_string()))),
            }
        }
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use std::time::Duration;

    fn listener(name: &str) -> (std::path::PathBuf, UnixListener) {
        let directory =
            std::env::temp_dir().join(format!("nexus-accepting-{name}-{}", std::process::id()));
        let _ = std::fs::remove_dir_all(&directory);
        std::fs::create_dir_all(&directory).expect("test directory");
        let listener = UnixListener::bind(directory.join("listener.sock")).expect("listener");
        (directory, listener)
    }

    #[test]
    fn connections_at_capacity_queue_and_one_ending_admits_the_next() {
        let (directory, listener) = listener("capacity");
        let path = directory.join("listener.sock");
        let gate = ConnectionGate::with_capacity(1);
        let _first_client = UnixStream::connect(&path).expect("first client");
        let first = listener.accept_within(&gate).expect("first accepted");
        assert_eq!(gate.open(), 1);
        // The second client is taken into the listen backlog while the
        // listener is full; its connect does not wait for the accept.
        let _second_client = UnixStream::connect(&path).expect("second client queued");
        let (accepted_tell, accepted) = std::sync::mpsc::channel();
        let accepting = {
            let gate = gate.clone();
            std::thread::spawn(move || {
                let second = listener.accept_within(&gate);
                accepted_tell.send(()).expect("accepted told");
                second
            })
        };
        assert!(
            accepted.recv_timeout(Duration::from_millis(100)).is_err(),
            "no second accept while the first connection is open"
        );
        assert_eq!(gate.open(), 1);
        drop(first);
        accepted
            .recv_timeout(Duration::from_secs(10))
            .expect("the first ending admits the second");
        let second = accepting
            .join()
            .expect("accept thread")
            .expect("second accepted");
        assert_eq!(gate.open(), 1);
        drop(second);
        assert_eq!(gate.open(), 0);
        std::fs::remove_dir_all(directory).expect("test directory removed");
    }

    #[test]
    fn an_accept_that_fails_below_the_capacity_ends_with_its_lane_failure() {
        let (directory, listener) = listener("failure");
        // A listener with nothing to accept that will not wait refuses the
        // accept: a failure below the capacity.
        listener.set_nonblocking(true).expect("nonblocking");
        let gate = ConnectionGate::with_capacity(4);
        let failure = listener.accept_within(&gate).err().expect("accept refused");
        assert!(
            matches!(failure, LaneFailure::Unaccepted(LaneFault(_))),
            "{failure:?}"
        );
        assert_eq!(gate.open(), 0, "the failed accept gives its place back");
        std::fs::remove_dir_all(directory).expect("test directory removed");
    }
}
