//! A Nexus's two listeners: the meta socket, bound at start, and the public
//! socket, bound once the Nexus is configured.

use std::os::unix::net::UnixStream;
use std::sync::{Condvar, Mutex};

use nexus_exchange::lane::PeerCalling;
use nexus_exchange::{
    Admitted, CapacityAccepting, ConnectionGate, ConnectionGating, ConnectionSlot, LaneFailure,
    Named, SocketPaths, ValidAdmission,
};
use shared_translator::placement::Place;
use shared_translator::{Caller, PeerIdentity};

use crate::listener::{OwnedListener, SocketBinding};
use crate::paired::{PairedPeer, PeerMatching};

/// The listeners of the Nexus `Component` in one place, each holding at most
/// the admission's connection capacity open, and the translator it is paired
/// with.
pub struct Listeners<Component> {
    paths: SocketPaths<Component>,
    meta: OwnedListener,
    meta_gate: ConnectionGate,
    public: Mutex<Option<OwnedListener>>,
    public_gate: ConnectionGate,
    public_ready: Condvar,
    admission: ValidAdmission,
    translator: PairedPeer,
}

/// A public connection accepted, with the authority its peer was admitted
/// under and the place it holds under the listener's capacity, held for as
/// long as the connection is served.
pub struct AcceptedPublic<Authority> {
    pub stream: UnixStream,
    pub authority: Authority,
    pub slot: ConnectionSlot,
}

/// Who besides the paired translator a Nexus's public socket admits.
pub trait PeerAdmitting {
    type Authority;

    /// The authority of the paired translator.
    fn translator() -> Self::Authority;

    /// The authority of a peer the Nexus has bound, or `None` for a caller it
    /// has not.
    fn peer(&self, caller: &Caller) -> Option<Self::Authority>;
}

pub trait Listening: Sized {
    type Component;

    /// Binds the meta socket alone, then pins the translator the Nexus is
    /// paired with.
    fn bind(
        place: &Place,
        admission: ValidAdmission,
        translator: PeerIdentity,
    ) -> std::io::Result<Self>;

    /// Binds the public socket, not yet accepting.
    fn stage_public(&self) -> std::io::Result<OwnedListener>;

    /// Hands the bound public socket to whoever waits to accept on it.
    fn publish_public(&self, listener: OwnedListener);

    /// Accepts the next meta connection from the paired translator, while
    /// fewer meta connections are open than the connection capacity, to be
    /// served on the runtime by `MetaConnection` holding its slot; any other
    /// is reported and closed. An accept that fails is the listener's
    /// `LaneFailure::Unaccepted`, and ends it.
    fn accept_meta(&self) -> Result<Admitted, LaneFailure>;

    /// Waits for the public socket, then accepts the next connection from the
    /// paired translator or a peer the Nexus admits, while fewer public
    /// connections are open than the connection capacity; any other is
    /// reported and closed. An accept that fails is the listener's
    /// `LaneFailure::Unaccepted`, and ends it.
    fn accept_public<Admitting: PeerAdmitting>(
        &self,
        admitting: &Admitting,
    ) -> Result<AcceptedPublic<Admitting::Authority>, LaneFailure>;

    fn paths(&self) -> &SocketPaths<Self::Component>;

    fn admission(&self) -> ValidAdmission;

    fn translator(&self) -> &PairedPeer;
}

impl<Component: Named> Listening for Listeners<Component> {
    type Component = Component;

    fn bind(
        place: &Place,
        admission: ValidAdmission,
        translator: PeerIdentity,
    ) -> std::io::Result<Self> {
        let paths = SocketPaths::<Component>::from(place);
        let meta = OwnedListener::bind(paths.meta.clone())?;
        Ok(Self {
            paths,
            meta,
            meta_gate: ConnectionGate::with_capacity(admission.connection_capacity),
            public: Mutex::new(None),
            public_gate: ConnectionGate::with_capacity(admission.connection_capacity),
            public_ready: Condvar::new(),
            admission,
            translator: PairedPeer::capture(translator)?,
        })
    }

    fn stage_public(&self) -> std::io::Result<OwnedListener> {
        OwnedListener::bind(self.paths.public.clone())
    }

    fn publish_public(&self, listener: OwnedListener) {
        let mut public = self
            .public
            .lock()
            .unwrap_or_else(|poisoned| poisoned.into_inner());
        *public = Some(listener);
        self.public_ready.notify_all();
    }

    fn accept_meta(&self) -> Result<Admitted, LaneFailure> {
        loop {
            let admitted = self.meta.listener().accept_within(&self.meta_gate)?;
            match self.translator.require_peer(&admitted.stream) {
                Ok(()) => return Ok(admitted),
                Err(error) => {
                    eprintln!("{} meta connection refused: {error}", Component::title().0)
                }
            }
        }
    }

    fn accept_public<Admitting: PeerAdmitting>(
        &self,
        admitting: &Admitting,
    ) -> Result<AcceptedPublic<Admitting::Authority>, LaneFailure> {
        let mut public = self
            .public
            .lock()
            .unwrap_or_else(|poisoned| poisoned.into_inner());
        while public.is_none() {
            public = self
                .public_ready
                .wait(public)
                .unwrap_or_else(|poisoned| poisoned.into_inner());
        }
        let listener = public
            .as_ref()
            .expect("public listener notified only after binding");
        loop {
            let Admitted { stream, slot } = listener.listener().accept_within(&self.public_gate)?;
            let actual = match stream.peer_caller() {
                Ok(actual) => actual,
                Err(error) => {
                    eprintln!(
                        "{} public peer lookup failed: {error}",
                        Component::title().0
                    );
                    continue;
                }
            };
            if self.translator.matches(&actual) {
                return Ok(AcceptedPublic {
                    stream,
                    authority: Admitting::translator(),
                    slot,
                });
            }
            match admitting.peer(&actual) {
                Some(authority) => {
                    return Ok(AcceptedPublic {
                        stream,
                        authority,
                        slot,
                    });
                }
                None => eprintln!("{} rejected unconfigured public peer", Component::title().0),
            }
        }
    }

    fn paths(&self) -> &SocketPaths<Component> {
        &self.paths
    }

    fn admission(&self) -> ValidAdmission {
        self.admission
    }

    fn translator(&self) -> &PairedPeer {
        &self.translator
    }
}
