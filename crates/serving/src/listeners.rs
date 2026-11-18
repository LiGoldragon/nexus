//! A Nexus's two listeners: the meta socket, bound at start, and the public
//! socket, bound once the Nexus is configured.

use std::os::unix::net::UnixStream;
use std::sync::{Condvar, Mutex};

use nexus_exchange::lane::PeerCalling;
use nexus_exchange::{DeadlineStream, Named, RetryingAccept, ValidAdmission};
use shared_translator::placement::Place;
use shared_translator::{Caller, PeerIdentity};
use signal::{Contracted, Delivery, Dispatch, FrameCapacity, Restorable, Signal, Signalizable};

use crate::generated::MetaAnswering;
use crate::listener::{OwnedListener, SocketBinding};
use crate::meta::MetaServing;
use crate::paired::{PairedPeer, PeerMatching};
use crate::paths::SocketPaths;

/// The listeners of the Nexus `Component` in one place, and the translator
/// it is paired with.
pub struct Listeners<Component> {
    paths: SocketPaths<Component>,
    meta: OwnedListener,
    public: Mutex<Option<OwnedListener>>,
    public_ready: Condvar,
    admission: ValidAdmission,
    translator: PairedPeer,
}

/// A public connection accepted, with the authority its peer was admitted
/// under.
pub struct AcceptedPublic<Authority> {
    pub stream: UnixStream,
    pub authority: Authority,
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

    /// Accepts the next meta connection and serves it whole. A connection not
    /// from the paired translator, or one that fails, is reported and ended;
    /// only a listener that cannot accept is an error.
    fn serve_next_meta<Answering>(
        &self,
        answering: &Answering,
    ) -> Result<(), Box<dyn std::error::Error>>
    where
        Answering: MetaAnswering,
        Answering::Query: Contracted,
        Dispatch<Answering::Query>: rkyv::Archive,
        Signal<Dispatch<Answering::Query>>: Restorable<Dispatch<Answering::Query>>,
        Delivery<Answering::Response>: Signalizable;

    /// Waits for the public socket, then accepts the next connection from the
    /// paired translator or a peer the Nexus admits; any other is reported
    /// and closed.
    fn accept_public<Admitting: PeerAdmitting>(
        &self,
        admitting: &Admitting,
    ) -> std::io::Result<AcceptedPublic<Admitting::Authority>>;

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
            public: Mutex::new(None),
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

    fn serve_next_meta<Answering>(
        &self,
        answering: &Answering,
    ) -> Result<(), Box<dyn std::error::Error>>
    where
        Answering: MetaAnswering,
        Answering::Query: Contracted,
        Dispatch<Answering::Query>: rkyv::Archive,
        Signal<Dispatch<Answering::Query>>: Restorable<Dispatch<Answering::Query>>,
        Delivery<Answering::Response>: Signalizable,
    {
        let (stream, _) = self
            .meta
            .listener()
            .accept_with_retry(self.admission.retry_delay)?;
        let exchange = (|| -> Result<(), Box<dyn std::error::Error>> {
            self.translator.require_peer(&stream)?;
            let mut stream = DeadlineStream::new(stream, self.admission.deadline)?;
            stream.serve_meta_connection(
                FrameCapacity::from(self.admission.frame_capacity),
                answering,
            )
        })();
        if let Err(error) = exchange {
            eprintln!("{} meta connection failed: {error}", Component::title().0);
        }
        Ok(())
    }

    fn accept_public<Admitting: PeerAdmitting>(
        &self,
        admitting: &Admitting,
    ) -> std::io::Result<AcceptedPublic<Admitting::Authority>> {
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
            let stream = listener
                .listener()
                .accept_with_retry(self.admission.retry_delay)?
                .0;
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
                });
            }
            match admitting.peer(&actual) {
                Some(authority) => return Ok(AcceptedPublic { stream, authority }),
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
