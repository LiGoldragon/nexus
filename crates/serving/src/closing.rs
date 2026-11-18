//! The closing of one subscription.

use std::future::Future;

use crate::generated::Closing;

/// The closing of one subscription, set once, by whoever ends it first, and
/// awaited by the task that follows its stream.
#[derive(Default)]
pub struct SubscriptionClosing {
    closing: tokio::sync::watch::Sender<Option<Closing>>,
}

pub trait Closable {
    /// Closes the subscription for `closing`, unless it is already closed for
    /// another reason, which stands.
    fn close(&self, closing: Closing);

    fn is_open(&self) -> bool;

    /// Waits until the subscription is closed, and says why.
    fn closed(&self) -> impl Future<Output = Closing> + Send;
}

impl Closable for SubscriptionClosing {
    fn close(&self, closing: Closing) {
        self.closing.send_if_modified(|current| {
            if current.is_some() {
                return false;
            }
            *current = Some(closing);
            true
        });
    }

    fn is_open(&self) -> bool {
        self.closing.borrow().is_none()
    }

    fn closed(&self) -> impl Future<Output = Closing> + Send {
        let mut watching = self.closing.subscribe();
        async move {
            loop {
                let current = watching.borrow_and_update().clone();
                if let Some(closing) = current {
                    return closing;
                }
                if watching.changed().await.is_err() {
                    return Closing::Abandoned;
                }
            }
        }
    }
}
