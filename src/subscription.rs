//! The objects central to this module are the `Subscription`, which is created for each potentially
//! long-standing request (currently `Ipfs::{connect, get_block}`), and the `SubscriptionRegistry`
//! that contains them. `SubscriptionFuture` is the `Future` bound to pending `Subscription`s and
//! sharing the same unique numeric identifier, the `SubscriptionId`.

use crate::{p2p::ConnectionTarget, RepoEvent};
use async_std::future::Future;
use async_std::task::{self, Context, Poll, Waker};
use cid::Cid;
use core::fmt::Debug;
use core::hash::Hash;
use core::pin::Pin;
use futures::channel::mpsc::Sender;
use futures::lock::Mutex;
use libp2p::{kad::QueryId, Multiaddr, PeerId};
use std::collections::HashMap;
use std::convert::TryFrom;
use std::fmt;
use std::mem;
use std::sync::{
    atomic::{AtomicBool, AtomicU64, Ordering},
    Arc,
};

// a counter used to assign unique identifiers to `Subscription`s and `SubscriptionFuture`s
// (which obtain the same number as their counterpart `Subscription`)
static GLOBAL_REQ_COUNT: AtomicU64 = AtomicU64::new(0);

/// The type of a request for subscription.
#[derive(Debug, PartialEq, Eq, Hash, Clone)]
pub enum RequestKind {
    /// A request to connect to the given `Multiaddr` or `PeerId`.
    Connect(ConnectionTarget),
    /// A request to obtain a `Block` with a specific `Cid`.
    GetBlock(Cid),
    /// A DHT request to Kademlia.
    KadQuery(QueryId),
    #[cfg(test)]
    Num(u32),
}

impl From<Multiaddr> for RequestKind {
    fn from(addr: Multiaddr) -> Self {
        Self::Connect(ConnectionTarget::Addr(addr))
    }
}

impl From<PeerId> for RequestKind {
    fn from(peer_id: PeerId) -> Self {
        Self::Connect(ConnectionTarget::PeerId(peer_id))
    }
}

impl From<ConnectionTarget> for RequestKind {
    fn from(target: ConnectionTarget) -> Self {
        Self::Connect(target)
    }
}

impl From<Cid> for RequestKind {
    fn from(cid: Cid) -> Self {
        Self::GetBlock(cid)
    }
}

impl From<QueryId> for RequestKind {
    fn from(id: QueryId) -> Self {
        Self::KadQuery(id)
    }
}

impl fmt::Display for RequestKind {
    fn fmt(&self, fmt: &mut fmt::Formatter<'_>) -> fmt::Result {
        match self {
            Self::Connect(tgt) => write!(fmt, "Connect to {:?}", tgt),
            Self::GetBlock(cid) => write!(fmt, "Obtain block {}", cid),
            Self::KadQuery(id) => write!(fmt, "Kad request {:?}", id),
            #[cfg(test)]
            Self::Num(n) => write!(fmt, "A test request for {}", n),
        }
    }
}

/// The unique identifier of a `Subscription`/`SubscriptionFuture`.
type SubscriptionId = u64;

/// The specific collection used to hold all the `Subscription`s.
pub type Subscriptions<T, E> = HashMap<RequestKind, HashMap<SubscriptionId, Subscription<T, E>>>;

/// A collection of all the live `Subscription`s.
pub struct SubscriptionRegistry<T: Debug + Clone + PartialEq, E: Debug + Clone> {
    pub(crate) subscriptions: Arc<Mutex<Subscriptions<T, E>>>,
    shutting_down: AtomicBool,
}

impl<T: Debug + Clone + PartialEq, E: Debug + Clone> fmt::Debug for SubscriptionRegistry<T, E> {
    fn fmt(&self, fmt: &mut fmt::Formatter) -> fmt::Result {
        write!(
            fmt,
            "{}(subscriptions: {:?})",
            std::any::type_name::<Self>(),
            self.subscriptions
        )
    }
}

impl<T: Debug + Clone + PartialEq, E: Debug + Clone> SubscriptionRegistry<T, E> {
    /// Creates a `Subscription` and returns its associated `Future`, the `SubscriptionFuture`.
    pub fn create_subscription(
        &self,
        kind: RequestKind,
        cancel_notifier: Option<Sender<RepoEvent>>,
    ) -> SubscriptionFuture<T, E> {
        let id = GLOBAL_REQ_COUNT.fetch_add(1, Ordering::SeqCst);
        debug!("Creating subscription {} to {}", id, kind);

        let mut subscription = Subscription::new(cancel_notifier);

        if self.shutting_down.load(Ordering::SeqCst) {
            subscription.cancel(id, kind.clone(), true);
        }

        task::block_on(async {
            self.subscriptions
                .lock()
                .await
                .entry(kind.clone())
                .or_default()
                .insert(id, subscription);
        });

        SubscriptionFuture {
            id,
            kind,
            subscriptions: Arc::clone(&self.subscriptions),
            cleanup_complete: false,
        }
    }

    /// Finalizes all pending subscriptions of the specified kind with the given `result`.
    ///
    pub fn finish_subscription(&self, req_kind: RequestKind, result: Result<T, E>) {
        let mut subscriptions = task::block_on(async { self.subscriptions.lock().await });
        let related_subs = subscriptions.get_mut(&req_kind);

        // find all the matching `Subscription`s and wake up their tasks; only `Pending`
        // ones have an associated `SubscriptionFuture` and there can be multiple of them
        // depending on how many times the given request kind was filed
        if let Some(related_subs) = related_subs {
            debug!("Finishing the subscription to {}", req_kind);

            let mut awoken = 0;
            for sub in related_subs.values_mut() {
                if let Subscription::Pending { .. } = sub {
                    sub.wake(result.clone());
                    awoken += 1;
                }
            }

            // ensure that the subscriptions are being handled correctly: normally
            // finish_subscriptions should result in some related futures being awoken
            debug_assert!(awoken != 0);

            trace!("Woke {} related subscription(s)", awoken);
        }
    }

    /// After `shutdown` all `SubscriptionFuture`s will return `Err(Cancelled)`.
    pub fn shutdown(&self) {
        if self.shutting_down.swap(true, Ordering::SeqCst) {
            return;
        }

        debug!("Shutting down {:?}", self);

        let mut cancelled = 0;
        let mut subscriptions = mem::take(&mut *task::block_on(async {
            self.subscriptions.lock().await
        }));

        for (kind, subs) in subscriptions.iter_mut() {
            for (id, sub) in subs.iter_mut() {
                sub.cancel(*id, kind.clone(), true);
                cancelled += 1;
            }
        }

        debug!("Cancelled {} subscriptions", cancelled);
    }
}

impl<T: Debug + Clone + PartialEq, E: Debug + Clone> Default for SubscriptionRegistry<T, E> {
    fn default() -> Self {
        Self {
            subscriptions: Default::default(),
            shutting_down: Default::default(),
        }
    }
}

impl<T: Debug + Clone + PartialEq, E: Debug + Clone> Drop for SubscriptionRegistry<T, E> {
    fn drop(&mut self) {
        self.shutdown();
    }
}

#[derive(Debug, PartialEq)]
pub enum SubscriptionErr<E: Debug + PartialEq> {
    /// A value returned when a `Subscription` and it's linked `SubscriptionFuture`
    /// is cancelled before completion or when the `Future` is aborted.
    Cancelled,
    /// Other errors not caused by cancellation.
    Failed(E),
}

impl<E: Debug + PartialEq> fmt::Display for SubscriptionErr<E> {
    fn fmt(&self, fmt: &mut fmt::Formatter) -> fmt::Result {
        write!(fmt, "{:?}", self)
    }
}

impl<E: Debug + PartialEq> std::error::Error for SubscriptionErr<E> {}

/// Represents a request for a resource at different stages of its lifetime.
pub enum Subscription<T, E> {
    /// A finished `Subscription` containing the desired `TRes` value.
    Ready(Result<T, E>),
    /// A standing request that hasn't been fulfilled yet.
    Pending {
        /// The waker of the task assigned to check if the `Subscription` is complete.
        waker: Option<Waker>,
        /// A `Sender` of a channel expecting notifications of subscription cancellations.
        cancel_notifier: Option<Sender<RepoEvent>>,
    },
    /// A void subscription that was either cancelled or otherwise aborted.
    Cancelled,
}

impl<T, E> fmt::Debug for Subscription<T, E> {
    fn fmt(&self, fmt: &mut fmt::Formatter<'_>) -> fmt::Result {
        use Subscription::*;
        match self {
            Ready(_) => write!(fmt, "Ready"),
            Pending {
                waker: Some(_),
                cancel_notifier: Some(_),
            } => write!(fmt, "Pending {{ waker: Some, cancel_notifier: Some }}"),
            Pending {
                waker: None,
                cancel_notifier: Some(_),
            } => write!(fmt, "Pending {{ waker: None, cancel_notifier: Some }}"),
            Pending {
                waker: Some(_),
                cancel_notifier: None,
            } => write!(fmt, "Pending {{ waker: Some, cancel_notifier: None }}"),
            Pending {
                waker: None,
                cancel_notifier: None,
            } => write!(fmt, "Pendnig {{ waker: None, cancel_notifier: None }}"),
            Cancelled => write!(fmt, "Cancelled"),
        }
    }
}

impl<T, E> Subscription<T, E> {
    fn new(cancel_notifier: Option<Sender<RepoEvent>>) -> Self {
        Self::Pending {
            waker: Default::default(),
            cancel_notifier,
        }
    }

    fn wake(&mut self, result: Result<T, E>) {
        let former_self = mem::replace(self, Subscription::Ready(result));
        if let Subscription::Pending { waker, .. } = former_self {
            if let Some(waker) = waker {
                waker.wake();
            }
        }
    }

    fn cancel(&mut self, id: SubscriptionId, kind: RequestKind, is_last: bool) {
        trace!("Cancelling subscription {} to {}", id, kind);
        let former_self = mem::replace(self, Subscription::Cancelled);
        if let Subscription::Pending {
            waker,
            cancel_notifier,
        } = former_self
        {
            // if this is the last `Subscription` related to the request kind,
            // send a cancel notification to the repo - the wantlist needs
            // to be updated
            if is_last {
                if let Some(mut sender) = cancel_notifier {
                    trace!("Last related subscription cancelled, sending a cancel notification");
                    let _ = sender.try_send(RepoEvent::try_from(kind).unwrap());
                }
            }

            if let Some(waker) = waker {
                waker.wake();
            }
        }
    }
}

/// A `Future` that resolves to the resource whose subscription was requested.
pub struct SubscriptionFuture<T: Debug + PartialEq, E: Debug> {
    /// The unique identifier of the subscription request and the secondary
    /// key in the `SubscriptionRegistry`.
    id: u64,
    /// The type of the request made; the primary key in the `SubscriptionRegistry`.
    kind: RequestKind,
    /// A reference to the subscriptions at the `SubscriptionRegistry`.
    subscriptions: Arc<Mutex<Subscriptions<T, E>>>,
    /// True if the cleanup is already done, false if `Drop` needs to do it
    cleanup_complete: bool,
}

impl<T: Debug + PartialEq, E: Debug + PartialEq> Future for SubscriptionFuture<T, E> {
    type Output = Result<T, SubscriptionErr<E>>;

    fn poll(mut self: Pin<&mut Self>, context: &mut Context) -> Poll<Self::Output> {
        use std::collections::hash_map::Entry::*;

        // FIXME: using task::block_on ever is quite unfortunate. alternatives which have been
        // discussed:
        //
        // - going back to std::sync::Mutex
        // - using a state machine
        //
        // std::sync::Mutex might be ok here as long as we don't really need to await after
        // acquiring. implementing the state machine manually might not be possible as all mutexes
        // lock futures seem to need a borrow, however using async fn does not allow implementing
        // Drop.
        let mut subscriptions = task::block_on(async { self.subscriptions.lock().await });

        if let Some(related_subs) = subscriptions.get_mut(&self.kind) {
            let (became_empty, ret) = match related_subs.entry(self.id) {
                // there were no related subs, it can only mean cancellation or polling after
                // Poll::Ready
                Vacant(_) => return Poll::Ready(Err(SubscriptionErr::Cancelled)),
                Occupied(mut oe) => {
                    let unwrapped = match oe.get_mut() {
                        Subscription::Pending { ref mut waker, .. } => {
                            // waker may have changed since the last time
                            *waker = Some(context.waker().clone());
                            return Poll::Pending;
                        }
                        Subscription::Cancelled => {
                            oe.remove();
                            Err(SubscriptionErr::Cancelled)
                        }
                        _ => match oe.remove() {
                            Subscription::Ready(result) => result.map_err(SubscriptionErr::Failed),
                            _ => unreachable!("already matched"),
                        },
                    };

                    (related_subs.is_empty(), unwrapped)
                }
            };

            if became_empty {
                // early cleanup if possible for cancelled and ready. the pending variant has the
                // chance of sending out the cancellation message so it cannot be treated here
                subscriptions.remove(&self.kind);
            }

            // need to drop manually to aid borrowck at least in 1.45
            drop(subscriptions);
            self.cleanup_complete = became_empty;
            Poll::Ready(ret)
        } else {
            Poll::Ready(Err(SubscriptionErr::Cancelled))
        }
    }
}

impl<T: Debug + PartialEq, E: Debug> Drop for SubscriptionFuture<T, E> {
    fn drop(&mut self) {
        trace!("Dropping subscription future {} to {}", self.id, self.kind);

        if self.cleanup_complete {
            // cleaned up the easier variants already
            return;
        }

        let (sub, is_last) = task::block_on(async {
            let mut subscriptions = self.subscriptions.lock().await;
            let related_subs = if let Some(subs) = subscriptions.get_mut(&self.kind) {
                subs
            } else {
                return (None, false);
            };
            let sub = related_subs.remove(&self.id);
            // check if this is the last subscription to this resource
            let is_last = related_subs.is_empty();

            if is_last {
                subscriptions.remove(&self.kind);
            }

            (sub, is_last)
        });

        if let Some(sub) = sub {
            // don't cancel anything that isn't `Pending`
            if let mut sub @ Subscription::Pending { .. } = sub {
                sub.cancel(self.id, self.kind.clone(), is_last);
            }
        }
    }
}

impl<T: Debug + PartialEq, E: Debug> fmt::Debug for SubscriptionFuture<T, E> {
    fn fmt(&self, fmt: &mut fmt::Formatter) -> fmt::Result {
        write!(
            fmt,
            "SubscriptionFuture<Output = Result<{}, {}>>",
            std::any::type_name::<T>(),
            std::any::type_name::<E>(),
        )
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    impl From<u32> for RequestKind {
        fn from(n: u32) -> Self {
            Self::Num(n)
        }
    }

    #[async_std::test]
    async fn subscription_basics() {
        let registry = SubscriptionRegistry::<u32, ()>::default();
        let s1 = registry.create_subscription(0.into(), None);
        let s2 = registry.create_subscription(0.into(), None);
        let s3 = registry.create_subscription(0.into(), None);
        registry.finish_subscription(0.into(), Ok(10));
        assert_eq!(s1.await.unwrap(), 10);
        assert_eq!(s2.await.unwrap(), 10);
        assert_eq!(s3.await.unwrap(), 10);
    }

    #[async_std::test]
    async fn subscription_cancelled_on_dropping_registry() {
        let registry = SubscriptionRegistry::<u32, ()>::default();
        let s1 = registry.create_subscription(0.into(), None);
        drop(registry);
        assert_eq!(s1.await, Err(SubscriptionErr::Cancelled));
    }

    #[async_std::test]
    async fn subscription_cancelled_on_shutdown() {
        let registry = SubscriptionRegistry::<u32, ()>::default();
        let s1 = registry.create_subscription(0.into(), None);
        registry.shutdown();
        assert_eq!(s1.await, Err(SubscriptionErr::Cancelled));
    }

    #[async_std::test]
    async fn new_subscriptions_cancelled_after_shutdown() {
        let registry = SubscriptionRegistry::<u32, ()>::default();
        registry.shutdown();
        let s1 = registry.create_subscription(0.into(), None);
        assert_eq!(s1.await, Err(SubscriptionErr::Cancelled));
    }

    #[async_std::test]
    async fn dropping_subscription_future_after_registering() {
        use async_std::future::timeout;
        use std::time::Duration;

        let registry = SubscriptionRegistry::<u32, ()>::default();
        let s1 = timeout(
            Duration::from_millis(1),
            registry.create_subscription(0.into(), None),
        );
        let s2 = registry.create_subscription(0.into(), None);

        // make sure it timed out but had time to register the waker
        s1.await.unwrap_err();

        // this will cause a call to waker installed by s1, but it shouldn't be a problem.
        registry.finish_subscription(0.into(), Ok(0));

        assert_eq!(s2.await.unwrap(), 0);
    }
}
