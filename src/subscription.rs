//! The objects central to this module are the `Subscription`, which is created for each potentially
//! long-standing request (currently `Ipfs::{connect, get_block}`), and the `SubscriptionRegistry`
//! that contains them. `SubscriptionFuture` is the `Future` bound to pending `Subscription`s and
//! sharing the same unique numeric identifier, the `SubscriptionId`.

use crate::RepoEvent;
use async_std::future::Future;
use async_std::task::{self, Context, Poll, Waker};
use core::fmt::Debug;
use core::hash::Hash;
use core::pin::Pin;
use futures::channel::mpsc::Sender;
use futures::lock::Mutex;
use libipld::Cid;
use libp2p::{kad::QueryId, Multiaddr};
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
    /// A request to connect to the given `Multiaddr`.
    Connect(Multiaddr),
    /// A request to obtain a `Block` with a specific `Cid`.
    GetBlock(Cid),
    /// A DHT request to Kademlia.
    KadQuery(QueryId),
    #[cfg(test)]
    Num(u32),
}

impl From<Multiaddr> for RequestKind {
    fn from(addr: Multiaddr) -> Self {
        Self::Connect(addr)
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
            Self::Connect(addr) => write!(fmt, "Connect to {}", addr),
            Self::GetBlock(cid) => write!(
                fmt,
                "Obtain block {}",
                cid.hash()
                    .digest()
                    .iter()
                    .map(|b| format!("{:02x}", b))
                    .collect::<String>()
            ),
            Self::KadQuery(id) => write!(fmt, "Kad request {:?}", id),
            #[cfg(test)]
            Self::Num(n) => write!(fmt, "A test request for {}", n),
        }
    }
}

/// The unique identifier of a `Subscription`/`SubscriptionFuture`.
type SubscriptionId = u64;

/// The specific collection used to hold all the `Subscription`s.
pub type Subscriptions<T> = HashMap<RequestKind, HashMap<SubscriptionId, Subscription<T>>>;

/// A collection of all the live `Subscription`s.
pub struct SubscriptionRegistry<TRes: Debug + Clone + PartialEq> {
    pub(crate) subscriptions: Arc<Mutex<Subscriptions<TRes>>>,
    shutting_down: AtomicBool,
}

impl<TRes: Debug + Clone + PartialEq> fmt::Debug for SubscriptionRegistry<TRes> {
    fn fmt(&self, fmt: &mut fmt::Formatter) -> fmt::Result {
        write!(
            fmt,
            "{}(subscriptions: {:?})",
            std::any::type_name::<Self>(),
            self.subscriptions
        )
    }
}

impl<TRes: Debug + Clone + PartialEq> SubscriptionRegistry<TRes> {
    /// Creates a `Subscription` and returns its associated `Future`, the `SubscriptionFuture`.
    pub fn create_subscription(
        &self,
        kind: RequestKind,
        cancel_notifier: Option<Sender<RepoEvent>>,
    ) -> SubscriptionFuture<TRes> {
        let id = GLOBAL_REQ_COUNT.fetch_add(1, Ordering::SeqCst);
        debug!("Creating subscription {} to {}", id, kind);

        let mut subscription = Subscription::new(cancel_notifier);

        if self.shutting_down.load(Ordering::SeqCst) {
            subscription.cancel(kind.clone(), true);
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
        }
    }

    /// Finalizes all pending subscriptions of the specified kind with the given `result`.
    ///
    pub fn finish_subscription(&self, req_kind: RequestKind, result: TRes) {
        let mut subscriptions = task::block_on(async { self.subscriptions.lock().await });
        let related_subs = subscriptions.get_mut(&req_kind);

        // find all the matching `Subscription`s and wake up their tasks; only `Pending`
        // ones have an associated `SubscriptionFuture` and there can be multiple of them
        // depending on how many times the given request kind was filed
        if let Some(related_subs) = related_subs {
            for sub in related_subs.values_mut() {
                if let Subscription::Pending { .. } = sub {
                    sub.wake(result.clone());
                }
            }
        }
    }

    /// After `shutdown` all `SubscriptionFuture`s will return `Err(Cancelled)`.
    pub fn shutdown(&self) {
        if self.shutting_down.swap(true, Ordering::SeqCst) {
            return;
        }

        log::debug!("Shutting down {:?}", self);

        let mut cancelled = 0;
        let mut subscriptions = mem::take(&mut *task::block_on(async {
            self.subscriptions.lock().await
        }));

        for (kind, subs) in subscriptions.iter_mut() {
            for sub in subs.values_mut() {
                sub.cancel(kind.clone(), true);
                cancelled += 1;
            }
        }

        log::debug!("Cancelled {} subscriptions", cancelled);
    }
}

impl<TRes: Debug + Clone + PartialEq> Default for SubscriptionRegistry<TRes> {
    fn default() -> Self {
        Self {
            subscriptions: Default::default(),
            shutting_down: Default::default(),
        }
    }
}

impl<TRes: Debug + Clone + PartialEq> Drop for SubscriptionRegistry<TRes> {
    fn drop(&mut self) {
        self.shutdown();
    }
}

/// A value returned when a `Subscription` and it's linked `SubscriptionFuture`
/// is cancelled before completion or when the `Future` is aborted.
#[derive(Debug, PartialEq, Eq)]
pub struct Cancelled;

impl fmt::Display for Cancelled {
    fn fmt(&self, fmt: &mut fmt::Formatter) -> fmt::Result {
        write!(fmt, "{:?}", self)
    }
}

impl std::error::Error for Cancelled {}

/// Represents a request for a resource at different stages of its lifetime.
#[derive(Debug)]
pub enum Subscription<TRes> {
    /// A finished `Subscription` containing the desired `TRes` value.
    Ready(TRes),
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

impl<TRes> Subscription<TRes> {
    fn new(cancel_notifier: Option<Sender<RepoEvent>>) -> Self {
        Self::Pending {
            waker: Default::default(),
            cancel_notifier,
        }
    }

    fn wake(&mut self, result: TRes) {
        let former_self = mem::replace(self, Subscription::Ready(result));
        if let Subscription::Pending { waker, .. } = former_self {
            if let Some(waker) = waker {
                waker.wake();
            }
        }
    }

    fn cancel(&mut self, kind: RequestKind, is_last: bool) {
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
pub struct SubscriptionFuture<TRes: Debug + PartialEq> {
    /// The unique identifier of the subscription request and the secondary
    /// key in the `SubscriptionRegistry`.
    id: u64,
    /// The type of the request made; the primary key in the `SubscriptionRegistry`.
    kind: RequestKind,
    /// A reference to the subscriptions at the `SubscriptionRegistry`.
    subscriptions: Arc<Mutex<Subscriptions<TRes>>>,
}

impl<TRes: Debug + PartialEq> Future for SubscriptionFuture<TRes> {
    type Output = Result<TRes, Cancelled>;

    fn poll(self: Pin<&mut Self>, context: &mut Context) -> Poll<Self::Output> {
        let mut subscription = {
            // don't hold the lock for too long, otherwise the `Drop` impl for `SubscriptionFuture`
            // can cause a stack overflow
            let mut subscriptions = task::block_on(async { self.subscriptions.lock().await });
            if let Some(sub) = subscriptions
                .get_mut(&self.kind)
                .and_then(|subs| subs.remove(&self.id))
            {
                sub
            } else {
                // the subscription must already have been cancelled
                return Poll::Ready(Err(Cancelled));
            }
        };

        match subscription {
            Subscription::Cancelled => Poll::Ready(Err(Cancelled)),
            Subscription::Pending { ref mut waker, .. } => {
                *waker = Some(context.waker().clone());
                task::block_on(async { self.subscriptions.lock().await })
                    .get_mut(&self.kind)
                    .and_then(|subs| subs.insert(self.id, subscription));
                Poll::Pending
            }
            Subscription::Ready(result) => Poll::Ready(Ok(result)),
        }
    }
}

impl<TRes: Debug + PartialEq> Drop for SubscriptionFuture<TRes> {
    fn drop(&mut self) {
        debug!("Dropping subscription {} to {}", self.id, self.kind);

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

            (sub, is_last)
        });

        if let Some(sub) = sub {
            // don't bother updating anything that isn't `Pending`
            if let mut sub @ Subscription::Pending { .. } = sub {
                debug!("It was the last related subscription, sending a cancel notification");
                sub.cancel(self.kind.clone(), is_last);
            }
        }
    }
}

impl<TRes: Debug + PartialEq> fmt::Debug for SubscriptionFuture<TRes> {
    fn fmt(&self, fmt: &mut fmt::Formatter) -> fmt::Result {
        write!(
            fmt,
            "SubscriptionFuture<Output = Result<{}, Cancelled>>",
            std::any::type_name::<TRes>()
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
        let registry = SubscriptionRegistry::<u32>::default();
        let s1 = registry.create_subscription(0.into(), None);
        let s2 = registry.create_subscription(0.into(), None);
        let s3 = registry.create_subscription(0.into(), None);
        registry.finish_subscription(0.into(), 10);
        assert_eq!(s1.await.unwrap(), 10);
        assert_eq!(s2.await.unwrap(), 10);
        assert_eq!(s3.await.unwrap(), 10);
    }

    #[async_std::test]
    async fn subscription_cancelled_on_dropping_registry() {
        let registry = SubscriptionRegistry::<u32>::default();
        let s1 = registry.create_subscription(0.into(), None);
        drop(registry);
        assert_eq!(s1.await, Err(Cancelled));
    }

    #[async_std::test]
    async fn subscription_cancelled_on_shutdown() {
        let registry = SubscriptionRegistry::<u32>::default();
        let s1 = registry.create_subscription(0.into(), None);
        registry.shutdown();
        assert_eq!(s1.await, Err(Cancelled));
    }

    #[async_std::test]
    async fn new_subscriptions_cancelled_after_shutdown() {
        let registry = SubscriptionRegistry::<u32>::default();
        registry.shutdown();
        let s1 = registry.create_subscription(0.into(), None);
        assert_eq!(s1.await, Err(Cancelled));
    }

    #[async_std::test]
    async fn dropping_subscription_future_after_registering() {
        use async_std::future::timeout;
        use std::time::Duration;

        let registry = SubscriptionRegistry::<u32>::default();
        let s1 = timeout(
            Duration::from_millis(1),
            registry.create_subscription(0.into(), None),
        );
        let s2 = registry.create_subscription(0.into(), None);

        // make sure it timed out but had time to register the waker
        s1.await.unwrap_err();

        // this will cause a call to waker installed by s1, but it shouldn't be a problem.
        registry.finish_subscription(0.into(), 0);

        assert_eq!(s2.await.unwrap(), 0);
    }
}
