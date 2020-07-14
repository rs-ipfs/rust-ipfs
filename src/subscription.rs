use crate::RepoEvent;
use async_std::future::Future;
use async_std::task::{Context, Poll, Waker};
use core::fmt::Debug;
use core::hash::Hash;
use core::pin::Pin;
use futures::channel::mpsc::Sender;
use futures::lock::Mutex;
use libipld::Cid;
use libp2p::Multiaddr;
use std::collections::HashMap;
use std::fmt;
use std::mem;
use std::sync::{
    atomic::{AtomicBool, AtomicU64, Ordering},
    Arc,
};

static GLOBAL_REQ_COUNT: AtomicU64 = AtomicU64::new(0);

#[derive(Debug, PartialEq, Eq, Hash, Clone)]
pub struct Request {
    pub(crate) kind: RequestKind,
    id: u64,
}

#[derive(Debug, PartialEq, Eq, Hash, Clone)]
pub enum RequestKind {
    Connect(Multiaddr),
    GetBlock(Cid),
    #[cfg(test)]
    Empty,
}

impl From<Multiaddr> for Request {
    fn from(addr: Multiaddr) -> Self {
        Self {
            kind: RequestKind::Connect(addr),
            id: GLOBAL_REQ_COUNT.fetch_add(1, Ordering::SeqCst),
        }
    }
}

impl From<Cid> for Request {
    fn from(cid: Cid) -> Self {
        Self {
            kind: RequestKind::GetBlock(cid),
            id: GLOBAL_REQ_COUNT.fetch_add(1, Ordering::SeqCst),
        }
    }
}

type SubscriptionId = u64;
type Subscriptions<T> = HashMap<SubscriptionId, Subscription<T>>;

pub struct SubscriptionRegistry<TRes: Debug + Clone> {
    subscriptions: Arc<Mutex<Subscriptions<TRes>>>,
    shutting_down: AtomicBool,
}

impl<TRes: Debug + Clone> fmt::Debug for SubscriptionRegistry<TRes> {
    fn fmt(&self, fmt: &mut fmt::Formatter) -> fmt::Result {
        write!(
            fmt,
            "{}(subscriptions: {:?})",
            std::any::type_name::<Self>(),
            self.subscriptions
        )
    }
}

impl<TRes: Debug + Clone> SubscriptionRegistry<TRes> {
    pub fn create_subscription(
        &self,
        req: Request,
        cancel_notifier: Option<Sender<RepoEvent>>,
    ) -> SubscriptionFuture<TRes> {
        let id = req.id;
        let mut subscription = Subscription::new(req, cancel_notifier);

        if self.shutting_down.load(Ordering::SeqCst) {
            subscription.cancel();
        }

        async_std::task::block_on(async {
            self.subscriptions.lock().await.insert(id, subscription)
        });

        SubscriptionFuture {
            id,
            subscriptions: Arc::clone(&self.subscriptions),
        }
    }

    pub fn finish_subscription(&self, req: &Request, res: TRes) {
        let mut subscriptions =
            async_std::task::block_on(async { self.subscriptions.lock().await });

        for sub in subscriptions.values_mut() {
            if let Subscription::Pending { request, .. } = sub {
                if request.kind == req.kind {
                    sub.wake(res.clone());
                }
            }
        }
    }

    /// After shutdown all SubscriptionFutures will return Err(Cancelled)
    pub fn shutdown(&self) {
        if self.shutting_down.swap(true, Ordering::SeqCst) {
            return;
        }

        log::debug!("Shutting down {:?}", self);

        let mut cancelled = 0;
        let mut subscriptions =
            async_std::task::block_on(async { self.subscriptions.lock().await });

        for (_idx, mut sub) in subscriptions.drain() {
            sub.cancel();
            cancelled += 1;
        }

        log::trace!("Cancelled {} subscriptions", cancelled,);
    }
}

impl<TRes: Debug + Clone> Default for SubscriptionRegistry<TRes> {
    fn default() -> Self {
        Self {
            subscriptions: Default::default(),
            shutting_down: Default::default(),
        }
    }
}

impl<TRes: Debug + Clone> Drop for SubscriptionRegistry<TRes> {
    fn drop(&mut self) {
        self.shutdown();
    }
}

/// Subscription and it's linked SubscriptionFutures were cancelled before completion.
#[derive(Debug, PartialEq, Eq)]
pub struct Cancelled;

impl fmt::Display for Cancelled {
    fn fmt(&self, fmt: &mut fmt::Formatter) -> fmt::Result {
        write!(fmt, "{:?}", self)
    }
}

impl std::error::Error for Cancelled {}

#[derive(Debug)]
pub enum Subscription<TRes> {
    Ready(TRes),
    Pending {
        request: Request,
        waker: Option<Waker>,
        cancel_notifier: Option<Sender<RepoEvent>>,
    },
    Cancelled,
}

impl<TRes: Clone> Subscription<TRes> {
    fn new(request: Request, cancel_notifier: Option<Sender<RepoEvent>>) -> Self {
        Self::Pending {
            request,
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

    fn cancel(&mut self) {
        let former_self = mem::replace(self, Subscription::Cancelled);
        if let Subscription::Pending {
            request,
            waker,
            cancel_notifier,
        } = former_self
        {
            if let Some(waker) = waker {
                waker.wake();
            }
            if let Some(mut sender) = cancel_notifier {
                let _ = sender.try_send(RepoEvent::from(request));
            }
        }
    }
}

pub struct SubscriptionFuture<TRes: Clone + Debug> {
    id: u64,
    subscriptions: Arc<Mutex<Subscriptions<TRes>>>,
}

impl<TRes: Clone + Debug> Future for SubscriptionFuture<TRes> {
    type Output = Result<TRes, Cancelled>;

    fn poll(self: Pin<&mut Self>, context: &mut Context) -> Poll<Self::Output> {
        let mut subscriptions =
            async_std::task::block_on(async { self.subscriptions.lock().await });
        let subscription = if let Some(sub) = subscriptions.get_mut(&self.id) {
            sub
        } else {
            return Poll::Ready(Err(Cancelled));
        };

        match subscription {
            Subscription::Cancelled => Poll::Ready(Err(Cancelled)),
            Subscription::Pending { ref mut waker, .. } => {
                *waker = Some(context.waker().clone());
                Poll::Pending
            }
            Subscription::Ready(result) => Poll::Ready(Ok(result.clone())),
        }
    }
}

impl<TRes: Clone + Debug> Drop for SubscriptionFuture<TRes> {
    fn drop(&mut self) {
        if let Some(mut sub) =
            async_std::task::block_on(async { self.subscriptions.lock().await.remove(&self.id) })
        {
            sub.cancel();
        }
    }
}

impl<TRes: Clone + Debug> fmt::Debug for SubscriptionFuture<TRes> {
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

    impl From<u32> for Request {
        fn from(_: u32) -> Self {
            Self {
                kind: RequestKind::Empty,
                id: GLOBAL_REQ_COUNT.fetch_add(1, Ordering::SeqCst),
            }
        }
    }

    #[async_std::test]
    async fn subscription_basics() {
        let registry = SubscriptionRegistry::<u32>::default();
        let s1 = registry.create_subscription(0.into(), None);
        let s2 = registry.create_subscription(0.into(), None);
        let s3 = registry.create_subscription(0.into(), None);
        registry.finish_subscription(&0.into(), 10);
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
        registry.finish_subscription(&0.into(), 0);

        assert_eq!(s2.await.unwrap(), 0);
    }
}
