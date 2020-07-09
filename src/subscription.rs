use crate::RepoEvent;
use async_std::future::Future;
use async_std::task::{Context, Poll, Waker};
use core::fmt::Debug;
use core::hash::Hash;
use core::pin::Pin;
use futures::channel::mpsc::Sender;
use libipld::Cid;
use libp2p::Multiaddr;
use std::collections::HashMap;
use std::fmt;
use std::mem;
use std::sync::{Arc, Mutex};

#[derive(Debug, PartialEq, Eq, Hash, Clone)]
pub enum Request {
    Connect(Multiaddr),
    GetBlock(Cid),
    #[cfg(test)]
    Empty,
}

impl From<Multiaddr> for Request {
    fn from(addr: Multiaddr) -> Self {
        Self::Connect(addr)
    }
}

impl From<Cid> for Request {
    fn from(cid: Cid) -> Self {
        Self::GetBlock(cid)
    }
}

pub struct SubscriptionRegistry<TRes: Debug> {
    subscriptions: HashMap<Request, Arc<Mutex<Subscription<TRes>>>>,
    shutting_down: bool,
}

impl<TRes: Debug> fmt::Debug for SubscriptionRegistry<TRes> {
    fn fmt(&self, fmt: &mut fmt::Formatter) -> fmt::Result {
        write!(
            fmt,
            "{}(subscriptions: {:?})",
            std::any::type_name::<Self>(),
            self.subscriptions
        )
    }
}

impl<TRes: Debug> SubscriptionRegistry<TRes> {
    pub fn create_subscription(
        &mut self,
        req: Request,
        cancel_notifier: Option<Sender<RepoEvent>>,
    ) -> SubscriptionFuture<TRes> {
        let subscription = self
            .subscriptions
            .entry(req.clone())
            .or_insert_with(|| Arc::new(Mutex::new(Subscription::new(req, cancel_notifier))))
            .clone();
        if self.shutting_down {
            subscription.lock().unwrap().cancel();
        }
        SubscriptionFuture { subscription }
    }

    pub fn finish_subscription(&mut self, req: &Request, res: TRes) {
        if let Some(subscription) = self.subscriptions.remove(req) {
            subscription.lock().unwrap().wake(res);
        }
    }

    /// After shutdown all SubscriptionFutures will return Err(Cancelled)
    pub fn shutdown(&mut self) {
        if self.shutting_down {
            return;
        }
        self.shutting_down = true;

        log::debug!("Shutting down {:?}", self);

        let mut cancelled = 0;
        let mut pending = Vec::new();

        for (_, sub) in self.subscriptions.drain() {
            {
                if let Ok(mut sub) = sub.try_lock() {
                    sub.cancel();
                    cancelled += 1;
                    continue;
                }
            }
            pending.push(sub);
        }

        log::trace!(
            "Cancelled {} subscriptions and {} are pending (not immediatedly locked)",
            cancelled,
            pending.len()
        );

        let remaining = pending.len();

        for sub in pending {
            if let Ok(mut sub) = sub.lock() {
                sub.cancel();
            }
        }

        log::debug!(
            "Remaining {} pending subscriptions cancelled (total of {})",
            remaining,
            cancelled + remaining
        );
    }
}

impl<TRes: Debug> Default for SubscriptionRegistry<TRes> {
    fn default() -> Self {
        Self {
            subscriptions: Default::default(),
            shutting_down: false,
        }
    }
}

impl<TRes: Debug> Drop for SubscriptionRegistry<TRes> {
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
        wakers: Vec<Waker>,
        cancel_notifier: Option<Sender<RepoEvent>>,
    },
    Cancelled,
}

impl<TRes> Subscription<TRes> {
    fn new(request: Request, cancel_notifier: Option<Sender<RepoEvent>>) -> Self {
        Self::Pending {
            request,
            wakers: Default::default(),
            cancel_notifier,
        }
    }

    fn wake(&mut self, result: TRes) {
        let former_self = mem::replace(self, Subscription::Ready(result));
        if let Subscription::Pending { mut wakers, .. } = former_self {
            for waker in wakers.drain(..) {
                waker.wake();
            }
        }
    }

    fn cancel(&mut self) {
        let former_self = mem::replace(self, Subscription::Cancelled);
        if let Subscription::Pending {
            request,
            mut wakers,
            cancel_notifier,
        } = former_self
        {
            for waker in wakers.drain(..) {
                waker.wake();
            }
            if let Some(mut sender) = cancel_notifier {
                let _ = sender.try_send(RepoEvent::from(request));
            }
        }
    }
}

pub struct SubscriptionFuture<TRes> {
    subscription: Arc<Mutex<Subscription<TRes>>>,
}

impl<TRes: Clone> Future for SubscriptionFuture<TRes> {
    type Output = Result<TRes, Cancelled>;

    fn poll(self: Pin<&mut Self>, context: &mut Context) -> Poll<Self::Output> {
        let mut subscription = self.subscription.lock().unwrap();

        match &mut *subscription {
            Subscription::Cancelled => Poll::Ready(Err(Cancelled)),
            Subscription::Pending { ref mut wakers, .. } => {
                let waker = context.waker();
                if !wakers.iter().any(|w| w.will_wake(waker)) {
                    wakers.push(waker.clone());
                }
                Poll::Pending
            }
            Subscription::Ready(result) => Poll::Ready(Ok(result.clone())),
        }
    }
}

impl<TRes> Drop for SubscriptionFuture<TRes> {
    fn drop(&mut self) {
        let strong_refs = Arc::strong_count(&self.subscription);

        if let sub @ Subscription::Pending { .. } = &mut *self.subscription.lock().unwrap() {
            // if this is the last future related to the request, cancel the subscription
            if strong_refs == 2 {
                sub.cancel();
            }
        }
    }
}

impl<TRes> fmt::Debug for SubscriptionFuture<TRes> {
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
            Self::Empty
        }
    }

    #[async_std::test]
    async fn subscription() {
        let mut registry = SubscriptionRegistry::<u32>::default();
        let s1 = registry.create_subscription(0.into(), None);
        let s2 = registry.create_subscription(0.into(), None);
        registry.finish_subscription(&0.into(), 10);
        assert_eq!(s1.await.unwrap(), 10);
        assert_eq!(s2.await.unwrap(), 10);
    }

    #[async_std::test]
    async fn subscription_cancelled_on_dropping_registry() {
        let mut registry = SubscriptionRegistry::<u32>::default();
        let s1 = registry.create_subscription(0.into(), None);
        drop(registry);
        assert_eq!(s1.await, Err(Cancelled));
    }

    #[async_std::test]
    async fn subscription_cancelled_on_shutdown() {
        let mut registry = SubscriptionRegistry::<u32>::default();
        let s1 = registry.create_subscription(0.into(), None);
        registry.shutdown();
        assert_eq!(s1.await, Err(Cancelled));
    }

    #[async_std::test]
    async fn new_subscriptions_cancelled_after_shutdown() {
        let mut registry = SubscriptionRegistry::<u32>::default();
        registry.shutdown();
        let s1 = registry.create_subscription(0.into(), None);
        assert_eq!(s1.await, Err(Cancelled));
    }

    #[async_std::test]
    async fn dropping_subscription_future_after_registering() {
        use async_std::future::timeout;
        use std::time::Duration;

        let mut registry = SubscriptionRegistry::<u32>::default();
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
