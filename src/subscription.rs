use async_std::future::Future;
use async_std::task::{Context, Poll, Waker};
use core::fmt::Debug;
use core::hash::Hash;
use core::pin::Pin;
use std::collections::HashMap;
use std::fmt;
use std::sync::{Arc, Mutex};

pub struct SubscriptionRegistry<TReq: Debug + Eq + Hash, TRes: Debug> {
    subscriptions: HashMap<TReq, Arc<Mutex<Subscription<TRes>>>>,
    shutting_down: bool,
}

impl<TReq: Debug + Eq + Hash, TRes: Debug> fmt::Debug for SubscriptionRegistry<TReq, TRes> {
    fn fmt(&self, fmt: &mut fmt::Formatter) -> fmt::Result {
        write!(
            fmt,
            "{}<{}, {}>(subscriptions: {:?})",
            std::any::type_name::<Self>(),
            std::any::type_name::<TReq>(),
            std::any::type_name::<TRes>(),
            self.subscriptions
        )
    }
}

impl<TReq: Debug + Eq + Hash, TRes: Debug> SubscriptionRegistry<TReq, TRes> {
    pub fn create_subscription(&mut self, req: TReq) -> SubscriptionFuture<TRes> {
        let subscription = self.subscriptions.entry(req).or_default().clone();
        if self.shutting_down {
            subscription.lock().unwrap().cancel();
        }
        SubscriptionFuture { subscription }
    }

    pub fn finish_subscription(&mut self, req: &TReq, res: TRes) {
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

impl<TReq: Debug + Eq + Hash, TRes: Debug> Default for SubscriptionRegistry<TReq, TRes> {
    fn default() -> Self {
        Self {
            subscriptions: Default::default(),
            shutting_down: false,
        }
    }
}

impl<TReq: Debug + Eq + Hash, TRes: Debug> Drop for SubscriptionRegistry<TReq, TRes> {
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

pub struct Subscription<TResult> {
    result: Option<TResult>,
    wakers: Vec<Waker>,
    cancelled: bool,
}

impl<TResult> fmt::Debug for Subscription<TResult> {
    fn fmt(&self, fmt: &mut fmt::Formatter) -> fmt::Result {
        write!(
            fmt,
            "Subscription<{}>(result: {}, wakers: {}, cancelled: {})",
            std::any::type_name::<TResult>(),
            if self.result.is_some() {
                "Some(_)"
            } else {
                "None"
            },
            self.wakers.len(),
            self.cancelled
        )
    }
}

impl<TResult> Subscription<TResult> {
    pub fn add_waker(&mut self, waker: Waker) {
        self.wakers.push(waker);
    }

    pub fn wake(&mut self, result: TResult) {
        self.result = Some(result);
        for waker in self.wakers.drain(..) {
            waker.wake();
        }
    }

    pub fn cancel(&mut self) {
        if self.cancelled {
            return;
        }
        self.cancelled = true;
        for waker in self.wakers.drain(..) {
            waker.wake();
        }
    }

    pub fn is_cancelled(&self) -> bool {
        self.cancelled
    }
}

impl<TResult: Clone> Subscription<TResult> {
    pub fn result(&self) -> Option<TResult> {
        self.result.clone()
    }
}

impl<TResult> Default for Subscription<TResult> {
    fn default() -> Self {
        Self {
            result: Default::default(),
            wakers: Default::default(),
            cancelled: false,
        }
    }
}

pub struct SubscriptionFuture<TResult> {
    subscription: Arc<Mutex<Subscription<TResult>>>,
}

impl<TResult: Clone> Future for SubscriptionFuture<TResult> {
    type Output = Result<TResult, Cancelled>;

    fn poll(self: Pin<&mut Self>, context: &mut Context) -> Poll<Self::Output> {
        let mut subscription = self.subscription.lock().unwrap();
        if subscription.is_cancelled() {
            return Poll::Ready(Err(Cancelled));
        }

        if let Some(result) = subscription.result() {
            Poll::Ready(Ok(result))
        } else {
            subscription.add_waker(context.waker().clone());
            Poll::Pending
        }
    }
}

impl<TResult> fmt::Debug for SubscriptionFuture<TResult> {
    fn fmt(&self, fmt: &mut fmt::Formatter) -> fmt::Result {
        write!(
            fmt,
            "SubscriptionFuture<Output = Result<{}, Cancelled>>",
            std::any::type_name::<TResult>()
        )
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[async_std::test]
    async fn subscription() {
        let mut registry = SubscriptionRegistry::<u32, u32>::default();
        let s1 = registry.create_subscription(0);
        let s2 = registry.create_subscription(0);
        registry.finish_subscription(&0, 10);
        assert_eq!(s1.await.unwrap(), 10);
        assert_eq!(s2.await.unwrap(), 10);
    }

    #[async_std::test]
    async fn subscription_cancelled_on_dropping_registry() {
        let mut registry = SubscriptionRegistry::<u32, u32>::default();
        let s1 = registry.create_subscription(0);
        drop(registry);
        s1.await.unwrap_err();
    }

    #[async_std::test]
    async fn subscription_cancelled_on_shutdown() {
        let mut registry = SubscriptionRegistry::<u32, u32>::default();
        let s1 = registry.create_subscription(0);
        registry.shutdown();
        s1.await.unwrap_err();
    }

    #[async_std::test]
    async fn new_subscriptions_cancelled_after_shutdown() {
        let mut registry = SubscriptionRegistry::<u32, u32>::default();
        registry.shutdown();
        let s1 = registry.create_subscription(0);
        s1.await.unwrap_err();
    }

    #[async_std::test]
    async fn dropping_subscription_future_after_registering() {
        use async_std::future::timeout;
        use std::time::Duration;

        let mut registry = SubscriptionRegistry::<u32, u32>::default();
        let s1 = timeout(Duration::from_millis(1), registry.create_subscription(0));
        let s2 = registry.create_subscription(0);

        // make sure it timeouted but had time to register the waker
        s1.await.unwrap_err();

        // this will cause a call to waker installed by s1, but it shouldn't be a problem.
        registry.finish_subscription(&0, 0);

        assert_eq!(s2.await.unwrap(), 0);
    }
}
