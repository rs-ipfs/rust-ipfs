use async_std::future::Future;
use async_std::task::{Context, Poll, Waker};
use core::fmt::Debug;
use core::hash::Hash;
use core::pin::Pin;
use std::collections::HashMap;
use std::sync::{Arc, Mutex};

#[derive(Debug)]
pub struct SubscriptionRegistry<TReq: Debug + Eq + Hash, TRes: Debug> {
    subscriptions: HashMap<TReq, Arc<Mutex<Subscription<TRes>>>>,
}

impl<TReq: Debug + Eq + Hash, TRes: Debug> SubscriptionRegistry<TReq, TRes> {
    pub fn new() -> Self {
        Self {
            subscriptions: Default::default(),
        }
    }

    pub fn create_subscription(&mut self, req: TReq) -> SubscriptionFuture<TRes> {
        let subscription = self.subscriptions.entry(req).or_default().clone();
        SubscriptionFuture { subscription }
    }

    pub fn finish_subscription(&mut self, req: &TReq, res: TRes) {
        if let Some(subscription) = self.subscriptions.remove(req) {
            subscription.lock().unwrap().wake(res);
        }
    }
}

impl<TReq: Debug + Eq + Hash, TRes: Debug> Default for SubscriptionRegistry<TReq, TRes> {
    fn default() -> Self {
        Self::new()
    }
}

#[derive(Debug)]
pub struct Subscription<TResult> {
    result: Option<TResult>,
    wakers: Vec<Waker>,
}

impl<TResult> Subscription<TResult> {
    pub fn new() -> Self {
        Self {
            result: Default::default(),
            wakers: Default::default(),
        }
    }

    pub fn add_waker(&mut self, waker: Waker) {
        self.wakers.push(waker);
    }

    pub fn wake(&mut self, result: TResult) {
        self.result = Some(result);
        for waker in self.wakers.drain(..) {
            waker.wake();
        }
    }
}

impl<TResult: Clone> Subscription<TResult> {
    pub fn result(&self) -> Option<TResult> {
        self.result.clone()
    }
}

impl<TResult> Default for Subscription<TResult> {
    fn default() -> Self {
        Self::new()
    }
}

pub struct SubscriptionFuture<TResult> {
    subscription: Arc<Mutex<Subscription<TResult>>>,
}

impl<TResult: Clone> Future for SubscriptionFuture<TResult> {
    type Output = TResult;

    fn poll(self: Pin<&mut Self>, context: &mut Context) -> Poll<Self::Output> {
        let mut subscription = self.subscription.lock().unwrap();
        if let Some(result) = subscription.result() {
            Poll::Ready(result)
        } else {
            subscription.add_waker(context.waker().clone());
            Poll::Pending
        }
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[async_std::test]
    async fn subscription() {
        let mut registry = SubscriptionRegistry::<u32, u32>::new();
        let s1 = registry.create_subscription(0);
        let s2 = registry.create_subscription(0);
        registry.finish_subscription(&0, 10);
        assert_eq!(s1.await, 10);
        assert_eq!(s2.await, 10);
    }
}
