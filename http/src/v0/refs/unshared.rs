use futures::stream::Stream;
use pin_project::pin_project;
use std::fmt;
use std::{
    pin::Pin,
    task::{Context, Poll},
};

/// Copied from https://docs.rs/crate/async-compression/0.3.2/source/src/unshared.rs ... Did not
/// keep the safety discussion comment because I am unsure if this is safe with the pinned
/// projections.
///
/// The reason why this is needed is because `warp` or `hyper` needs it. `hyper` needs it because
/// of compiler bug https://github.com/hyperium/hyper/issues/2159 and the future or stream we
/// combine up with `async-stream` is not `Sync`, because the `async_trait` builds up a
/// `Pin<Box<dyn std::future::Future<Output = _> + Send + '_>>`. The lifetime of those futures is
/// not an issue, because at higher level (`refs_path`) those are within the owned values that
/// method receives. It is unclear for me at least if the compiler is too strict with the `Sync`
/// requirement which is derives for any reference or if the root cause here is that `hyper`
/// suffers from that compiler issue.
///
/// Related: https://internals.rust-lang.org/t/what-shall-sync-mean-across-an-await/12020
/// Related: https://github.com/dtolnay/async-trait/issues/77
#[pin_project]
pub struct Unshared<T> {
    #[pin]
    inner: T,
}

impl<T> Unshared<T> {
    pub fn new(inner: T) -> Self {
        Unshared { inner }
    }
}

unsafe impl<T> Sync for Unshared<T> {}

impl<T> fmt::Debug for Unshared<T> {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        f.debug_struct(core::any::type_name::<T>()).finish()
    }
}

impl<S: Stream> Stream for Unshared<S> {
    type Item = S::Item;

    fn poll_next(self: Pin<&mut Self>, ctx: &mut Context) -> Poll<Option<Self::Item>> {
        let this = self.project();
        this.inner.poll_next(ctx)
    }
}
