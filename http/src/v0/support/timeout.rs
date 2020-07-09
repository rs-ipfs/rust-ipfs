//! Quite complex implementation for possibly timed out operation. Needed to be this complex so
//! that any Unpin traits and such the inner future might have are properly reimplemented (by
//! leveraging futures combinators).

use futures::future::{Either, Map};
use std::future::Future;
use std::time::Duration;
use tokio::time::{timeout, Elapsed, Timeout};

pub type MaybeTimeout<F> =
    Either<Timeout<F>, Map<F, fn(<F as Future>::Output) -> Result<<F as Future>::Output, Elapsed>>>;

pub fn maybe_timeout<F: Future>(
    maybe_timeout: Option<impl Into<Duration>>,
    fut: F,
) -> MaybeTimeout<F>
where
{
    use futures::future::FutureExt;
    let maybe_timeout: Option<Duration> = maybe_timeout.map(|to_d| to_d.into());

    if let Some(dur) = maybe_timeout {
        Either::Left(timeout(dur, fut))
    } else {
        Either::Right(fut.map(ok_never_elapsed))
    }
}

fn ok_never_elapsed<V>(v: V) -> Result<V, Elapsed> {
    Ok(v)
}

pub trait MaybeTimeoutExt {
    fn maybe_timeout<D: Into<Duration>>(self, maybe_duration: Option<D>) -> MaybeTimeout<Self>
    where
        Self: Future + Sized,
    {
        maybe_timeout(maybe_duration, self)
    }
}

impl<T: Future> MaybeTimeoutExt for T {}
