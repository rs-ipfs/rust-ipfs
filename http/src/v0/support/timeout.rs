//! Quite complex implementation for possibly timed out operation. Needed to be this complex so
//! that any Unpin traits and such the inner future might have are properly reimplemented (by
//! leveraging futures combinators).

use futures::future::{Either, Map};
use std::future::Future;
use std::time::Duration;
use tokio::time::{error::Elapsed, timeout, Timeout};

pub type MaybeTimeout<F> =
    Either<Timeout<F>, Map<F, fn(<F as Future>::Output) -> Result<<F as Future>::Output, Elapsed>>>;

fn ok_never_elapsed<V>(v: V) -> Result<V, Elapsed> {
    Ok(v)
}

/// Extends futures with `maybe_timeout` which allows timeouting based on the repeated `timeout:
/// Option<StringSerialized<humantime::Duration>>` field in the API method options.
pub trait MaybeTimeoutExt {
    /// Possibly wraps this future in a timeout, depending of whether the duration is given or not.
    ///
    /// Returns a wrapper which will always return whatever the this future returned, wrapped in a
    /// Result with the possibility of elapsing the `maybe_duration`.
    fn maybe_timeout<D: Into<Duration>>(self, maybe_duration: Option<D>) -> MaybeTimeout<Self>
    where
        Self: Future + Sized,
    {
        use futures::future::FutureExt;

        let maybe_duration: Option<Duration> = maybe_duration.map(|to_d| to_d.into());

        if let Some(dur) = maybe_duration {
            Either::Left(timeout(dur, self))
        } else {
            Either::Right(self.map(ok_never_elapsed))
        }
    }
}

impl<T: Future> MaybeTimeoutExt for T {}
