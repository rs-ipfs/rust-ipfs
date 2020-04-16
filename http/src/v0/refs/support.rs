use std::error::Error as StdError;
use std::fmt;
use warp::hyper::body::Bytes;
use warp::hyper::Body;
use warp::{reply::Response, Reply};

use futures::stream::{TryStream, TryStreamExt};

pub struct StreamResponse<S>(pub S);

impl<S> Reply for StreamResponse<S>
where
    S: TryStream + Send + Sync + 'static,
    S::Ok: Into<Bytes>,
    S::Error: StdError + Send + Sync + 'static,
{
    fn into_response(self) -> Response {
        Response::new(Body::wrap_stream(self.0.into_stream()))
    }
}

/// Empty struct implementing std::error::Error, which we can use to mark the serde_json::Error as
/// "handled" (by logging).
#[derive(Debug)]
pub struct HandledErr;

impl StdError for HandledErr {}

impl fmt::Display for HandledErr {
    fn fmt(&self, _fmt: &mut fmt::Formatter) -> fmt::Result {
        Ok(())
    }
}
