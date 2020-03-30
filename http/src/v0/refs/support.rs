use std::error::Error as StdError;
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
