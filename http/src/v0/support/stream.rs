use futures::stream::{TryStream, TryStreamExt};
use std::error::Error as StdError;
use warp::http::header::{HeaderValue, CONTENT_TYPE, TRAILER};
use warp::http::Response;
use warp::hyper::body::Bytes;
use warp::hyper::Body;
use warp::Reply;

pub struct StreamResponse<S>(pub S);

impl<S> Reply for StreamResponse<S>
where
    S: TryStream + Send + 'static,
    S::Ok: Into<Bytes>,
    S::Error: StdError + Send + Sync + 'static,
{
    fn into_response(self) -> warp::reply::Response {
        // while it may seem like the S::Error is handled somehow it currently just means the
        // response will stop. hopefully later it can be used to become trailer headers.
        let mut resp = Response::new(Body::wrap_stream(self.0.into_stream()));
        let headers = resp.headers_mut();

        // FIXME: unable to send this header with warp/hyper right now
        headers.insert(TRAILER, HeaderValue::from_static("X-Stream-Error"));
        headers.insert(CONTENT_TYPE, HeaderValue::from_static("application/json"));
        headers.insert("X-Chunked-Output", HeaderValue::from_static("1"));

        resp
    }
}
