use ipfs::Ipfs;
use log::warn;
use serde::Serialize;
use std::borrow::Cow;
use std::convert::Infallible;
use warp::{reject, Rejection};

/// The common responses apparently returned by the go-ipfs HTTP api on errors.
/// See also: https://github.com/ferristseng/rust-ipfs-api/blob/master/ipfs-api/src/response/error.rs
#[derive(Debug, Serialize)]
#[serde(rename_all = "PascalCase")]
pub struct MessageResponse {
    message: Cow<'static, str>,
    code: usize,
    r#type: MessageKind,
}

impl MessageResponse {
    fn to_json_reply(&self) -> warp::reply::Json {
        warp::reply::json(self)
    }
}

/// The `MessageResponse` has this field, unsure if it can be anything other than "error".
#[derive(Debug, Clone, Serialize)]
#[serde(rename_all = "lowercase")]
pub enum MessageKind {
    Error,
}

impl MessageKind {
    // FIXME: haven't found a spec for these codes yet
    pub fn with_code(self, code: usize) -> MessageResponseBuilder {
        MessageResponseBuilder(self, code)
    }
}

/// Combining `MessageKind` and `code` using `MessageKind::with_code` returns a
/// `MessageResponseBuilder` which will only need a message to become `MessageResponse`.
#[derive(Debug, Clone)]
pub struct MessageResponseBuilder(MessageKind, usize);

impl MessageResponseBuilder {
    pub fn with_message<S: Into<Cow<'static, str>>>(self, message: S) -> MessageResponse {
        let Self(kind, code) = self;
        MessageResponse {
            message: message.into(),
            code,
            r#type: kind,
        }
    }
}

/// Clones the handle to the filters
pub fn with_ipfs(
    ipfs: &Ipfs,
) -> impl warp::Filter<Extract = (Ipfs,), Error = std::convert::Infallible> + Clone {
    use warp::Filter;
    let ipfs = ipfs.clone();
    warp::any().map(move || ipfs.clone())
}

/// Marker for `warp` specific rejections when something is unimplemented
#[derive(Debug)]
pub(crate) struct NotImplemented;
impl warp::reject::Reject for NotImplemented {}
impl From<NotImplemented> for Rejection {
    fn from(err: NotImplemented) -> Self {
        reject::custom(err)
    }
}

/// PeerId parsing error, details from `libp2p::identity::ParseError` are lost.
#[derive(Debug)]
pub(crate) struct InvalidPeerId;
impl warp::reject::Reject for InvalidPeerId {}
impl From<InvalidPeerId> for Rejection {
    fn from(err: InvalidPeerId) -> Self {
        reject::custom(err)
    }
}

/// PeerId parsing error, details from `libp2p::identity::ParseError` are lost.
#[derive(Debug)]
pub(crate) struct InvalidMultipartFormData;
impl warp::reject::Reject for InvalidMultipartFormData {}
impl From<InvalidMultipartFormData> for Rejection {
    fn from(err: InvalidMultipartFormData) -> Self {
        reject::custom(err)
    }
}

/// Default placeholder for ipfs::Error but once we get more typed errors we could start making
/// them more readable, if needed.
#[derive(Debug)]
pub(crate) struct StringError(Cow<'static, str>);
impl warp::reject::Reject for StringError {}
impl From<StringError> for Rejection {
    fn from(err: StringError) -> Self {
        reject::custom(err)
    }
}

// FIXME: it's a bit questionable to keep this but in the beginning it might help us glide in the
// right direction.
impl<T: core::fmt::Display> From<T> for StringError {
    fn from(e: T) -> Self {
        StringError(format!("{}", e).into())
    }
}

/// Common rejection handling strategy for ipfs http api compatible error responses
pub async fn recover_as_message_response(
    err: warp::reject::Rejection,
) -> Result<impl warp::Reply, Infallible> {
    use warp::http::StatusCode;
    use warp::reject::{InvalidQuery, MethodNotAllowed};

    let resp: Box<dyn warp::Reply>;
    let status;

    if err.find::<NotImplemented>().is_some() {
        resp = Box::new(
            MessageKind::Error
                .with_code(0)
                .with_message("Not implemented")
                .to_json_reply(),
        );
        status = StatusCode::NOT_IMPLEMENTED;
    } else if let Some(e) = err.find::<InvalidQuery>() {
        // invalidquery contains box<std::error::Error + Sync + Static>
        resp = Box::new(
            MessageKind::Error
                .with_code(0)
                .with_message(e.to_string())
                .to_json_reply(),
        );
        status = StatusCode::BAD_REQUEST;
    } else if let Some(StringError(msg)) = err.find::<StringError>() {
        resp = Box::new(
            MessageKind::Error
                .with_code(0)
                .with_message(msg.to_owned())
                .to_json_reply(),
        );
        status = StatusCode::INTERNAL_SERVER_ERROR;
    } else if err.find::<InvalidPeerId>().is_some() {
        resp = Box::new(
            MessageKind::Error
                .with_code(0)
                .with_message("invalid peer id")
                .to_json_reply(),
        );
        status = StatusCode::BAD_REQUEST;
    } else if err.is_not_found() || err.find::<MethodNotAllowed>().is_some() {
        // strangely  this here needs to match last, since the methodnotallowed can come after
        // InvalidQuery as well.
        // go-ipfs sends back a "404 Not Found" with body "404 page not found"
        resp = Box::new("404 page not found");
        status = StatusCode::NOT_FOUND;
    } else {
        // FIXME: use log
        warn!("unhandled rejection: {:?}", err);
        resp = Box::new(
            MessageKind::Error
                .with_code(0)
                .with_message("UNHANDLED REJECTION")
                .to_json_reply(),
        );
        status = StatusCode::INTERNAL_SERVER_ERROR;
    }

    Ok(warp::reply::with_status(resp, status))
}
