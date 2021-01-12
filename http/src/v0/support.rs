use ipfs::{Ipfs, IpfsTypes};
use serde::Serialize;
use std::borrow::Cow;
use std::error::Error as StdError;
use std::fmt;

pub mod option_parsing;

mod stream;
pub use stream::StreamResponse;

mod body;
pub use body::{try_only_named_multipart, OnlyMultipartFailure};

mod timeout;
pub use timeout::MaybeTimeoutExt;

mod serdesupport;
pub use serdesupport::StringSerialized;

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

impl Default for MessageResponseBuilder {
    fn default() -> Self {
        MessageResponseBuilder(MessageKind::Error, 0)
    }
}

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
pub fn with_ipfs<T: IpfsTypes>(
    ipfs: &Ipfs<T>,
) -> impl warp::Filter<Extract = (Ipfs<T>,), Error = std::convert::Infallible> + Clone {
    use warp::Filter;
    let ipfs = ipfs.clone();
    warp::any().map(move || ipfs.clone())
}

/// Special rejection from `pubsub/pub`
#[derive(Debug)]
pub(crate) struct NonUtf8Topic;
impl warp::reject::Reject for NonUtf8Topic {}

/// Used by `pubsub/pub`
#[derive(Debug)]
pub(crate) struct RequiredArgumentMissing(pub(crate) &'static str);
impl warp::reject::Reject for RequiredArgumentMissing {}

#[derive(Debug)]
pub(crate) struct InvalidMultipartFormData;
impl warp::reject::Reject for InvalidMultipartFormData {}

/// Marker for `warp` specific rejections when something is unimplemented
#[derive(Debug)]
pub(crate) struct NotImplemented;
impl warp::reject::Reject for NotImplemented {}

/// PeerId parsing error, details from `libp2p::identity::ParseError` are lost.
#[derive(Debug)]
pub(crate) struct InvalidPeerId;
impl warp::reject::Reject for InvalidPeerId {}

/// Default placeholder for ipfs::Error but once we get more typed errors we could start making
/// them more readable, if needed.
// TODO: needs to be considered if this is even needed..
#[derive(Debug)]
pub(crate) struct StringError(Cow<'static, str>);
impl warp::reject::Reject for StringError {}

impl<D: std::fmt::Display> From<D> for StringError {
    fn from(d: D) -> Self {
        Self(d.to_string().into())
    }
}

impl StringError {
    // Allowing this as dead since it hopefully doesn't stay unused for long
    #[allow(dead_code)]
    pub fn new(cow: Cow<'static, str>) -> Self {
        StringError(cow)
    }
}

/// Common rejection handling strategy for ipfs http api compatible error responses
pub async fn recover_as_message_response(
    err: warp::reject::Rejection,
) -> Result<impl warp::Reply, warp::Rejection> {
    use warp::http::StatusCode;
    use warp::reject::{InvalidQuery, LengthRequired, MethodNotAllowed};

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
    } else if let Some(e) = err.find::<RequiredArgumentMissing>() {
        resp = Box::new(
            MessageKind::Error
                .with_code(0)
                .with_message(format!("required argument {:?} missing", e.0))
                .to_json_reply(),
        );
        status = StatusCode::BAD_REQUEST;
    } else if err.find::<NonUtf8Topic>().is_some() {
        resp = Box::new(
            MessageKind::Error
                .with_code(0)
                .with_message("non utf8 topic")
                .to_json_reply(),
        );
        status = StatusCode::BAD_REQUEST;
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
    } else if err.find::<LengthRequired>().is_some() {
        resp = Box::new(
            MessageKind::Error
                .with_code(0)
                .with_message("Missing header: content-length")
                .to_json_reply(),
        );
        status = StatusCode::BAD_REQUEST;
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
