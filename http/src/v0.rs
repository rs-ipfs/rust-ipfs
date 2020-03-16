use serde::Serialize;
use std::borrow::Cow;
use std::convert::Infallible;
use ipfs::{Ipfs, IpfsTypes};

pub mod id;
pub mod swarm;
pub mod version;

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
pub fn with_ipfs<T: IpfsTypes>(
    ipfs: &Ipfs<T>,
) -> impl warp::Filter<Extract = (Ipfs<T>,), Error = std::convert::Infallible> + Clone {
    use warp::Filter;
    let ipfs = ipfs.clone();
    warp::any().map(move || ipfs.clone())
}

/// Marker for `warp` specific rejections when something is unimplemented
#[derive(Debug)]
pub(crate) struct NotImplemented;
impl warp::reject::Reject for NotImplemented {}
impl Into<warp::reject::Rejection> for NotImplemented {
    fn into(self) -> warp::reject::Rejection {
        warp::reject::custom(self)
    }
}

/// PeerId parsing error, details from `libp2p::identity::ParseError` are lost.
#[derive(Debug)]
pub(crate) struct InvalidPeerId;
impl warp::reject::Reject for InvalidPeerId {}

/// Default placeholder for ipfs::Error but once we get more typed errors we could start making
/// them more readable, if needed.
#[derive(Debug)]
pub(crate) struct StringError(Cow<'static, str>);
impl warp::reject::Reject for StringError {}

// FIXME: it's a bit questionable to keep this but in the beginning it might help us glide in the
// right direction.
impl From<ipfs::Error> for StringError {
    fn from(e: ipfs::Error) -> Self {
        Self(format!("{}", e).into())
    }
}

/// Common rejection handling strategy for ipfs http api compatible error responses
pub async fn recover_as_message_response(err: warp::reject::Rejection) -> Result<impl warp::Reply, Infallible> {
    use warp::http::StatusCode;
    use warp::reject::{InvalidQuery, MethodNotAllowed};

    let resp: Box<dyn warp::Reply>;
    let status;

    if let Some(_) = err.find::<NotImplemented>() {
        resp = Box::new(MessageKind::Error.with_code(0).with_message("Not implemented").to_json_reply());
        status = StatusCode::NOT_IMPLEMENTED;
    } else if let Some(e) = err.find::<InvalidQuery>() {
        // invalidquery contains box<std::error::Error + Sync + Static>
        resp = Box::new(MessageKind::Error.with_code(0).with_message(e.to_string()).to_json_reply());
        status = StatusCode::BAD_REQUEST;
    } else if let Some(StringError(msg)) = err.find::<StringError>() {
        resp = Box::new(MessageKind::Error.with_code(0).with_message(msg.to_owned()).to_json_reply());
        status = StatusCode::INTERNAL_SERVER_ERROR;
    } else if let Some(_) = err.find::<InvalidPeerId>() {
        resp = Box::new(MessageKind::Error.with_code(0).with_message("invalid peer id").to_json_reply());
        status = StatusCode::BAD_REQUEST;
    } else if err.is_not_found() || matches!(err.find::<MethodNotAllowed>(), Some(_)) {
        // strangely  this here needs to match last, since the methodnotallowed can come after
        // InvalidQuery as well.
        // go-ipfs sends back a "404 Not Found" with body "404 page not found"
        resp = Box::new("404 page not found");
        status = StatusCode::NOT_FOUND;
    } else {
        // FIXME: use log
        eprintln!("unhandled rejection: {:?}", err);
        resp = Box::new(MessageKind::Error.with_code(0).with_message("UNHANDLED REJECTION").to_json_reply());
        status = StatusCode::INTERNAL_SERVER_ERROR;
    }

    Ok(warp::reply::with_status(resp, status))
}
