use serde::Serialize;
use std::borrow::Cow;

pub mod id;
pub mod swarm;
pub mod version;

/// The common responses apparently returned by the go-ipfs HTTP api on errors.
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
