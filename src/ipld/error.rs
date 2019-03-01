use crate::ipld::{Ipld, SubPath};
use cid::Codec;

#[derive(Debug)]
pub enum IpldError {
    UnsupportedCodec(Codec),
    InvalidPath(String),
    ResolveError {
        ipld: Ipld,
        path: SubPath,
    },
}

impl std::error::Error for IpldError {
    fn description(&self) -> &str {
        match *self {
            IpldError::UnsupportedCodec(_) => "unsupported codec",
            IpldError::InvalidPath(_) => "invalid path",
            IpldError::ResolveError { .. } => "error resolving path",
        }
    }
}

impl std::fmt::Display for IpldError {
    fn fmt(&self, f: &mut std::fmt::Formatter) -> std::fmt::Result {
        match *self {
            IpldError::UnsupportedCodec(ref codec) => {
                write!(f, "Unsupported codec {:?}", codec)
            }
            IpldError::InvalidPath(ref path) => {
                write!(f, "Invalid path {:?}", path)
            }
            IpldError::ResolveError { ref path, .. } => {
                write!(f, "Can't resolve {}", path.to_string())
            }
        }
    }
}
