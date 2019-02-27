use crate::ipld::cbor::CborError;
use crate::ipld::{Ipld, SubPath};
use cid::{Codec, Error as CidError};
use std::error::Error;
use std::fmt::{self, Debug, Display};

#[derive(Debug)]
pub enum IpldError {
    UnsupportedCodec(Codec),
    CodecError(Box<dyn CodecError>),
    InvalidPath(String),
    ResolveError {
        ipld: Ipld,
        path: SubPath,
    },
    IoError(std::io::Error),
}

pub trait CodecError: Display + Debug + Error {}

impl Error for IpldError {
    fn description(&self) -> &str {
        match *self {
            IpldError::UnsupportedCodec(_) => "unsupported codec",
            IpldError::CodecError(_) => "codec error",
            IpldError::InvalidPath(_) => "invalid path",
            IpldError::ResolveError { .. } => "error resolving path",
            IpldError::IoError(_) => "io error",
        }
    }
}

impl Display for IpldError {
    fn fmt(&self, f: &mut fmt::Formatter) -> fmt::Result {
        match *self {
            IpldError::UnsupportedCodec(ref codec) => {
                write!(f, "Unsupported codec {:?}", codec)
            }
            IpldError::CodecError(ref err) => {
                write!(f, "{}", err)
            }
            IpldError::InvalidPath(ref path) => {
                write!(f, "Invalid path {:?}", path)
            }
            IpldError::ResolveError { ref path, .. } => {
                write!(f, "Can't resolve {}", path.to_string())
            }
            IpldError::IoError(ref err) => {
                write!(f, "{}", err)
            }
        }
    }
}

impl<T: CodecError + 'static> From<T> for IpldError {
    fn from(error: T) -> Self {
        IpldError::CodecError(Box::new(error))
    }
}

impl CodecError for CidError {}
impl CodecError for CborError {}

impl From<std::io::Error> for IpldError {
    fn from(error: std::io::Error) -> Self {
        IpldError::IoError(error)
    }
}
