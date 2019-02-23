use crate::ipld::cbor::CborError;
use cid::{Codec, Error as CidError};
use std::error::Error;
use std::fmt::{self, Debug, Display};

#[derive(Debug)]
pub enum IpldError {
    UnsupportedCodec(Codec),
    CodecError(Box<dyn CodecError>),
}

pub trait CodecError: Display + Debug + Error {}

impl Error for IpldError {
    fn description(&self) -> &str {
        match *self {
            IpldError::UnsupportedCodec(_) => "unsupported codec",
            IpldError::CodecError(_) => "codec error",
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
